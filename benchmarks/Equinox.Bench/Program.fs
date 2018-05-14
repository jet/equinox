module Equinox.Bench.Program

open Argu
open Domain
open Infrastructure
open Equinox.Cosmos
open Equinox.EventStore
open Serilog
open Serilog.Events
open System
open System.IO
open System.Threading

[<NoEquality; NoComparison>]
type Arguments =
    | [<AltCommandLine("-l")>] LogFile of string
    | [<AltCommandLine("-vd")>] VerboseDomain
    | [<AltCommandLine("-vc")>] VerboseConsole
    | [<AltCommandLine("-S")>] LocalSeq

    | [<AltCommandLine("-f")>] TestsPerSecond of int
    | [<AltCommandLine("-d")>] DurationM of float
    | [<AltCommandLine("-e")>] ErrorCutoff of int64
    | [<AltCommandLine("-i")>] ReportIntervalS of int
    | [<CliPrefix(CliPrefix.None)>] Mem of ParseResults<MemArguments>
    | [<CliPrefix(CliPrefix.None)>] Es of ParseResults<EsArguments>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | LogFile _ -> "specify a log file to write the result breakdown."
            | VerboseDomain -> "Include low level Domain logging."
            | VerboseConsole -> "Include low level Domain and Store logging in screen output."
            | LocalSeq -> "Configures writing to a local Seq endpoint at http://localhost:5341, see https://getseq.net"
            | TestsPerSecond _ -> "specify a target number of requests per second (default: 1000)."
            | DurationM _ -> "specify a run duration in minutes (default: 1)."
            | ErrorCutoff _ -> "specify an error cutoff (default: 10000)."
            | ReportIntervalS _ -> "specify reporting intervals in seconds (default: 10)."
            | Mem _ -> "specify whether perform loadtest against In Memory Volatile Store"
            | Es _ -> "specify whether perform loadtest against EventStore"
and [<NoEquality; NoComparison>] MemArguments =
    | [<AltCommandLine("-b")>] Backoff of float
    | [<AltCommandLine("-o")>] Timeout of float
    | [<AltCommandLine("-r")>] Retries of int
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | Backoff _ -> "specify backoff baseline in seconds (default: 5)."
            | Timeout _ -> "specify operation timeout in seconds (default: 5)."
            | Retries _ -> "specify operation retries (default: 1)."
and [<NoEquality; NoComparison>] EsArguments =
    | [<AltCommandLine("-g")>] Host of string
    | [<AltCommandLine("-u")>] Username of string
    | [<AltCommandLine("-p")>] Password of string

    | [<AltCommandLine("-ve")>] VerboseEs

    | [<AltCommandLine("-c")>] ConcurrentOperationsLimit of int

    | [<AltCommandLine("-h")>] HeartbeatTimeout of float
    | [<AltCommandLine("-o")>] Timeout of float
    | [<AltCommandLine("-r")>] Retries of int
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | Host _ -> "specify a DNS query, using Gossip-driven discovery against all A records returned (default: localhost), if -eqx option has been passed, this should be the connection string for the Cosmos Account"
            | Username _ -> "specify a username (default: admin)."
            | Password _ -> "specify a Password (default: changeit)."
            | ConcurrentOperationsLimit _ -> "max concurrent operations in flight (default: 5000)."
            | VerboseEs -> "Include low level Store logging."
            | HeartbeatTimeout _ -> "specify heartbeat timeout in seconds (default: 1.5)."
            | Timeout _ -> "specify operation timeout in seconds (default: 5)."
            | Retries _ -> "specify operation retries (default: 1)."
[<RequireQualifiedAccess; NoEquality; NoComparison>]
type Connection =
    | Mem of Equinox.MemoryStore.VolatileStore
    | Es of GesGateway

let run log testsPerSecond duration errorCutoff reportingIntervals (clients : ClientId[]) runSingleTest =
    let mutable idx = -1L
    let selectClient () =
        let clientIndex = Interlocked.Increment(&idx) |> int
        clients.[clientIndex % clients.Length]
    let selectClient = async { return async { return selectClient() } }
    Local.runLoadTest log reportingIntervals testsPerSecond errorCutoff duration selectClient runSingleTest

/// To establish a local node to run the tests against:
///   1. cinst eventstore-oss -y # where cinst is an invocation of the Chocolatey Package Installer on Windows
///   2. & $env:ProgramData\chocolatey\bin\EventStore.ClusterNode.exe --gossip-on-single-node --discover-via-dns 0 --ext-http-port=30778
let connectToEventStoreNode (log: ILogger) (dnsQuery, heartbeatTimeout, col) (username, password) (operationTimeout, operationRetries) =
    GesConnector(username, password, reqTimeout=operationTimeout, reqRetries=operationRetries,
            heartbeatTimeout=heartbeatTimeout, concurrentOperationsLimit = col,
            log=(if log.IsEnabled(LogEventLevel.Debug) then Logger.SerilogVerbose log else Logger.SerilogNormal log),
            tags=["M", Environment.MachineName; "I", Guid.NewGuid() |> string])
        .Establish("Equinox-loadtests", Discovery.GossipDns dnsQuery, ConnectionStrategy.ClusterTwinPreferSlaveReads)
let createGesGateway connection batchSize = GesGateway(connection, GesBatchingPolicy(maxBatchSize = batchSize))

/// Create an Equinox is complicated,
/// To run the test,
/// Either replace connection below with a real equinox,
/// Or create a local Equinox using provisioning script
/// create Equinox with dbName "test" and collectionName "test" to perform test
//let connectToLocalEquinoxNode connStr (dbName, collName) operationTimeout (maxRetyForThrottling, maxRetryWaitTime) =
//    EqxConnector(requestTimeout=operationTimeout, maxRetryAttemptsOnThrottledRequests=maxRetyForThrottling, maxRetryWaitTimeInSeconds=maxRetryWaitTime)
//        .Connect(Discovery.ConnectionString(connStr, dbName, collName))

let defaultBatchSize = 500

let serializationSettings = Newtonsoft.Json.Converters.FSharp.Settings.CreateCorrect()
let genCodec<'Union when 'Union :> TypeShape.UnionContract.IUnionContract>() = Equinox.UnionCodec.JsonUtf8.Create<'Union>(serializationSettings)

let fold, initial = Domain.Favorites.Folds.fold, Domain.Favorites.Folds.initial

let codec = genCodec<Domain.Favorites.Events.Event>()
let createService conn log =
    let resolveStream cet streamName =
        match conn with
        | Connection.Mem store ->
            Equinox.MemoryStore.MemoryStreamBuilder(store, fold, initial).Create(streamName)
        | Connection.Es gateway ->
            GesStreamBuilder(gateway, codec, fold, initial, Equinox.EventStore.CompactionStrategy.EventType cet).Create(streamName)
    Backend.Favorites.Service(log, resolveStream)
let runFavoriteTest (service : Backend.Favorites.Service) clientId = async {
    let sku = Guid.NewGuid() |> SkuId
    do! service.Execute clientId (Favorites.Command.Favorite(DateTimeOffset.Now, [sku]))
    let! items = service.Read clientId
    if items |> Array.exists (fun x -> x.skuId = sku) |> not then invalidOp "Added item not found" }

let createStoreLog verboseStore verboseConsole maybeSeqEndpoint =
    let c = LoggerConfiguration()//.Destructure.FSharpTypes()
    let c = if verboseStore then c.MinimumLevel.Debug() else c
    let c = c.WriteTo.Console((if verboseConsole then LogEventLevel.Debug else LogEventLevel.Information), theme = Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code)
    let c = match maybeSeqEndpoint with None -> c | Some endpoint -> c.WriteTo.Seq(endpoint)
    c.CreateLogger() :> ILogger
let domainLog verboseDomain verboseConsole maybeSeqEndpoint =
    let c = LoggerConfiguration()(*.Destructure.FSharpTypes()*).Enrich.FromLogContext()
    let c = if verboseDomain then c.MinimumLevel.Debug() else c
    let c = c.WriteTo.Console((if verboseConsole then LogEventLevel.Debug else LogEventLevel.Warning), theme = Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code)
    let c = match maybeSeqEndpoint with None -> c | Some endpoint -> c.WriteTo.Seq(endpoint)
    c.CreateLogger()

let createResultLog fileName =
    LoggerConfiguration()
        .Destructure.FSharpTypes()
        .WriteTo.File(fileName)
        .CreateLogger()

[<EntryPoint>]
let main argv =
    let parser = ArgumentParser.Create<Arguments>(programName = System.Diagnostics.Process.GetCurrentProcess().MainModule.FileName)
    try
        let args = parser.ParseCommandLine argv
        let errorCutoff = args.GetResult(ErrorCutoff,10000L)
        let testsPerSecond = args.GetResult(TestsPerSecond,1000)
        let duration = args.GetResult(DurationM,1.) |> TimeSpan.FromMinutes
        let reportingIntervals =
            match args.GetResults(ReportIntervalS) with
            | [] -> TimeSpan.FromSeconds 10.|> Seq.singleton
            | intervals -> seq { for i in intervals -> TimeSpan.FromSeconds(float i) }
            |> fun intervals -> [| yield duration; yield! intervals |]
        let verboseConsole = args.Contains(VerboseConsole)
        let maybeSeq = if args.Contains LocalSeq then Some "http://localhost:5341" else None
        let report = args.GetResult(LogFile,"log.txt") |> fun n -> FileInfo(n).FullName
        let log, conn =
            match args.TryGetSubCommand() with
            | Some (Mem memArgs) ->
                let timeout = memArgs.GetResult(MemArguments.Timeout,5.) |> TimeSpan.FromSeconds
                let retries = memArgs.GetResult(MemArguments.Retries,5)
                let log = Log.Logger
                log.Information(
                    "Running for In Memory Volatile Store for {duration}\n" +
                    "Test freq {tps} hits/s; Operation timeout: {timeout} and {retries} retries; max errors: {errorCutOff}\n" +
                    "Reporting intervals: {ri}, report file: {report}",
                    ([| duration; testsPerSecond; timeout; retries; errorCutoff; reportingIntervals; report |] : obj[]))
                // TODO implement backoffs
                log, Connection.Mem (Equinox.MemoryStore.VolatileStore())
            | Some (Es esargs) ->
                let verboseStore = esargs.Contains(VerboseEs)
                let log = createStoreLog verboseStore verboseConsole maybeSeq
                let host = esargs.GetResult(Host,"localhost")
                let creds = esargs.GetResult(Username,"admin"), esargs.GetResult(Password,"changeit")
                let (timeout, retries) as operationThrottling = esargs.GetResult(EsArguments.Timeout,5.) |> float |> TimeSpan.FromSeconds,esargs.GetResult(EsArguments.Retries,1)
                let heartbeatTimeout = esargs.GetResult(HeartbeatTimeout,1.5) |> float |> TimeSpan.FromSeconds
                let concurrentOperationsLimit = esargs.GetResult(ConcurrentOperationsLimit,5000)
                log.Information(
                    "Running for EventStore for {duration}, targeting {host} with heartbeat: {heartbeat}, max concurrent requests: {concurrency}\n" +
                    "Test freq {tps} hits/s; Operation timeout: {timeout} and {retries} retries; max errors: {errorCutOff}\n" +
                    "Reporting intervals: {ri}, report file: {report}",
                    ([| duration; host; heartbeatTimeout; concurrentOperationsLimit; testsPerSecond; timeout; retries; errorCutoff; reportingIntervals; report |] : obj[]))
                let conn = connectToEventStoreNode log (host, heartbeatTimeout, concurrentOperationsLimit) creds operationThrottling |> Async.RunSynchronously
                log, Connection.Es (createGesGateway conn defaultBatchSize)
            | _ ->
                failwith "Storage argument is required"

        let clients = Array.init (testsPerSecond * 2) (fun _ -> Guid.NewGuid () |> ClientId)
        let verbose = args.Contains(VerboseDomain)
        let domainLog = domainLog verbose verboseConsole maybeSeq
        let runSingleTest clientId =
            if verbose then domainLog.Information("Using session {sessionId}", ([|clientId|] : obj []))
            runFavoriteTest (createService conn domainLog) clientId
        let results = run log testsPerSecond (duration.Add(TimeSpan.FromSeconds 5.)) errorCutoff reportingIntervals clients runSingleTest |> Async.RunSynchronously
        let resultFile = createResultLog report
        for r in results do
            resultFile.Information("Aggregate: {aggregate}", r)
        log.Information("Run completed, current allocation: {bytes:n0}",GC.GetTotalMemory(true))
        0
    with e ->
        printfn "%s" e.Message
        1