module Equinox.Cli.Program

open Argu
open Domain
open Equinox.EventStore
open Infrastructure
open Serilog
open Serilog.Events
open System
open System.IO
open System.Threading

[<NoEquality; NoComparison>]
type Arguments =
    | [<AltCommandLine("-vd")>] VerboseDomain
    | [<AltCommandLine("-vc")>] VerboseConsole
    | [<AltCommandLine("-S")>] LocalSeq
    | [<AltCommandLine("-l")>] LogFile of string
    | [<CliPrefix(CliPrefix.None)>] Memory of ParseResults<TestArguments>
    | [<CliPrefix(CliPrefix.None)>] Es of ParseResults<EsArguments>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | VerboseDomain -> "Include low level Domain logging."
            | VerboseConsole -> "Include low level Domain and Store logging in screen output."
            | LocalSeq -> "Configures writing to a local Seq endpoint at http://localhost:5341, see https://getseq.net"
            | LogFile _ -> "specify a log file to write the result breakdown (default: Equinox.Cli.log)."
            | Memory _ -> "specify In-Memory Volatile Store baseline test"
            | Es _ -> "specify EventStore actions"
and TestArguments =
    | Name of Test
    | [<AltCommandLine("-f")>] TestsPerSecond of int
    | [<AltCommandLine("-d")>] DurationM of float
    | [<AltCommandLine("-e")>] ErrorCutoff of int64
    | [<AltCommandLine("-i")>] ReportIntervalS of int
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | Name _ -> "Specify which test to run. (default: Favorites)"
            | TestsPerSecond _ -> "specify a target number of requests per second (default: 1000)."
            | DurationM _ -> "specify a run duration in minutes (default: 1)."
            | ErrorCutoff _ -> "specify an error cutoff (default: 10000)."
            | ReportIntervalS _ -> "specify reporting intervals in seconds (default: 10)."
and Test = | Favorites
and [<NoEquality; NoComparison>] EsArguments =
    | [<AltCommandLine("-vs")>] VerboseStore
    | [<AltCommandLine("-o")>] Timeout of float
    | [<AltCommandLine("-r")>] Retries of int
    | [<AltCommandLine("-g")>] Host of string
    | [<AltCommandLine("-u")>] Username of string
    | [<AltCommandLine("-p")>] Password of string
    | [<AltCommandLine("-c")>] ConcurrentOperationsLimit of int
    | [<AltCommandLine("-h")>] HeartbeatTimeout of float

    | [<CliPrefix(CliPrefix.None)>] Run of ParseResults<TestArguments>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | VerboseStore -> "Include low level Store logging."
            | Timeout _ -> "specify operation timeout in seconds (default: 5)."
            | Retries _ -> "specify operation retries (default: 1)."
            | Host _ -> "specify a DNS query, using Gossip-driven discovery against all A records returned (default: localhost)."
            | Username _ -> "specify a username (default: admin)."
            | Password _ -> "specify a Password (default: changeit)."
            | ConcurrentOperationsLimit _ -> "max concurrent operations in flight (default: 5000)."
            | HeartbeatTimeout _ -> "specify heartbeat timeout in seconds (default: 1.5)."
            | Run _ -> "Run a load test."

let defaultBatchSize = 500

module EventStore =
    /// To establish a local node to run the tests against:
    ///   1. cinst eventstore-oss -y # where cinst is an invocation of the Chocolatey Package Installer on Windows
    ///   2. & $env:ProgramData\chocolatey\bin\EventStore.ClusterNode.exe --gossip-on-single-node --discover-via-dns 0 --ext-http-port=30778
    let connect (log: ILogger) (dnsQuery, heartbeatTimeout, col) (username, password) (operationTimeout, operationRetries) =
        GesConnector(username, password, reqTimeout=operationTimeout, reqRetries=operationRetries,
                heartbeatTimeout=heartbeatTimeout, concurrentOperationsLimit = col,
                log=(if log.IsEnabled(LogEventLevel.Debug) then Logger.SerilogVerbose log else Logger.SerilogNormal log),
                tags=["M", Environment.MachineName; "I", Guid.NewGuid() |> string])
            .Establish("equinox-cli", Discovery.GossipDns dnsQuery, ConnectionStrategy.ClusterTwinPreferSlaveReads)
    let createGateway connection batchSize = GesGateway(connection, GesBatchingPolicy(maxBatchSize = batchSize))

[<RequireQualifiedAccess; NoEquality; NoComparison>]
type Store =
    | Mem of Equinox.MemoryStore.VolatileStore
    | Es of GesGateway

module Test =
    let run log testsPerSecond duration errorCutoff reportingIntervals (clients : ClientId[]) runSingleTest =
        let mutable idx = -1L
        let selectClient () =
            let clientIndex = Interlocked.Increment(&idx) |> int
            clients.[clientIndex % clients.Length]
        let selectClient = async { return async { return selectClient() } }
        Local.runLoadTest log reportingIntervals testsPerSecond errorCutoff duration selectClient runSingleTest
    let fold, initial = Domain.Favorites.Folds.fold, Domain.Favorites.Folds.initial
    let serializationSettings = Newtonsoft.Json.Converters.FSharp.Settings.CreateCorrect()
    let genCodec<'Union when 'Union :> TypeShape.UnionContract.IUnionContract>() = Equinox.UnionCodec.JsonUtf8.Create<'Union>(serializationSettings)
    let codec = genCodec<Domain.Favorites.Events.Event>()
    let createFavoritesService store log =
        let resolveStream cet streamName =
            match store with
            | Store.Mem store ->
                Equinox.MemoryStore.MemoryStreamBuilder(store, fold, initial).Create(streamName)
            | Store.Es gateway ->
                GesStreamBuilder(gateway, codec, fold, initial, Equinox.EventStore.CompactionStrategy.EventType cet).Create(streamName)
        Backend.Favorites.Service(log, resolveStream)
    let runFavoriteTest (service : Backend.Favorites.Service) clientId = async {
        let sku = Guid.NewGuid() |> SkuId
        do! service.Execute clientId (Favorites.Command.Favorite(DateTimeOffset.Now, [sku]))
        let! items = service.Read clientId
        if items |> Array.exists (fun x -> x.skuId = sku) |> not then invalidOp "Added item not found" }

let createStoreLog verbose verboseConsole maybeSeqEndpoint =
    let c = LoggerConfiguration().Destructure.FSharpTypes()
    let c = if verbose then c.MinimumLevel.Debug() else c
    let c = c.WriteTo.Console((if verboseConsole then LogEventLevel.Debug else LogEventLevel.Information), theme = Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code)
    let c = match maybeSeqEndpoint with None -> c | Some endpoint -> c.WriteTo.Seq(endpoint)
    c.CreateLogger() :> ILogger
let domainLog verbose verboseConsole maybeSeqEndpoint =
    let c = LoggerConfiguration().Destructure.FSharpTypes().Enrich.FromLogContext()
    let c = if verbose then c.MinimumLevel.Debug() else c
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
    let programName = System.Reflection.Assembly.GetEntryAssembly().GetName().Name
    let parser = ArgumentParser.Create<Arguments>(programName = programName)
    try
        let args = parser.ParseCommandLine argv
        let verboseConsole = args.Contains(VerboseConsole)
        let maybeSeq = if args.Contains LocalSeq then Some "http://localhost:5341" else None
        let report = args.GetResult(LogFile,programName+".log") |> fun n -> FileInfo(n).FullName
        let runTest (log: ILogger) conn (targs: ParseResults<TestArguments>) =
            let verbose = args.Contains(VerboseDomain)
            let domainLog = domainLog verbose verboseConsole maybeSeq
            let service = Test.createFavoritesService conn domainLog

            let errorCutoff = targs.GetResult(ErrorCutoff,10000L)
            let testsPerSecond = targs.GetResult(TestsPerSecond,1000)
            let duration = targs.GetResult(DurationM,1.) |> TimeSpan.FromMinutes
            let reportingIntervals =
                match targs.GetResults(ReportIntervalS) with
                | [] -> TimeSpan.FromSeconds 10.|> Seq.singleton
                | intervals -> seq { for i in intervals -> TimeSpan.FromSeconds(float i) }
                |> fun intervals -> [| yield duration; yield! intervals |]
            let clients = Array.init (testsPerSecond * 2) (fun _ -> Guid.NewGuid () |> ClientId)

            let test = targs.GetResult(Name,Favorites)
            log.Information( "Running {test} for {duration} with test freq {tps} hits/s; max errors: {errorCutOff}\n" +
                "Reporting intervals: {ri}, report file: {report}",
                test, duration, testsPerSecond, errorCutoff, reportingIntervals, report)
            let runSingleTest clientId =
                if verbose then domainLog.Information("Using session {sessionId}", ([|clientId|] : obj []))
                Test.runFavoriteTest service clientId
            let results = Test.run log testsPerSecond (duration.Add(TimeSpan.FromSeconds 5.)) errorCutoff reportingIntervals clients runSingleTest |> Async.RunSynchronously
            let resultFile = createResultLog report
            for r in results do
                resultFile.Information("Aggregate: {aggregate}", r)
            log.Information("Run completed, current allocation: {bytes:n0}",GC.GetTotalMemory(true))
            0

        match args.GetSubCommand() with
        | Memory targs ->
            let verboseStore = false
            let log = createStoreLog verboseStore verboseConsole maybeSeq
            log.Information( "Using In-memory Volatile Store")
            // TODO implement backoffs
            let conn = Store.Mem (Equinox.MemoryStore.VolatileStore())
            runTest log conn targs
        | Es sargs ->
            let verboseStore = sargs.Contains(EsArguments.VerboseStore)
            let log = createStoreLog verboseStore verboseConsole maybeSeq
            let host = sargs.GetResult(Host,"localhost")
            let creds = sargs.GetResult(Username,"admin"), sargs.GetResult(Password,"changeit")
            let (timeout, retries) as operationThrottling =
                sargs.GetResult(EsArguments.Timeout,5.) |> float |> TimeSpan.FromSeconds,
                sargs.GetResult(EsArguments.Retries,1)
            let heartbeatTimeout = sargs.GetResult(HeartbeatTimeout,1.5) |> float |> TimeSpan.FromSeconds
            let concurrentOperationsLimit = sargs.GetResult(ConcurrentOperationsLimit,5000)
            log.Information("Using EventStore targeting {host} with heartbeat: {heartbeat}, max concurrent requests: {concurrency}\n" +
                "Operation timeout: {timeout} with {retries} retries",
                host, heartbeatTimeout, concurrentOperationsLimit, timeout, retries)
            let conn = EventStore.connect log (host, heartbeatTimeout, concurrentOperationsLimit) creds operationThrottling |> Async.RunSynchronously
            let store = Store.Es (EventStore.createGateway conn defaultBatchSize)
            match sargs.TryGetSubCommand() with
            | Some (EsArguments.Run targs) -> runTest log store targs
            | _ -> failwith "run is required"
        | _ -> failwith "ERROR: please specify memory or es Store"
    with e ->
        printfn "%s" e.Message
        1