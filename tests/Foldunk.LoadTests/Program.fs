module Foldunk.EventStore.LoadTests.Program

open Foldunk.EventStore.LoadTests.Infrastructure

open Argu
open Serilog
open System
open Foldunk.EventStore
open Domain
open System.Threading
open Serilog.Events
open System.IO

type Arguments =
    | [<AltCommandLine("-g")>] Host of string
    | [<AltCommandLine("-u")>] Username of string
    | [<AltCommandLine("-p")>] Password of string

    | [<AltCommandLine("-l")>] LogFile of string
    | [<AltCommandLine("-v")>] Verbose

    | [<AltCommandLine("-f")>] TestsPerSecond of int
    | [<AltCommandLine("-c")>] ConcurrentOperationsLimit of int
    | [<AltCommandLine("-d")>] DurationM of float
    | [<AltCommandLine("-e")>] ErrorCutoff of int64
    | [<AltCommandLine("-i")>] ReportIntervalS of int

    | [<AltCommandLine("-h")>] HeartbeatTimeout of float
    | [<AltCommandLine("-o")>] Timeout of float
    | [<AltCommandLine("-r")>] Retries of int
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | Host _ -> "specify a DNS query, using Gossip-driven discovery against all A records returned (default: localhost)."
            | Username _ -> "specify a username (default: admin)."
            | Password _ -> "specify a Password (default: changeit)."
            | LogFile _ -> "specify a log file to write the result breakdown."
            | Verbose -> "Include low level logging in screen output."
            | TestsPerSecond _ -> "specify a target number of requests per second (default: 1000)."
            | ConcurrentOperationsLimit _ -> "max concurrent operations in flight (default: 5000)."
            | DurationM _ -> "specify a run duration in minutes (default: 1)."
            | ErrorCutoff _ -> "specify an error cutoff (default: 10000)."
            | ReportIntervalS _ -> "specify reporting intervals in seconds (default: 10)."
            | HeartbeatTimeout _ -> "specify heartbeat timeout in seconds (default: 1.5)."
            | Timeout _ -> "specify operation timeout in seconds (default: 5)."
            | Retries _ -> "specify operation retries (default: 1)."

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
let connectToEventStoreNode log (dnsQuery, heartbeatTimeout, col) (username, password) (operationTimeout, operationRetries) =
    GesConnector(username, password, requireMaster=false, reqTimeout=operationTimeout, reqRetries=operationRetries,
            heartbeatTimeout=heartbeatTimeout, concurrentOperationsLimit = col, log=Logger.SerilogVerbose log)
        .Connect(Discovery.GossipDns dnsQuery)

let defaultBatchSize = 500
let createGesGateway connection batchSize = GesGateway(GesConnection(connection), GesBatchingPolicy(maxBatchSize = batchSize))

let serializationSettings = Foldunk.Serialization.Settings.CreateDefault()
let genCodec<'T> = Foldunk.UnionCodec.generateJsonUtf8UnionCodec<'T> serializationSettings

let fold, initial = Domain.Favorites.Folds.fold, Domain.Favorites.Folds.initial

let codec = genCodec<Domain.Favorites.Events.Event>
let createServiceGes eventStoreConnection =
    let gateway = createGesGateway eventStoreConnection defaultBatchSize
    Backend.Favorites.Service(fun cet -> GesStreamBuilder(gateway, codec, fold, initial, CompactionStrategy.EventType cet).Create)

let runFavoriteTest log _clientId conn = async {
    let service = createServiceGes conn
    // Load test runner does not guarantee no overlapping work on a session so cannot use clientId for now
    let clientId = Guid.NewGuid() |> ClientId
    let sku = Guid.NewGuid() |> SkuId
    do! service.Execute log clientId (Favorites.Command.Favorite(DateTimeOffset.Now, [sku]))
    let! items = service.Read log clientId
    if items |> Array.exists (fun x -> x.skuId = sku) |> not then invalidOp "Added item not found" }

let log =
    LoggerConfiguration()
        .Destructure.FSharpTypes()
        .WriteTo.Console(LogEventLevel.Debug, theme = Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code)
        .CreateLogger()
let domainLog verbose =
    LoggerConfiguration()
        .Destructure.FSharpTypes()
        .WriteTo.Console((if verbose then LogEventLevel.Debug else LogEventLevel.Warning), theme = Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code)
        .CreateLogger()
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
        let host = args.GetResult(Host,"localhost")
        let creds = args.GetResult(Username,"admin"), args.GetResult(Password,"changeit")
        let (timeout, retries) as operationThrottling = args.GetResult(Timeout,5.) |> float |> TimeSpan.FromSeconds,args.GetResult(Retries,1)
        let heartbeatTimeout = args.GetResult(HeartbeatTimeout,1.5) |> float |> TimeSpan.FromSeconds
        let concurrentOperationsLimit = args.GetResult(ConcurrentOperationsLimit,5000)
        let report = args.GetResult(LogFile,"log.txt") |> fun n -> FileInfo(n).FullName
        let conn = connectToEventStoreNode log (host, heartbeatTimeout, concurrentOperationsLimit) creds operationThrottling |> Async.RunSynchronously

        let clients = Array.init (testsPerSecond * 2) (fun _ -> Guid.NewGuid () |> ClientId)
        let verbose = args.Contains(Verbose)
        let domainLog = domainLog verbose
        let runSingleTest clientId =
            if verbose then domainLog.Information("Using session {sessionId}", ([|clientId|] : obj []))
            runFavoriteTest domainLog clientId conn
        log.Information(
            "Running for {duration}, targeting {host} with heartbeat: {heartbeat}, max concurrent requests: {concurrency}\n" +
            "Test freq {tps} hits/s; Operation timeout: {timeout} and {retries} retries; max errors: {errorCutOff}\n" +
            "Reporting intervals: {ri}, report file: {report}",
            ([| duration; host; heartbeatTimeout; concurrentOperationsLimit; testsPerSecond; timeout; retries; errorCutoff; reportingIntervals; report |] : obj[]))
        let results = run log testsPerSecond (duration.Add(TimeSpan.FromSeconds 5.)) errorCutoff reportingIntervals clients runSingleTest |> Async.RunSynchronously
        let resultFile = createResultLog report
        for r in results do
            resultFile.Information("Aggregate: {aggregate}", r)
        log.Information("Run completed, current allocation: {bytes:n0}",GC.GetTotalMemory(true))
        0
    with e ->
        printfn "%s" e.Message
        1