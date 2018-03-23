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
    | [<AltCommandLine("-u")>] Username of string
    | [<AltCommandLine("-p")>] Password of string
    | [<AltCommandLine("-l")>] LogFile of string
    | [<AltCommandLine("-t")>] TestsPerSecond of int
    | [<AltCommandLine("-e")>] ErrorCutoff of int64
    | [<AltCommandLine("-h")>] HeartbeatTimeout of float
    | [<AltCommandLine("-o")>] Timeout of float
    | [<AltCommandLine("-c")>] Retries of int
    | [<AltCommandLine("-r")>] ReportIntervalS of int
    | [<AltCommandLine("-m")>] DurationM of int
    | [<AltCommandLine("-s")>] DurationS of int
    | [<AltCommandLine("-g")>] Host of string
    | [<AltCommandLine("-R")>] SkipReconnect of bool
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | Host _ -> "specify a DNS query, using Gossip-driven discovery against all A records returned (default: localhost)."
            | Username _ -> "specify a username (default: admin)."
            | Password _ -> "specify a Password (default: changeit)."
            | HeartbeatTimeout _ -> "specify heartbeat timeout in seconds (default: 5)."
            | SkipReconnect _ -> "inhibit auto-reconnect on connection close."
            | Timeout _ -> "specify operation timeout in seconds (default: 2)."
            | Retries _ -> "specify operation retries (default: 1)."
            | DurationM _ -> "specify a run duration in minutes (default: 1)."
            | DurationS _ -> "specify a run duration in seconds."
            | TestsPerSecond _ -> "specify a target number of requests per second."
            | ReportIntervalS _ -> "specify reporting intervals in seconds (default: 10)."
            | ErrorCutoff _ -> "specify an error cutoff (default: 1000)."
            | LogFile _ -> "specify a log file to write the result breakdown."

let run log testsPerSecond duration errorCutoff reportingIntervals (clients : ClientId[]) runSingleTest =
    let mutable idx = ref -1L
    let selectClient () =
        let clientIndex = Interlocked.Increment(idx) |> int
        clients.[clientIndex % clients.Length]
    let selectClient = async { return async { return selectClient() } }
    Local.runLoadTest log reportingIntervals testsPerSecond errorCutoff duration selectClient runSingleTest

/// To establish a local node to run the tests against:
/// PS> cinst eventstore-oss -y
/// PS> & $env:ProgramData\chocolatey\bin\EventStore.ClusterNode.exe --gossip-on-single-node --discover-via-dns 0 --ext-http-port=30778
let connectToEventStoreNode log (dnsQuery, heartbeatTimeout, skipReconnect) (username, password) (operationTimeout, operationRetries) = async {
    let connector = GesConnector(username, password, reqTimeout=operationTimeout, reqRetries=operationRetries, requireMaster=false, heartbeatTimeout=heartbeatTimeout, log=Logger.SerilogVerbose log)
    if skipReconnect then return! connector.ConnectViaGossipAsync dnsQuery else

    let connect = async {
        let! gesConn = connector.ConnectViaGossipAsync(dnsQuery)
        return gesConn.Connection }
    let monitoredConnection = new ConnectionMonitor.EventStoreConnectionCorrect(connect, log)
    return new GesConnection(monitoredConnection) }

let defaultBatchSize = 500
let createGesGateway connection batchSize = GesGateway(connection, GesBatchingPolicy(maxBatchSize = batchSize))

let serializationSettings = Foldunk.Serialization.Settings.CreateDefault()
let genCodec<'T> = Foldunk.EventSumCodec.generateJsonUtf8EventSumEncoder<'T> serializationSettings

let fold, initial = Domain.Favorites.Folds.fold, Domain.Favorites.Folds.initial

let codec = genCodec<Domain.Favorites.Events.Event>
let createServiceGes eventStoreConnection =
    let gateway = createGesGateway eventStoreConnection defaultBatchSize
    Backend.Favorites.Service(fun cet -> GesStreamBuilder(gateway, codec, fold, initial, CompactionStrategy.EventType cet).Create)

let runFavoriteTest log clientId store = async {
    let service = createServiceGes store
    let sku = Guid.NewGuid() |> SkuId
    let! _ = service.Read log clientId
    do! service.Execute log clientId (Favorites.Command.Favorite(DateTimeOffset.Now, [sku])) }

let log =
    LoggerConfiguration()
        .Destructure.FSharpTypes()
        .WriteTo.Console(LogEventLevel.Debug, theme = Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code)
        .CreateLogger()
let domainLog =
    LoggerConfiguration()
        .Destructure.FSharpTypes()
        .WriteTo.Console(LogEventLevel.Warning, theme = Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code)
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
        let errorCutoff = args.GetResult(ErrorCutoff,1000L)
        let testsPerSecond = args.GetResult(TestsPerSecond,250)
        let m, s = args.GetResult(DurationM,1),args.GetResult(DurationS, 0)
        let duration = TimeSpan(0,m,s)
        let reportingIntervals =
            match args.GetResults(ReportIntervalS) with
            | [] -> TimeSpan.FromSeconds 10.|> Seq.singleton
            | intervals -> seq { for i in intervals -> TimeSpan.FromSeconds(float i) }
            |> fun intervals -> [| yield duration; yield! intervals |]
        let host = args.GetResult(Host,"localhost")
        let creds = args.GetResult(Username,"admin"), args.GetResult(Password,"changeit")
        let (timeout, retries) as operationThrottling = args.GetResult(Timeout,2.) |> float |> TimeSpan.FromSeconds,args.GetResult(Retries,1)
        let heartbeatTimeout = args.GetResult(HeartbeatTimeout,2.) |> float |> TimeSpan.FromSeconds
        let report = args.GetResult(LogFile,"log.txt") |> fun n -> FileInfo(n).FullName
        let skipReconnect = args.GetResult(SkipReconnect,false)
        let store = connectToEventStoreNode log (host, heartbeatTimeout, skipReconnect) creds operationThrottling |> Async.RunSynchronously

        let clients = Array.init (testsPerSecond * 2) (fun _ -> Guid.NewGuid () |> ClientId)
        let runSingleTest clientId = runFavoriteTest domainLog clientId store
        log.Information(
            "Running for {duration}, targeting {host} using heartbeat: {heartbeat}, skipReconnect: {skipReconnect}, with {tps} hits/s with a timeout of {timeout} and {retries} retries\n" +
            "max errors: {errorCutOff} reporting intervals: {ri}, report file: {report}",
            ([| duration; host; heartbeatTimeout; skipReconnect; testsPerSecond; timeout; retries; errorCutoff; reportingIntervals; report |] : obj[]))
        let results = run log testsPerSecond (duration.Add(TimeSpan.FromSeconds 5.)) errorCutoff reportingIntervals clients runSingleTest |> Async.RunSynchronously
        let resultFile = createResultLog report
        for r in results do
            resultFile.Information("Aggregate: {aggregate}", r)
        log.Information("Run completed, current allocation: {bytes:n0}",GC.GetTotalMemory(true))
        0
    with e ->
        printfn "%s" e.Message
        1