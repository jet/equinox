module Equinox.Tool.Program

open Argu
open Domain.Infrastructure
open Jet.ConfluentKafka.FSharp
open Equinox.Store.Infrastructure
open Equinox.Tool.Infrastructure
open FSharp.UMX
open Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing
open Microsoft.Extensions.DependencyInjection
open Samples.Infrastructure
open Serilog
open Propulsion.Cosmos
open Propulsion.Streams
open Propulsion.Kafka.Codec
open Serilog.Events
open System
open System.Net.Http
open System.Threading
open System.Collections.Generic
open System.Diagnostics

[<NoEquality; NoComparison>]
type Arguments =
    | [<AltCommandLine("-v")>] Verbose
    | [<AltCommandLine("-vc")>] VerboseConsole
    | [<AltCommandLine("-S")>] LocalSeq
    | [<AltCommandLine("-l")>] LogFile of string
    | [<CliPrefix(CliPrefix.None); Last; Unique>] Run of ParseResults<TestArguments>
    | [<CliPrefix(CliPrefix.None); Last; Unique>] Init of ParseResults<InitArguments>
    | [<CliPrefix(CliPrefix.None); Last; Unique>] InitAux of ParseResults<InitAuxArguments>
    | [<CliPrefix(CliPrefix.None); Last; Unique>] Project of ParseResults<ProjectArguments>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | Verbose -> "Include low level logging regarding specific test runs."
            | VerboseConsole -> "Include low level test and store actions logging in on-screen output to console."
            | LocalSeq -> "Configures writing to a local Seq endpoint at http://localhost:5341, see https://getseq.net"
            | LogFile _ -> "specify a log file to write the result breakdown into (default: eqx.log)."
            | Run _ -> "Run a load test"
            | Init _ -> "Initialize Store/Collection (presently only relevant for `cosmos`; also handles adjusting RU/s provisioning adjustment)."
            | InitAux _ -> "Initialize auxilliary store (presently only relevant for `cosmos`, when you intend to run the Projector)."
            | Project _ -> "Project from store specified as the last argument, storing state in the specified `aux` Store (see initAux)."
and [<NoComparison>]InitArguments =
    | [<AltCommandLine("-ru"); Mandatory>] Rus of int
    | [<AltCommandLine("-D")>] Shared
    | [<AltCommandLine("-P")>] SkipStoredProc
    | [<CliPrefix(CliPrefix.None)>] Cosmos of ParseResults<Storage.Cosmos.Arguments>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | Rus _ -> "Specify RU/s level to provision for the Collection."
            | Shared -> "Use Database-level RU allocations (Default: Use Container-level allocation)."
            | SkipStoredProc -> "Inhibit creation of stored procedure in cited Collection."
            | Cosmos _ -> "Cosmos Connection parameters."
and [<NoComparison>]InitDbArguments =
    | [<AltCommandLine("-ru"); Mandatory>] Rus of int
    | [<AltCommandLine("-P")>] SkipStoredProc
    | [<CliPrefix(CliPrefix.None)>] Cosmos of ParseResults<Storage.Cosmos.Arguments>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | Rus _ -> "Specify RU/s level to provision for the Database."
            | SkipStoredProc -> "Inhibit creation of stored procedure in cited Collection."
            | Cosmos _ -> "Cosmos Connection parameters."
and [<NoComparison>]InitAuxArguments =
    | [<AltCommandLine("-ru"); Mandatory>] Rus of int
    | [<AltCommandLine("-s")>] Suffix of string
    | [<CliPrefix(CliPrefix.None)>] Cosmos of ParseResults<Storage.Cosmos.Arguments>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | Rus _ -> "Specify RU/s level to provision for the Aux Collection."
            | Suffix _ -> "Specify Collection Name suffix (default: `-aux`)."

            | Cosmos _ -> "Cosmos Connection parameters."
and [<NoComparison; RequireSubcommand>]ProjectArguments =
    | [<MainCommand; ExactlyOnce>] LeaseId of string
    | [<AltCommandLine("-s"); Unique>] Suffix of string
    | [<AltCommandLine("-z"); Unique>] FromTail
    | [<AltCommandLine("-md"); Unique>] MaxDocuments of int
    | [<AltCommandLine("-l"); Unique>] LagFreqM of float
    | [<CliPrefix(CliPrefix.None); Last>] Stats of ParseResults<StatsTarget>
    | [<CliPrefix(CliPrefix.None); Last>] Kafka of ParseResults<KafkaTarget>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | LeaseId _ -> "Projector instance context name."
            | Suffix _ -> "Specify Collection Name suffix (default: `-aux`)."
            | FromTail _ -> "(iff `suffix` represents a fresh projection) - force starting from present Position. Default: Ensure each and every event is projected from the start."
            | MaxDocuments _ -> "Maximum item count to supply to Changefeed Api when querying. Default: Unlimited"
            | LagFreqM _ -> "Specify frequency to dump lag stats. Default: off"

            | Stats _ -> "Do not emit events, only stats."
            | Kafka _ -> "Project to Kafka."
and [<NoComparison>] KafkaTarget =
    | [<AltCommandLine("-t"); Unique; MainCommand>] Topic of string
    | [<AltCommandLine("-b"); Unique>] Broker of string
    | [<CliPrefix(CliPrefix.None); Last>] Cosmos of ParseResults<Storage.Cosmos.Arguments>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | Topic _ -> "Specify target topic. Default: Use $env:EQUINOX_KAFKA_TOPIC"
            | Broker _ -> "Specify target broker. Default: Use $env:EQUINOX_KAFKA_BROKER"
            | Cosmos _ -> "Cosmos Connection parameters."
and [<NoComparison>] StatsTarget =
    | [<CliPrefix(CliPrefix.None); Last; Unique>] Cosmos of ParseResults<Storage.Cosmos.Arguments>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | Cosmos _ -> "Cosmos Connection parameters."
and [<NoComparison>]WebArguments =
    | [<AltCommandLine("-u")>] Endpoint of string
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | Endpoint _ -> "Target address. Default: https://localhost:5001"
and [<NoComparison>]
    TestArguments =
    | [<AltCommandLine("-t"); Unique>] Name of Test
    | [<AltCommandLine("-s")>] Size of int
    | [<AltCommandLine("-C")>] Cached
    | [<AltCommandLine("-U")>] Unfolds
    | [<AltCommandLine("-m")>] BatchSize of int
    | [<AltCommandLine("-f")>] TestsPerSecond of int
    | [<AltCommandLine("-d")>] DurationM of float
    | [<AltCommandLine("-e")>] ErrorCutoff of int64
    | [<AltCommandLine("-i")>] ReportIntervalS of int
    | [<CliPrefix(CliPrefix.None); Last; Unique>] Memory of ParseResults<Storage.MemoryStore.Arguments>
    | [<CliPrefix(CliPrefix.None); Last; Unique>] Es of ParseResults<Storage.EventStore.Arguments>
    | [<CliPrefix(CliPrefix.None); Last; Unique>] Cosmos of ParseResults<Storage.Cosmos.Arguments>
    | [<CliPrefix(CliPrefix.None); Last; Unique>] Web of ParseResults<WebArguments>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | Name _ -> "specify which test to run. (default: Favorite)."
            | Size _ -> "For `-t Todo`: specify random title length max size to use (default 100)."
            | Cached -> "employ a 50MB cache, wire in to Stream configuration."
            | Unfolds -> "employ a store-appropriate Rolling Snapshots and/or Unfolding strategy."
            | BatchSize _ -> "Maximum item count to supply when querying. Default: 500"
            | TestsPerSecond _ -> "specify a target number of requests per second (default: 1000)."
            | DurationM _ -> "specify a run duration in minutes (default: 30)."
            | ErrorCutoff _ -> "specify an error cutoff; test ends when exceeded (default: 10000)."
            | ReportIntervalS _ -> "specify reporting intervals in seconds (default: 10)."
            | Memory _ -> "target in-process Transient Memory Store (Default if not other target specified)."
            | Es _ -> "Run transactions in-process against EventStore."
            | Cosmos _ -> "Run transactions in-process against CosmosDb."
            | Web _ -> "Run transactions against a Web endpoint."
and TestInfo(args: ParseResults<TestArguments>) =
    member __.Options = args.GetResults Cached @ args.GetResults Unfolds
    member __.Cache = __.Options |> List.contains Cached
    member __.Unfolds = __.Options |> List.contains Unfolds
    member __.BatchSize = args.GetResult(BatchSize,500)
    member __.Test = args.GetResult(Name,Test.Favorite)
    member __.ErrorCutoff = args.GetResult(ErrorCutoff,10000L)
    member __.TestsPerSecond = args.GetResult(TestsPerSecond,1000)
    member __.Duration = args.GetResult(DurationM,30.) |> TimeSpan.FromMinutes
    member __.ReportingIntervals =
        match args.GetResults(ReportIntervalS) with
        | [] -> TimeSpan.FromSeconds 10.|> Seq.singleton
        | intervals -> seq { for i in intervals -> TimeSpan.FromSeconds(float i) }
        |> fun intervals -> [| yield __.Duration; yield! intervals |]
    member __.ConfigureStore(log : ILogger, createStoreLog) = 
        match args.TryGetSubCommand() with
        | Some (Es sargs) ->
            let storeLog = createStoreLog <| sargs.Contains Storage.EventStore.Arguments.VerboseStore
            log.Information("Running transactions in-process against EventStore with storage options: {options:l}", __.Options)
            storeLog, Storage.EventStore.config (log,storeLog) (__.Cache, __.Unfolds, __.BatchSize) sargs
        | Some (Cosmos sargs) ->
            let storeLog = createStoreLog <| sargs.Contains Storage.Cosmos.Arguments.VerboseStore
            log.Information("Running transactions in-process against CosmosDb with storage options: {options:l}", __.Options)
            storeLog, Storage.Cosmos.config (log,storeLog) (__.Cache, __.Unfolds, __.BatchSize) (Storage.Cosmos.Info sargs)
        | _  | Some (Memory _) ->
            log.Warning("Running transactions in-process against Volatile Store with storage options: {options:l}", __.Options)
            createStoreLog false, Storage.MemoryStore.config ()
    member __.Tests =
        match args.GetResult(Name,Favorite) with
        | Favorite -> Tests.Favorite
        | SaveForLater -> Tests.SaveForLater
        | Todo -> Tests.Todo (args.GetResult(Size,100))
and Test = Favorite | SaveForLater | Todo

let createStoreLog verbose verboseConsole maybeSeqEndpoint =
    let c = LoggerConfiguration().Destructure.FSharpTypes()
    let c = if verbose then c.MinimumLevel.Debug() else c
    let c = c.WriteTo.Sink(Equinox.Cosmos.Store.Log.InternalMetrics.Stats.LogSink())
    let c = c.WriteTo.Sink(Equinox.EventStore.Log.InternalMetrics.Stats.LogSink())
    let c = c.WriteTo.Console((if verbose && verboseConsole then LogEventLevel.Debug else LogEventLevel.Warning), theme = Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code)
    let c = match maybeSeqEndpoint with None -> c | Some endpoint -> c.WriteTo.Seq(endpoint)
    c.CreateLogger() :> ILogger

module LoadTest =
    open Equinox.Tools.TestHarness

    let private runLoadTest log testsPerSecond duration errorCutoff reportingIntervals (clients : ClientId[]) runSingleTest =
        let mutable idx = -1L
        let selectClient () =
            let clientIndex = Interlocked.Increment(&idx) |> int
            clients.[clientIndex % clients.Length]
        let selectClient = async { return async { return selectClient() } }
        Local.runLoadTest log reportingIntervals testsPerSecond errorCutoff duration selectClient runSingleTest
    let private decorateWithLogger (domainLog : ILogger, verbose) (run: 't -> Async<unit>) =
        let execute clientId =
            if not verbose then run clientId
            else async {
                domainLog.Information("Executing for client {sessionId}", clientId)
                try return! run clientId
                with e -> domainLog.Warning(e, "Test threw an exception"); e.Reraise () }
        execute
    let private createResultLog fileName = LoggerConfiguration().WriteTo.File(fileName).CreateLogger()
    let run (log: ILogger) (verbose,verboseConsole,maybeSeq) reportFilename (args: ParseResults<TestArguments>) =
        let a = TestInfo args
        let storage = args.TryGetSubCommand()

        let createStoreLog verboseStore = createStoreLog verboseStore verboseConsole maybeSeq
        let storeLog, storeConfig, httpClient: ILogger * Storage.StorageConfig option * HttpClient option =
            match storage with
            | Some (Web wargs) ->
                let uri = wargs.GetResult(WebArguments.Endpoint,"https://localhost:5001") |> Uri
                log.Information("Running web test targetting: {url}", uri)
                createStoreLog false, None, new HttpClient(BaseAddress=uri) |> Some
            | Some (Es sargs) ->
                let storeLog = createStoreLog <| sargs.Contains Storage.EventStore.Arguments.VerboseStore
                log.Information("Running transactions in-process against EventStore with storage options: {options:l}", a.Options)
                storeLog, Storage.EventStore.config (log,storeLog) (a.Cache, a.Unfolds, a.BatchSize) sargs |> Some, None
            | Some (Cosmos sargs) ->
                let storeLog = createStoreLog <| sargs.Contains Storage.Cosmos.Arguments.VerboseStore
                log.Information("Running transactions in-process against CosmosDb with storage options: {options:l}", a.Options)
                storeLog, Storage.Cosmos.config (log,storeLog) (a.Cache, a.Unfolds, a.BatchSize) (Storage.Cosmos.Info sargs) |> Some, None
            | _  | Some (Memory _) ->
                log.Warning("Running transactions in-process against Volatile Store with storage options: {options:l}", a.Options)
                createStoreLog false, Storage.MemoryStore.config () |> Some, None
        let test, duration = a.Tests, a.Duration
        let runSingleTest : ClientId -> Async<unit> =
            match storeConfig, httpClient with
            | None, Some client ->
                let execForClient = Tests.executeRemote client test
                decorateWithLogger (log,verbose) execForClient
            | Some storeConfig, _ ->
                let services = ServiceCollection()
                let codecGen : Services.ICodecGen = Services.NewtonsoftJsonCodecGen() :> _ 
                Samples.Infrastructure.Services.register(services, storeConfig, storeLog, codecGen)
                let container = services.BuildServiceProvider()
                let execForClient = Tests.executeLocal container test
                decorateWithLogger (log, verbose) execForClient
            | None, None -> invalidOp "impossible None, None"
        let clients = Array.init (a.TestsPerSecond * 2) (fun _ -> % Guid.NewGuid())

        log.Information( "Running {test} for {duration} @ {tps} hits/s across {clients} clients; Max errors: {errorCutOff}, reporting intervals: {ri}, report file: {report}",
            test, a.Duration, a.TestsPerSecond, clients.Length, a.ErrorCutoff, a.ReportingIntervals, reportFilename)
        // Reset the start time based on which the shared global metrics will be computed
        let _ = Equinox.Cosmos.Store.Log.InternalMetrics.Stats.LogSink.Restart() 
        let _ = Equinox.EventStore.Log.InternalMetrics.Stats.LogSink.Restart() 
        let results = runLoadTest log a.TestsPerSecond (duration.Add(TimeSpan.FromSeconds 5.)) a.ErrorCutoff a.ReportingIntervals clients runSingleTest |> Async.RunSynchronously

        let resultFile = createResultLog reportFilename
        for r in results do
            resultFile.Information("Aggregate: {aggregate}", r)
        log.Information("Run completed; Current memory allocation: {bytes:n2} MiB", (GC.GetTotalMemory(true) |> float) / 1024./1024.)

        match storeConfig with
        | Some (Storage.StorageConfig.Cosmos _) ->
            Equinox.Cosmos.Store.Log.InternalMetrics.dump log
        | Some (Storage.StorageConfig.Es _) ->
            Equinox.EventStore.Log.InternalMetrics.dump log
        | _ -> ()

let createDomainLog verbose verboseConsole maybeSeqEndpoint =
    let c = LoggerConfiguration().Destructure.FSharpTypes().Enrich.FromLogContext()
    let c = if verbose then c.MinimumLevel.Debug() else c
    let c = c.WriteTo.Sink(Equinox.EventStore.Log.InternalMetrics.Stats.LogSink())
    let c = c.WriteTo.Sink(Equinox.Cosmos.Store.Log.InternalMetrics.Stats.LogSink())
    let c = c.WriteTo.Console((if verboseConsole then LogEventLevel.Debug else LogEventLevel.Information), theme = Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code)
    let c = match maybeSeqEndpoint with None -> c | Some endpoint -> c.WriteTo.Seq(endpoint)
    c.CreateLogger()

module CosmosInit =
    open Equinox.Cosmos.Store.Sync.Initialization
    let containerAndOrDb (log: ILogger, verboseConsole, maybeSeq) (iargs: ParseResults<InitArguments>) = async {
        match iargs.TryGetSubCommand() with
        | Some (InitArguments.Cosmos sargs) -> 
            let rus, skipStoredProc = iargs.GetResult(InitArguments.Rus), iargs.Contains InitArguments.SkipStoredProc
            let mode = if iargs.Contains InitArguments.Shared then Provisioning.Database rus else Provisioning.Container rus
            let storeLog = createStoreLog (sargs.Contains Storage.Cosmos.Arguments.VerboseStore) verboseConsole maybeSeq
            let discovery, dbName, collName, connector = Storage.Cosmos.connection (log,storeLog) (Storage.Cosmos.Info sargs)
            let modeStr, rus = match mode with Provisioning.Container rus -> "Container",rus | Provisioning.Database rus -> "Database",rus
            log.Information("Provisioning `Equinox.Cosmos` Store Collection at {mode:l} level for {rus:n0} RU/s", modeStr, rus)
            let! conn = connector.Connect("equinox-tool", discovery)
            return! init log conn.Client (dbName,collName) mode skipStoredProc
        | _ -> failwith "please specify a `cosmos` endpoint" }
    let aux (log: ILogger, verboseConsole, maybeSeq) (iargs: ParseResults<InitAuxArguments>) = async {
        match iargs.TryGetSubCommand() with
        | Some (InitAuxArguments.Cosmos sargs) ->
            let storeLog = createStoreLog (sargs.Contains Storage.Cosmos.Arguments.VerboseStore) verboseConsole maybeSeq
            let discovery, dbName, baseCollName, connector = Storage.Cosmos.connection (log,storeLog) (Storage.Cosmos.Info sargs)
            let auxCollName = let collSuffix = iargs.GetResult(InitAuxArguments.Suffix,"-aux") in baseCollName + collSuffix
            let rus = iargs.GetResult(InitAuxArguments.Rus)
            log.Information("Provisioning Lease/`aux` Collection {collName} for {rus:n0} RU/s", auxCollName, rus)
            let! conn = connector.Connect("equinox-tool", discovery)
            return! initAux conn.Client (dbName,auxCollName) rus
        | _ -> failwith "please specify a `cosmos` endpoint" }

[<EntryPoint>]
let main argv =
    let programName = System.Reflection.Assembly.GetEntryAssembly().GetName().Name
    let parser = ArgumentParser.Create<Arguments>(programName = programName)
    try
        let args = parser.ParseCommandLine argv
        let verboseConsole = args.Contains VerboseConsole
        let maybeSeq = if args.Contains LocalSeq then Some "http://localhost:5341" else None
        let verbose = args.Contains Verbose
        let log = createDomainLog verbose verboseConsole maybeSeq
        match args.GetSubCommand() with
        | Init iargs -> CosmosInit.containerAndOrDb (log, verboseConsole, maybeSeq) iargs |> Async.RunSynchronously
        | InitAux iargs -> CosmosInit.aux (log, verboseConsole, maybeSeq) iargs |> Async.RunSynchronously
        | Project pargs ->
            let envBackstop msg key =
                match Environment.GetEnvironmentVariable key with
                | null -> failwithf "Please provide a %s, either as an argment or via the %s environment variable" msg key
                | x -> x 
            let broker, topic, storeArgs =
                match pargs.GetSubCommand() with
                | Kafka kargs ->
                    let broker = match kargs.TryGetResult Broker with Some x -> x | None -> envBackstop "Broker" "EQUINOX_KAFKA_BROKER"
                    let topic = match kargs.TryGetResult Topic with Some x -> x | None -> envBackstop "Topic" "EQUINOX_KAFKA_TOPIC"
                    Some broker, Some topic,kargs.GetResult KafkaTarget.Cosmos
                | Stats sargs -> None, None, sargs.GetResult StatsTarget.Cosmos
                | x -> failwithf "Invalid subcommand %A" x
            let storeLog = createStoreLog (storeArgs.Contains Storage.Cosmos.Arguments.VerboseStore) verboseConsole maybeSeq
            let discovery, dbName, collName, connector = Storage.Cosmos.connection (log, storeLog) (Storage.Cosmos.Info storeArgs)
            pargs.TryGetResult MaxDocuments |> Option.iter (fun bs -> log.Information("Requesting ChangeFeed Maximum Document Count {changeFeedMaxItemCount}", bs))
            pargs.TryGetResult LagFreqM |> Option.iter (fun s -> log.Information("Dumping lag stats at {lagS:n0}m intervals", s))
            let auxCollName = collName + pargs.GetResult(ProjectArguments.Suffix,"-aux")
            let leaseId = pargs.GetResult(LeaseId)
            log.Information("Processing using LeaseId {leaseId} in Aux coll {auxCollName}", leaseId, auxCollName)
            if pargs.Contains FromTail then log.Warning("(If new projection prefix) Skipping projection of all existing events.")
            let source = { database = dbName; collection = collName }
            let aux = { database = dbName; collection = auxCollName }

            let buildRangeProjector () =
                let sw = Stopwatch.StartNew() // we'll report the warmup/connect time on the first batch
                let producer, disposeProducer =
                    match broker,topic with
                    | Some b,Some t ->
                        let cfg = KafkaProducerConfig.Create("equinox-tool", Uri b, Confluent.Kafka.Acks.Leader, Confluent.Kafka.CompressionType.Lz4)
                        let p = BatchedProducer.CreateWithConfigOverrides(log, cfg, t)
                        Some p, (p :> IDisposable).Dispose
                    | _ -> None, id
                let projectBatch (log : ILogger) (ctx : IChangeFeedObserverContext) (docs : IReadOnlyList<Microsoft.Azure.Documents.Document>) = async {
                    sw.Stop() // Stop the clock after CFP hands off to us
                    let render (e: StreamEvent<_>) = RenderedSpan.ofStreamSpan e.stream { StreamSpan.index = e.index; events=[| e.event |] }
                    let pt, events = (fun () -> docs |> Seq.collect DocumentParser.enumEvents |> Seq.map render |> Array.ofSeq) |> Stopwatch.Time 
                    let! et = async {
                        match producer with
                        | None ->
                            let! et,() = ctx.Checkpoint() |> Stopwatch.Time
                            return et
                        | Some producer ->
                            let es = [| for e in events -> e.s, Newtonsoft.Json.JsonConvert.SerializeObject e |]
                            let! et,() = async {
                                let! _ = producer.ProduceBatch es
                                return! ctx.Checkpoint() } |> Stopwatch.Time 
                            return et }
                            
                    if log.IsEnabled LogEventLevel.Debug then log.Debug("Response Headers {0}", let hs = ctx.FeedResponse.ResponseHeaders in [for h in hs -> h, hs.[h]])
                    let r = ctx.FeedResponse
                    log.Information("{range} Fetch: {token} {requestCharge:n0}RU {count} docs {l:n1}s; Parse: s {streams} e {events} {p:n3}s; Emit: {e:n1}s",
                        ctx.PartitionKeyRangeId, r.ResponseContinuation.Trim[|'"'|], r.RequestCharge, docs.Count, float sw.ElapsedMilliseconds / 1000., 
                        events.Length, (let e = pt.Elapsed in e.TotalSeconds), (let e = et.Elapsed in e.TotalSeconds))
                    sw.Restart() // restart the clock as we handoff back to the CFP
                }
                ChangeFeedObserver.Create(log, projectBatch, dispose = disposeProducer)

            let run = async {
                let logLag (interval : TimeSpan) remainingWork = async {
                    let logLevel = if remainingWork |> Seq.exists (fun (_r,rw) -> rw <> 0L) then Events.LogEventLevel.Information else Events.LogEventLevel.Debug
                    log.Write(logLevel, "Lags {@rangeLags} <- [Range Id, documents count] ", remainingWork)
                    return! Async.Sleep(int interval.TotalMilliseconds) }
                let maybeLogLag = pargs.TryGetResult LagFreqM |> Option.map (TimeSpan.FromMinutes >> logLag)
                let! _cfp =
                    ChangeFeedProcessor.Start
                      ( log, discovery, connector.ConnectionPolicy, source, aux, buildRangeProjector,
                        leasePrefix = leaseId,
                        startFromTail = pargs.Contains FromTail,
                        ?maxDocuments = pargs.TryGetResult MaxDocuments,
                        ?reportLagAndAwaitNextEstimation = maybeLogLag)
                return! Async.AwaitKeyboardInterrupt() }
            Async.RunSynchronously run
        | Run rargs ->
            let reportFilename = args.GetResult(LogFile,programName+".log") |> fun n -> System.IO.FileInfo(n).FullName
            LoadTest.run log (verbose,verboseConsole,maybeSeq) reportFilename rargs
        | _ -> failwith "Please specify a valid subcommand :- init, initAux, project or run"
        0
    with :? Argu.ArguParseException as e -> eprintfn "%s" e.Message; 1
        | Storage.MissingArg msg -> eprintfn "%s" msg; 1
        | e -> eprintfn "%s" e.Message; 1