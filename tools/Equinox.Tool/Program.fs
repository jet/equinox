module Equinox.Tool.Program

open Argu
open Domain.Infrastructure
#if !NET461
open Equinox.Cosmos.Projection
#endif
open Equinox.Tool.Infrastructure
open FSharp.UMX
#if !NET461
open Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing
#endif
open Microsoft.Extensions.DependencyInjection
open Samples.Infrastructure.Log
open Samples.Infrastructure.Storage
open Serilog
open Serilog.Events
open System
open System.Net.Http
open System.Threading
open System.Collections.Generic

[<NoEquality; NoComparison>]
type Arguments =
    | [<AltCommandLine("-v")>] Verbose
    | [<AltCommandLine("-vc")>] VerboseConsole
    | [<AltCommandLine("-S")>] LocalSeq
    | [<AltCommandLine("-l")>] LogFile of string
    | [<CliPrefix(CliPrefix.None); Last; Unique>] Run of ParseResults<TestArguments>
    | [<CliPrefix(CliPrefix.None); Last; Unique>] Init of ParseResults<InitArguments>
    | [<AltCommandLine("initAux"); CliPrefix(CliPrefix.None); Last; Unique>] InitAux of ParseResults<InitAuxArguments>
#if !NET461
    | [<CliPrefix(CliPrefix.None); Last; Unique>] Project of ParseResults<ProjectArguments>
#endif
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | Verbose -> "Include low level logging regarding specific test runs."
            | VerboseConsole -> "Include low level test and store actions logging in on-screen output to console."
            | LocalSeq -> "Configures writing to a local Seq endpoint at http://localhost:5341, see https://getseq.net"
            | LogFile _ -> "specify a log file to write the result breakdown into (default: eqx.log)."
            | Run _ -> "Run a load test"
            | Init _ -> "Initialize store (presently only relevant for `cosmos`, where it creates database+collection+stored proc if not already present)."
            | InitAux _ -> "Initialize auxilliary store (presently only relevant for `cosmos`, when you intend to run the Projector)."
#if !NET461
            | Project _ -> "Project from store specified as the last argument, storing state in the specified `aux` Store (see initAux)."
#endif
and [<NoComparison>]InitArguments =
    | [<AltCommandLine("-ru"); Mandatory>] Rus of int
    | [<AltCommandLine("-P")>] SkipStoredProc
    | [<CliPrefix(CliPrefix.None)>] Cosmos of ParseResults<CosmosArguments>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | Rus _ -> "Specify RU/s level to provision for the Application Collection."
            | SkipStoredProc -> "Inhibit creation of stored procedure in cited Collection."
            | Cosmos _ -> "Cosmos Connection parameters."
and [<NoComparison>]InitAuxArguments =
    | [<AltCommandLine("-ru"); Mandatory>] Rus of int
    | [<AltCommandLine("-s")>] Suffix of string
    | [<CliPrefix(CliPrefix.None)>] Cosmos of ParseResults<CosmosArguments>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | Rus _ -> "Specify RU/s level to provision for the Application Collection."
            | Suffix _ -> "Specify Collection Name suffix (default: `-aux`)."
            | Cosmos _ -> "Cosmos Connection parameters."
#if !NET461
and [<NoComparison>]ProjectArguments =
    | [<MainCommand; ExactlyOnce>] LeaseId of string
    | [<AltCommandLine("-s"); Unique>] Suffix of string
    | [<AltCommandLine("-b"); Unique>] ChangeFeedBatchSize of int
    | [<AltCommandLine("-f"); Unique>] ForceStartFromHere
    | [<AltCommandLine("-c"); Unique>] WriterCheckpointFreqS of float
    | [<AltCommandLine("-l"); Unique>] LagFreqS of float
    | [<AltCommandLine("-v"); Unique>] ChangeFeedVerbose
    | [<CliPrefix(CliPrefix.None); Last; Unique>] Cosmos of ParseResults<CosmosArguments>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | LeaseId _ -> "Projector instance context name."
            | Suffix _ -> "Specify Collection Name suffix (default: `-aux`)."
            | ChangeFeedBatchSize _ -> "Maximum item count to supply to Changefeed Api when querying. Default: 1000"
            | ForceStartFromHere _ -> "(Where this is `suffix` represents a fresh projection) - force starting from present Position. Default: Ensure each and eveery event is projected from the start."
            | WriterCheckpointFreqS _ -> "Specify frequency to update progress checkpoints. Default: 30s"
            | LagFreqS _ -> "Specify frequency to dump lag stats. Default: off"
            | ChangeFeedVerbose -> "Request Verbose Logging from ChangeFeedProcessor. Default: off"
            | Cosmos _ -> "Cosmos Connection parameters."
#endif
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
    | [<AltCommandLine("-f")>] TestsPerSecond of int
    | [<AltCommandLine("-d")>] DurationM of float
    | [<AltCommandLine("-e")>] ErrorCutoff of int64
    | [<AltCommandLine("-i")>] ReportIntervalS of int
    | [<CliPrefix(CliPrefix.None); Last; Unique>] Memory of ParseResults<Samples.Infrastructure.Storage.MemArguments>
    | [<CliPrefix(CliPrefix.None); Last; Unique>] Es of ParseResults<Samples.Infrastructure.Storage.EsArguments>
    | [<CliPrefix(CliPrefix.None); Last; Unique>] Cosmos of ParseResults<Samples.Infrastructure.Storage.CosmosArguments>
    | [<CliPrefix(CliPrefix.None); Last; Unique>] Web of ParseResults<WebArguments>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | Name _ -> "specify which test to run. (default: Favorite)."
            | Size _ -> "For `-t Todo`: specify random title length max size to use (default 100)."
            | Cached -> "employ a 50MB cache, wire in to Stream configuration."
            | Unfolds -> "employ a store-appropriate Rolling Snapshots and/or Unfolding strategy."
            | TestsPerSecond _ -> "specify a target number of requests per second (default: 1000)."
            | DurationM _ -> "specify a run duration in minutes (default: 30)."
            | ErrorCutoff _ -> "specify an error cutoff; test ends when exceeded (default: 10000)."
            | ReportIntervalS _ -> "specify reporting intervals in seconds (default: 10)."
            | Memory _ -> "target in-process Transient Memory Store (Default if not other target specified)."
            | Es _ -> "Run transactions in-process against EventStore."
            | Cosmos _ -> "Run transactions in-process against CosmosDb."
            | Web _ -> "Run transactions against a Web endpoint."
and Test = Favorite | SaveForLater | Todo

let createStoreLog verbose verboseConsole maybeSeqEndpoint =
    let c = LoggerConfiguration().Destructure.FSharpTypes()
    let c = if verbose then c.MinimumLevel.Debug() else c
    let c = c.WriteTo.Sink(RuCounterSink())
    let c = c.WriteTo.Console((if verbose && verboseConsole then LogEventLevel.Debug else LogEventLevel.Warning), theme = Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code)
    let c = match maybeSeqEndpoint with None -> c | Some endpoint -> c.WriteTo.Seq(endpoint)
    c.CreateLogger() :> ILogger

module LoadTest =
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
        let storage = args.TryGetSubCommand()

        let createStoreLog verboseStore = createStoreLog verboseStore verboseConsole maybeSeq
        let storeLog, storeConfig, httpClient: ILogger * StorageConfig option * HttpClient option =
            let options = args.GetResults Cached @ args.GetResults Unfolds
            let cache, unfolds = options |> List.contains Cached, options |> List.contains Unfolds
            match storage with
            | Some (Web wargs) ->
                let uri = wargs.GetResult(WebArguments.Endpoint,"https://localhost:5001") |> Uri
                log.Information("Running web test targetting: {url}", uri)
                createStoreLog false, None, new HttpClient(BaseAddress=uri) |> Some
            | Some (Es sargs) ->
                let storeLog = createStoreLog <| sargs.Contains EsArguments.VerboseStore
                log.Information("Running transactions in-process against EventStore with storage options: {options:l}", options)
                storeLog, EventStore.config (log,storeLog) (cache, unfolds) sargs |> Some, None
            | Some (Cosmos sargs) ->
                let storeLog = createStoreLog <| sargs.Contains CosmosArguments.VerboseStore
                log.Information("Running transactions in-process against CosmosDb with storage options: {options:l}", options)
                storeLog, Cosmos.config (log,storeLog) (cache, unfolds) sargs |> Some, None
            | _  | Some (Memory _) ->
                log.Warning("Running transactions in-process against Volatile Store with storage options: {options:l}", options)
                createStoreLog false, MemoryStore.config () |> Some, None
        let test =
            match args.GetResult(Name,Favorite) with
            | Favorite -> Tests.Favorite
            | SaveForLater -> Tests.SaveForLater
            | Todo -> Tests.Todo (args.GetResult(Size,100))
        let runSingleTest : ClientId -> Async<unit> =
            match storeConfig, httpClient with
            | None, Some client ->
                let execForClient = Tests.executeRemote client test
                decorateWithLogger (log,verbose) execForClient
            | Some storeConfig, _ ->
                let services = ServiceCollection()
                Samples.Infrastructure.Services.register(services, storeConfig, storeLog)
                let container = services.BuildServiceProvider()
                let execForClient = Tests.executeLocal container test
                decorateWithLogger (log, verbose) execForClient
            | None, None -> invalidOp "impossible None, None"
        let errorCutoff = args.GetResult(ErrorCutoff,10000L)
        let testsPerSecond = args.GetResult(TestsPerSecond,1000)
        let duration = args.GetResult(DurationM,30.) |> TimeSpan.FromMinutes
        let reportingIntervals =
            match args.GetResults(ReportIntervalS) with
            | [] -> TimeSpan.FromSeconds 10.|> Seq.singleton
            | intervals -> seq { for i in intervals -> TimeSpan.FromSeconds(float i) }
            |> fun intervals -> [| yield duration; yield! intervals |]
        let clients = Array.init (testsPerSecond * 2) (fun _ -> % Guid.NewGuid())

        log.Information( "Running {test} for {duration} @ {tps} hits/s across {clients} clients; Max errors: {errorCutOff}, reporting intervals: {ri}, report file: {report}",
            test, duration, testsPerSecond, clients.Length, errorCutoff, reportingIntervals, reportFilename)
        let results = runLoadTest log testsPerSecond (duration.Add(TimeSpan.FromSeconds 5.)) errorCutoff reportingIntervals clients runSingleTest |> Async.RunSynchronously

        let resultFile = createResultLog reportFilename
        for r in results do
            resultFile.Information("Aggregate: {aggregate}", r)
        log.Information("Run completed; Current memory allocation: {bytes:n2} MiB", (GC.GetTotalMemory(true) |> float) / 1024./1024.)

        match storeConfig with
        | Some (StorageConfig.Cosmos _) ->
            let stats =
              [ "Read", RuCounterSink.Read
                "Write", RuCounterSink.Write
                "Resync", RuCounterSink.Resync ]
            let mutable totalCount, totalRc, totalMs = 0L, 0., 0L
            let logActivity name count rc lat =
                log.Information("{name}: {count:n0} requests costing {ru:n0} RU (average: {avg:n2}); Average latency: {lat:n0}ms",
                    name, count, rc, (if count = 0L then Double.NaN else rc/float count), (if count = 0L then Double.NaN else float lat/float count))
            for name, stat in stats do
                let ru = float stat.rux100 / 100.
                totalCount <- totalCount + stat.count
                totalRc <- totalRc + ru
                totalMs <- totalMs + stat.ms
                logActivity name stat.count ru stat.ms
            logActivity "TOTAL" totalCount totalRc totalMs
            let measures : (string * (TimeSpan -> float)) list =
              [ "s", fun x -> x.TotalSeconds
                "m", fun x -> x.TotalMinutes
                "h", fun x -> x.TotalHours ]
            let logPeriodicRate name count ru = log.Information("rp{name} {count:n0} = ~{ru:n0} RU", name, count, ru)
            let duration = args.GetResult(DurationM,1.) |> TimeSpan.FromMinutes
            for uom, f in measures do let d = f duration in if d <> 0. then logPeriodicRate uom (float totalCount/d |> int64) (totalRc/d)
        | _ -> ()

let createDomainLog verbose verboseConsole maybeSeqEndpoint =
    let c = LoggerConfiguration().Destructure.FSharpTypes().Enrich.FromLogContext()
    let c = if verbose then c.MinimumLevel.Debug() else c
    let c = c.WriteTo.Sink(RuCounterSink())
    let c = c.WriteTo.Console((if verboseConsole then LogEventLevel.Debug else LogEventLevel.Information), theme = Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code)
    let c = match maybeSeqEndpoint with None -> c | Some endpoint -> c.WriteTo.Seq(endpoint)
    c.CreateLogger()

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
        | Init iargs ->
            let rus = iargs.GetResult(InitArguments.Rus)
            match iargs.TryGetSubCommand() with
            | Some (InitArguments.Cosmos sargs) ->
                let storeLog = createStoreLog (sargs.Contains CosmosArguments.VerboseStore) verboseConsole maybeSeq
                let dbName, collName, (_pageSize: int), conn = Cosmos.connect (log,storeLog) sargs
                log.Information("Configuring CosmosDb Collection {collName} with Throughput Provision: {rus:n0} RU/s", collName, rus)
                Async.RunSynchronously <| async {
                    do! Equinox.Cosmos.Store.Sync.Initialization.createDatabaseIfNotExists conn.Client dbName
                    do! Equinox.Cosmos.Store.Sync.Initialization.createBatchAndTipCollectionIfNotExists conn.Client (dbName,collName) rus
                    let collectionUri = Microsoft.Azure.Documents.Client.UriFactory.CreateDocumentCollectionUri(dbName,collName)
                    if not (iargs.Contains SkipStoredProc) then
                        do! Equinox.Cosmos.Store.Sync.Initialization.createSyncStoredProcIfNotExists (Some (upcast log)) conn.Client collectionUri }
            | _ -> failwith "please specify a `cosmos` endpoint"
        | InitAux iargs ->
            let rus = iargs.GetResult(InitAuxArguments.Rus)
            match iargs.TryGetSubCommand() with
            | Some (InitAuxArguments.Cosmos sargs) ->
                let storeLog = createStoreLog (sargs.Contains CosmosArguments.VerboseStore) verboseConsole maybeSeq
                let dbName, collName, (_pageSize: int), conn = Cosmos.connect (log,storeLog) sargs
                let collName = collName + iargs.GetResult(InitAuxArguments.Suffix,"-aux")
                log.Information("Configuring CosmosDb Aux Collection {collName} with Throughput Provision: {rus:n0} RU/s", collName, rus)
                Async.RunSynchronously <| async {
                    do! Equinox.Cosmos.Store.Sync.Initialization.createDatabaseIfNotExists conn.Client dbName
                    do! Equinox.Cosmos.Store.Sync.Initialization.createAuxCollectionIfNotExists conn.Client (dbName,collName) rus }
            | _ -> failwith "please specify a `cosmos` endpoint"
#if !NET461
        | Project pargs ->
            match pargs.TryGetSubCommand() with
            | Some (ProjectArguments.Cosmos args) ->
                let storeLog = createStoreLog (args.Contains CosmosArguments.VerboseStore) verboseConsole maybeSeq
                let (endpointUri, masterKey), dbName, collName, connectionPolicy = Cosmos.connectionPolicy (log,storeLog) args
                pargs.TryGetResult LagFreqS |> Option.iter (fun s -> log.Information("Dumping lag stats at {lagS:n0}s intervals", s))
                let auxCollName = collName + pargs.GetResult(ProjectArguments.Suffix,"-aux")
                let leaseId = pargs.GetResult(LeaseId)
                log.Information("Processing using LeaseId {leaseId} in Aux coll {auxCollName}", leaseId, auxCollName)
                let _checkpointInterval = pargs.GetResult(WriterCheckpointFreqS,30.) |> TimeSpan.FromSeconds
                if pargs.Contains ForceStartFromHere then log.Warning("(If new projection prefix) Skipping projection of all existing events.")
                let source = { database = dbName; collection = collName }
                let aux = { database = dbName; collection = auxCollName }
                let run = async {
                    let observe (context : IChangeFeedObserverContext) (docs : IReadOnlyList<Microsoft.Azure.Documents.Document>) = async { 
                        log.Information("Range {rangeId} Observed {count} docs rc={requestCharge}", context.PartitionKeyRangeId, docs.Count, context.FeedResponse.RequestCharge)
                    }
                    let changeFeedObserver = IChangeFeedObserver.Create(log, observe)
                    let! feedEventHost =
                        ChangeFeedProcessor.Start
                          ( log, endpointUri, masterKey, connectionPolicy, source, aux, changeFeedObserver,
                            leasePrefix = leaseId,
                            forceSkipExistingEvents = pargs.Contains ForceStartFromHere,
                            ?cfBatchSize = pargs.TryGetResult ChangeFeedBatchSize,
                            ?lagMonitorInterval = (pargs.TryGetResult LagFreqS |> Option.map TimeSpan.FromSeconds))
                    do! Async.AwaitKeyboardInterrupt()
                    log.Warning("Stopping...")
                    do! feedEventHost.StopAsync() |> Async.AwaitTaskCorrect }
                if pargs.Contains ChangeFeedVerbose then Log.Logger <- storeLog // LibLog will write to the global logger, if you let it
                Async.RunSynchronously run
            | _ -> failwith "please specify a `cosmos` endpoint"
#endif
        | Run rargs ->
            let reportFilename = args.GetResult(LogFile,programName+".log") |> fun n -> System.IO.FileInfo(n).FullName
            LoadTest.run log (verbose,verboseConsole,maybeSeq) reportFilename rargs
        | _ -> failwith "Please specify a valid subcommand :- init, initAux, project or run"
        0
    with e ->
        printfn "%s" e.Message
        1