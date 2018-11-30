module Equinox.Cli.Program

open Argu
open Domain.Infrastructure
open Equinox.Cosmos
open Equinox.EventStore
open Equinox.Cli.Infrastructure
open Serilog
open Serilog.Events
open System
open System.Threading

[<NoEquality; NoComparison>]
type Arguments =
    | [<AltCommandLine("-vd")>] VerboseDomain
    | [<AltCommandLine("-vc")>] VerboseConsole
    | [<AltCommandLine("-S")>] LocalSeq
    | [<AltCommandLine("-l")>] LogFile of string
    | [<CliPrefix(CliPrefix.None)>] Memory of ParseResults<TestArguments>
    | [<CliPrefix(CliPrefix.None)>] Es of ParseResults<EsArguments>
    | [<CliPrefix(CliPrefix.None)>] Cosmos of ParseResults<CosmosArguments>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | VerboseDomain -> "Include low level Domain logging."
            | VerboseConsole -> "Include low level Domain and Store logging in screen output."
            | LocalSeq -> "Configures writing to a local Seq endpoint at http://localhost:5341, see https://getseq.net"
            | LogFile _ -> "specify a log file to write the result breakdown (default: Equinox.Cli.log)."
            | Memory _ -> "specify In-Memory Volatile Store baseline test"
            | Es _ -> "specify EventStore actions"
            | Cosmos _ -> "specify CosmosDb actions"
and TestArguments =
    | [<AltCommandLine("-t"); MainCommand>] Name of Test
    | [<AltCommandLine("-C")>] Cached
    | [<AltCommandLine("-U")>] Unfolds
    | [<AltCommandLine("-f")>] TestsPerSecond of int
    | [<AltCommandLine("-d")>] DurationM of float
    | [<AltCommandLine("-e")>] ErrorCutoff of int64
    | [<AltCommandLine("-i")>] ReportIntervalS of int
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | Name _ -> "Specify which test to run. (default: Favorites)"
            | Cached -> "Employ a 50MB cache"
            | Unfolds -> "Employ a store-appropriate Rolling Snapshots and/or Unfolding strategy"
            | TestsPerSecond _ -> "specify a target number of requests per second (default: 1000)."
            | DurationM _ -> "specify a run duration in minutes (default: 30)."
            | ErrorCutoff _ -> "specify an error cutoff (default: 10000)."
            | ReportIntervalS _ -> "specify reporting intervals in seconds (default: 10)."
and Test = Favorites | SaveForLater
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
and [<NoEquality; NoComparison>] CosmosArguments =
    | [<AltCommandLine("-vs")>] VerboseStore
    | [<AltCommandLine("-o")>] Timeout of float
    | [<AltCommandLine("-r")>] Retries of int
    | [<AltCommandLine("-s")>] Connection of string
    | [<AltCommandLine("-d")>] Database of string
    | [<AltCommandLine("-c")>] Collection of string
    | [<AltCommandLine("-rt")>] RetriesWaitTime of int
    | [<AltCommandLine("-a")>] PageSize of int

    | [<CliPrefix(CliPrefix.None)>] Provision of ParseResults<CosmosProvisionArguments>
    | [<CliPrefix(CliPrefix.None)>] Run of ParseResults<TestArguments>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | VerboseStore -> "Include low level Store logging."
            | Timeout _ -> "specify operation timeout in seconds (default: 5)."
            | Retries _ -> "specify operation retries (default: 1)."
            | Connection _ -> "specify a connection string for a Cosmos account (defaults: envvar:EQUINOX_COSMOS_CONNECTION, Cosmos Emulator)."
            | Database _ -> "specify a database name for Cosmos account (defaults: envvar:EQUINOX_COSMOS_DATABASE, test)."
            | Collection _ -> "specify a collection name for Cosmos account (defaults: envvar:EQUINOX_COSMOS_COLLECTION, test)."
            | RetriesWaitTime _ -> "specify max wait-time for retry when being throttled by Cosmos in seconds (default: 5)"
            | PageSize _ -> "Specify maximum number of events to record on a page before switching to a new one (default: 1)"
            | Provision _ -> "Initialize a store collection."
            | Run _ -> "Run a load test."
and CosmosProvisionArguments =
    | [<AltCommandLine("-ru"); Mandatory>] Rus of int
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | Rus _ -> "Specify RUs to Allocate for the Application Collection."

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

module Cosmos =
    /// Standing up an Equinox instance is necessary to run for test purposes; You'll need to either:
    /// 1) replace connection below with a connection string or Uri+Key for an initialized Equinox instance with a database and collection named "equinox-test"
    /// 2) Set the 3x environment variables and create a local Equinox using cli/Equinox.cli/bin/Release/net461/Equinox.Cli `
    ///     cosmos -s $env:EQUINOX_COSMOS_CONNECTION -d $env:EQUINOX_COSMOS_DATABASE -c $env:EQUINOX_COSMOS_COLLECTION provision -ru 1000
    let connect (log: ILogger) discovery operationTimeout (maxRetryForThrottling, maxRetryWaitTime) =
        EqxConnector(log=log, requestTimeout=operationTimeout, maxRetryAttemptsOnThrottledRequests=maxRetryForThrottling, maxRetryWaitTimeInSeconds=maxRetryWaitTime)
            .Connect("equinox-cli", discovery)
    let createGateway connection (maxItems,maxEvents) = EqxGateway(connection, EqxBatchingPolicy(defaultMaxItems=maxItems, maxEventsPerSlice=maxEvents))

[<RequireQualifiedAccess; NoEquality; NoComparison>]
type Store =
    | Mem of Equinox.MemoryStore.VolatileStore
    | Es of GesGateway
    | Cosmos of EqxGateway * databaseId: string * collectionId: string

module Test =
    let run log testsPerSecond duration errorCutoff reportingIntervals (clients : ClientId[]) runSingleTest =
        let mutable idx = -1L
        let selectClient () =
            let clientIndex = Interlocked.Increment(&idx) |> int
            clients.[clientIndex % clients.Length]
        let selectClient = async { return async { return selectClient() } }
        Local.runLoadTest log reportingIntervals testsPerSecond errorCutoff duration selectClient runSingleTest
    let serializationSettings = Newtonsoft.Json.Converters.FSharp.Settings.CreateCorrect()
    let genCodec<'Union when 'Union :> TypeShape.UnionContract.IUnionContract>() = Equinox.UnionCodec.JsonUtf8.Create<'Union>(serializationSettings)
    type EsResolver(useCache) =
        member val Cache =
            if useCache then
                let c = Equinox.EventStore.Caching.Cache("Cli", sizeMb = 50)
                CachingStrategy.SlidingWindow (c, TimeSpan.FromMinutes 20.) |> Some
            else None
        member __.CreateAccessStrategy snapshot =
            match snapshot with
            | None -> None
            | Some snapshot -> Equinox.EventStore.AccessStrategy.RollingSnapshots snapshot |> Some
    type CosmosResolver(useCache) =
        member val Cache =
            if useCache then
                let c = Equinox.Cosmos.Caching.Cache("Cli", sizeMb = 50)
                Equinox.Cosmos.CachingStrategy.SlidingWindow (c, TimeSpan.FromMinutes 20.) |> Some
            else None
        member __.CreateAccessStrategy snapshot =
            match snapshot with
            | None -> None
            | Some snapshot -> AccessStrategy.Snapshot snapshot |> Some
    type Builder(store, useCache, useUnfolds) =
        member __.ResolveStream
            (   codec : Equinox.UnionCodec.IUnionEncoder<'event,byte[]>,
                fold: ('state -> 'event seq -> 'state),
                initial: 'state,
                snapshot: (('event -> bool) * ('state -> 'event))) =
            let snapshot = if useUnfolds then Some snapshot else None
            match store with
            | Store.Mem store ->
                Equinox.MemoryStore.MemResolver(store, fold, initial).Resolve
            | Store.Es gateway ->
                let resolver = EsResolver(useCache)
                GesResolver<'event,'state>(gateway, codec, fold, initial, ?access = resolver.CreateAccessStrategy(snapshot), ?caching = resolver.Cache).Resolve
            | Store.Cosmos (gateway, databaseId, connectionId) ->
                let resolver = CosmosResolver(useCache)
                let store = EqxStore(gateway, EqxCollections(databaseId, connectionId))
                EqxResolver<'event,'state>(store, codec, fold, initial, ?access = resolver.CreateAccessStrategy snapshot, ?caching = resolver.Cache).Resolve

    let createTest store test (cache,unfolds)  log =
        let builder = Builder(store, cache, unfolds)
        match test with
        | Favorites ->
            let fold, initial, snapshot = Domain.Favorites.Folds.fold, Domain.Favorites.Folds.initial, Domain.Favorites.Folds.snapshot
            let codec = genCodec<Domain.Favorites.Events.Event>()
            let service = Backend.Favorites.Service(log, builder.ResolveStream(codec,fold,initial,snapshot))
            fun clientId -> async {
                let sku = Guid.NewGuid() |> SkuId
                do! service.Favorite(clientId,[sku])
                let! items = service.List clientId
                if items |> Array.exists (fun x -> x.skuId = sku) |> not then invalidOp "Added item not found" }
        | SaveForLater ->
            let fold, initial, snapshot = Domain.SavedForLater.Folds.fold, Domain.SavedForLater.Folds.initial, Domain.SavedForLater.Folds.snapshot
            let codec = genCodec<Domain.SavedForLater.Events.Event>()
            let service = Backend.SavedForLater.Service(log, builder.ResolveStream(codec,fold,initial,snapshot), maxSavedItems=50, maxAttempts=3)
            fun clientId -> async {
                let skus = [Guid.NewGuid() |> SkuId; Guid.NewGuid() |> SkuId; Guid.NewGuid() |> SkuId]
                let! saved = service.Save(clientId,skus)
                if saved then
                    let! items = service.List clientId
                    if skus |> List.forall (fun sku -> items |> Array.exists (fun x -> x.skuId = sku)) |> not then invalidOp "Added item not found"
                else
                    let! current = service.List clientId
                    let resolveSkus _hasSku = async {
                        return [|for x in current -> x.skuId|] }
                    let! removed = service.Remove(clientId, resolveSkus)
                    if not removed then invalidOp "Remove failed" }

    let createRunner conn domainLog verbose (targs: ParseResults<TestArguments>) =
        let test = targs.GetResult(Name,Favorites)
        let options = targs.GetResults Cached @ targs.GetResults Unfolds
        let cache, unfold = options |> List.contains Cached, options |> List.contains Unfolds
        let run = createTest conn test (cache,unfold) domainLog
        let execute clientId =
            if not verbose then run clientId
            else async {
                domainLog.Information("Executing for client {sessionId}", clientId)
                try return! run clientId
                with e -> domainLog.Warning(e, "Test threw an exception"); e.Reraise () }
        test,options,execute

[<AutoOpen>]
module SerilogHelpers =
    let inline (|Stats|) ({ interval = i; ru = ru }: Equinox.Cosmos.Store.Log.Measurement) = ru, let e = i.Elapsed in int64 e.TotalMilliseconds
    open Equinox.Cosmos.Store
    let (|CosmosReadRc|CosmosWriteRc|CosmosResyncRc|CosmosResponseRc|) = function
        | Log.Tip (Stats s)
        | Log.TipNotFound (Stats s)
        | Log.TipNotModified (Stats s)
        | Log.Query (_,_, (Stats s)) -> CosmosReadRc s
        // slices are rolled up into batches so be sure not to double-count
        | Log.Response (_,(Stats s)) -> CosmosResponseRc s
        | Log.SyncSuccess (Stats s)
        | Log.SyncConflict (Stats s) -> CosmosWriteRc s
        | Log.SyncResync (Stats s) -> CosmosResyncRc s
    let (|SerilogScalar|_|) : Serilog.Events.LogEventPropertyValue -> obj option = function
        | (:? ScalarValue as x) -> Some x.Value
        | _ -> None
    let (|CosmosMetric|_|) (logEvent : LogEvent) : Log.Event option =
        match logEvent.Properties.TryGetValue("cosmosEvt") with
        | true, SerilogScalar (:? Log.Event as e) -> Some e
        | _ -> None
    type RuCounter =
        { mutable rux100: int64; mutable count: int64; mutable ms: int64 }
        static member Create() = { rux100 = 0L; count = 0L; ms = 0L }
        member __.Ingest (ru, ms) =
            Interlocked.Increment(&__.count) |> ignore
            Interlocked.Add(&__.rux100, int64 (ru*100.)) |> ignore
            Interlocked.Add(&__.ms, ms) |> ignore
    type RuCounterSink() =
        static member val Read = RuCounter.Create()
        static member val Write = RuCounter.Create()
        static member val Resync = RuCounter.Create()
        interface Serilog.Core.ILogEventSink with
            member __.Emit logEvent = logEvent |> function
                | CosmosMetric (CosmosReadRc stats) -> RuCounterSink.Read.Ingest stats
                | CosmosMetric (CosmosWriteRc stats) -> RuCounterSink.Write.Ingest stats
                | CosmosMetric (CosmosResyncRc stats) -> RuCounterSink.Resync.Ingest stats
                | _ -> ()

let createStoreLog verbose verboseConsole maybeSeqEndpoint =
    let c = LoggerConfiguration().Destructure.FSharpTypes()
    let c = if verbose then c.MinimumLevel.Debug() else c
    let c = c.WriteTo.Sink(RuCounterSink())
    let c = c.WriteTo.Console((if verboseConsole then LogEventLevel.Debug else LogEventLevel.Information), theme = Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code)
    let c = match maybeSeqEndpoint with None -> c | Some endpoint -> c.WriteTo.Seq(endpoint)
    c.CreateLogger() :> ILogger
let createDomainLog verbose verboseConsole maybeSeqEndpoint =
    let c = LoggerConfiguration().Destructure.FSharpTypes().Enrich.FromLogContext()
    let c = if verbose then c.MinimumLevel.Debug() else c
    let c = c.WriteTo.Sink(RuCounterSink())
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
        let report = args.GetResult(LogFile,programName+".log") |> fun n -> System.IO.FileInfo(n).FullName
        let runTest (log: ILogger) conn (targs: ParseResults<TestArguments>) =
            let verbose = args.Contains(VerboseDomain)
            let domainLog = createDomainLog verbose verboseConsole maybeSeq
            let test, options, runTest = Test.createRunner conn domainLog verbose targs

            let errorCutoff = targs.GetResult(ErrorCutoff,10000L)
            let testsPerSecond = targs.GetResult(TestsPerSecond,1000)
            let duration = targs.GetResult(DurationM,30.) |> TimeSpan.FromMinutes
            let reportingIntervals =
                match targs.GetResults(ReportIntervalS) with
                | [] -> TimeSpan.FromSeconds 10.|> Seq.singleton
                | intervals -> seq { for i in intervals -> TimeSpan.FromSeconds(float i) }
                |> fun intervals -> [| yield duration; yield! intervals |]
            let clients = Array.init (testsPerSecond * 2) (fun _ -> Guid.NewGuid () |> ClientId)

            log.Information( "Running {test} {options:l} for {duration} @ {tps} hits/s across {clients} clients; Max errors: {errorCutOff}, reporting intervals: {ri}, report file: {report}",
                test, options, duration, testsPerSecond, clients.Length, errorCutoff, reportingIntervals, report)
            let results = Test.run log testsPerSecond (duration.Add(TimeSpan.FromSeconds 5.)) errorCutoff reportingIntervals clients runTest |> Async.RunSynchronously
            let resultFile = createResultLog report
            for r in results do
                resultFile.Information("Aggregate: {aggregate}", r)
            log.Information("Run completed; Current memory allocation: {bytes:n2} MiB", (GC.GetTotalMemory(true) |> float) / 1024./1024.)
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
            // TODO implement backoffs
            let log = createStoreLog verboseStore verboseConsole maybeSeq
            let host = sargs.GetResult(Host,"localhost")
            let creds = sargs.GetResult(Username,"admin"), sargs.GetResult(Password,"changeit")
            let (timeout, retries) as operationThrottling =
                sargs.GetResult(EsArguments.Timeout,5.) |> float |> TimeSpan.FromSeconds,
                sargs.GetResult(EsArguments.Retries,1)
            let heartbeatTimeout = sargs.GetResult(HeartbeatTimeout,1.5) |> float |> TimeSpan.FromSeconds
            let concurrentOperationsLimit = sargs.GetResult(ConcurrentOperationsLimit,5000)
            log.Information("Using EventStore targeting {host} with heartbeat: {heartbeat}, max concurrent requests: {concurrency}. " +
                "Operation timeout: {timeout} with {retries} retries",
                host, heartbeatTimeout, concurrentOperationsLimit, timeout, retries)
            let conn = EventStore.connect log (host, heartbeatTimeout, concurrentOperationsLimit) creds operationThrottling |> Async.RunSynchronously
            let store = Store.Es (EventStore.createGateway conn defaultBatchSize)
            match sargs.TryGetSubCommand() with
            | Some (EsArguments.Run targs) -> runTest log store targs
            | _ -> failwith "run is required"
        | Cosmos sargs ->
            let verboseStore = sargs.Contains(VerboseStore)
            let log = createStoreLog verboseStore verboseConsole maybeSeq
            let read key = Environment.GetEnvironmentVariable key |> Option.ofObj

            let (Discovery.UriAndKey (connUri,_)) as discovery =
                sargs.GetResult(Connection, defaultArg (read "EQUINOX_COSMOS_CONNECTION") "AccountEndpoint=https://localhost:8081;AccountKey=C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==;")
                |> Discovery.FromConnectionString

            let dbName = sargs.GetResult(Database, defaultArg (read "EQUINOX_COSMOS_DATABASE") "equinox-test")
            let collName = sargs.GetResult(Collection, defaultArg (read "EQUINOX_COSMOS_COLLECTION") "equinox-test")
            let timeout = sargs.GetResult(Timeout,5.) |> float |> TimeSpan.FromSeconds
            let (retries, maxRetryWaitTime) as operationThrottling = sargs.GetResult(Retries, 1), sargs.GetResult(RetriesWaitTime, 5)
            let pageSize = sargs.GetResult(PageSize,1)
            log.Information("Using CosmosDb Connection {connection} Database: {database} Collection: {collection} maxEventsPerSlice: {pageSize}. " +
                "Request timeout: {timeout} with {retries} retries; throttling MaxRetryWaitTime {maxRetryWaitTime}",
                connUri, dbName, collName, pageSize, timeout, retries, maxRetryWaitTime)
            let conn = Cosmos.connect log discovery timeout operationThrottling |> Async.RunSynchronously
            match sargs.TryGetSubCommand() with
            | Some (Provision args) ->
                let rus = args.GetResult(Rus)
                log.Information("Configuring CosmosDb Collection with Throughput Provision: {rus:n0} RU/s", rus)
                Equinox.Cosmos.Store.Sync.Initialization.initialize log conn.Client dbName collName rus |> Async.RunSynchronously
                0
            | Some (Run targs) ->
                let conn = Store.Cosmos (Cosmos.createGateway conn (defaultBatchSize,pageSize), dbName, collName)
                let res = runTest log conn targs
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
                let duration = targs.GetResult(DurationM,1.) |> TimeSpan.FromMinutes
                for uom, f in measures do let d = f duration in if d <> 0. then logPeriodicRate uom (float totalCount/d |> int64) (totalRc/d)
                res
            | _ -> failwith "init or run is required"
        | _ -> failwith "ERROR: please specify memory, es or cosmos Store"
    with e ->
        printfn "%s" e.Message
        1