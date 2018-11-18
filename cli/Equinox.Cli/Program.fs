module Equinox.Cli.Program

open Argu
open Domain
open Equinox.Cosmos
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
    | Name of Test
    | Cached
    | Indexed
    | [<AltCommandLine("-f")>] TestsPerSecond of int
    | [<AltCommandLine("-d")>] DurationM of float
    | [<AltCommandLine("-e")>] ErrorCutoff of int64
    | [<AltCommandLine("-i")>] ReportIntervalS of int
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | Name _ -> "Specify which test to run. (default: Favorites)"
            | Cached -> "Whether to employ a Cache"
            | Indexed -> "Whether to employ an Index for Cosmos"
            | TestsPerSecond _ -> "specify a target number of requests per second (default: 1000)."
            | DurationM _ -> "specify a run duration in minutes (default: 1)."
            | ErrorCutoff _ -> "specify an error cutoff (default: 10000)."
            | ReportIntervalS _ -> "specify reporting intervals in seconds (default: 10)."
and Test = Favorites
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
    let createGateway connection (batchSize,pageSize) = EqxGateway(connection, EqxBatchingPolicy(getMaxBatchSize = (fun () -> batchSize), maxEventsPerSlice = pageSize))

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
    let fold, initial, compact, index = Domain.Favorites.Folds.fold, Domain.Favorites.Folds.initial, Domain.Favorites.Folds.compact, Domain.Favorites.Folds.index
    let serializationSettings = Newtonsoft.Json.Converters.FSharp.Settings.CreateCorrect()
    let genCodec<'Union when 'Union :> TypeShape.UnionContract.IUnionContract>() = Equinox.UnionCodec.JsonUtf8.Create<'Union>(serializationSettings)
    let codec = genCodec<Domain.Favorites.Events.Event>()
    let createFavoritesService store (targs: ParseResults<TestArguments>) log =
        let esCache =
            if targs.Contains Cached then
                let c = Caching.Cache("Cli", sizeMb = 50)
                CachingStrategy.SlidingWindow (c, TimeSpan.FromMinutes 20.) |> Some
            else None
        let eqxCache =
            if targs.Contains Cached then
                let c = Equinox.Cosmos.Caching.Cache("Cli", sizeMb = 50)
                Equinox.Cosmos.CachingStrategy.SlidingWindow (c, TimeSpan.FromMinutes 20.) |> Some
            else None
        let resolveStream streamName =
            match store with
            | Store.Mem store ->
                Equinox.MemoryStore.MemoryStreamBuilder(store, fold, initial).Create(streamName)
            | Store.Es gateway ->
                GesStreamBuilder(gateway, codec, fold, initial, Equinox.EventStore.AccessStrategy.RollingSnapshots compact, ?caching = esCache).Create(streamName)
            | Store.Cosmos (gateway, databaseId, connectionId) ->
                if targs.Contains Indexed then
                    EqxStreamBuilder(gateway, codec, fold, initial, Equinox.Cosmos.AccessStrategy.IndexedSearch index, ?caching = cache)
                        .Create(databaseId, connectionId, streamName)
                else
                    EqxStreamBuilder(gateway, codec, fold, initial, Equinox.Cosmos.AccessStrategy.RollingSnapshots compact, ?caching = cache)
                        .Create(databaseId, connectionId, streamName)
        Backend.Favorites.Service(log, resolveStream)
    let runFavoriteTest (service : Backend.Favorites.Service) clientId = async {
        let sku = Guid.NewGuid() |> SkuId
        do! service.Execute clientId (Favorites.Command.Favorite(DateTimeOffset.Now, [sku]))
        let! items = service.Read clientId
        if items |> Array.exists (fun x -> x.skuId = sku) |> not then invalidOp "Added item not found" }

[<AutoOpen>]
module SerilogHelpers =
    let inline (|Stats|) ({ interval = i; ru = ru }: Equinox.Cosmos.Log.Measurement) = ru, let e = i.Elapsed in int64 e.TotalMilliseconds
    let (|CosmosReadRu|CosmosWriteRu|CosmosResyncRu|CosmosSliceRu|) (evt : Equinox.Cosmos.Log.Event) =
        match evt with
        | Equinox.Cosmos.Log.Index (Stats s)
        | Equinox.Cosmos.Log.IndexNotFound (Stats s)
        | Equinox.Cosmos.Log.IndexNotModified (Stats s)
        | Equinox.Cosmos.Log.Batch (_,_, (Stats s)) -> CosmosReadRu s
        | Equinox.Cosmos.Log.WriteSuccess (Stats s)
        | Equinox.Cosmos.Log.WriteConflict (Stats s) -> CosmosWriteRu s
        | Equinox.Cosmos.Log.WriteResync (Stats s) -> CosmosResyncRu s
        // slices are rolled up into batches so be sure not to double-count
        | Equinox.Cosmos.Log.Slice (_,{ ru = ru }) -> CosmosSliceRu ru
    let (|SerilogScalar|_|) : Serilog.Events.LogEventPropertyValue -> obj option = function
        | (:? ScalarValue as x) -> Some x.Value
        | _ -> None
    let (|CosmosMetric|_|) (logEvent : LogEvent) : Equinox.Cosmos.Log.Event option =
        match logEvent.Properties.TryGetValue("cosmosEvt") with
        | true, SerilogScalar (:? Equinox.Cosmos.Log.Event as e) -> Some e
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
                | CosmosMetric (CosmosReadRu stats) -> RuCounterSink.Read.Ingest stats
                | CosmosMetric (CosmosWriteRu stats) -> RuCounterSink.Write.Ingest stats
                | CosmosMetric (CosmosResyncRu stats) -> RuCounterSink.Resync.Ingest stats
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
        let report = args.GetResult(LogFile,programName+".log") |> fun n -> FileInfo(n).FullName
        let runTest (log: ILogger) conn (targs: ParseResults<TestArguments>) =
            let verbose = args.Contains(VerboseDomain)
            let domainLog = createDomainLog verbose verboseConsole maybeSeq
            let service = Test.createFavoritesService conn targs domainLog

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
            log.Information( "Running {test} with caching: {cached}, indexing: {indexed}. "+
                "Duration for {duration} with test freq {tps} hits/s; max errors: {errorCutOff}, reporting intervals: {ri}, report file: {report}",
                test, targs.Contains Cached, targs.Contains Indexed, duration, testsPerSecond, errorCutoff, reportingIntervals, report)
            let runSingleTest clientId =
                if not verbose then Test.runFavoriteTest service clientId
                else async {
                    domainLog.Information("Executing for client {sessionId}", ([|clientId|] : obj []))
                    try return! Test.runFavoriteTest service clientId
                    with e -> domainLog.Warning(e, "Test threw an exception"); e.Reraise () }
            let results = Test.run log testsPerSecond (duration.Add(TimeSpan.FromSeconds 5.)) errorCutoff reportingIntervals clients runSingleTest |> Async.RunSynchronously
            let resultFile = createResultLog report
            for r in results do
                resultFile.Information("Aggregate: {aggregate}", r)
            log.Information("Run completed; Current memory allocation: {bytes:n2}MB", (GC.GetTotalMemory(true) |> float) / 1024./1024.)
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
            log.Information("Using CosmosDb Connection {connection} Database: {database} Collection: {collection} with page size: {pageSize}. " +
                "Request timeout: {timeout} with {retries} retries; throttling MaxRetryWaitTime {maxRetryWaitTime}",
                connUri, dbName, collName, pageSize, timeout, retries, maxRetryWaitTime)
            let conn = Cosmos.connect log discovery timeout operationThrottling |> Async.RunSynchronously
            match sargs.TryGetSubCommand() with
            | Some (Provision args) ->
                let rus = args.GetResult(Rus)
                log.Information("Configuring CosmosDb with Request Units (RU) Provision: {rus:n0}", rus)
                Equinox.Cosmos.Initialization.initialize log conn.Client dbName collName rus |> Async.RunSynchronously
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