module Samples.Infrastructure.Storage

open Argu
open System

exception MissingArg of string

[<RequireQualifiedAccess; NoEquality; NoComparison>]
type StorageConfig =
    | Memory of Equinox.MemoryStore.VolatileStore
    | Es of Equinox.EventStore.GesGateway * Equinox.EventStore.CachingStrategy option * unfolds: bool
    | Cosmos of Equinox.Cosmos.CosmosGateway * Equinox.Cosmos.CachingStrategy * unfolds: bool * databaseId: string * collectionId: string
    
module MemoryStore =
    type [<NoEquality; NoComparison>] Arguments =
        | [<AltCommandLine("-vs")>] VerboseStore
        interface IArgParserTemplate with
            member a.Usage = a |> function
                | VerboseStore ->       "Include low level Store logging."
    let config () =
        StorageConfig.Memory (Equinox.MemoryStore.VolatileStore())

module Cosmos =
    let envBackstop msg key =
        match Environment.GetEnvironmentVariable key with
        | null -> raise <| MissingArg (sprintf "Please provide a %s, either as an argment or via the %s environment variable" msg key)
        | x -> x 

    type [<NoEquality; NoComparison>] Arguments =
        | [<AltCommandLine("-vs")>] VerboseStore
        | [<AltCommandLine("-m")>] ConnectionMode of Equinox.Cosmos.ConnectionMode
        | [<AltCommandLine("-o")>] Timeout of float
        | [<AltCommandLine("-r")>] Retries of int
        | [<AltCommandLine("-rt")>] RetriesWaitTime of int
        | [<AltCommandLine("-s")>] Connection of string
        | [<AltCommandLine("-d")>] Database of string
        | [<AltCommandLine("-c")>] Collection of string
        interface IArgParserTemplate with
            member a.Usage =
                match a with
                | VerboseStore ->       "Include low level Store logging."
                | Timeout _ ->          "specify operation timeout in seconds (default: 5)."
                | Retries _ ->          "specify operation retries (default: 1)."
                | RetriesWaitTime _ ->  "specify max wait-time for retry when being throttled by Cosmos in seconds (default: 5)"
                | Connection _ ->       "specify a connection string for a Cosmos account (defaults: envvar:EQUINOX_COSMOS_CONNECTION, Cosmos Emulator)."
                | ConnectionMode _ ->   "override the connection mode (default: DirectTcp)."
                | Database _ ->         "specify a database name for Cosmos account (defaults: envvar:EQUINOX_COSMOS_DATABASE, test)."
                | Collection _ ->       "specify a collection name for Cosmos account (defaults: envvar:EQUINOX_COSMOS_COLLECTION, test)."
    type Info(args : ParseResults<Arguments>) =
        member __.Connection =  match args.TryGetResult Connection  with Some x -> x | None -> envBackstop "Connection" "EQUINOX_COSMOS_CONNECTION"
        member __.Database =    match args.TryGetResult Database    with Some x -> x | None -> envBackstop "Database"   "EQUINOX_COSMOS_DATABASE"
        member __.Collection =  match args.TryGetResult Collection  with Some x -> x | None -> envBackstop "Collection" "EQUINOX_COSMOS_COLLECTION"

        member __.Timeout = args.GetResult(Timeout,5.) |> TimeSpan.FromSeconds
        member __.Mode = args.GetResult(ConnectionMode,Equinox.Cosmos.ConnectionMode.DirectTcp)
        member __.Retries = args.GetResult(Retries,1)
        member __.MaxRetryWaitTime = args.GetResult(RetriesWaitTime, 5)

    /// Standing up an Equinox instance is necessary to run for test purposes; You'll need to either:
    /// 1) replace connection below with a connection string or Uri+Key for an initialized Equinox instance with a database and collection named "equinox-test"
    /// 2) Set the 3x environment variables and create a local Equinox using tools/Equinox.Tool/bin/Release/net461/eqx.exe `
    ///     init -ru 1000 cosmos -s $env:EQUINOX_COSMOS_CONNECTION -d $env:EQUINOX_COSMOS_DATABASE -c $env:EQUINOX_COSMOS_COLLECTION
    open Equinox.Cosmos
    open Serilog

    let private createGateway connection maxItems = CosmosGateway(connection, CosmosBatchingPolicy(defaultMaxItems=maxItems))
    let private ctx (log: ILogger, storeLog: ILogger) (a : Info) =
        let (Discovery.UriAndKey (endpointUri,_)) as discovery = a.Connection|> Discovery.FromConnectionString
        log.Information("CosmosDb {mode} {connection} Database {database} Collection {collection}",
            a.Mode, endpointUri, a.Database, a.Collection)
        log.Information("CosmosDb timeout: {timeout}s, {retries} retries; Throttling maxRetryWaitTime {maxRetryWaitTime}",
            (let t = a.Timeout in t.TotalSeconds), a.Retries, a.MaxRetryWaitTime)
        let c = CosmosConnector(log=storeLog, mode=a.Mode, requestTimeout=a.Timeout, maxRetryAttemptsOnThrottledRequests=a.Retries, maxRetryWaitTimeInSeconds=a.MaxRetryWaitTime)
        discovery, a.Database, a.Collection, c
    let connect (log : ILogger, storeLog) info =
        let discovery, dbName, collName, connector = ctx (log,storeLog) info
        dbName, collName, connector.Connect("equinox-tool", discovery) |> Async.RunSynchronously
    let connectionPolicy (log, storeLog) info =
        let (Discovery.UriAndKey (endpointUri, masterKey)), dbName, collName, connector = ctx (log, storeLog) info
        (endpointUri, masterKey), dbName, collName, connector.ConnectionPolicy
    let config (log: ILogger, storeLog) (cache, unfolds, batchSize) info =
        let dbName, collName, conn = connect (log, storeLog) info
        let cacheStrategy =
            if cache then
                let c = Caching.Cache("equinox-tool", sizeMb = 50)
                CachingStrategy.SlidingWindow (c, TimeSpan.FromMinutes 20.)
            else CachingStrategy.NoCaching
        StorageConfig.Cosmos (createGateway conn batchSize, cacheStrategy, unfolds, dbName, collName)

    open Serilog.Events
    open Equinox.Cosmos.Store

    let inline (|Stats|) ({ interval = i; ru = ru }: Log.Measurement) = ru, let e = i.Elapsed in int64 e.TotalMilliseconds

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
    let (|SerilogScalar|_|) : LogEventPropertyValue -> obj option = function
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
            System.Threading.Interlocked.Increment(&__.count) |> ignore
            System.Threading.Interlocked.Add(&__.rux100, int64 (ru*100.)) |> ignore
            System.Threading.Interlocked.Add(&__.ms, ms) |> ignore
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

    let dumpStats duration (log: Serilog.ILogger) =
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
        for uom, f in measures do let d = f duration in if d <> 0. then logPeriodicRate uom (float totalCount/d |> int64) (totalRc/d)

/// To establish a local node to run the tests against:
///   1. cinst eventstore-oss -y # where cinst is an invocation of the Chocolatey Package Installer on Windows
///   2. & $env:ProgramData\chocolatey\bin\EventStore.ClusterNode.exe --gossip-on-single-node --discover-via-dns 0 --ext-http-port=30778
module EventStore =
    type [<NoEquality; NoComparison>] Arguments =
        | [<AltCommandLine("-vs")>] VerboseStore
        | [<AltCommandLine("-o")>] Timeout of float
        | [<AltCommandLine("-r")>] Retries of int
        | [<AltCommandLine("-g")>] Host of string
        | [<AltCommandLine("-u")>] Username of string
        | [<AltCommandLine("-p")>] Password of string
        | [<AltCommandLine("-c")>] ConcurrentOperationsLimit of int
        | [<AltCommandLine("-h")>] HeartbeatTimeout of float
        interface IArgParserTemplate with
            member a.Usage = a |> function
                | VerboseStore ->       "include low level Store logging."
                | Timeout _ ->          "specify operation timeout in seconds (default: 5)."
                | Retries _ ->          "specify operation retries (default: 1)."
                | Host _ ->             "specify a DNS query, using Gossip-driven discovery against all A records returned (default: localhost)."
                | Username _ ->         "specify a username (default: admin)."
                | Password _ ->         "specify a Password (default: changeit)."
                | ConcurrentOperationsLimit _ -> "max concurrent operations in flight (default: 5000)."
                | HeartbeatTimeout _ -> "specify heartbeat timeout in seconds (default: 1.5)."

    open Equinox.EventStore

    type Info(args : ParseResults<Arguments>) =
        member __.Host = args.GetResult(Host,"localhost")
        member __.Credentials = args.GetResult(Username,"admin"), args.GetResult(Password,"changeit")

        member __.Timeout = args.GetResult(Timeout,5.) |> TimeSpan.FromSeconds
        member __.Retries = args.GetResult(Retries, 1)
        member __.HeartbeatTimeout = args.GetResult(HeartbeatTimeout,1.5) |> float |> TimeSpan.FromSeconds
        member __.ConcurrentOperationsLimit = args.GetResult(ConcurrentOperationsLimit,5000)

    open Serilog

    let private connect (log: ILogger) (dnsQuery, heartbeatTimeout, col) (username, password) (operationTimeout, operationRetries) =
        GesConnector(username, password, reqTimeout=operationTimeout, reqRetries=operationRetries,
                heartbeatTimeout=heartbeatTimeout, concurrentOperationsLimit = col,
                log=(if log.IsEnabled(Serilog.Events.LogEventLevel.Debug) then Logger.SerilogVerbose log else Logger.SerilogNormal log),
                tags=["M", Environment.MachineName; "I", Guid.NewGuid() |> string])
            .Establish("equinox-tool", Discovery.GossipDns dnsQuery, ConnectionStrategy.ClusterTwinPreferSlaveReads)
    let private createGateway connection batchSize = GesGateway(connection, GesBatchingPolicy(maxBatchSize = batchSize))
    let config (log: ILogger, storeLog) (cache, unfolds, batchSize) (args : ParseResults<Arguments>) =
        let a = Info(args)
        let (timeout, retries) as operationThrottling = a.Timeout, a.Retries
        let heartbeatTimeout = a.HeartbeatTimeout
        let concurrentOperationsLimit = a.ConcurrentOperationsLimit
        log.Information("EventStore {host} heartbeat: {heartbeat}s timeout: {timeout}s concurrent reqs: {concurrency} retries {retries}",
            a.Host, heartbeatTimeout.TotalSeconds, timeout.TotalSeconds, concurrentOperationsLimit, retries)
        let conn = connect storeLog (a.Host, heartbeatTimeout, concurrentOperationsLimit) a.Credentials operationThrottling |> Async.RunSynchronously
        let cacheStrategy =
            if cache then
                let c = Caching.Cache("equinox-tool", sizeMb = 50)
                CachingStrategy.SlidingWindow (c, TimeSpan.FromMinutes 20.) |> Some
            else None
        StorageConfig.Es ((createGateway conn batchSize), cacheStrategy, unfolds)