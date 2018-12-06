module Samples.Infrastructure.Storage

open Argu
open Serilog
open System

type [<NoEquality; NoComparison>] MemArguments =
    | [<AltCommandLine("-vs")>] VerboseStore
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | VerboseStore -> "Include low level Store logging."
type [<NoEquality; NoComparison>] EsArguments =
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
            | VerboseStore -> "Include low level Store logging."
            | Timeout _ -> "specify operation timeout in seconds (default: 5)."
            | Retries _ -> "specify operation retries (default: 1)."
            | Host _ -> "specify a DNS query, using Gossip-driven discovery against all A records returned (default: localhost)."
            | Username _ -> "specify a username (default: admin)."
            | Password _ -> "specify a Password (default: changeit)."
            | ConcurrentOperationsLimit _ -> "max concurrent operations in flight (default: 5000)."
            | HeartbeatTimeout _ -> "specify heartbeat timeout in seconds (default: 1.5)."
type [<NoEquality; NoComparison>] CosmosArguments =
    | [<AltCommandLine("-vs")>] VerboseStore
    | [<AltCommandLine("-m")>] ConnectionMode of Equinox.Cosmos.ConnectionMode
    | [<AltCommandLine("-o")>] Timeout of float
    | [<AltCommandLine("-r")>] Retries of int
    | [<AltCommandLine("-s")>] Connection of string
    | [<AltCommandLine("-d")>] Database of string
    | [<AltCommandLine("-c")>] Collection of string
    | [<AltCommandLine("-rt")>] RetriesWaitTime of int
    | [<AltCommandLine("-a")>] PageSize of int
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | VerboseStore -> "Include low level Store logging."
            | ConnectionMode _ -> "Override the connection mode (default: DirectTcp)."
            | Timeout _ -> "specify operation timeout in seconds (default: 5)."
            | Retries _ -> "specify operation retries (default: 1)."
            | Connection _ -> "specify a connection string for a Cosmos account (defaults: envvar:EQUINOX_COSMOS_CONNECTION, Cosmos Emulator)."
            | Database _ -> "specify a database name for Cosmos account (defaults: envvar:EQUINOX_COSMOS_DATABASE, test)."
            | Collection _ -> "specify a collection name for Cosmos account (defaults: envvar:EQUINOX_COSMOS_COLLECTION, test)."
            | RetriesWaitTime _ -> "specify max wait-time for retry when being throttled by Cosmos in seconds (default: 5)"
            | PageSize _ -> "Specify maximum number of events to record on a page before switching to a new one (default: 1)"

let defaultBatchSize = 500

[<RequireQualifiedAccess; NoEquality; NoComparison>]
type StorageConfig =
    | Memory of Equinox.MemoryStore.VolatileStore
    | Es of Equinox.EventStore.GesGateway * Equinox.EventStore.CachingStrategy option * unfolds: bool
    | Cosmos of Equinox.Cosmos.EqxGateway * Equinox.Cosmos.CachingStrategy option * unfolds: bool * databaseId: string * collectionId: string

module MemoryStore =
    let config () =
        StorageConfig.Memory (Equinox.MemoryStore.VolatileStore())

module EventStore =
    open Equinox.EventStore

    /// To establish a local node to run the tests against:
    ///   1. cinst eventstore-oss -y # where cinst is an invocation of the Chocolatey Package Installer on Windows
    ///   2. & $env:ProgramData\chocolatey\bin\EventStore.ClusterNode.exe --gossip-on-single-node --discover-via-dns 0 --ext-http-port=30778
    let private connect (log: ILogger) (dnsQuery, heartbeatTimeout, col) (username, password) (operationTimeout, operationRetries) =
        GesConnector(username, password, reqTimeout=operationTimeout, reqRetries=operationRetries,
                heartbeatTimeout=heartbeatTimeout, concurrentOperationsLimit = col,
                log=(if log.IsEnabled(Serilog.Events.LogEventLevel.Debug) then Logger.SerilogVerbose log else Logger.SerilogNormal log),
                tags=["M", Environment.MachineName; "I", Guid.NewGuid() |> string])
            .Establish("equinox-tool", Discovery.GossipDns dnsQuery, ConnectionStrategy.ClusterTwinPreferSlaveReads)
    let private createGateway connection batchSize = GesGateway(connection, GesBatchingPolicy(maxBatchSize = batchSize))
    let config (log: ILogger, storeLog) (cache, unfolds) (sargs : ParseResults<EsArguments>) =
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
        let conn = connect storeLog (host, heartbeatTimeout, concurrentOperationsLimit) creds operationThrottling |> Async.RunSynchronously
        let cacheStrategy =
            if cache then
                let c = Caching.Cache("Cli", sizeMb = 50)
                CachingStrategy.SlidingWindow (c, TimeSpan.FromMinutes 20.) |> Some
            else None
        StorageConfig.Es ((createGateway conn defaultBatchSize), cacheStrategy, unfolds)

module Cosmos =
    open Equinox.Cosmos

    /// Standing up an Equinox instance is necessary to run for test purposes; You'll need to either:
    /// 1) replace connection below with a connection string or Uri+Key for an initialized Equinox instance with a database and collection named "equinox-test"
    /// 2) Set the 3x environment variables and create a local Equinox using cli/Equinox.cli/bin/Release/net461/Equinox.Cli `
    ///     cosmos -s $env:EQUINOX_COSMOS_CONNECTION -d $env:EQUINOX_COSMOS_DATABASE -c $env:EQUINOX_COSMOS_COLLECTION provision -ru 1000
    let private connect (log: ILogger) mode discovery operationTimeout (maxRetryForThrottling, maxRetryWaitTime) =
        EqxConnector(log=log, mode=mode, requestTimeout=operationTimeout, maxRetryAttemptsOnThrottledRequests=maxRetryForThrottling, maxRetryWaitTimeInSeconds=maxRetryWaitTime)
            .Connect("equinox-cli", discovery)
    let private createGateway connection (maxItems,maxEvents) = EqxGateway(connection, EqxBatchingPolicy(defaultMaxItems=maxItems, maxEventsPerSlice=maxEvents))
    let conn (log: ILogger, storeLog) (sargs : ParseResults<CosmosArguments>) =
        let read key = Environment.GetEnvironmentVariable key |> Option.ofObj

        let (Discovery.UriAndKey (connUri,_)) as discovery =
            sargs.GetResult(Connection, defaultArg (read "EQUINOX_COSMOS_CONNECTION") "AccountEndpoint=https://localhost:8081;AccountKey=C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==;")
            |> Discovery.FromConnectionString

        let dbName = sargs.GetResult(Database, defaultArg (read "EQUINOX_COSMOS_DATABASE") "equinox-test")
        let collName = sargs.GetResult(Collection, defaultArg (read "EQUINOX_COSMOS_COLLECTION") "equinox-test")
        let timeout = sargs.GetResult(Timeout,5.) |> float |> TimeSpan.FromSeconds
        let mode = sargs.GetResult(ConnectionMode,ConnectionMode.DirectTcp)
        let (retries, maxRetryWaitTime) as operationThrottling = sargs.GetResult(Retries, 1), sargs.GetResult(RetriesWaitTime, 5)
        let pageSize = sargs.GetResult(PageSize,1)
        log.Information("Using CosmosDb {mode} Connection {connection} Database: {database} Collection: {collection} maxEventsPerSlice: {pageSize}. " +
            "Request timeout: {timeout} with {retries} retries; throttling MaxRetryWaitTime {maxRetryWaitTime}",
            mode, connUri, dbName, collName, pageSize, timeout, retries, maxRetryWaitTime)
        dbName, collName, pageSize, connect storeLog mode discovery timeout operationThrottling |> Async.RunSynchronously
    let config (log: ILogger, storeLog) (cache, unfolds) (sargs : ParseResults<CosmosArguments>) =
        let dbName, collName, pageSize, conn = conn (log, storeLog) sargs
        let cacheStrategy =
            if cache then
                let c = Caching.Cache("Cli", sizeMb = 50)
                CachingStrategy.SlidingWindow (c, TimeSpan.FromMinutes 20.) |> Some
            else None
        StorageConfig.Cosmos (createGateway conn (defaultBatchSize,pageSize), cacheStrategy, unfolds, dbName, collName)