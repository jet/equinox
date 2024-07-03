module Samples.Infrastructure.Store

open Argu
open Equinox
open Serilog
open System

[<RequireQualifiedAccess; NoEquality; NoComparison>]
type Config =
    // For MemoryStore, we keep the events as UTF8 arrays - we could use FsCodec.Codec.Box to remove the JSON encoding, which would improve perf but can conceal problems
    | Memory of MemoryStore.VolatileStore<ReadOnlyMemory<byte>>
    | Cosmos of CosmosStore.CosmosStoreContext * CachingStrategy * unfolds: bool
    | Dynamo of DynamoStore.DynamoStoreContext * CachingStrategy * unfolds: bool
    | Es     of EventStoreDb.EventStoreContext * CachingStrategy * unfolds: bool
    | Mdb    of MessageDb.MessageDbContext * CachingStrategy
    | Sql    of SqlStreamStore.SqlStreamStoreContext * CachingStrategy * unfolds: bool

module MemoryStore =
    type [<NoEquality; NoComparison>] Parameters =
        | [<AltCommandLine "-V">]        StoreVerbose
        interface IArgParserTemplate with
            member a.Usage = a |> function
                | StoreVerbose ->       "Include low level Store logging."
    let config () =
        Config.Memory (Equinox.MemoryStore.VolatileStore())

let [<Literal>] appName = "equinox-tool"

let private envVarTryGet varName = Environment.GetEnvironmentVariable varName |> Option.ofObj
let private envVarOrThrow varName argName =
    match envVarTryGet varName with
    | None -> failwith $"Please provide a %s{argName}, either as an argument or via the %s{varName} environment variable"
    | Some x -> x

// Standing up an Equinox instance is necessary to run for test purposes; You'll need to either:
// 1) replace connection below with a connection string or Uri+Key for an initialized Equinox instance with a database and collection named "equinox-test"
// 2) Set the 3x environment variables and create a local Equinox using tools/Equinox.Tool/bin/Release/net6.0/eqx.exe `
//     init -ru 1000 cosmos -s $env:EQUINOX_COSMOS_CONNECTION -d $env:EQUINOX_COSMOS_DATABASE -c $env:EQUINOX_COSMOS_CONTAINER
// see https://github.com/jet/equinox#provisioning-cosmosdb
module Cosmos =

    open Equinox.CosmosStore

    type [<NoEquality; NoComparison>] Parameters =
        | [<AltCommandLine "-V">]       StoreVerbose
        | [<AltCommandLine "-m">]       ConnectionMode of Microsoft.Azure.Cosmos.ConnectionMode
        | [<AltCommandLine "-o">]       Timeout of float
        | [<AltCommandLine "-r">]       Retries of int
        | [<AltCommandLine "-rt">]      RetriesWaitTimeS of float
        | [<AltCommandLine "-s">]       Connection of string
        | [<AltCommandLine "-d">]       Database of string
        | [<AltCommandLine "-c">]       Container of string
        | [<AltCommandLine "-s2">]      ArchiveConnection of string
        | [<AltCommandLine "-d2">]      ArchiveDatabase of string
        | [<AltCommandLine "-c2">]      ArchiveContainer of string
        | [<AltCommandLine "-te">]      TipMaxEvents of int
        | [<AltCommandLine "-tl">]      TipMaxJsonLength of int
        | [<AltCommandLine "-b">]       QueryMaxItems of int
        interface IArgParserTemplate with
            member a.Usage = a |> function
                | StoreVerbose ->       "Include low level Store logging."
                | Timeout _ ->          "specify operation timeout in seconds (default: 5)."
                | Retries _ ->          "specify operation retries (default: 1)."
                | RetriesWaitTimeS _ -> "specify max wait-time for retry when being throttled by Cosmos in seconds (default: 5)"
                | ConnectionMode _ ->   "override the connection mode. Default: Direct."
                | Connection _ ->       "specify a connection string for a Cosmos account. (optional if environment variable EQUINOX_COSMOS_CONNECTION specified)"
                | Database _ ->         "specify a database name for store. (optional if environment variable EQUINOX_COSMOS_DATABASE specified)"
                | Container _ ->        "specify a container name for store. (optional if environment variable EQUINOX_COSMOS_CONTAINER specified)"
                | ArchiveConnection _ ->"specify a connection string for Archive Cosmos account. Default: use same as Primary Connection"
                | ArchiveDatabase _ ->  "specify a database name for Archive store. Default: use same as Primary Database"
                | ArchiveContainer _ -> "specify a container name for Archive store. Default: use same as Primary Container"
                | TipMaxEvents _ ->     "specify maximum number of events to hold in Tip before calving off to a frozen Batch. Default: 256"
                | TipMaxJsonLength _ -> "specify maximum length of JSON (as measured by JSON.stringify) to hold in Tip before calving off to a frozen Batch. Default: 30,000"
                | QueryMaxItems _ ->    "specify maximum number of batches of events to retrieve in per query response. Default: 10/9999"
    type Arguments(p : ParseResults<Parameters>) =
        member val Verbose =            p.Contains StoreVerbose
        member val Mode =               p.TryGetResult ConnectionMode
        member val Connection =         p.GetResult(Connection, fun () -> envVarOrThrow "EQUINOX_COSMOS_CONNECTION" "Connection")
        member val Database =           p.GetResult(Database, fun () -> envVarOrThrow "EQUINOX_COSMOS_DATABASE"   "Database")
        member val Container =          p.GetResult(Container, fun () -> envVarOrThrow "EQUINOX_COSMOS_CONTAINER"  "Container")
        member val private ArchiveConnection = p.TryGetResult ArchiveConnection
        member private x.ArchiveDatabase = p.GetResult(ArchiveDatabase, fun () -> x.Database)
        member private x.ArchiveContainer = p.GetResult(ArchiveContainer, fun () -> x.Container)
        member x.Archive =              if p.Contains ArchiveConnection || p.Contains ArchiveDatabase || p.Contains ArchiveContainer
                                        then Some (x.ArchiveConnection, x.ArchiveDatabase, x.ArchiveContainer)
                                        else None

        member _.Timeout =              p.GetResult(Timeout, 5.) |> TimeSpan.FromSeconds
        member _.Retries =              p.GetResult(Retries, 1)
        member _.MaxRetryWaitTime =     p.GetResult(RetriesWaitTimeS, 5.) |> TimeSpan.FromSeconds
        member _.TipMaxEvents =         p.GetResult(TipMaxEvents, 256)
        member _.TipMaxJsonLength =     p.GetResult(TipMaxJsonLength, 30_000)
        member _.QueryMaxItemsOr(def: int) = p.GetResult(QueryMaxItems, def)
        member x.QueryMaxItems =        x.QueryMaxItemsOr 10

    let logContainer (log: ILogger) role (mode, endpoint, db, container) =
        log.Information("CosmosDB {role:l} {mode} {connection} Database {database} Container {container}",
                        role, defaultArg mode Microsoft.Azure.Cosmos.ConnectionMode.Direct, endpoint, db, container)

    // NOTE: this is a big song and dance, don't blindly copy!
    // - In normal usage, you typically connect to a single container only.
    // - In hot-warm scenarios, the Archive Container will frequently be within the same account and hence can share a CosmosClient
    // For these typical purposes, CosmosStoreClient.Connect should be used to establish the Client, not custom wiring as we have here
    let createConnector (a: Arguments) connectionString =
        CosmosStoreConnector(Discovery.ConnectionString connectionString, a.Timeout, a.Retries, a.MaxRetryWaitTime, ?mode = a.Mode)
    let connect (log: ILogger) (a: Arguments) =
        let primaryConnector, primaryDatabase, primaryContainer as primary = createConnector a a.Connection, a.Database, a.Container
        logContainer log "Primary" (a.Mode, primaryConnector.Endpoint, primaryDatabase, primaryContainer)
        let archive =
            match a.Archive with
            | Some (Some c2, db, container) -> Some (createConnector a c2, db, container)
            | Some (None, db, container) -> Some (primaryConnector, db, container)
            | None -> None
        archive |> Option.iter (fun (client, db, container) -> logContainer log "Archive" (a.Mode, client.Endpoint, db, container))
        primary, archive
    let config (log: ILogger) (cache, unfolds) (a: Arguments) =
        let context =
            match connect log a with
            | (connector, databaseId, containerId), None ->
                let client = connector.Connect(databaseId, [| containerId |]) |> Async.RunSynchronously
                CosmosStoreContext(client, databaseId, containerId, a.TipMaxEvents, tipMaxJsonLength = a.TipMaxJsonLength, queryMaxItems = a.QueryMaxItems 10)
            | (connector, databaseId, containerId), Some (aConnector, aDatabaseId, aContainerId) ->
                let cosmosClient = connector.CreateAndInitialize(databaseId, [| containerId |]) |> Async.RunSynchronously
                let archiveCosmosClient = aConnector.CreateAndInitialize(aDatabaseId, [| aContainerId |]) |> Async.RunSynchronously
                let client = CosmosStoreClient(cosmosClient, archiveCosmosClient)
                CosmosStoreContext(client, databaseId, containerId, a.TipMaxEvents, tipMaxJsonLength = a.TipMaxJsonLength, queryMaxItems = a.QueryMaxItems 10,
                                   archiveDatabaseId = aDatabaseId, archiveContainerId = aContainerId)
        log.Information("CosmosStore Tip thresholds: {maxTipJsonLength}b {maxTipEvents}e Query paging {queryMaxItems} items",
                        a.TipMaxJsonLength, a.TipMaxEvents, a.QueryMaxItems)
        let cacheStrategy = match cache with Some c -> CachingStrategy.SlidingWindow (c, TimeSpan.FromMinutes 20.) | None -> CachingStrategy.NoCaching
        Config.Cosmos (context, cacheStrategy, unfolds)

module Dynamo =

    type Equinox.DynamoStore.DynamoStoreConnector with

        member x.LogConfiguration(log: ILogger) =
            log.Information("DynamoDB {endpoint} Timeout {timeoutS}s Retries {retries}",
                            x.Endpoint, (let t = x.Timeout in t.TotalSeconds), x.Retries)

    type Equinox.DynamoStore.DynamoStoreContext with

        member internal x.LogConfiguration(log: ILogger, role, tableName: string, ?archiveTableName: string) =
            log.Information("DynamoStore {role:l} Table {table} Archive {archive}", role, tableName, Option.toObj archiveTableName)
            log.Information("DynamoStore Tip thresholds: {maxTipBytes}b {maxTipEvents}e Query paging {queryMaxItems} items",
                            x.TipOptions.MaxBytes, Option.toNullable x.TipOptions.MaxEvents, x.QueryOptions.MaxItems)

    open Equinox.DynamoStore
    let [<Literal>] REGION =            "EQUINOX_DYNAMO_REGION"
    let [<Literal>] SERVICE_URL =       "EQUINOX_DYNAMO_SERVICE_URL"
    let [<Literal>] ACCESS_KEY =        "EQUINOX_DYNAMO_ACCESS_KEY_ID"
    let [<Literal>] SECRET_KEY =        "EQUINOX_DYNAMO_SECRET_ACCESS_KEY"
    let [<Literal>] TABLE =             "EQUINOX_DYNAMO_TABLE"
    let [<Literal>] ARCHIVE_TABLE =     "EQUINOX_DYNAMO_TABLE_ARCHIVE"
    type [<NoEquality; NoComparison>] Parameters =
        | [<AltCommandLine "-V">]       StoreVerbose
        | [<AltCommandLine "-sr">]      RegionProfile of string
        | [<AltCommandLine "-su">]      ServiceUrl of string
        | [<AltCommandLine "-sa">]      AccessKey of string
        | [<AltCommandLine "-ss">]      SecretKey of string
        | [<AltCommandLine "-t">]       Table of string
        | [<AltCommandLine "-ta">]      ArchiveTable of string
        | [<AltCommandLine "-r">]       Retries of int
        | [<AltCommandLine "-rt">]      RetriesTimeoutS of float
        | [<AltCommandLine "-tb">]      TipMaxBytes of int
        | [<AltCommandLine "-te">]      TipMaxEvents of int
        | [<AltCommandLine "-b">]       QueryMaxItems of int
        interface IArgParserTemplate with
            member a.Usage = a |> function
                | StoreVerbose ->       "Include low level Store logging."
                | RegionProfile _ ->    "specify an AWS Region (aka System Name, e.g. \"us-east-1\") to connect to using the implicit AWS SDK/tooling config and/or environment variables etc. Optional if:\n" +
                                        "1) $" + REGION + " specified OR\n" +
                                        "2) Explicit `ServiceUrl`/$" + SERVICE_URL + "+`AccessKey`/$" + ACCESS_KEY + "+`Secret Key`/$" + SECRET_KEY + " specified.\n" +
                                        "See https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html for details"
                | ServiceUrl _ ->       "specify a server endpoint for a Dynamo account. (Not applicable if `ServiceRegion`/$" + REGION + " specified; Optional if $" + SERVICE_URL + " specified)"
                | AccessKey _ ->        "specify an access key id for a Dynamo account. (Not applicable if `ServiceRegion`/$" + REGION + " specified; Optional if $" + ACCESS_KEY + " specified)"
                | SecretKey _ ->        "specify a secret access key for a Dynamo account. (Not applicable if `ServiceRegion`/$" + REGION + " specified; Optional if $" + SECRET_KEY + " specified)"
                | Table _ ->            "specify a table name for the primary store. (optional if $" + TABLE + " specified)"
                | ArchiveTable _ ->     "specify a table name for the Archive; Optional if $" + ARCHIVE_TABLE + " specified.\n" +
                                        "Default: Do not attempt to look in an Archive store as a Fallback to locate pruned events."
                | Retries _ ->          "specify operation retries (default: 1)."
                | RetriesTimeoutS _ ->  "specify max wait-time including retries in seconds (default: 5)"
                | TipMaxBytes _ ->      "specify maximum number of bytes to hold in Tip before calving off to a frozen Batch. Default: 32K"
                | TipMaxEvents _ ->     "specify maximum number of events to hold in Tip before calving off to a frozen Batch. Default: limited by Max Bytes"
                | QueryMaxItems _ ->    "specify maximum number of batches of events to retrieve in per query response. Default: 10"
    type Arguments(p : ParseResults<Parameters>) =
        let conn =
                                        match p.TryGetResult RegionProfile |> Option.orElseWith (fun () -> envVarTryGet REGION) with
                                        | Some systemName ->
                                            Choice1Of2 systemName
                                        | None ->
                                            let serviceUrl =  p.GetResult(ServiceUrl, fun () -> envVarOrThrow SERVICE_URL "ServiceUrl")
                                            let accessKey =   p.GetResult(AccessKey,  fun () -> envVarOrThrow ACCESS_KEY  "AccessKey")
                                            let secretKey =   p.GetResult(SecretKey,  fun () -> envVarOrThrow SECRET_KEY  "SecretKey")
                                            Choice2Of2 (serviceUrl, accessKey, secretKey)
        let retries =                   p.GetResult(Retries, 1)
        let timeout =                   p.GetResult(RetriesTimeoutS, 5.) |> TimeSpan.FromSeconds
        member val Connector =          match conn with
                                        | Choice1Of2 systemName -> DynamoStoreConnector(systemName, timeout, retries)
                                        | Choice2Of2 (serviceUrl, accessKey, secretKey) -> DynamoStoreConnector(serviceUrl, accessKey, secretKey, timeout, retries)
        member val Table =              p.GetResult(Table, fun () -> envVarOrThrow TABLE "Table")
        member val ArchiveTable =       p.TryGetResult ArchiveTable |> Option.orElseWith (fun () -> envVarTryGet ARCHIVE_TABLE)

        member val TipMaxEvents =       p.TryGetResult TipMaxEvents
        member val TipMaxBytes =        p.GetResult(TipMaxBytes, 32 * 1024)
        member val QueryMaxItems =      p.GetResult(QueryMaxItems, 10)

    let config (log : ILogger) (cache, unfolds) (a : Arguments) =
        a.Connector.LogConfiguration(log)
        let client = a.Connector.CreateDynamoStoreClient()
        let context = DynamoStoreContext(client, a.Table, maxBytes = a.TipMaxBytes, queryMaxItems = a.QueryMaxItems,
                                         ?tipMaxEvents = a.TipMaxEvents, ?archiveTableName = a.ArchiveTable)
        context.LogConfiguration(log, "Main", a.Table, ?archiveTableName = a.ArchiveTable)
        let cacheStrategy = match cache with Some c -> CachingStrategy.SlidingWindow (c, TimeSpan.FromMinutes 20.) | None -> CachingStrategy.NoCaching
        Config.Dynamo (context, cacheStrategy, unfolds)

/// To establish a local node to run the tests against, follow https://developers.eventstore.com/server/v21.10/installation.html#use-docker-compose
/// and/or do `docker compose up` in github.com/jet/equinox
module EventStore =

    open Equinox.EventStoreDb

    type [<NoEquality; NoComparison>] Parameters =
        | [<AltCommandLine "-V">]       StoreVerbose
        | [<AltCommandLine "-o">]       Timeout of float
        | [<AltCommandLine "-r">]       Retries of int
        | [<AltCommandLine "-g">]       ConnectionString of string
        | [<AltCommandLine "-p">]       Credentials of string
        | [<AltCommandLine "-b">]       BatchSize of int
        interface IArgParserTemplate with
            member a.Usage = a |> function
                | StoreVerbose ->       "include low level Store logging."
                | Timeout _ ->          "specify operation timeout in seconds (default: 5)."
                | Retries _ ->          "specify operation retries (default: 1)."
                | ConnectionString _ -> "Portion of connection string that's safe to write to console or log. default: esdb://localhost:2111,localhost:2112,localhost:2113?tls=true&tlsVerifyCert=false"
                | Credentials _ ->      "specify a sensitive portion of the connection string that should not be logged. Default: none"
                | BatchSize _ ->        "Maximum number of Events to request per batch / RollingSnapshots frequency. Default 500."
    type Arguments(a : ParseResults<Parameters>) =
        member _.ConnectionString =     a.GetResult(ConnectionString, "esdb://localhost:2111,localhost:2112,localhost:2113?tls=true&tlsVerifyCert=false")
        member _.Credentials =          a.GetResult(Credentials, null)

        member _.Timeout =              a.GetResult(Timeout,5.) |> TimeSpan.FromSeconds
        member _.Retries =              a.GetResult(Retries, 1)
        member _.BatchSize =            a.GetResult(BatchSize, 500)

    let private connect connectionString credentialsString operationTimeout =
        let cs = match credentialsString with null -> connectionString | x -> String.Join(";", connectionString, x)
        let tags = ["M", Environment.MachineName; "I", Guid.NewGuid() |> string]
        EventStoreConnector(reqTimeout = operationTimeout, tags = tags)
            .Establish(appName, Discovery.ConnectionString cs, ConnectionStrategy.ClusterTwinPreferSlaveReads)
    let config (log : ILogger) (cache, unfolds) (p : ParseResults<Parameters>) =
        let a = Arguments(p)
        let timeout = a.Timeout
        log.Information("EventStoreDB {connectionString} {timeout}s", a.ConnectionString, timeout.TotalSeconds)
        let connection = connect a.ConnectionString a.Credentials timeout
        let cacheStrategy = match cache with Some c -> CachingStrategy.SlidingWindow (c, TimeSpan.FromMinutes 20.) | None -> CachingStrategy.NoCaching
        Config.Es (EventStoreContext(connection, batchSize = a.BatchSize), cacheStrategy, unfolds)

// see https://github.com/jet/equinox#provisioning-mssql
module Sql =

    open Equinox.SqlStreamStore

    let cacheStrategy = function Some c -> CachingStrategy.SlidingWindow (c, TimeSpan.FromMinutes 20.) | None -> CachingStrategy.NoCaching
    module Ms =
        type [<NoEquality; NoComparison>] Parameters =
            | [<AltCommandLine "-c"; Mandatory>] ConnectionString of string
            | [<AltCommandLine "-s">]       Schema of string
            | [<AltCommandLine "-p">]       Credentials of string
            | [<AltCommandLine "-A">]       AutoCreate
            | [<AltCommandLine "-b">]       BatchSize of int
            interface IArgParserTemplate with
                member a.Usage = a |> function
                    | ConnectionString _ -> "Database connection string"
                    | Schema _ ->           "Database schema name"
                    | Credentials _ ->      "Database credentials"
                    | AutoCreate ->         "AutoCreate schema"
                    | BatchSize _ ->        "Maximum number of Events to request per batch. Default 500."
        type Arguments(p : ParseResults<Parameters>) =
            member _.ConnectionString =     p.GetResult ConnectionString
            member _.Schema =               p.GetResult(Schema,null)
            member _.Credentials =          p.GetResult(Credentials,null)
            member _.AutoCreate =           p.Contains AutoCreate
            member _.BatchSize =            p.GetResult(BatchSize, 500)
        let connect (log : ILogger) (connectionString,schema,credentials,autoCreate) =
            let sssConnectionString = String.Join(";", connectionString, credentials)
            log.Information("SqlStreamStore MsSql Connection {connectionString} Schema {schema} AutoCreate {autoCreate}", connectionString, schema, autoCreate)
            Equinox.SqlStreamStore.MsSql.Connector(sssConnectionString,schema,autoCreate=autoCreate).Establish()
        let config (log: ILogger) (cache, unfolds) (p : ParseResults<Parameters>) =
            let a = Arguments(p)
            let connection = connect log (a.ConnectionString, a.Schema, a.Credentials, a.AutoCreate) |> Async.RunSynchronously
            Config.Sql (SqlStreamStoreContext(connection, batchSize = a.BatchSize), cacheStrategy cache, unfolds)
    module My =
        type [<NoEquality; NoComparison>] Parameters =
            | [<AltCommandLine "-c"; Mandatory>] ConnectionString of string
            | [<AltCommandLine "-p">]       Credentials of string
            | [<AltCommandLine "-A">]       AutoCreate
            | [<AltCommandLine "-b">]       BatchSize of int
            interface IArgParserTemplate with
                member a.Usage = a |> function
                    | ConnectionString _ -> "Database connection string"
                    | Credentials _ ->      "Database credentials"
                    | AutoCreate ->         "AutoCreate schema"
                    | BatchSize _ ->        "Maximum number of Events to request per batch. Default 500."
        type Arguments(p : ParseResults<Parameters>) =
            member _.ConnectionString =     p.GetResult ConnectionString
            member _.Credentials =          p.GetResult(Credentials,null)
            member _.AutoCreate =           p.Contains AutoCreate
            member _.BatchSize =            p.GetResult(BatchSize, 500)
        let connect (log : ILogger) (connectionString,credentials,autoCreate) =
            let sssConnectionString = String.Join(";", connectionString, credentials)
            log.Information("SqlStreamStore MySql Connection {connectionString} AutoCreate {autoCreate}", connectionString, autoCreate)
            Equinox.SqlStreamStore.MySql.Connector(sssConnectionString,autoCreate=autoCreate).Establish()
        let config (log: ILogger) (cache, unfolds) (p : ParseResults<Parameters>) =
            let a = Arguments(p)
            let connection = connect log (a.ConnectionString, a.Credentials, a.AutoCreate) |> Async.RunSynchronously
            Config.Sql (SqlStreamStoreContext(connection, batchSize = a.BatchSize), cacheStrategy cache, unfolds)
     module Pg =
        type [<NoEquality; NoComparison>] Parameters =
            | [<AltCommandLine "-c"; Mandatory>] ConnectionString of string
            | [<AltCommandLine "-s">]       Schema of string
            | [<AltCommandLine "-p">]       Credentials of string
            | [<AltCommandLine "-A">]       AutoCreate
            | [<AltCommandLine "-b">]       BatchSize of int
            interface IArgParserTemplate with
                member a.Usage = a |> function
                    | ConnectionString _ -> "Database connection string"
                    | Schema _ ->           "Database schema name"
                    | Credentials _ ->      "Database credentials"
                    | AutoCreate ->         "AutoCreate schema"
                    | BatchSize _ ->        "Maximum number of Events to request per batch. Default 500."
        type Arguments(p : ParseResults<Parameters>) =
            member _.ConnectionString =     p.GetResult ConnectionString
            member _.Schema =               p.GetResult(Schema,null)
            member _.Credentials =          p.GetResult(Credentials,null)
            member _.AutoCreate =           p.Contains AutoCreate
            member _.BatchSize =            p.GetResult(BatchSize, 500)
        let connect (log : ILogger) (connectionString,schema,credentials,autoCreate) =
            let sssConnectionString = String.Join(";", connectionString, credentials)
            log.Information("SqlStreamStore Postgres Connection {connectionString} Schema {schema} AutoCreate {autoCreate}", connectionString, schema, autoCreate)
            Equinox.SqlStreamStore.Postgres.Connector(sssConnectionString,schema,autoCreate=autoCreate).Establish()
        let config (log : ILogger) (cache, unfolds) (p : ParseResults<Parameters>) =
            let a = Arguments(p)
            let connection = connect log (a.ConnectionString, a.Schema, a.Credentials, a.AutoCreate) |> Async.RunSynchronously
            Config.Sql (SqlStreamStoreContext(connection, batchSize = a.BatchSize), cacheStrategy cache, unfolds)
module MessageDb =
    open Equinox.MessageDb
    type [<NoEquality; NoComparison>] Parameters =
    | [<AltCommandLine "-c"; Mandatory>] ConnectionString of string
    | [<AltCommandLine "-b">]       BatchSize of int
        interface IArgParserTemplate with
            member a.Usage = a |> function
                | ConnectionString _ -> "Database connection string"
                | BatchSize _ ->        "Maximum number of Events to request per batch. Default 500."
    type Arguments(p : ParseResults<Parameters>) =
         member _.ConnectionString = p.GetResult ConnectionString
         member _.BatchSize =        p.GetResult(BatchSize, 500)
    let connect (log : ILogger) (connectionString: string) =
        log.Information("MessageDB Connection {connectionString}", connectionString)
        MessageDbClient(connectionString)
    let config (log : ILogger) cache (p : ParseResults<Parameters>) =
        let a = Arguments(p)
        let connection = connect log a.ConnectionString
        let cache = match cache with Some c -> CachingStrategy.SlidingWindow(c, TimeSpan.FromMinutes 20.) | None -> CachingStrategy.NoCaching
        Config.Mdb (MessageDbContext(connection, batchSize = a.BatchSize), cache)
