module Samples.Infrastructure.Store

open Argu
open Equinox
open Serilog
open System

[<RequireQualifiedAccess; NoEquality; NoComparison>]
type Context =
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
        Context.Memory (Equinox.MemoryStore.VolatileStore())

let [<Literal>] appName = "equinox-tool"

exception MissingArg of string
let missingArg msg = raise (MissingArg msg)

let private envVarTryGet varName = Environment.GetEnvironmentVariable varName |> Option.ofObj
let private getEnvVarForArgumentOrThrow varName argName =
    match envVarTryGet varName with
    | None -> missingArg (sprintf "Please provide a %s, either as an argument or via the %s environment variable" argName varName)
    | Some x -> x
let private defaultWithEnvVar varName argName = function
    | None -> getEnvVarForArgumentOrThrow varName argName
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
                | QueryMaxItems _ ->    "specify maximum number of batches of events to retrieve in per query response. Default: 10"
    type Arguments(p : ParseResults<Parameters>) =
        member _.Mode =                 p.TryGetResult ConnectionMode
        member _.Connection =           p.TryGetResult Connection |> defaultWithEnvVar "EQUINOX_COSMOS_CONNECTION" "Connection"
        member _.Database =             p.TryGetResult Database   |> defaultWithEnvVar "EQUINOX_COSMOS_DATABASE"   "Database"
        member _.Container =            p.TryGetResult Container  |> defaultWithEnvVar "EQUINOX_COSMOS_CONTAINER"  "Container"
        member private _.ArchiveConnection = p.TryGetResult ArchiveConnection
        member private x.ArchiveDatabase = p.TryGetResult ArchiveDatabase  |> Option.defaultWith (fun () -> x.Database)
        member private x.ArchiveContainer = p.TryGetResult ArchiveContainer |> Option.defaultWith (fun () -> x.Container)
        member x.Archive =              if p.Contains ArchiveConnection || p.Contains ArchiveDatabase || p.Contains ArchiveContainer
                                        then Some (x.ArchiveConnection, x.ArchiveDatabase, x.ArchiveContainer)
                                        else None

        member x.Timeout =              p.GetResult(Timeout,5.) |> TimeSpan.FromSeconds
        member x.Retries =              p.GetResult(Retries,1)
        member x.MaxRetryWaitTime =     p.GetResult(RetriesWaitTimeS, 5.) |> TimeSpan.FromSeconds
        member x.TipMaxEvents =         p.GetResult(TipMaxEvents, 256)
        member x.TipMaxJsonLength =     p.GetResult(TipMaxJsonLength, 30_000)
        member x.QueryMaxItems =        p.GetResult(QueryMaxItems, 10)

    let logContainer (log: ILogger) name (mode, endpoint, db, container) =
        log.Information("CosmosDB {name:l} {mode} {connection} Database {database} Container {container}",
                        name, defaultArg mode Microsoft.Azure.Cosmos.ConnectionMode.Direct, endpoint, db, container)

    // NOTE: this is a big song and dance, don't blindly copy!
    // - In normal usage, you typically connect to a single container only.
    // - In hot-warm scenarios, the Archive Container will frequently be within the same account and hence can share a CosmosClient
    // For these typical purposes, CosmosStoreClient.Connect should be used to establish the Client and Connection, not custom wiring as we have here
    let createClient (a : Arguments) connectionString =
        let connector = CosmosStoreConnector(Discovery.ConnectionString connectionString, a.Timeout, a.Retries, a.MaxRetryWaitTime, ?mode=a.Mode)
        connector.CreateUninitialized()
    let connect (log: ILogger) (a: Arguments) =
        let primaryClient, primaryDatabase, primaryContainer as primary = createClient a a.Connection, a.Database, a.Container
        logContainer log "Primary" (a.Mode, primaryClient.Endpoint, primaryDatabase, primaryContainer)
        let archive =
            match a.Archive with
            | Some (Some c2, db, container) -> Some (createClient a c2, db, container)
            | Some (None, db, container) -> Some (primaryClient, db, container)
            | None -> None
        archive |> Option.iter (fun (client, db, container) -> logContainer log "Archive" (a.Mode, client.Endpoint, db, container))
        primary, archive
    let config (log : ILogger) (cache, unfolds) (a : Arguments) =
        let context =
            match connect log a with
            | (client, databaseId, containerId), None ->
                let c = CosmosStoreClient client
                CosmosStoreContext(c, databaseId, containerId, a.TipMaxEvents, queryMaxItems = a.QueryMaxItems, tipMaxJsonLength = a.TipMaxJsonLength)
            | (client, databaseId, containerId), Some (aClient, aDatabaseId, aContainerId) ->
                let c = CosmosStoreClient(client, aClient)
                CosmosStoreContext(c, databaseId, containerId, a.TipMaxEvents, queryMaxItems = a.QueryMaxItems, tipMaxJsonLength = a.TipMaxJsonLength,
                                   archiveDatabaseId = aDatabaseId, archiveContainerId = aContainerId)
        log.Information("CosmosStore Max Events in Tip: {maxTipEvents}e {maxTipJsonLength}b Items in Query: {queryMaxItems}",
                        a.TipMaxEvents, a.TipMaxJsonLength, a.QueryMaxItems)
        let cacheStrategy = match cache with Some c -> CachingStrategy.SlidingWindow (c, TimeSpan.FromMinutes 20.) | None -> CachingStrategy.NoCaching
        Context.Cosmos (context, cacheStrategy, unfolds)

module Dynamo =

    type Equinox.DynamoStore.DynamoStoreConnector with

        member x.LogConfiguration(log: ILogger) =
            log.Information("DynamoStore {endpoint} Timeout {timeoutS}s Retries {retries}",
                            x.Endpoint, (let t = x.Timeout in t.TotalSeconds), x.Retries)

    type Equinox.DynamoStore.DynamoStoreContext with

        member internal x.LogConfiguration(role, log: ILogger) =
            log.Information("DynamoStore {role:l} Table {table} Archive {archive}", role, x.TableName, Option.toObj x.ArchiveTableName)
            log.Information("DynamoStore Tip thresholds: {maxTipBytes}b {maxTipEvents}e Query Paging {queryMaxItems} items",
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
                                            let serviceUrl =  p.TryGetResult ServiceUrl |> defaultWithEnvVar SERVICE_URL   "ServiceUrl"
                                            let accessKey =   p.TryGetResult AccessKey  |> defaultWithEnvVar ACCESS_KEY    "AccessKey"
                                            let secretKey =   p.TryGetResult SecretKey  |> defaultWithEnvVar SECRET_KEY    "SecretKey"
                                            Choice2Of2 (serviceUrl, accessKey, secretKey)
        let retries =                   p.GetResult(Retries, 1)
        let timeout =                   p.GetResult(RetriesTimeoutS, 5.) |> TimeSpan.FromSeconds
        member val Connector =          match conn with
                                        | Choice1Of2 systemName -> DynamoStoreConnector(systemName, timeout, retries)
                                        | Choice2Of2 (serviceUrl, accessKey, secretKey) -> DynamoStoreConnector(serviceUrl, accessKey, secretKey, timeout, retries)
        member val Table =              p.TryGetResult Table        |> defaultWithEnvVar TABLE "Table"
        member val ArchiveTable =       p.TryGetResult ArchiveTable |> Option.orElseWith (fun () -> envVarTryGet ARCHIVE_TABLE)

        member val TipMaxEvents =       p.TryGetResult TipMaxEvents
        member val TipMaxBytes =        p.GetResult(TipMaxBytes, 32 * 1024)
        member val QueryMaxItems =      p.GetResult(QueryMaxItems, 10)

    let config (log : ILogger) (cache, unfolds) (a : Arguments) =
        a.Connector.LogConfiguration(log)
        let client = a.Connector.CreateClient() |> DynamoStoreClient
        let context = DynamoStoreContext(client, a.Table, maxBytes = a.TipMaxBytes, queryMaxItems = a.QueryMaxItems,
                                         ?tipMaxEvents = a.TipMaxEvents, ?archiveTableName = a.ArchiveTable)
        context.LogConfiguration("Main", log)
        let cacheStrategy = match cache with Some c -> CachingStrategy.SlidingWindow (c, TimeSpan.FromMinutes 20.) | None -> CachingStrategy.NoCaching
        Context.Dynamo (context, cacheStrategy, unfolds)

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
        Context.Es (EventStoreContext(connection, batchSize = a.BatchSize), cacheStrategy, unfolds)

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
                    | AutoCreate _ ->       "AutoCreate schema"
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
            Context.Sql (SqlStreamStoreContext(connection, batchSize = a.BatchSize), cacheStrategy cache, unfolds)
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
                    | AutoCreate _ ->       "AutoCreate schema"
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
            Context.Sql (SqlStreamStoreContext(connection, batchSize = a.BatchSize), cacheStrategy cache, unfolds)
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
                    | AutoCreate _ ->       "AutoCreate schema"
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
            Context.Sql (SqlStreamStoreContext(connection, batchSize = a.BatchSize), cacheStrategy cache, unfolds)
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
        Context.Mdb (MessageDbContext(connection, batchSize = a.BatchSize), cache)
