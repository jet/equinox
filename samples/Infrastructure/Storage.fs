module Samples.Infrastructure.Storage

open Argu
open System

exception MissingArg of string

[<RequireQualifiedAccess; NoEquality; NoComparison>]
type StorageConfig =
    // For MemoryStore, we keep the events as UTF8 arrays - we could use FsCodec.Codec.Box to remove the JSON encoding, which would improve perf but can conceal problems
    | Memory of Equinox.MemoryStore.VolatileStore<byte[]>
    | Es     of Equinox.EventStore.Context * Equinox.EventStore.CachingStrategy option * unfolds: bool
    | Cosmos of Equinox.Cosmos.Gateway * Equinox.Cosmos.CachingStrategy * unfolds: bool * databaseId: string * containerId: string
    | Sql    of Equinox.SqlStreamStore.Context * Equinox.SqlStreamStore.CachingStrategy option * unfolds: bool

module MemoryStore =
    type [<NoEquality; NoComparison>] Arguments =
        | [<AltCommandLine "-V">]        VerboseStore
        interface IArgParserTemplate with
            member a.Usage = a |> function
                | VerboseStore ->       "Include low level Store logging."
    let config () =
        StorageConfig.Memory (Equinox.MemoryStore.VolatileStore())

let [<Literal>] appName = "equinox-tool"

module Cosmos =
    let private getEnvVarForArgumentOrThrow varName argName =
        match Environment.GetEnvironmentVariable varName with
        | null -> raise (MissingArg(sprintf "Please provide a %s, either as an argument or via the %s environment variable" argName varName))
        | x -> x
    let private defaultWithEnvVar varName argName = function
        | None -> getEnvVarForArgumentOrThrow varName argName
        | Some x -> x

    type [<NoEquality; NoComparison>] Arguments =
        | [<AltCommandLine "-V">]       VerboseStore
        | [<AltCommandLine "-m">]       ConnectionMode of Microsoft.Azure.Cosmos.ConnectionMode
        | [<AltCommandLine "-o">]       Timeout of float
        | [<AltCommandLine "-r">]       Retries of int
        | [<AltCommandLine "-rt">]      RetriesWaitTimeS of float
        | [<AltCommandLine "-s">]       Connection of string
        | [<AltCommandLine "-d">]       Database of string
        | [<AltCommandLine "-c">]       Container of string
        interface IArgParserTemplate with
            member a.Usage =
                match a with
                | VerboseStore ->       "Include low level Store logging."
                | Timeout _ ->          "specify operation timeout in seconds (default: 5)."
                | Retries _ ->          "specify operation retries (default: 1)."
                | RetriesWaitTimeS _ -> "specify max wait-time for retry when being throttled by Cosmos in seconds (default: 5)"
                | ConnectionMode _ ->   "override the connection mode. Default: Direct."
                | Connection _ ->       "specify a connection string for a Cosmos account. (optional if environment variable EQUINOX_COSMOS_CONNECTION specified)"
                | Database _ ->         "specify a database name for store. (optional if environment variable EQUINOX_COSMOS_DATABASE specified)"
                | Container _ ->        "specify a container name for store. (optional if environment variable EQUINOX_COSMOS_CONTAINER specified)"
    type Info(args : ParseResults<Arguments>) =
        member __.Mode =                args.GetResult(ConnectionMode,Microsoft.Azure.Cosmos.ConnectionMode.Direct)
        member __.Connection =          args.TryGetResult Connection |> defaultWithEnvVar "EQUINOX_COSMOS_CONNECTION" "Connection"
        member __.Database =            args.TryGetResult Database   |> defaultWithEnvVar "EQUINOX_COSMOS_DATABASE"   "Database"
        member __.Container =           args.TryGetResult Container  |> defaultWithEnvVar "EQUINOX_COSMOS_CONTAINER"  "Container"

        member __.Timeout =             args.GetResult(Timeout,5.) |> TimeSpan.FromSeconds
        member __.Retries =             args.GetResult(Retries,1)
        member __.MaxRetryWaitTime =    args.GetResult(RetriesWaitTimeS, 5.) |> TimeSpan.FromSeconds

    /// Standing up an Equinox instance is necessary to run for test purposes; You'll need to either:
    /// 1) replace connection below with a connection string or Uri+Key for an initialized Equinox instance with a database and collection named "equinox-test"
    /// 2) Set the 3x environment variables and create a local Equinox using tools/Equinox.Tool/bin/Release/net461/eqx.exe `
    ///     init -ru 1000 cosmos -s $env:EQUINOX_COSMOS_CONNECTION -d $env:EQUINOX_COSMOS_DATABASE -c $env:EQUINOX_COSMOS_CONTAINER
    open Equinox.Cosmos
    open Serilog

    let private createGateway connection maxItems = Gateway(connection, BatchingPolicy(defaultMaxItems=maxItems))
    let connection (log: ILogger, storeLog: ILogger) (a : Info) =
        let (Discovery.UriAndKey (endpointUri,_)) as discovery = a.Connection |> Discovery.FromConnectionString
        log.Information("CosmosDb {mode} {connection} Database {database} Container {container}",
            a.Mode, endpointUri, a.Database, a.Container)
        log.Information("CosmosDb timeout {timeout}s; Throttling retries {retries}, max wait {maxRetryWaitTime}s",
            (let t = a.Timeout in t.TotalSeconds), a.Retries, let x = a.MaxRetryWaitTime in x.TotalSeconds)
        discovery, a.Database, a.Container, Connector(a.Timeout, a.Retries, a.MaxRetryWaitTime, log=storeLog, mode=a.Mode)
    let config (log: ILogger, storeLog) (cache, unfolds, batchSize) info =
        let discovery, dName, cName, connector = connection (log, storeLog) info
        let conn = connector.Connect(appName, discovery) |> Async.RunSynchronously
        let cacheStrategy = match cache with Some c -> CachingStrategy.SlidingWindow (c, TimeSpan.FromMinutes 20.) | None -> CachingStrategy.NoCaching
        StorageConfig.Cosmos (createGateway conn batchSize, cacheStrategy, unfolds, dName, cName)

/// To establish a local node to run the tests against:
///   1. cinst eventstore-oss -y # where cinst is an invocation of the Chocolatey Package Installer on Windows
///   2. & $env:ProgramData\chocolatey\bin\EventStore.ClusterNode.exe --gossip-on-single-node --discover-via-dns 0 --ext-http-port=30778
module EventStore =
    type [<NoEquality; NoComparison>] Arguments =
        | [<AltCommandLine("-V")>]      VerboseStore
        | [<AltCommandLine("-o")>]      Timeout of float
        | [<AltCommandLine("-r")>]      Retries of int
        | [<AltCommandLine("-g")>]      Host of string
        | [<AltCommandLine("-u")>]      Username of string
        | [<AltCommandLine("-p")>]      Password of string
        | [<AltCommandLine("-c")>]      ConcurrentOperationsLimit of int
        | [<AltCommandLine("-h")>]      HeartbeatTimeout of float
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
        member __.Host =                args.GetResult(Host,"localhost")
        member __.Credentials =         args.GetResult(Username,"admin"), args.GetResult(Password,"changeit")

        member __.Timeout =             args.GetResult(Timeout,5.) |> TimeSpan.FromSeconds
        member __.Retries =             args.GetResult(Retries, 1)
        member __.HeartbeatTimeout =    args.GetResult(HeartbeatTimeout,1.5) |> float |> TimeSpan.FromSeconds
        member __.ConcurrentOperationsLimit = args.GetResult(ConcurrentOperationsLimit,5000)

    open Serilog

    let private connect (log: ILogger) (dnsQuery, heartbeatTimeout, col) (username, password) (operationTimeout, operationRetries) =
        Connector(username, password, reqTimeout=operationTimeout, reqRetries=operationRetries,
                heartbeatTimeout=heartbeatTimeout, concurrentOperationsLimit = col,
                log=(if log.IsEnabled(Serilog.Events.LogEventLevel.Debug) then Logger.SerilogVerbose log else Logger.SerilogNormal log),
                tags=["M", Environment.MachineName; "I", Guid.NewGuid() |> string])
            .Establish(appName, Discovery.GossipDns dnsQuery, ConnectionStrategy.ClusterTwinPreferSlaveReads)
    let private createGateway connection batchSize = Context(connection, BatchingPolicy(maxBatchSize = batchSize))
    let config (log: ILogger, storeLog) (cache, unfolds, batchSize) (args : ParseResults<Arguments>) =
        let a = Info(args)
        let (timeout, retries) as operationThrottling = a.Timeout, a.Retries
        let heartbeatTimeout = a.HeartbeatTimeout
        let concurrentOperationsLimit = a.ConcurrentOperationsLimit
        log.Information("EventStore {host} heartbeat: {heartbeat}s timeout: {timeout}s concurrent reqs: {concurrency} retries {retries}",
            a.Host, heartbeatTimeout.TotalSeconds, timeout.TotalSeconds, concurrentOperationsLimit, retries)
        let conn = connect storeLog (a.Host, heartbeatTimeout, concurrentOperationsLimit) a.Credentials operationThrottling |> Async.RunSynchronously
        let cacheStrategy = cache |> Option.map (fun c -> CachingStrategy.SlidingWindow (c, TimeSpan.FromMinutes 20.))
        StorageConfig.Es ((createGateway conn batchSize), cacheStrategy, unfolds)

module Sql =
    open Equinox.SqlStreamStore
    open Serilog
    let cacheStrategy cache = cache |> Option.map (fun c -> CachingStrategy.SlidingWindow (c, TimeSpan.FromMinutes 20.))
    module Ms =
        type [<NoEquality; NoComparison>] Arguments =
            | [<AltCommandLine "-c"; Mandatory>] ConnectionString of string
            | [<AltCommandLine "-s">]       Schema of string
            | [<AltCommandLine "-p">]       Credentials of string
            | [<AltCommandLine "-A">]       AutoCreate
            interface IArgParserTemplate with
                member a.Usage = a |> function
                    | ConnectionString _ -> "Database connection string"
                    | Schema _ ->           "Database schema name"
                    | Credentials _ ->      "Database credentials"
                    | AutoCreate _ ->       "AutoCreate schema"
        type Info(args : ParseResults<Arguments>) =
            member __.ConnectionString =    args.GetResult ConnectionString
            member __.Schema =              args.GetResult(Schema,null)
            member __.Credentials =         args.GetResult(Credentials,null)
            member __.AutoCreate =          args.Contains AutoCreate
        let connect (log : ILogger) (connectionString,schema,credentials,autoCreate) =
            let sssConnectionString = String.Join(";", connectionString, credentials)
            log.Information("SqlStreamStore MsSql Connection {connectionString} Schema {schema} AutoCreate {autoCreate}", connectionString, schema, autoCreate)
            Equinox.SqlStreamStore.MsSql.Connector(sssConnectionString,schema,autoCreate=autoCreate).Establish(appName)
        let private createGateway connection batchSize = Context(connection, BatchingPolicy(maxBatchSize = batchSize))
        let config (log: ILogger) (cache, unfolds, batchSize) (args : ParseResults<Arguments>) =
            let a = Info(args)
            let conn = connect log (a.ConnectionString, a.Schema, a.Credentials, a.AutoCreate) |> Async.RunSynchronously
            StorageConfig.Sql((createGateway conn batchSize), cacheStrategy cache, unfolds)
    module My =
        type [<NoEquality; NoComparison>] Arguments =
            | [<AltCommandLine "-c"; Mandatory>] ConnectionString of string
            | [<AltCommandLine "-p">]       Credentials of string
            | [<AltCommandLine "-A">]       AutoCreate
            interface IArgParserTemplate with
                member a.Usage = a |> function
                    | ConnectionString _ -> "Database connection string"
                    | Credentials _ ->      "Database credentials"
                    | AutoCreate _ ->       "AutoCreate schema"
        type Info(args : ParseResults<Arguments>) =
            member __.ConnectionString =    args.GetResult ConnectionString
            member __.Credentials =         args.GetResult(Credentials,null)
            member __.AutoCreate =          args.Contains AutoCreate
        let connect (log : ILogger) (connectionString,credentials,autoCreate) =
            let sssConnectionString = String.Join(";", connectionString, credentials)
            log.Information("SqlStreamStore MySql Connection {connectionString} AutoCreate {autoCreate}", connectionString, autoCreate)
            Equinox.SqlStreamStore.MySql.Connector(sssConnectionString,autoCreate=autoCreate).Establish(appName)
        let private createGateway connection batchSize = Context(connection, BatchingPolicy(maxBatchSize = batchSize))
        let config (log: ILogger) (cache, unfolds, batchSize) (args : ParseResults<Arguments>) =
            let a = Info(args)
            let conn = connect log (a.ConnectionString, a.Credentials, a.AutoCreate) |> Async.RunSynchronously
            StorageConfig.Sql((createGateway conn batchSize), cacheStrategy cache, unfolds)
     module Pg =
        type [<NoEquality; NoComparison>] Arguments =
            | [<AltCommandLine "-c"; Mandatory>] ConnectionString of string
            | [<AltCommandLine "-s">]       Schema of string
            | [<AltCommandLine "-p">]       Credentials of string
            | [<AltCommandLine "-A">]       AutoCreate
            interface IArgParserTemplate with
                member a.Usage = a |> function
                    | ConnectionString _ -> "Database connection string"
                    | Schema _ ->           "Database schema name"
                    | Credentials _ ->      "Database credentials"
                    | AutoCreate _ ->       "AutoCreate schema"
        type Info(args : ParseResults<Arguments>) =
            member __.ConnectionString =    args.GetResult ConnectionString
            member __.Schema =              args.GetResult(Schema,null)
            member __.Credentials =         args.GetResult(Credentials,null)
            member __.AutoCreate =          args.Contains AutoCreate
        let connect (log : ILogger) (connectionString,schema,credentials,autoCreate) =
            let sssConnectionString = String.Join(";", connectionString, credentials)
            log.Information("SqlStreamStore Postgres Connection {connectionString} Schema {schema} AutoCreate {autoCreate}", connectionString, schema, autoCreate)
            Equinox.SqlStreamStore.Postgres.Connector(sssConnectionString,schema,autoCreate=autoCreate).Establish(appName)
        let private createGateway connection batchSize = Context(connection, BatchingPolicy(maxBatchSize = batchSize))
        let config (log: ILogger) (cache, unfolds, batchSize) (args : ParseResults<Arguments>) =
            let a = Info(args)
            let conn = connect log (a.ConnectionString, a.Schema, a.Credentials, a.AutoCreate) |> Async.RunSynchronously
            StorageConfig.Sql((createGateway conn batchSize), cacheStrategy cache, unfolds)
