module Samples.Infrastructure.Storage

open Argu
open Serilog
open System

type [<NoEquality; NoComparison>] MemArguments =
    | [<AltCommandLine("-vs")>] VerboseStore
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | VerboseStore -> "Include low level Store logging."
and [<NoEquality; NoComparison>] EsArguments =
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
let defaultBatchSize = 500

[<RequireQualifiedAccess; NoEquality; NoComparison>]
type StorageConfig =
    | Memory of Equinox.MemoryStore.VolatileStore
    | Es of Equinox.EventStore.GesGateway * Equinox.EventStore.CachingStrategy option * unfolds: bool

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
            .Establish("equinox-cli", Discovery.GossipDns dnsQuery, ConnectionStrategy.ClusterTwinPreferSlaveReads)
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