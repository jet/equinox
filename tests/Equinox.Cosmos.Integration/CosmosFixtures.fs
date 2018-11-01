[<AutoOpen>]
module Equinox.Cosmos.Integration.CosmosFixtures

open Equinox.Cosmos
open System

/// Standing up an Equinox instance is necessary to run for test purposes; either:
/// - replace connection below with a connection string or Uri+Key for an initialized Equinox instance
/// - Create a local Equinox via cli/Equinox.Cli/bin/Release/net461/Equinox.Cli with args:
///   cosmos -s $env:EQUINOX_COSMOS_CONNECTION -d test -c $env:EQUINOX_COSMOS_COLLECTION provision -ru 10000
let private connectToCosmos (log: Serilog.ILogger) name discovery =
    EqxConnector(log=log, requestTimeout=TimeSpan.FromSeconds 3., maxRetryAttemptsOnThrottledRequests=2, maxRetryWaitTimeInSeconds=60)
       .Connect(name, discovery)
let private read env = Environment.GetEnvironmentVariable env |> Option.ofObj

let connectToSpecifiedCosmosOrSimulator (log: Serilog.ILogger) =
    match read "EQUINOX_COSMOS_CONNECTION" with
    | None ->
        Discovery.UriAndKey(Uri "https://localhost:8081", "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==")
        |> connectToCosmos log "localDocDbSim"
    | Some connectionString ->
        Discovery.FromConnectionString connectionString
        |> connectToCosmos log "EQUINOX_COSMOS_CONNECTION"

let (|StreamArgs|) streamName =
    let databaseId, collectionId = defaultArg (read "EQUINOX_COSMOS_DATABASE") "test", defaultArg (read "EQUINOX_COSMOS_COLLECTION") "test"
    databaseId, collectionId, streamName

let defaultBatchSize = 500
let createEqxGateway connection batchSize = EqxGateway(connection, EqxBatchingPolicy(maxBatchSize = batchSize))