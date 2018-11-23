[<AutoOpen>]
module Equinox.Cosmos.Integration.CosmosFixtures

open Equinox.Cosmos.Builder
open System

module Option =
    let defaultValue def option = defaultArg option def

/// Standing up an Equinox instance is necessary to run for test purposes; either:
/// - replace connection below with a connection string or Uri+Key for an initialized Equinox instance
/// - Create a local Equinox via dotnet run cli/Equinox.cli -s $env:EQUINOX_COSMOS_CONNECTION -d test -c $env:EQUINOX_COSMOS_COLLECTION provision -ru 10000
let private connectToCosmos (log: Serilog.ILogger) name discovery =
    EqxConnector(log=log, requestTimeout=TimeSpan.FromSeconds 3., maxRetryAttemptsOnThrottledRequests=2, maxRetryWaitTimeInSeconds=60)
       .Connect(name, discovery)
let private read env = Environment.GetEnvironmentVariable env |> Option.ofObj
let (|Default|) def name = (read name),def ||> defaultArg

let connectToSpecifiedCosmosOrSimulator (log: Serilog.ILogger) =
    match read "EQUINOX_COSMOS_CONNECTION" with
    | None ->
        Discovery.UriAndKey(Uri "https://localhost:8081", "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==")
        |> connectToCosmos log "localDocDbSim"
    | Some connectionString ->
        Discovery.FromConnectionString connectionString
        |> connectToCosmos log "EQUINOX_COSMOS_CONNECTION"

let defaultBatchSize = 500

let collections =
    EqxCollections(
        read "EQUINOX_COSMOS_DATABASE" |> Option.defaultValue "equinox-test",
        read "EQUINOX_COSMOS_COLLECTION" |> Option.defaultValue "equinox-test")

let createEqxStore connection batchSize =
    let gateway = EqxGateway(connection, EqxBatchingPolicy(defaultMaxItems=batchSize))
    EqxStore(gateway, collections)