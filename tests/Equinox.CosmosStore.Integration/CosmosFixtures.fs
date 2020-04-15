[<AutoOpen>]
module Equinox.CosmosStore.Integration.CosmosFixtures

open Equinox.CosmosStore
open System

module Option =
    let defaultValue def option = defaultArg option def

/// Standing up an Equinox instance is necessary to run for test purposes; either:
/// - replace connection below with a connection string or Uri+Key for an initialized Equinox instance
/// - Create a local Equinox via dotnet run cli/Equinox.cli -s $env:EQUINOX_COSMOS_CONNECTION -d test -c $env:EQUINOX_COSMOS_CONTAINER provision -ru 10000
let private read env = Environment.GetEnvironmentVariable env |> Option.ofObj
let (|Default|) def name = (read name),def ||> defaultArg

let private databaseId = read "EQUINOX_COSMOS_DATABASE" |> Option.defaultValue "equinox-test"
let private containerId = read "EQUINOX_COSMOS_CONTAINER" |> Option.defaultValue "equinox-test"

let private connectToCosmos batchSize client  =
    CosmosStoreContext(client, defaultMaxItems = batchSize)

let createSpecifiedCosmosOrSimulatorClient (log : Serilog.ILogger) =
    let createClient name discovery =
        let factory = CosmosStoreClientFactory(requestTimeout=TimeSpan.FromSeconds 3., maxRetryAttemptsOnRateLimitedRequests=2, maxRetryWaitTimeOnRateLimitedRequests=TimeSpan.FromMinutes 1.)
        let client = factory.Create discovery
        log.Information("Connection {name} to {endpoint}", name, client.Endpoint)
        CosmosStoreConnection(client, databaseId, containerId)

    match read "EQUINOX_COSMOS_CONNECTION" with
    | None ->
        Discovery.AccountUriAndKey(Uri "https://localhost:8081", "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==")
        |> createClient "localDocDbSim"
    | Some connectionString ->
        Discovery.ConnectionString connectionString
        |> createClient "EQUINOX_COSMOS_CONNECTION"

let connectToSpecifiedCosmosOrSimulator (log: Serilog.ILogger) batchSize =
    createSpecifiedCosmosOrSimulatorClient log
    |> connectToCosmos batchSize

let createSpecifiedCoreContext log defaultBatchSize =
    let client = createSpecifiedCosmosOrSimulatorClient log
    Equinox.CosmosStore.Core.EventsContext(client.Client, log, databaseId, containerId, ?defaultMaxItems = defaultBatchSize)

let defaultBatchSize = 500
