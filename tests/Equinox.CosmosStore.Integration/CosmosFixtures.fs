﻿[<AutoOpen>]
module Equinox.CosmosStore.Integration.CosmosFixtures

open Equinox.CosmosStore
open System

module Option =
    let defaultValue def option = defaultArg option def

/// Standing up an Equinox instance is necessary to run for test purposes; either:
/// - replace connection below with a connection string or Uri+Key for an initialized Equinox instance
/// - Create a local Equinox via (e.g.) dotnet run cli/Equinox.Tool init -ru 1000 cosmos -s $env:EQUINOX_COSMOS_CONNECTION -d test -c $env:EQUINOX_COSMOS_CONTAINER
let private tryRead env = Environment.GetEnvironmentVariable env |> Option.ofObj
let (|Default|) def name = (tryRead name),def ||> defaultArg

let private databaseId = tryRead "EQUINOX_COSMOS_DATABASE" |> Option.defaultValue "equinox-test"
let private containerId = tryRead "EQUINOX_COSMOS_CONTAINER" |> Option.defaultValue "equinox-test"

let createSpecifiedCosmosOrSimulatorConnection (log : Serilog.ILogger) =
    let createConnection name discovery =
        let factory = CosmosStoreClientFactory(requestTimeout=TimeSpan.FromSeconds 3., maxRetryAttemptsOnRateLimitedRequests=2, maxRetryWaitTimeOnRateLimitedRequests=TimeSpan.FromMinutes 1.)
        let client = factory.Create discovery
        log.Information("CosmosDb Connecting {name} to {endpoint}", name, client.Endpoint)
        CosmosStoreConnection(client, databaseId, containerId)

    match tryRead "EQUINOX_COSMOS_CONNECTION" with
    | None ->
        Discovery.AccountUriAndKey(Uri "https://localhost:8081", "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==")
        |> createConnection "localDocDbSim"
    | Some connectionString ->
        Discovery.ConnectionString connectionString
        |> createConnection "EQUINOX_COSMOS_CONNECTION"

// TODO rename to something with Context in the name
let connectToSpecifiedCosmosOrSimulator (log: Serilog.ILogger) batchSize =
    let conn = createSpecifiedCosmosOrSimulatorConnection log
    CosmosStoreContext(conn, defaultMaxItems = batchSize)

let createSpecifiedCoreContext log defaultBatchSize =
    let batchSize = defaultArg defaultBatchSize 500
    let conn = connectToSpecifiedCosmosOrSimulator log batchSize
    Equinox.CosmosStore.Core.EventsContext(conn, log, ?defaultMaxItems = defaultBatchSize)

let defaultBatchSize = 500
