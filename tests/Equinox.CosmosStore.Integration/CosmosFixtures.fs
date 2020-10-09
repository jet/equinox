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
let private containerId2 = tryRead "EQUINOX_COSMOS_CONTAINER2" |> Option.defaultValue "equinox-test2"

let discoverConnection () =
    match tryRead "EQUINOX_COSMOS_CONNECTION" with
    | None -> "localDocDbSim", Discovery.AccountUriAndKey(Uri "https://localhost:8081", "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==")
    | Some connectionString -> "EQUINOX_COSMOS_CONNECTION", Discovery.ConnectionString connectionString

let createClient (log : Serilog.ILogger) name discovery =
    let factory = CosmosStoreClientFactory(requestTimeout=TimeSpan.FromSeconds 3., maxRetryAttemptsOnRateLimitedRequests=2, maxRetryWaitTimeOnRateLimitedRequests=TimeSpan.FromMinutes 1.)
    let client = factory.Create discovery
    log.Information("CosmosDb Connecting {name} to {endpoint}", name, client.Endpoint)
    client

let connectPrimary (log : Serilog.ILogger) =
    let name, discovery = discoverConnection ()
    let client = createClient log name discovery
    CosmosStoreConnection(client, databaseId, containerId)

let connectSecondary (log : Serilog.ILogger) =
    let name, discovery = discoverConnection ()
    let client = createClient log name discovery
    CosmosStoreConnection(client, databaseId, containerId2)

let connectWithFallback (log : Serilog.ILogger) =
    let name, discovery = discoverConnection ()
    let client = createClient log name discovery
    CosmosStoreConnection(client, databaseId, containerId, containerId2=containerId2)

let createPrimaryContext (log: Serilog.ILogger) queryMaxItems =
    let conn = connectPrimary log
    CosmosStoreContext(conn, defaultMaxItems = queryMaxItems)

let createSecondaryContext (log: Serilog.ILogger) queryMaxItems =
    let conn = connectSecondary log
    CosmosStoreContext(conn, defaultMaxItems = queryMaxItems)

let createFallbackContext (log: Serilog.ILogger) queryMaxItems =
    let conn = connectWithFallback log
    CosmosStoreContext(conn, defaultMaxItems = queryMaxItems)

let defaultQueryMaxItems = 10

let createPrimaryEventsContext log queryMaxItems =
    let queryMaxItems = defaultArg queryMaxItems defaultQueryMaxItems
    let context = createPrimaryContext log queryMaxItems
    Equinox.CosmosStore.Core.EventsContext(context, log, defaultMaxItems = queryMaxItems)

let createSecondaryEventsContext log queryMaxItems =
    let queryMaxItems = defaultArg queryMaxItems defaultQueryMaxItems
    let ctx = createSecondaryContext log queryMaxItems
    Equinox.CosmosStore.Core.EventsContext(ctx, log, defaultMaxItems = queryMaxItems)

let createFallbackEventsContext log queryMaxItems =
    let queryMaxItems = defaultArg queryMaxItems defaultQueryMaxItems
    let ctx = createFallbackContext log queryMaxItems
    Equinox.CosmosStore.Core.EventsContext(ctx, log, defaultMaxItems = queryMaxItems)
