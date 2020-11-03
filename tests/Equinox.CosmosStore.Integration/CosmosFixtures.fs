[<AutoOpen>]
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
    log.Information("CosmosDB Connecting {name} to {endpoint}", name, client.Endpoint)
    client

let connectPrimary log =
    let name, discovery = discoverConnection ()
    let client = createClient log name discovery
    CosmosStoreConnection(client, databaseId, containerId)

let connectSecondary log =
    let name, discovery = discoverConnection ()
    let client = createClient log name discovery
    CosmosStoreConnection(client, databaseId, containerId2)

let connectWithFallback log =
    let name, discovery = discoverConnection ()
    let client = createClient log name discovery
    CosmosStoreConnection(client, databaseId, containerId, containerId2 = containerId2)

let createPrimaryContextIgnoreMissing conn queryMaxItems tipMaxEvents ignoreMissing =
    CosmosStoreContext.Create(conn, defaultMaxItems = queryMaxItems, tipMaxEvents = tipMaxEvents, ignoreMissingEvents = ignoreMissing)

let createPrimaryContextEx log queryMaxItems tipMaxEvents =
    let conn = connectPrimary log
    createPrimaryContextIgnoreMissing conn queryMaxItems tipMaxEvents false

let defaultTipMaxEvents = 10

let createPrimaryContext log queryMaxItems =
    createPrimaryContextEx log queryMaxItems defaultTipMaxEvents

let createSecondaryContext log queryMaxItems =
    let conn = connectSecondary log
    CosmosStoreContext.Create(conn, defaultMaxItems = queryMaxItems, tipMaxEvents = defaultTipMaxEvents)

let createFallbackContext log queryMaxItems =
    let conn = connectWithFallback log
    CosmosStoreContext.Create(conn, defaultMaxItems = queryMaxItems, tipMaxEvents = defaultTipMaxEvents)

let defaultQueryMaxItems = 10

let createPrimaryEventsContext log queryMaxItems tipMaxItems =
    let context = createPrimaryContextEx log queryMaxItems tipMaxItems
    Equinox.CosmosStore.Core.EventsContext(context, log)

let createPrimaryEventsContextWithUnsafe log queryMaxItems tipMaxItems =
    let conn = connectPrimary log
    let create ignoreMissing =
        let ctx = createPrimaryContextIgnoreMissing conn queryMaxItems tipMaxItems ignoreMissing
        Equinox.CosmosStore.Core.EventsContext(ctx, log)
    create false, create true

let createSecondaryEventsContext log queryMaxItems =
    let context = createSecondaryContext log queryMaxItems
    Equinox.CosmosStore.Core.EventsContext(context, log)

let createFallbackEventsContext log queryMaxItems =
    let context = createFallbackContext log queryMaxItems
    Equinox.CosmosStore.Core.EventsContext(context, log)
