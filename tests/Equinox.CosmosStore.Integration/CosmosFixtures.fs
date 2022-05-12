[<AutoOpen>]
module Equinox.CosmosStore.Integration.CosmosFixtures

open Equinox.CosmosStore
open System

/// Standing up an Equinox instance is necessary to run for test purposes; either:
/// - replace connection below with a connection string or Uri+Key for an initialized Equinox instance
/// - Create a local Equinox via (e.g.) dotnet run tools/Equinox.Tool init -ru 1000 cosmos -s $env:EQUINOX_COSMOS_CONNECTION -d test -c $env:EQUINOX_COSMOS_CONTAINER
let private tryRead env = Environment.GetEnvironmentVariable env |> Option.ofObj

let private databaseId = tryRead "EQUINOX_COSMOS_DATABASE" |> Option.defaultValue "equinox-test"
let private containerId = tryRead "EQUINOX_COSMOS_CONTAINER" |> Option.defaultValue "equinox-test"
let private archiveContainerId = tryRead "EQUINOX_COSMOS_CONTAINER_ARCHIVE" |> Option.defaultValue "equinox-test-archive"

let discoverConnection () =
    match tryRead "EQUINOX_COSMOS_CONNECTION" with
    | None -> "localDocDbSim", Discovery.AccountUriAndKey(Uri "https://localhost:8081", "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==")
    | Some connectionString -> "EQUINOX_COSMOS_CONNECTION", Discovery.ConnectionString connectionString

let createClient (log : Serilog.ILogger) name (discovery : Discovery) =
    let connector = CosmosStoreConnector(discovery, requestTimeout=TimeSpan.FromSeconds 3., maxRetryAttemptsOnRateLimitedRequests=2, maxRetryWaitTimeOnRateLimitedRequests=TimeSpan.FromMinutes 1.)
    log.Information("CosmosDB Connecting {name} to {endpoint}", name, discovery.Endpoint)
    connector.CreateUninitialized()

let connectPrimary log =
    let name, discovery = discoverConnection ()
    let client = createClient log name discovery
    CosmosStoreClient(client, databaseId, containerId)

let connectArchive log =
    let name, discovery = discoverConnection ()
    let client = createClient log name discovery
    CosmosStoreClient(client, databaseId, archiveContainerId)

let connectWithFallback log =
    let name, discovery = discoverConnection ()
    let client = createClient log name discovery
    CosmosStoreClient(client, databaseId, containerId, archiveContainerId = archiveContainerId)

[<Xunit.CollectionDefinition "DocStore">]
type DocStoreCollection() =
    do ()

type StoreContext = CosmosStoreContext
type StoreCategory<'E, 'S> = CosmosStoreCategory<'E, 'S, obj>

let createPrimaryContextIgnoreMissing client queryMaxItems tipMaxEvents ignoreMissing =
    StoreContext(client, tipMaxEvents = tipMaxEvents, queryMaxItems = queryMaxItems, ignoreMissingEvents = ignoreMissing)

let createPrimaryContextEx log queryMaxItems tipMaxEvents =
    let connection = connectPrimary log
    createPrimaryContextIgnoreMissing connection queryMaxItems tipMaxEvents false

let defaultTipMaxEvents = 10

let createPrimaryContext log queryMaxItems =
    createPrimaryContextEx log queryMaxItems defaultTipMaxEvents

let createArchiveContext log queryMaxItems =
    let connection = connectArchive log
    StoreContext(connection, defaultTipMaxEvents, queryMaxItems = queryMaxItems)

let createFallbackContext log queryMaxItems =
    let connection = connectWithFallback log
    StoreContext(connection, defaultTipMaxEvents, queryMaxItems = queryMaxItems)

let defaultQueryMaxItems = 10

let createPrimaryEventsContext log queryMaxItems tipMaxItems =
    let context = createPrimaryContextEx log queryMaxItems tipMaxItems
    Core.EventsContext(context, log)

let createPrimaryEventsContextWithUnsafe log queryMaxItems tipMaxItems =
    let connection = connectPrimary log
    let create ignoreMissing =
        let context = createPrimaryContextIgnoreMissing connection queryMaxItems tipMaxItems ignoreMissing
        Core.EventsContext(context, log)
    create false, create true

let createArchiveEventsContext log queryMaxItems =
    let context = createArchiveContext log queryMaxItems
    Core.EventsContext(context, log)

let createFallbackEventsContext log queryMaxItems =
    let context = createFallbackContext log queryMaxItems
    Core.EventsContext(context, log)
