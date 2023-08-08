#if STORE_DYNAMO
[<AutoOpen>]
module Equinox.DynamoStore.Integration.CosmosFixtures

open Amazon.DynamoDBv2
open Equinox.DynamoStore

// docker compose up dynamodb-local will stand up a simulator instance that this wiring can connect to
let private tryRead env = System.Environment.GetEnvironmentVariable env |> Option.ofObj
let private tableName = tryRead "EQUINOX_DYNAMO_TABLE" |> Option.defaultValue "equinox-test"
let private archiveTableName = tryRead "EQUINOX_DYNAMO_TABLE_ARCHIVE" |> Option.defaultValue "equinox-test-archive"

let discoverConnection () =
    // NOTE NOT using the EQUINOX_DYNAMO_SERVICE_URL env var that's commonly used in dotnet-templates, as we don't want to go provisioning 2 tables in a random DB
    match tryRead "EQUINOX_DYNAMO_CONNECTION" with
    | None -> "dynamodb-local", "http://localhost:8000"
    | Some connectionString -> "EQUINOX_DYNAMO_CONNECTION", connectionString // e.g "https://dynamodb.eu-west-1.amazonaws.com"
let isSimulatorServiceUrl url = System.Uri(url).IsLoopback

let createClient (log : Serilog.ILogger) name serviceUrl =
    // See https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.DownloadingAndRunning.html#docker for details of how to deploy a simulator instance
    let clientConfig = AmazonDynamoDBConfig(ServiceURL = serviceUrl)
    log.Information("DynamoStore {name} {endpoint}", name, serviceUrl)
    if isSimulatorServiceUrl serviceUrl then
        // Credentials are not validated if connecting to local instance so anything will do (this avoids it looking for profiles to be configured)
        let credentials = Amazon.Runtime.BasicAWSCredentials("A", "A")
        new AmazonDynamoDBClient(credentials, clientConfig) |> DynamoStoreClient
    else
        // omitting credentials to ctor in order to trigger use of keychain configured access
        new AmazonDynamoDBClient(clientConfig) |> DynamoStoreClient

let connect log =
    let name, serviceUrl = discoverConnection ()
    createClient log name serviceUrl

// Prepares the two required tables that the tests use via connect + tableName/archiveTableName
type DynamoTablesFixture() =

    interface Xunit.IAsyncLifetime with
        member _.InitializeAsync() =
            let name, serviceUrl = discoverConnection ()
            let client = createClient Serilog.Log.Logger name serviceUrl
            let throughput = ProvisionedThroughput (100L, 100L)
            let throughput = Throughput.Provisioned throughput
            DynamoStoreContext.Establish(client, tableName, archiveTableName = archiveTableName, mode = CreateIfNotExists throughput)
            |> Async.Ignore<DynamoStoreContext>
            |> Async.StartImmediateAsTask
            |> FSharp.Control.Task.ignore
        member _.DisposeAsync() = task { () }

[<Xunit.CollectionDefinition "DocStore">]
type DocStoreCollection() =
    interface Xunit.ICollectionFixture<DynamoTablesFixture>

let createPrimaryContextIgnoreMissing client tableName queryMaxItems tipMaxEvents ignoreMissing =
    DynamoStoreContext(client, tableName, tipMaxEvents = tipMaxEvents, queryMaxItems = queryMaxItems, ignoreMissingEvents = ignoreMissing)
let defaultTipMaxEvents = 10
let createArchiveContext log queryMaxItems =
    let client = connect log
    DynamoStoreContext(client, archiveTableName, defaultTipMaxEvents, queryMaxItems = queryMaxItems)
let createFallbackContext log queryMaxItems =
    let client = connect log
    DynamoStoreContext(client, tableName, defaultTipMaxEvents, queryMaxItems = queryMaxItems, archiveTableName = archiveTableName)

type StoreContext = DynamoStoreContext
type StoreCategory<'E, 'S> = DynamoStoreCategory<'E, 'S, unit>
let primaryTarget = tableName
#else
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

// see https://github.com/jet/equinox-provisioning-cosmosdb for details of what's expected in terms of provisioned containers etc
let discoverConnection () =
    match tryRead "EQUINOX_COSMOS_CONNECTION" with
    | None -> "localDocDbSim", Discovery.AccountUriAndKey(Uri "https://localhost:8081", "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==")
    | Some connectionString -> "EQUINOX_COSMOS_CONNECTION", Discovery.ConnectionString connectionString

let createClient (log : Serilog.ILogger) name (discovery : Discovery) =
    let connector = CosmosStoreConnector(discovery, requestTimeout = TimeSpan.FromSeconds 3.,
                                         maxRetryAttemptsOnRateLimitedRequests = 2, maxRetryWaitTimeOnRateLimitedRequests = TimeSpan.FromMinutes 1.)
    log.Information("CosmosStore {name} {endpoint}", name, discovery.Endpoint)
    connector.CreateUninitialized()

let connect log =
    let name, discovery = discoverConnection ()
    let client = createClient log name discovery
    CosmosStoreClient(client)

[<Xunit.CollectionDefinition "DocStore">]
type DocStoreCollection() =
    do ()

let createPrimaryContextIgnoreMissing client containerId queryMaxItems tipMaxEvents ignoreMissing =
    CosmosStoreContext(client, databaseId, containerId, tipMaxEvents = tipMaxEvents, queryMaxItems = queryMaxItems, ignoreMissingEvents = ignoreMissing)

let defaultTipMaxEvents = 10
let createArchiveContext log queryMaxItems =
    let client = connect log
    CosmosStoreContext(client, databaseId, containerId, defaultTipMaxEvents, queryMaxItems = queryMaxItems)
let createFallbackContext log queryMaxItems =
    let client = connect log
    CosmosStoreContext(client, databaseId, containerId, defaultTipMaxEvents, queryMaxItems = queryMaxItems, archiveContainerId = archiveContainerId)

type StoreContext = CosmosStoreContext
type StoreCategory<'E, 'S> = CosmosStoreCategory<'E, 'S, unit>
let primaryTarget = containerId
#endif

let createPrimaryContextEx log queryMaxItems tipMaxEvents =
    let client = connect log
    createPrimaryContextIgnoreMissing client primaryTarget queryMaxItems tipMaxEvents false

let createPrimaryContext log queryMaxItems =
    createPrimaryContextEx log queryMaxItems defaultTipMaxEvents

let defaultQueryMaxItems = 10

let createPrimaryEventsContext log queryMaxItems tipMaxItems =
    let context = createPrimaryContextEx log queryMaxItems tipMaxItems
    Core.EventsContext(context, log)

let createPrimaryEventsContextWithUnsafe log queryMaxItems tipMaxItems =
    let client = connect log
    let create ignoreMissing =
        let context = createPrimaryContextIgnoreMissing client primaryTarget queryMaxItems tipMaxItems ignoreMissing
        Core.EventsContext(context, log)
    create false, create true

let createArchiveEventsContext log queryMaxItems =
    let context = createArchiveContext log queryMaxItems
    Core.EventsContext(context, log)

let createFallbackEventsContext log queryMaxItems =
    let context = createFallbackContext log queryMaxItems
    Core.EventsContext(context, log)
