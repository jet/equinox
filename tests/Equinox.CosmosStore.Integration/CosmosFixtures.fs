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
let createConnector log =
    connect log

// Prepares the two required tables that the tests use via connect + tableName/archiveTableName
type DynamoTablesFixture() =

    interface Xunit.IAsyncLifetime with
        member _.InitializeAsync() =
            let client = connect Serilog.Log.Logger
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
    DynamoStoreContext(createConnector log, archiveTableName, defaultTipMaxEvents, queryMaxItems = queryMaxItems)
let createFallbackContext log queryMaxItems =
    DynamoStoreContext(createConnector log, tableName, defaultTipMaxEvents, queryMaxItems = queryMaxItems, archiveTableName = archiveTableName)

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

// see https://github.com/jet/equinox#provisioning-cosmosdb for details of what's expected in terms of provisioned containers etc
let discoverConnection () =
    match tryRead "EQUINOX_COSMOS_CONNECTION" with
    | None -> "localDocDbSim", Discovery.AccountUriAndKey(Uri "https://localhost:8081", "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==")
    | Some connectionString -> "EQUINOX_COSMOS_CONNECTION", Discovery.ConnectionString connectionString

let createConnector (log: Serilog.ILogger) =
    let name, discovery = discoverConnection ()
    let connector = CosmosStoreConnector(discovery, requestTimeout = TimeSpan.FromSeconds 3.,
                                         maxRetryAttemptsOnRateLimitedRequests = 2, maxRetryWaitTimeOnRateLimitedRequests = TimeSpan.FromMinutes 1.)
    log.Information("CosmosStore {name} {endpoint}", name, connector.Endpoint)
    connector

[<Xunit.CollectionDefinition "DocStore">]
type DocStoreCollection() =
    do ()

let createPrimaryContextIgnoreMissing (connector: CosmosStoreConnector) containerId queryMaxItems tipMaxEvents ignoreMissing =
    let client = connector.Connect(databaseId, [| containerId |]) |> Async.RunSynchronously
    CosmosStoreContext(client, databaseId, containerId, tipMaxEvents = tipMaxEvents, queryMaxItems = queryMaxItems, ignoreMissingEvents = ignoreMissing)

let defaultTipMaxEvents = 10
let createArchiveContext log queryMaxItems =
    let client = (createConnector log).Connect(databaseId, [| archiveContainerId |]) |> Async.RunSynchronously
    CosmosStoreContext(client, databaseId, archiveContainerId, defaultTipMaxEvents, queryMaxItems = queryMaxItems)
let createFallbackContext log queryMaxItems =
    let client = (createConnector log).Connect(databaseId, [| containerId; archiveContainerId |]) |> Async.RunSynchronously
    CosmosStoreContext(client, databaseId, containerId, defaultTipMaxEvents, queryMaxItems = queryMaxItems, archiveContainerId = archiveContainerId)

type StoreContext = CosmosStoreContext
type StoreCategory<'E, 'S> = CosmosStoreCategory<'E, 'S, unit>
let primaryTarget = containerId
#endif

let createPrimaryContextEx log queryMaxItems tipMaxEvents =
    createPrimaryContextIgnoreMissing (createConnector log) primaryTarget queryMaxItems tipMaxEvents false

let createPrimaryContext log queryMaxItems =
    createPrimaryContextEx log queryMaxItems defaultTipMaxEvents

let createPrimaryEventsContext log queryMaxItems tipMaxItems =
    Core.EventsContext(createPrimaryContextEx log queryMaxItems tipMaxItems, log)

let createPrimaryEventsContextWithUnsafe log queryMaxItems tipMaxItems =
    let connector = createConnector log
    let create ignoreMissing = Core.EventsContext(createPrimaryContextIgnoreMissing connector primaryTarget queryMaxItems tipMaxItems ignoreMissing, log)
    create false, create true

let createArchiveEventsContext log queryMaxItems =
    Core.EventsContext(createArchiveContext log queryMaxItems, log)

let createFallbackEventsContext log queryMaxItems =
    Core.EventsContext(createFallbackContext log queryMaxItems, log)

let defaultQueryMaxItems = 10
