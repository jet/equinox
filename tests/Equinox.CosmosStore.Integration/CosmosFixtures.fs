#if STORE_DYNAMO
[<AutoOpen>]
module Equinox.DynamoStore.Integration.CosmosFixtures

open Amazon.DynamoDBv2
open Equinox.DynamoStore
open System

// docker compose up dynamodb-local will stand up a simulator instance that this wiring can connect to
let private tryRead env = Environment.GetEnvironmentVariable env |> Option.ofObj
let private tableName = tryRead "EQUINOX_DYNAMO_TABLE" |> Option.defaultValue "equinox-test"
let private archiveTableName = tryRead "EQUINOX_DYNAMO_TABLE_ARCHIVE" |> Option.defaultValue "equinox-test-archive"

let discoverConnection () =
    match tryRead "EQUINOX_DYNAMO_CONNECTION" with
    | None -> "dynamodb-local", "http://localhost:8000"
    | Some connectionString -> "EQUINOX_DYNAMO_CONNECTION", connectionString

let createClient (log : Serilog.ILogger) name serviceUrl =
    // See https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.DownloadingAndRunning.html#docker for details of how to deploy a simulator instance
    let clientConfig = AmazonDynamoDBConfig(ServiceURL = serviceUrl)
    log.Information("DynamoStore {name} {endpoint}", name, serviceUrl)
    // Credentials are not validated if connecting to local instance so anything will do (this avoids it looking for profiles to be configured)
    let credentials = Amazon.Runtime.BasicAWSCredentials("A", "A")
    new AmazonDynamoDBClient(credentials, clientConfig) :> IAmazonDynamoDB

let connectPrimary log =
    let name, serviceUrl = discoverConnection ()
    let client = createClient log name serviceUrl
    DynamoStoreClient(client, tableName)

let connectArchive log =
    let name, serviceUrl = discoverConnection ()
    let client = createClient log name serviceUrl
    DynamoStoreClient(client, archiveTableName)

let connectWithFallback log =
    let name, serviceUrl = discoverConnection ()
    let client = createClient log name serviceUrl
    DynamoStoreClient(client, tableName, archiveTableName = archiveTableName)

// Prepares the two required tables that the test lea on via connectPrimary/Archive/WithFallback
type DynamoTablesFixture() =

    interface Xunit.IAsyncLifetime with
        member _.InitializeAsync() =
            let name, serviceUrl = discoverConnection ()
            let client = createClient Serilog.Log.Logger name serviceUrl
            let throughput = ProvisionedThroughput (100L, 100L)
            let throughput = Throughput.Provisioned throughput
            DynamoStoreClient.Connect(client, tableName, archiveTableName = archiveTableName, mode = CreateIfNotExists throughput)
            |> Async.StartImmediateAsTask
            :> System.Threading.Tasks.Task
        member _.DisposeAsync() = task { () }

[<Xunit.CollectionDefinition "DocStore">]
type DocStoreCollection() =
    interface Xunit.ICollectionFixture<DynamoTablesFixture>

type StoreContext = DynamoStoreContext
type StoreCategory<'E, 'S> = DynamoStoreCategory<'E, 'S, obj>
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
#endif

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
