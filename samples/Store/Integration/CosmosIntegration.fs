[<AutoOpen>]
module Samples.Store.Integration.CosmosIntegration

open Equinox.Cosmos
open System

/// Standing up an Equinox instance is complicated; to run for test purposes either:
/// - replace connection below with a connection string or Uri+Key for an initialized Equinox instance
/// - Create a local Equinox with dbName "test" and collectionName "test" using script:
///   /src/Equinox.Cosmos/EquinoxManager.fsx
let connectToLocalEquinoxNode log =
    EqxConnector(log=log, requestTimeout=TimeSpan.FromSeconds 3., maxRetryAttemptsOnThrottledRequests=2, maxRetryWaitTimeInSeconds=60)
        .Establish("equinoxStoreSampleIntegration", Discovery.UriAndKey(Uri "https://localhost:8081", "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=="))
let defaultBatchSize = 500
let createEqxGateway connection batchSize = EqxGateway(connection, EqxBatchingPolicy(maxBatchSize = batchSize))
// Typically, one will split different categories of stream into Cosmos collections - hard coding this is thus an oversimplification
let (|StreamArgs|) streamName =
    let databaseId, collectionId = "test", "test"
    databaseId, collectionId, streamName