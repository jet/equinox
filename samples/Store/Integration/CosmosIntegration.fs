[<AutoOpen>]
module Samples.Store.Integration.CosmosIntegration

open Equinox.Cosmos
open System

/// Create an Equinox is compalicated,
/// To run the test,
/// Either replace connection below with a real equinox,
/// Or create a local Equinox using script: https://jet-tfs.visualstudio.com/Jet/Marvel/_git/marvel?path=%2Fservices%2Feqx-man%2Fexample.fsx&version=GBmaster&_a=contents
/// create Equinox with dbName "test" and collectionName "test" to perform test
let connectToLocalEquinoxNode() =
    EqxConnector(requestTimeout=TimeSpan.FromSeconds 3., maxRetryAttemptsOnThrottledRequests=2, maxRetryWaitTimeInSeconds=60)
        .Connect(Discovery.UriAndKey((Uri "https://localhost:8081"), "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==", "test", "test"))
let defaultBatchSize = 500
let createEqxGateway connection batchSize = EqxGateway(EqxConnection(connection), EqxBatchingPolicy(maxBatchSize = batchSize))