[<AutoOpen>]
module Samples.Store.Integration.EventStoreIntegration

open Equinox.EventStoreDb
open System

// NOTE: use `docker compose up` to establish the standard 3 node config at ports 1113/2113
let connectToLocalEventStoreNode (_log : Serilog.ILogger) =
    // NOTE: disable cert validation for this test suite. ABSOLUTELY DO NOT DO THIS FOR ANY CODE THAT WILL EVER HIT A STAGING OR PROD SERVER
    EventStoreConnector(customize = (fun c -> c.ConnectivitySettings.Insecure <- true),
                        reqTimeout=TimeSpan.FromSeconds 3., reqRetries=3, tags=["I",Guid.NewGuid() |> string]
    // Connect to the locally running EventStore Node using Gossip-driven discovery
    ).Establish("Equinox-sample", Discovery.ConnectionString "esdb://localhost:2111,localhost:2112,localhost:2113?tls=true&tlsVerifyCert=false", ConnectionStrategy.ClusterSingle EventStore.Client.NodePreference.Leader)
let defaultBatchSize = 500
let createContext connection batchSize = EventStoreContext(connection, BatchingPolicy(maxBatchSize = batchSize))
