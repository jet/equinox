[<AutoOpen>]
module Samples.Store.Integration.EventStoreIntegration

open Equinox.EventStore
open System

let connectToLocalEventStoreNode log =
    // NOTE: disable cert validation for this test suite. ABSOLUTELY DO NOT DO THIS FOR ANY CODE THAT WILL EVER HIT A STAGING OR PROD SERVER
    EventStoreConnector("admin", "changeit", custom = (fun c -> c.DisableServerCertificateValidation()),
                        reqTimeout=TimeSpan.FromSeconds 3., reqRetries=3, log=Logger.SerilogVerbose log, tags=["I",Guid.NewGuid() |> string]
    // Connect directly to the locally running EventStore Node without using Gossip-driven discovery
    ).Establish("Equinox-sample", Discovery.Uri(Uri "tcp://localhost:1113"), ConnectionStrategy.ClusterSingle NodePreference.Master)
let defaultBatchSize = 500
let createContext connection batchSize = EventStoreContext(connection, BatchingPolicy(maxBatchSize = batchSize))
