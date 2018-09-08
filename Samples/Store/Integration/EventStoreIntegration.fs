[<AutoOpen>]
module Samples.Store.Integration.EventStoreIntegration

open Foldunk.EventStore
open System

let serializationSettings = Foldunk.Serialization.Settings.CreateDefault()
let genCodec<'T> = Foldunk.UnionCodec.generateJsonUtf8UnionCodec<'T> serializationSettings

/// Connect with Gossip based cluster discovery using the default Commercial edition Manager port config
/// Such a config can be simulated on a single node with zero config via the EventStore OSS package:-
///   1. cinst eventstore-oss -y # where cinst is an invocation of the Chocolatey Package Installer on Windows
///   2. & $env:ProgramData\chocolatey\bin\EventStore.ClusterNode.exe --gossip-on-single-node --discover-via-dns 0 --ext-http-port=30778
/// (the normal external port also hosts the server metadata endpoint; with above, can see gossip info by going to http://127.0.0.1:30778/gossip)
let connectToLocalEventStoreNode log =
    GesConnector("admin", "changeit", reqTimeout=TimeSpan.FromSeconds 3., reqRetries=3, log=Logger.SerilogVerbose log, tags=["I",Guid.NewGuid() |> string])
        .Establish("Foldunk-sample", Discovery.GossipDns "localhost", ConnectionStrategy.ClusterTwinPreferSlaveReads)
let defaultBatchSize = 500
let createGesGateway connection batchSize = GesGateway(connection, GesBatchingPolicy(maxBatchSize = batchSize))