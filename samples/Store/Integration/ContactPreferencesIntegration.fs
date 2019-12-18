module Samples.Store.Integration.ContactPreferencesIntegration

open Equinox
open Equinox.Cosmos.Integration
open Swensen.Unquote
open Xunit

#nowarn "1182" // From hereon in, we may have some 'unused' privates (the tests)

let fold, initial = Domain.ContactPreferences.Fold.fold, Domain.ContactPreferences.Fold.initial

let createMemoryStore () =
    new MemoryStore.VolatileStore<_>()
let createServiceMemory log store =
    Backend.ContactPreferences.Service(log, MemoryStore.Resolver(store, FsCodec.Box.Codec.Create(), fold, initial).Resolve)

let codec = Domain.ContactPreferences.Events.codec
let resolveStreamGesWithOptimizedStorageSemantics gateway =
    EventStore.Resolver(gateway 1, codec, fold, initial, access = EventStore.AccessStrategy.LatestKnownEvent).Resolve
let resolveStreamGesWithoutAccessStrategy gateway =
    EventStore.Resolver(gateway defaultBatchSize, codec, fold, initial).Resolve

let resolveStreamCosmosWithLatestKnownEventSemantics gateway =
    Cosmos.Resolver(gateway 1, codec, fold, initial, Cosmos.CachingStrategy.NoCaching, Cosmos.AccessStrategy.LatestKnownEvent).Resolve
let resolveStreamCosmosUnoptimized gateway =
    Cosmos.Resolver(gateway defaultBatchSize, codec, fold, initial, Cosmos.CachingStrategy.NoCaching, Cosmos.AccessStrategy.Unoptimized).Resolve
let resolveStreamCosmosRollingUnfolds gateway =
    let access = Cosmos.AccessStrategy.Custom(Domain.ContactPreferences.Fold.isOrigin, Domain.ContactPreferences.Fold.transmute)
    Cosmos.Resolver(gateway defaultBatchSize, codec, fold, initial, Cosmos.CachingStrategy.NoCaching, access).Resolve

type Tests(testOutputHelper) =
    let testOutput = TestOutputAdapter testOutputHelper
    let createLog () = createLogger testOutput

    let act (service : Backend.ContactPreferences.Service) (id,value) = async {
        let (Domain.ContactPreferences.Id email) = id
        do! service.Update email value

        let! actual = service.Read email
        test <@ value = actual @> }

    [<AutoData>]
    let ``Can roundtrip in Memory, correctly folding the events`` args = Async.RunSynchronously <| async {
        let service = let log, store = createLog (), createMemoryStore () in createServiceMemory log store
        do! act service args
    }

    let arrange connect choose resolve = async {
        let log = createLog ()
        let! conn = connect log
        let gateway = choose conn
        return Backend.ContactPreferences.Service(log, resolve gateway) }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_EVENTSTORE")>]
    let ``Can roundtrip against EventStore, correctly folding the events with normal semantics`` args = Async.RunSynchronously <| async {
        let! service = arrange connectToLocalEventStoreNode createGesGateway resolveStreamGesWithoutAccessStrategy
        do! act service args
    }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_EVENTSTORE")>]
    let ``Can roundtrip against EventStore, correctly folding the events with compaction semantics`` args = Async.RunSynchronously <| async {
        let! service = arrange connectToLocalEventStoreNode createGesGateway resolveStreamGesWithOptimizedStorageSemantics
        do! act service args
    }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``Can roundtrip against Cosmos, correctly folding the events with Unoptimized semantics`` args = Async.RunSynchronously <| async {
        let! service = arrange connectToSpecifiedCosmosOrSimulator createCosmosContext resolveStreamCosmosUnoptimized
        do! act service args
    }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``Can roundtrip against Cosmos, correctly folding the events with LatestKnownEvent semantics`` args = Async.RunSynchronously <| async {
        let! service = arrange connectToSpecifiedCosmosOrSimulator createCosmosContext resolveStreamCosmosWithLatestKnownEventSemantics
        do! act service args
    }
    
    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``Can roundtrip against Cosmos, correctly folding the events with RollingUnfold semantics`` args = Async.RunSynchronously <| async {
        let! service = arrange connectToSpecifiedCosmosOrSimulator createCosmosContext resolveStreamCosmosRollingUnfolds
        do! act service args
    }