module Samples.Store.Integration.ContactPreferencesIntegration

open Equinox
open Equinox.Cosmos.Integration
open Swensen.Unquote
open Xunit

#nowarn "1182" // From hereon in, we may have some 'unused' privates (the tests)

let fold, initial = Domain.ContactPreferences.Folds.fold, Domain.ContactPreferences.Folds.initial

let createMemoryStore () =
    new MemoryStore.VolatileStore()
let createServiceMemory log store =
    Backend.ContactPreferences.Service(log, MemoryStore.Resolver(store, fold, initial).Resolve)

let codec = genCodec<Domain.ContactPreferences.Events.Event>()
let resolveStreamGesWithOptimizedStorageSemantics gateway =
    EventStore.Resolver(gateway 1, codec, fold, initial, access = EventStore.AccessStrategy.EventsAreState).Resolve
let resolveStreamGesWithoutAccessStrategy gateway =
    EventStore.Resolver(gateway defaultBatchSize, codec, fold, initial).Resolve

let resolveStreamCosmosWithKnownEventTypeSemantics gateway =
    Cosmos.Resolver(gateway 1, codec, fold, initial, Cosmos.CachingStrategy.NoCaching, Cosmos.AccessStrategy.AnyKnownEventType).Resolve
let resolveStreamCosmosWithoutCustomAccessStrategy gateway =
    Cosmos.Resolver(gateway defaultBatchSize, codec, fold, initial, Cosmos.CachingStrategy.NoCaching).Resolve
let resolveStreamCosmosRollingUnfolds gateway =
    let access = Cosmos.AccessStrategy.RollingUnfolds(Domain.ContactPreferences.Folds.isOrigin, Domain.ContactPreferences.Folds.transmute)
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

    let arrange connect choose resolveStream = async {
        let log = createLog ()
        let! conn = connect log
        let gateway = choose conn
        return Backend.ContactPreferences.Service(log, resolveStream gateway) }

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
    let ``Can roundtrip against Cosmos, correctly folding the events with normal semantics`` args = Async.RunSynchronously <| async {
        let! service = arrange connectToSpecifiedCosmosOrSimulator createCosmosContext resolveStreamCosmosWithoutCustomAccessStrategy
        do! act service args
    }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``Can roundtrip against Cosmos, correctly folding the events with compaction semantics`` args = Async.RunSynchronously <| async {
        let! service = arrange connectToSpecifiedCosmosOrSimulator createCosmosContext resolveStreamCosmosWithKnownEventTypeSemantics
        do! act service args
    }
    
    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``Can roundtrip against Cosmos, correctly folding the events with RollingUnfold semantics`` args = Async.RunSynchronously <| async {
        let! service = arrange connectToSpecifiedCosmosOrSimulator createCosmosContext resolveStreamCosmosRollingUnfolds
        do! act service args
    }