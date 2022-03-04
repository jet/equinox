module Samples.Store.Integration.ContactPreferencesIntegration

open Domain
open Equinox
open Equinox.CosmosStore.Integration
open Swensen.Unquote

#nowarn "1182" // From hereon in, we may have some 'unused' privates (the tests)

let fold, initial = ContactPreferences.Fold.fold, ContactPreferences.Fold.initial

let createMemoryStore () = MemoryStore.VolatileStore<_>()
let createServiceMemory log store =
    ContactPreferences.create log (MemoryStore.MemoryStoreCategory(store, FsCodec.Box.Codec.Create(), fold, initial).Resolve)

let codec = ContactPreferences.Events.codec
let codecStj = ContactPreferences.Events.codecStj
let resolveStreamGesWithOptimizedStorageSemantics context =
    EventStore.EventStoreCategory(context 1, codec, fold, initial, access = EventStore.AccessStrategy.LatestKnownEvent).Resolve
let resolveStreamGesWithoutAccessStrategy context =
    EventStore.EventStoreCategory(context defaultBatchSize, codec, fold, initial).Resolve

let resolveStreamCosmosWithLatestKnownEventSemantics context =
    CosmosStore.CosmosStoreCategory(context, codecStj, fold, initial, CosmosStore.CachingStrategy.NoCaching, CosmosStore.AccessStrategy.LatestKnownEvent).Resolve
let resolveStreamCosmosUnoptimized context =
    CosmosStore.CosmosStoreCategory(context, codecStj, fold, initial, CosmosStore.CachingStrategy.NoCaching, CosmosStore.AccessStrategy.Unoptimized).Resolve
let resolveStreamCosmosRollingUnfolds context =
    let access = CosmosStore.AccessStrategy.Custom(ContactPreferences.Fold.isOrigin, ContactPreferences.Fold.transmute)
    CosmosStore.CosmosStoreCategory(context, codecStj, fold, initial, CosmosStore.CachingStrategy.NoCaching, access).Resolve

type Tests(testOutputHelper) =
    let testOutput = TestOutputAdapter testOutputHelper
    let createLog () = createLogger testOutput

    let act (service : ContactPreferences.Service) (id,value) = async {
        do! service.Update(id, value)

        let! actual = service.Read id
        test <@ value = actual @> }

    [<AutoData>]
    let ``Can roundtrip in Memory, correctly folding the events`` args = Async.RunSynchronously <| async {
        let log, store = createLog (), createMemoryStore ()
        let service = createServiceMemory log store
        do! act service args
    }

    let arrangeEs connect choose resolveStream = async {
        let log = createLog ()
        let! client = connect log
        let context = choose client
        return ContactPreferences.create log (resolveStream context) }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_EVENTSTORE")>]
    let ``Can roundtrip against EventStore, correctly folding the events with normal semantics`` args = Async.RunSynchronously <| async {
        let! service = arrangeEs connectToLocalEventStoreNode createContext resolveStreamGesWithoutAccessStrategy
        do! act service args
    }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_EVENTSTORE")>]
    let ``Can roundtrip against EventStore, correctly folding the events with compaction semantics`` args = Async.RunSynchronously <| async {
        let! service = arrangeEs connectToLocalEventStoreNode createContext resolveStreamGesWithOptimizedStorageSemantics
        do! act service args
    }

    let arrangeCosmos connect resolveStream queryMaxItems =
        let log = createLog ()
        let context: CosmosStore.CosmosStoreContext = connect log queryMaxItems
        ContactPreferences.create log (resolveStream context)

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``Can roundtrip against Cosmos, correctly folding the events with Unoptimized semantics`` args = Async.RunSynchronously <| async {
        let service = arrangeCosmos createPrimaryContext resolveStreamCosmosUnoptimized defaultQueryMaxItems
        do! act service args
    }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``Can roundtrip against Cosmos, correctly folding the events with LatestKnownEvent semantics`` args = Async.RunSynchronously <| async {
        let service = arrangeCosmos createPrimaryContext resolveStreamCosmosWithLatestKnownEventSemantics 1
        do! act service args
    }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``Can roundtrip against Cosmos, correctly folding the events with RollingUnfold semantics`` args = Async.RunSynchronously <| async {
        let service = arrangeCosmos createPrimaryContext resolveStreamCosmosRollingUnfolds defaultQueryMaxItems
        do! act service args
    }
