module Samples.Store.Integration.ContactPreferencesIntegration

open Equinox
open Equinox.CosmosStore.Integration
open Swensen.Unquote

#nowarn "1182" // From hereon in, we may have some 'unused' privates (the tests)

let fold, initial = Domain.ContactPreferences.Fold.fold, Domain.ContactPreferences.Fold.initial

let createMemoryStore () = MemoryStore.VolatileStore<_>()
let createServiceMemory log store =
    Backend.ContactPreferences.create log (MemoryStore.Resolver(store, FsCodec.Box.Codec.Create(), fold, initial).Resolve)

let codec = Domain.ContactPreferences.Events.codec
let resolveStreamGesWithOptimizedStorageSemantics gateway =
    EventStore.Resolver(gateway 1, codec, fold, initial, access = EventStore.AccessStrategy.LatestKnownEvent).Resolve
let resolveStreamGesWithoutAccessStrategy gateway =
    EventStore.Resolver(gateway defaultBatchSize, codec, fold, initial).Resolve

let resolveStreamCosmosWithLatestKnownEventSemantics context =
    CosmosStore.CosmosStoreCategory(context, codec, fold, initial, CosmosStore.CachingStrategy.NoCaching, CosmosStore.AccessStrategy.LatestKnownEvent).Resolve
let resolveStreamCosmosUnoptimized context =
    CosmosStore.CosmosStoreCategory(context, codec, fold, initial, CosmosStore.CachingStrategy.NoCaching, CosmosStore.AccessStrategy.Unoptimized).Resolve
let resolveStreamCosmosRollingUnfolds context =
    let access = CosmosStore.AccessStrategy.Custom(Domain.ContactPreferences.Fold.isOrigin, Domain.ContactPreferences.Fold.transmute)
    CosmosStore.CosmosStoreCategory(context, codec, fold, initial, CosmosStore.CachingStrategy.NoCaching, access).Resolve

type Tests(testOutputHelper) =
    let testOutput = TestOutputAdapter testOutputHelper
    let createLog () = createLogger testOutput

    let act (service : Backend.ContactPreferences.Service) (id,value) = async {
        do! service.Update(id, value)

        let! actual = service.Read id
        test <@ value = actual @> }

    [<AutoData>]
    let ``Can roundtrip in Memory, correctly folding the events`` args = Async.RunSynchronously <| async {
        let log, store = createLog (), createMemoryStore ()
        let service = createServiceMemory log store
        do! act service args
    }

    let arrangeEs connect choose resolve = async {
        let log = createLog ()
        let! conn = connect log
        let gateway = choose conn
        return Backend.ContactPreferences.create log (resolve gateway) }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_EVENTSTORE")>]
    let ``Can roundtrip against EventStore, correctly folding the events with normal semantics`` args = Async.RunSynchronously <| async {
        let! service = arrangeEs connectToLocalEventStoreNode createGesGateway resolveStreamGesWithoutAccessStrategy
        do! act service args
    }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_EVENTSTORE")>]
    let ``Can roundtrip against EventStore, correctly folding the events with compaction semantics`` args = Async.RunSynchronously <| async {
        let! service = arrangeEs connectToLocalEventStoreNode createGesGateway resolveStreamGesWithOptimizedStorageSemantics
        do! act service args
    }

    let arrangeCosmos connect resolve queryMaxItems =
        let log = createLog ()
        let ctx: CosmosStore.CosmosStoreContext = connect log queryMaxItems
        Backend.ContactPreferences.create log (resolve ctx)

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
