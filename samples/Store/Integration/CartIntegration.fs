module Samples.Store.Integration.CartIntegration

open Domain
open Equinox
open Equinox.CosmosStore.Integration.CosmosFixtures
open Swensen.Unquote

let fold, initial = Cart.Fold.fold, Cart.Fold.initial
let snapshot = Cart.Fold.isOrigin, Cart.Fold.snapshot

let createMemoryStore () = MemoryStore.VolatileStore<byte[]>()
let createServiceMemory log store =
    Cart.create log (MemoryStore.MemoryStoreCategory(store, Cart.Events.codec, fold, initial).Resolve)

let codec = Cart.Events.codec
let codecStj = Cart.Events.codecStj

let resolveGesStreamWithRollingSnapshots context =
    EventStoreDb.EventStoreCategory(context, codec, fold, initial, access = EventStoreDb.AccessStrategy.RollingSnapshots snapshot).Resolve
let resolveGesStreamWithoutCustomAccessStrategy context =
    EventStoreDb.EventStoreCategory(context, codec, fold, initial).Resolve

let resolveCosmosStreamWithSnapshotStrategy context =
    CosmosStore.CosmosStoreCategory(context, codecStj, fold, initial, CosmosStore.CachingStrategy.NoCaching, CosmosStore.AccessStrategy.Snapshot snapshot).Resolve
let resolveCosmosStreamWithoutCustomAccessStrategy context =
    CosmosStore.CosmosStoreCategory(context, codecStj, fold, initial, CosmosStore.CachingStrategy.NoCaching, CosmosStore.AccessStrategy.Unoptimized).Resolve

let addAndThenRemoveItemsManyTimesExceptTheLastOne context cartId skuId (service: Cart.Service) count =
    service.ExecuteManyAsync(cartId, false, seq {
        for i in 1..count do
            yield Cart.SyncItem (context, skuId, Some i, None)
            if i <> count then
                yield Cart.SyncItem (context, skuId, Some 0, None) })

type Tests(testOutputHelper) =
    let testOutput = TestOutput testOutputHelper
    let log = testOutput.CreateLogger()

    let act service (context,cartId,skuId) = async {
        do! addAndThenRemoveItemsManyTimesExceptTheLastOne context cartId skuId service 5

        let! state = service.Read cartId
        test <@ 5 = match state with { items = [{ quantity = quantity }] } -> quantity | _ -> failwith "nope" @>
    }

    [<AutoData>]
    let ``Can roundtrip in Memory, correctly folding the events`` args = Async.RunSynchronously <| async {
        let store = createMemoryStore ()
        let service = createServiceMemory log store
        do! act service args
    }

    let arrangeEs connect choose resolveStream = async {
        let client = connect log
        let context = choose client defaultBatchSize
        return Cart.create log (resolveStream context) }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_EVENTSTORE")>]
    let ``Can roundtrip against EventStore, correctly folding the events without compaction semantics`` args = Async.RunSynchronously <| async {
        let! service = arrangeEs connectToLocalEventStoreNode createContext resolveGesStreamWithoutCustomAccessStrategy
        do! act service args
    }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_EVENTSTORE")>]
    let ``Can roundtrip against EventStore, correctly folding the events with RollingSnapshots`` args = Async.RunSynchronously <| async {
        let! service = arrangeEs connectToLocalEventStoreNode createContext resolveGesStreamWithRollingSnapshots
        do! act service args
    }

    let arrangeCosmos connect resolveStream =
        let context : CosmosStore.CosmosStoreContext = connect log defaultQueryMaxItems
        Cart.create log (resolveStream context)

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``Can roundtrip against Cosmos, correctly folding the events without custom access strategy`` args = Async.RunSynchronously <| async {
        let service = arrangeCosmos createPrimaryContext resolveCosmosStreamWithoutCustomAccessStrategy
        do! act service args
    }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``Can roundtrip against Cosmos, correctly folding the events with With Snapshotting`` args = Async.RunSynchronously <| async {
        let service = arrangeCosmos createPrimaryContext resolveCosmosStreamWithSnapshotStrategy
        do! act service args
    }
