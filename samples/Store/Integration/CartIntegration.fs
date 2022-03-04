module Samples.Store.Integration.CartIntegration

open Domain
open Equinox
open Equinox.CosmosStore.Integration
open Swensen.Unquote

#nowarn "1182" // From hereon in, we may have some 'unused' privates (the tests)

let fold, initial = Cart.Fold.fold, Cart.Fold.initial
let snapshot = Cart.Fold.isOrigin, Cart.Fold.snapshot

let createMemoryStore () = MemoryStore.VolatileStore<byte[]>()
let createServiceMemory log store =
    Cart.create log (fun (id,opt) -> MemoryStore.MemoryStoreCategory(store, Domain.Cart.Events.codec, fold, initial).Resolve(id,?option=opt))

let codec = Cart.Events.codec
let codecStj = Cart.Events.codecStj

let resolveGesStreamWithRollingSnapshots context =
    fun (id,opt) -> EventStore.EventStoreCategory(context, codec, fold, initial, access = EventStore.AccessStrategy.RollingSnapshots snapshot).Resolve(id,?option=opt)
let resolveGesStreamWithoutCustomAccessStrategy context =
    fun (id,opt) -> EventStore.EventStoreCategory(context, codec, fold, initial).Resolve(id,?option=opt)

let resolveCosmosStreamWithSnapshotStrategy context =
    fun (id,opt) -> CosmosStore.CosmosStoreCategory(context, codecStj, fold, initial, CosmosStore.CachingStrategy.NoCaching, CosmosStore.AccessStrategy.Snapshot snapshot).Resolve(id,?option=opt)
let resolveCosmosStreamWithoutCustomAccessStrategy context =
    fun (id,opt) -> CosmosStore.CosmosStoreCategory(context, codecStj, fold, initial, CosmosStore.CachingStrategy.NoCaching, CosmosStore.AccessStrategy.Unoptimized).Resolve(id,?option=opt)

let addAndThenRemoveItemsManyTimesExceptTheLastOne context cartId skuId (service: Cart.Service) count =
    service.ExecuteManyAsync(cartId, false, seq {
        for i in 1..count do
            yield Cart.SyncItem (context, skuId, Some i, None)
            if i <> count then
                yield Cart.SyncItem (context, skuId, Some 0, None) })

type Tests(testOutputHelper) =
    let testOutput = TestOutputAdapter testOutputHelper
    let createLog () = createLogger testOutput

    let act service (context,cartId,skuId) = async {
        do! addAndThenRemoveItemsManyTimesExceptTheLastOne context cartId skuId service 5

        let! state = service.Read cartId
        test <@ 5 = match state with { items = [{ quantity = quantity }] } -> quantity | _ -> failwith "nope" @>
    }

    [<AutoData>]
    let ``Can roundtrip in Memory, correctly folding the events`` args = Async.RunSynchronously <| async {
        let log, store = createLog (), createMemoryStore ()
        let service = createServiceMemory log store
        do! act service args
    }

    let arrangeEs connect choose resolveStream = async {
        let log = createLog ()
        let! client = connect log
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
        let log = createLog ()
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
