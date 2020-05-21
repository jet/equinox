module Samples.Store.Integration.CartIntegration

open Equinox
open Equinox.Cosmos.Integration
open Swensen.Unquote

#nowarn "1182" // From hereon in, we may have some 'unused' privates (the tests)

let fold, initial = Domain.Cart.Fold.fold, Domain.Cart.Fold.initial
let snapshot = Domain.Cart.Fold.isOrigin, Domain.Cart.Fold.snapshot

let createMemoryStore () = MemoryStore.VolatileStore<byte[]>()
let createServiceMemory log store =
    Backend.Cart.create log (fun (id,opt) -> MemoryStore.Resolver(store, Domain.Cart.Events.codec, fold, initial).Resolve(id,?option=opt))

let codec = Domain.Cart.Events.codec

let resolveGesStreamWithRollingSnapshots gateway =
    fun (id,opt) -> EventStore.Resolver(gateway, codec, fold, initial, access = EventStore.AccessStrategy.RollingSnapshots snapshot).Resolve(id,?option=opt)
let resolveGesStreamWithoutCustomAccessStrategy gateway =
    fun (id,opt) -> EventStore.Resolver(gateway, codec, fold, initial).Resolve(id,?option=opt)

let resolveCosmosStreamWithSnapshotStrategy gateway =
    fun (id,opt) -> Cosmos.Resolver(gateway, codec, fold, initial, Cosmos.CachingStrategy.NoCaching, Cosmos.AccessStrategy.Snapshot snapshot).Resolve(id,?option=opt)
let resolveCosmosStreamWithoutCustomAccessStrategy gateway =
    fun (id,opt) -> Cosmos.Resolver(gateway, codec, fold, initial, Cosmos.CachingStrategy.NoCaching, Cosmos.AccessStrategy.Unoptimized).Resolve(id,?option=opt)

let addAndThenRemoveItemsManyTimesExceptTheLastOne context cartId skuId (service: Backend.Cart.Service) count =
    service.ExecuteManyAsync(cartId, false, seq {
        for i in 1..count do
            yield Domain.Cart.SyncItem (context, skuId, Some i, None)
            if i <> count then
                yield Domain.Cart.SyncItem (context, skuId, Some 0, None) })

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

    let arrange connect choose resolve = async {
        let log = createLog ()
        let! conn = connect log
        let gateway = choose conn defaultBatchSize
        return Backend.Cart.create log (resolve gateway) }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_EVENTSTORE")>]
    let ``Can roundtrip against EventStore, correctly folding the events without compaction semantics`` args = Async.RunSynchronously <| async {
        let! service = arrange connectToLocalEventStoreNode createGesGateway resolveGesStreamWithoutCustomAccessStrategy
        do! act service args
    }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_EVENTSTORE")>]
    let ``Can roundtrip against EventStore, correctly folding the events with RollingSnapshots`` args = Async.RunSynchronously <| async {
        let! service = arrange connectToLocalEventStoreNode createGesGateway resolveGesStreamWithRollingSnapshots
        do! act service args
    }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``Can roundtrip against Cosmos, correctly folding the events without custom access strategy`` args = Async.RunSynchronously <| async {
        let! service = arrange connectToSpecifiedCosmosOrSimulator createCosmosContext resolveCosmosStreamWithoutCustomAccessStrategy
        do! act service args
    }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``Can roundtrip against Cosmos, correctly folding the events with With Snapshotting`` args = Async.RunSynchronously <| async {
        let! service = arrange connectToSpecifiedCosmosOrSimulator createCosmosContext resolveCosmosStreamWithSnapshotStrategy
        do! act service args
    }
