﻿module Samples.Store.Integration.CartIntegration

open Equinox
open Equinox.CosmosStore.Integration
open Equinox.EventStore
open Equinox.MemoryStore
open Swensen.Unquote

#nowarn "1182" // From hereon in, we may have some 'unused' privates (the tests)

let fold, initial = Domain.Cart.Fold.fold, Domain.Cart.Fold.initial
let snapshot = Domain.Cart.Fold.isOrigin, Domain.Cart.Fold.snapshot

let createMemoryStore () =
    // we want to validate that the JSON UTF8 is working happily
    VolatileStore<byte[]>()
let createServiceMemory log store =
    Backend.Cart.create log (fun (id,opt) -> MemoryStore.Resolver(store, Domain.Cart.Events.codecNewtonsoft, fold, initial).Resolve(id,?option=opt))

let eventStoreCodec = Domain.Cart.Events.codecNewtonsoft
let resolveGesStreamWithRollingSnapshots gateway =
    fun (id,opt) -> EventStore.Resolver(gateway, eventStoreCodec, fold, initial, access = AccessStrategy.RollingSnapshots snapshot).Resolve(id,?option=opt)
let resolveGesStreamWithoutCustomAccessStrategy gateway =
    fun (id,opt) -> EventStore.Resolver(gateway, eventStoreCodec, fold, initial).Resolve(id,?option=opt)

let cosmosCodec = Domain.Cart.Events.codecStj (FsCodec.SystemTextJson.Options.Create())
let resolveCosmosStreamWithSnapshotStrategy context =
    fun (id,opt) -> CosmosStore.CosmosStoreCategory(context, cosmosCodec, fold, initial, CosmosStore.CachingStrategy.NoCaching, CosmosStore.AccessStrategy.Snapshot snapshot).Resolve(id,?option=opt)
let resolveCosmosStreamWithoutCustomAccessStrategy context =
    fun (id,opt) -> CosmosStore.CosmosStoreCategory(context, cosmosCodec, fold, initial, CosmosStore.CachingStrategy.NoCaching, CosmosStore.AccessStrategy.Unoptimized).Resolve(id,?option=opt)

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

    let arrangeEs connect choose resolve = async {
        let log = createLog ()
        let! conn = connect log
        let gateway = choose conn defaultBatchSize
        return Backend.Cart.create log (resolve gateway) }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_EVENTSTORE")>]
    let ``Can roundtrip against EventStore, correctly folding the events without compaction semantics`` args = Async.RunSynchronously <| async {
        let! service = arrangeEs connectToLocalEventStoreNode createGesGateway resolveGesStreamWithoutCustomAccessStrategy
        do! act service args
    }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_EVENTSTORE")>]
    let ``Can roundtrip against EventStore, correctly folding the events with RollingSnapshots`` args = Async.RunSynchronously <| async {
        let! service = arrangeEs connectToLocalEventStoreNode createGesGateway resolveGesStreamWithRollingSnapshots
        do! act service args
    }

    let arrangeCosmos connect resolve =
        let log = createLog ()
        let ctx: CosmosStore.CosmosStoreContext = connect log defaultBatchSize
        Backend.Cart.create log (resolve ctx)

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``Can roundtrip against Cosmos, correctly folding the events without custom access strategy`` args = Async.RunSynchronously <| async {
        let service = arrangeCosmos connectToSpecifiedCosmosOrSimulator resolveCosmosStreamWithoutCustomAccessStrategy
        do! act service args
    }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``Can roundtrip against Cosmos, correctly folding the events with With Snapshotting`` args = Async.RunSynchronously <| async {
        let service = arrangeCosmos connectToSpecifiedCosmosOrSimulator resolveCosmosStreamWithSnapshotStrategy
        do! act service args
    }
