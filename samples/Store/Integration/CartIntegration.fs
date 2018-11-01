module Samples.Store.Integration.CartIntegration

open Equinox.Cosmos
open Equinox.Cosmos.Integration
open Equinox.EventStore
open Equinox.MemoryStore
open Swensen.Unquote

#nowarn "1182" // From hereon in, we may have some 'unused' privates (the tests)

let fold, initial, snapshot = Domain.Cart.Folds.fold, Domain.Cart.Folds.initial,  Domain.Cart.Folds.snapshot

let createMemoryStore () =
    new VolatileStore ()
let createServiceMem log store =
    Backend.Cart.Service(log, MemoryStreamBuilder(store, fold, initial).Create)

let codec = Equinox.EventStore.Integration.EventStoreIntegration.genCodec<Domain.Cart.Events.Event>()

let resolveGesStreamWithRollingSnapshots gateway =
    GesStreamBuilder(gateway, codec, fold, initial, AccessStrategy.RollingSnapshots snapshot).Create
let resolveGesStreamWithoutCustomAccessStrategy gateway =
    GesStreamBuilder(gateway, codec, fold, initial).Create

let resolveEqxStreamWithCompactionEventType gateway compactionEventType (StreamArgs args) =
    EqxStreamBuilder(gateway, codec, fold, initial, Equinox.Cosmos.CompactionStrategy.EventType compactionEventType).Create(args)
let resolveEqxStreamWithoutCompactionSemantics gateway _compactionEventType (StreamArgs args) =
    EqxStreamBuilder(gateway, codec, fold, initial).Create(args)

let addAndThenRemoveItemsManyTimesExceptTheLastOne context cartId skuId (service: Backend.Cart.Service) count =
    service.FlowAsync(cartId, fun _ctx execute ->
        for i in 1..count do
            execute <| Domain.Cart.AddItem (context, skuId, i)
            if i <> count then
                execute <| Domain.Cart.RemoveItem (context, skuId))

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
        let service = createServiceMem log store
        do! act service args
    }

    let arrange connect choose resolveStream = async {
        let log = createLog ()
        let! conn = connect log
        let gateway = choose conn defaultBatchSize
        return Backend.Cart.Service(log, resolveStream gateway) }

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
    let ``Can roundtrip against Cosmos, correctly folding the events without compaction semantics`` args = Async.RunSynchronously <| async {
        let! service = arrange connectToSpecifiedCosmosOrSimulator createEqxGateway resolveEqxStreamWithoutCompactionSemantics
        do! act service args
    }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``Can roundtrip against Cosmos, correctly folding the events with compaction`` args = Async.RunSynchronously <| async {
        let! service = arrange connectToSpecifiedCosmosOrSimulator createEqxGateway resolveEqxStreamWithCompactionEventType
        do! act service args
    }