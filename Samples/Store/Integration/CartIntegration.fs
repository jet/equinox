module Samples.Store.Integration.CartIntegration

open Foldunk.EventStore
open Foldunk.MemoryStore
open Swensen.Unquote

#nowarn "1182" // From hereon in, we may have some 'unused' privates (the tests)

let fold, initial = Domain.Cart.Folds.fold, Domain.Cart.Folds.initial

let createMemoryStore () =
    new VolatileStore ()
let createServiceMem store =
    Backend.Cart.Service(fun _compactionEventType -> MemoryStreamBuilder(store, fold, initial).Create)

let codec = genCodec<Domain.Cart.Events.Event>
let createServiceGes connection batchSize =
    let gateway = createGesGateway connection batchSize
    Backend.Cart.Service(fun cet -> GesStreamBuilder(gateway, codec, fold, initial, CompactionStrategy.EventType cet).Create)
let createServiceGesWithoutCompactionSemantics eventStoreConnection batchSize =
    let gateway = createGesGateway eventStoreConnection batchSize
    Backend.Cart.Service(fun _ignoreCompactionEventType -> GesStreamBuilder(gateway, codec, fold, initial).Create)

let addAndThenRemoveItemsManyTimesExceptTheLastOne context cartId skuId log (service: Backend.Cart.Service) count =
    service.FlowAsync(log, cartId, fun _ctx execute ->
        for i in 1..count do
            execute <| Domain.Cart.AddItem (context, skuId, i)
            if i <> count then
                execute <| Domain.Cart.RemoveItem (context, skuId))

type Tests(testOutputHelper) =
    let testOutput = TestOutputAdapter testOutputHelper
    let createLog () = createLogger (testOutput.Subscribe >> ignore)

    [<AutoData>]
    let ``Can roundtrip in Memory, correctly folding the events`` context cartId skuId = Async.RunSynchronously <| async {
        let store = createMemoryStore ()
        let log, service = createLog (), createServiceMem store

        do! addAndThenRemoveItemsManyTimesExceptTheLastOne context cartId skuId log service 5

        let! state = service.Read log cartId
        test <@ 5 = match state with { items = [{ quantity = quantity }] } -> quantity | _ -> failwith "nope" @>
    }

    [<AutoData>]
    let ``Can roundtrip against EventStore, correctly folding the events without compaction semantics`` context cartId skuId = Async.RunSynchronously <| async {
        let! conn = connectToLocalEventStoreNode ()
        let log, service = createLog (), createServiceGesWithoutCompactionSemantics conn defaultBatchSize

        do! addAndThenRemoveItemsManyTimesExceptTheLastOne context cartId skuId log service 5

        let! state = service.Read log cartId
        test <@ 5 = match state with { items = [{ quantity = quantity }] } -> quantity | _ -> failwith "nope" @>
    }

    [<AutoData>]
    let ``Can roundtrip against EventStore, correctly folding the events with compaction`` context cartId skuId = Async.RunSynchronously <| async {
        let! conn = connectToLocalEventStoreNode ()
        let log, service = createLog (), createServiceGes conn defaultBatchSize

        do! addAndThenRemoveItemsManyTimesExceptTheLastOne context cartId skuId log service 5

        let! state = service.Read log cartId
        test <@ 5 = match state with { items = [{ quantity = quantity }] } -> quantity | _ -> failwith "nope" @>
    }