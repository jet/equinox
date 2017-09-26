module Example.Integration.CartIntegration

open Backend
open Domain
open Foldunk
open Swensen.Unquote

#nowarn "1182" // From hereon in, we may have some 'unused' privates (the tests)

let createServiceWithInMemoryStore () =
    let store = createInMemoryStore ()
    Carts.Service(fun _codec -> store)

let createServiceWithEventStore eventStoreConnection = Carts.Service(createGesStreamer eventStoreConnection 500)

let addAndThenRemoveItemsManyTimesExceptTheLastOne context cartId skuId log (service: Carts.Service) count =
    let decide (ctx : DecisionContext<_,_>) = async {
        let run cmd = ctx.Execute(Cart.Commands.interpret cmd)
        for i in 1..count do
            run <| Cart.Commands.AddItem (context, skuId, i)
            if i <> count then
                run <| Cart.Commands.RemoveItem (context, skuId)
        return ctx.Complete() }
    service.Run log cartId decide

type Tests(testOutputHelper) =
    let testOutput = TestOutputAdapter testOutputHelper
    let createLog () = createLogger (testOutput.Subscribe >> ignore)

    [<AutoData>]
    let ``Can roundtrip against InMemoryStore, correctly folding the events`` context cartId skuId = Async.RunSynchronously <| async {
        let log, service = createLog (), createServiceWithInMemoryStore ()

        do! addAndThenRemoveItemsManyTimesExceptTheLastOne context cartId skuId log service 5

        let! state = service.Load log cartId
        test <@ 5 = match state with { items = [{ quantity = quantity }] } -> quantity | _ -> failwith "nope" @>
    }

    [<AutoData>]
    let ``Can roundtrip against EventStore, correctly folding the events`` context cartId skuId = Async.RunSynchronously <| async {
        let! conn = connectToLocalEventStoreNode ()
        let log, service = createLog (), createServiceWithEventStore conn

        do! addAndThenRemoveItemsManyTimesExceptTheLastOne context cartId skuId log service 5

        let! state = service.Load log cartId
        test <@ 5 = match state with { items = [{ quantity = quantity }] } -> quantity | _ -> failwith "nope" @>
    }