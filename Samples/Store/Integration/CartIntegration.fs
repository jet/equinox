module Example.Integration.CartIntegration

open Swensen.Unquote

#nowarn "1182" // From hereon in, we may have some 'unused' privates (the tests)

let createServiceMem () =
    let store = createMemStore ()
    Backend.Cart.Service(fun _codec -> createMemStream store)

let createServiceGes eventStoreConnection =
    let gateway = createGesGateway eventStoreConnection 500
    Backend.Cart.Service(createGesStream gateway)

let addAndThenRemoveItemsManyTimesExceptTheLastOne context cartId skuId log (service: Backend.Cart.Service) count =
    let decide (ctx : Foldunk.DecisionContext<_,_>) = async {
        let run cmd = ctx.Execute(Domain.Cart.Commands.interpret cmd)
        for i in 1..count do
            run <| Domain.Cart.Commands.AddItem (context, skuId, i)
            if i <> count then
                run <| Domain.Cart.Commands.RemoveItem (context, skuId)
        return ctx.Complete() }
    service.Run log cartId decide

type Tests(testOutputHelper) =
    let testOutput = TestOutputAdapter testOutputHelper
    let createLog () = createLogger (testOutput.Subscribe >> ignore)

    [<AutoData>]
    let ``Can roundtrip in Memory, correctly folding the events`` context cartId skuId = Async.RunSynchronously <| async {
        let log, service = createLog (), createServiceMem ()

        do! addAndThenRemoveItemsManyTimesExceptTheLastOne context cartId skuId log service 5

        let! state = service.Load log cartId
        test <@ 5 = match state with { items = [{ quantity = quantity }] } -> quantity | _ -> failwith "nope" @>
    }

    [<AutoData>]
    let ``Can roundtrip against EventStore, correctly folding the events`` context cartId skuId = Async.RunSynchronously <| async {
        let! conn = connectToLocalEventStoreNode ()
        let log, service = createLog (), createServiceGes conn

        do! addAndThenRemoveItemsManyTimesExceptTheLastOne context cartId skuId log service 5

        let! state = service.Load log cartId
        test <@ 5 = match state with { items = [{ quantity = quantity }] } -> quantity | _ -> failwith "nope" @>
    }