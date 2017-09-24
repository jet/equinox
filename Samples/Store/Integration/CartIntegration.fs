module Example.Integration.CartIntegration

open Backend
open Domain
open Foldunk
open Swensen.Unquote

#nowarn "1182" // From hereon in, we may have some 'unused' privates (the tests)

let createServiceWithInMemoryStore () = Carts.Service(fun _codec -> createInMemoryStreamer ())

let createServiceWithEventStore eventStoreConnection = Carts.Service(createGesStreamer eventStoreConnection 500)

type Tests(testOutputHelper) =
    let testOutput = TestOutputAdapter testOutputHelper
    let createLog () = createLogger (testOutput.Subscribe >> ignore)

    let addAndThenRemoveAnItem context cartId skuId log (service: Carts.Service) count =
        let decide (ctx : DecisionContext<_,_>) = async {
            let run cmd = ctx.Execute(Cart.Commands.interpret cmd)
            for i in 1..count do
                for c in [Cart.Commands.AddItem (context, skuId, i); Cart.Commands.RemoveItem (context, skuId)] do
                    run c
            return ctx.Complete() }
        service.Run log cartId decide

    let validateCartIsEmpty cartId log (service: Carts.Service) = async {
        let! state = service.Load log cartId
        test <@ Seq.isEmpty state.items @>
    }

    [<AutoData>]
    let ``Can roundtrip against InMemoryStore, correctly folding the events`` context cartId skuId = Async.RunSynchronously <| async {
        let log, service = createLog (), createServiceWithInMemoryStore ()

        do! addAndThenRemoveAnItem context cartId skuId log service 5
        do! validateCartIsEmpty cartId log service
    }

    [<AutoData>]
    let ``Can roundtrip against EventStore, correctly folding the events`` context cartId skuId = Async.RunSynchronously <| async {
        let! conn = connectToLocalEventStoreNode ()
        let log, service = createLog (), createServiceWithEventStore conn

        do! addAndThenRemoveAnItem context cartId skuId log service 5

        do! validateCartIsEmpty cartId log service
    }