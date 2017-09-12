module Integration.CartIntegration

open Backend.Carts
open Domain
open Foldunk
open Swensen.Unquote

#nowarn "1182" // From hereon in, we may have some 'unused' privates (the tests)

type Tests(testOutputHelper) =
    let testOutput = TestOutputAdapter testOutputHelper
    let createLog () = createLogger (testOutput.Subscribe >> ignore)

    let createServiceWithInMemoryStore () =
        let store : Handler.IEventStream<_,_> = Stores.InMemoryStore.MemoryStreamStore() :> _
        CartService(store)

    [<AutoData>]
    let ``Basic tracer bullet, sending a command and verifying the folded result directly and via a reload``
            cartId1 cartId2 ((_,skuId,quantity) as args) = Async.RunSynchronously <| async {
        let log, service = createLog (), createServiceWithInMemoryStore ()
        let decide (ctx: DecisionState<_,_>) = async {
            Cart.Commands.AddItem args |> Cart.Commands.interpret |> ctx.Execute
            return ctx.Complete ctx.State }

        // Act: Run the decision twice...
        let actTrappingStateAsSaved cartId =
            service.Run log cartId decide
        let actLoadingStateSeparately cartId = async {
            let! _ = service.Run log cartId decide
            return! service.Load log cartId }
        let! expected = cartId1 |> actTrappingStateAsSaved
        let! actual = cartId2 |> actLoadingStateSeparately 

        // Assert 1. Despite being on different streams (and being in-memory vs re-loaded) we expect the same outcome
        test <@ expected = actual @>

        // Assert 2. Verify that the Command got correctly reflected in the state, with no extraneous effects
        let verifyFoldedStateReflectsCommand = function
            | { Cart.Folds.State.items = [ item ] } ->
                let expectedItem : Cart.Folds.ItemInfo = { skuId = skuId; quantity = quantity; returnsWaived = false }
                test <@ expectedItem = item @>
            | x -> x |> failwithf "Expected to find item, got %A"
        verifyFoldedStateReflectsCommand expected
        verifyFoldedStateReflectsCommand actual
    }

    let addAndThenRemoveAnItem context cartId skuId log (service: CartService) count =
        let decide (ctx : DecisionState<_,_>) = async {
            let run cmd = ctx.Execute(Cart.Commands.interpret cmd)
            for _ in 1..count do
                for c in [Cart.Commands.AddItem (context, skuId, 1); Cart.Commands.RemoveItem (context, skuId)] do
                    run c
            return ctx.Complete() }
        service.Run log cartId decide

    let validateCartIsEmpty cartId log (service: CartService) = async {
        let! state = service.Load log cartId
        test <@ Seq.isEmpty state.items @>
    }

    [<AutoData>]
    let ``Can roundtrip against in memory store, correctly folding the events`` context cartId skuId = Async.RunSynchronously <| async {
        let log, service = createLog (), createServiceWithInMemoryStore ()

        do! addAndThenRemoveAnItem context cartId skuId log service 5
        
        do! validateCartIsEmpty cartId log service
    }