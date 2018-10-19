﻿module Foldunk.MemoryStore.Integration.MemoryStoreIntegration

open Swensen.Unquote
open Foldunk.MemoryStore

let createMemoryStore () =
    new VolatileStore()

let createServiceMem log store =
    Backend.Cart.Service(log, fun _eventTypePredicate -> MemoryStreamBuilder(store, Domain.Cart.Folds.fold, Domain.Cart.Folds.initial).Create)

#nowarn "1182" // From hereon in, we may have some 'unused' privates (the tests)

type Tests(testOutputHelper) =
    let testOutput = TestOutputAdapter testOutputHelper
    let createLog () = createLogger testOutput

    [<AutoData>]
    let ``Basic tracer bullet, sending a command and verifying the folded result directly and via a reload``
            cartId1 cartId2 ((_,skuId,quantity) as args) = Async.RunSynchronously <| async {
        let store = createMemoryStore ()
        let service = let log = createLog () in createServiceMem log store
        let flow (ctx: Foldunk.Context<_,_>) execute =
            Domain.Cart.AddItem args |> execute
            ctx.State

        // Act: Run the decision twice...
        let actTrappingStateAsSaved cartId =
            service.FlowAsync(cartId, flow)

        let actLoadingStateSeparately cartId = async {
            let! _ = service.FlowAsync(cartId, flow)
            return! service.Read cartId }
        let! expected = cartId1 |> actTrappingStateAsSaved
        let! actual = cartId2 |> actLoadingStateSeparately

        // Assert 1. Despite being on different streams (and being in-memory vs re-loaded) we expect the same outcome
        test <@ expected = actual @>

        // Assert 2. Verify that the Command got correctly reflected in the state, with no extraneous effects
        let verifyFoldedStateReflectsCommand = function
            | { Domain.Cart.Folds.State.items = [ item ] } ->
                let expectedItem : Domain.Cart.Folds.ItemInfo = { skuId = skuId; quantity = quantity; returnsWaived = false }
                test <@ expectedItem = item @>
            | x -> x |> failwithf "Expected to find item, got %A"
        verifyFoldedStateReflectsCommand expected
        verifyFoldedStateReflectsCommand actual
    }