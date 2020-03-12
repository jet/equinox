﻿module Equinox.MemoryStore.Integration.MemoryStoreIntegration

open Swensen.Unquote
open Equinox.MemoryStore

let createMemoryStore () =
    VolatileStore<_>()

let createServiceMemory log store =
    let resolver = Resolver(store, FsCodec.Box.Codec.Create(), Domain.Cart.Fold.fold, Domain.Cart.Fold.initial)
    let resolve (id,opt) = resolver.Resolve(id,?option=opt)
    Backend.Cart.create log resolve

#nowarn "1182" // From hereon in, we may have some 'unused' privates (the tests)

type Tests(testOutputHelper) =
    let testOutput = TestOutputAdapter testOutputHelper
    let createLog () = createLogger testOutput

    [<AutoData>]
    let ``Basic tracer bullet, sending a command and verifying the folded result directly and via a reload``
            cartId1 cartId2 ((_,skuId,quantity) as args) = Async.RunSynchronously <| async {
        let store = createMemoryStore ()
        let service = let log = createLog () in createServiceMemory log store
        let command = Domain.Cart.AddItem args

        // Act: Run the decision twice...
        let actTrappingStateAsSaved cartId =
            service.Run(cartId, false, [command])

        let actLoadingStateSeparately cartId = async {
            let! _ = service.ExecuteManyAsync(cartId, false, [command])
            return! service.Read cartId }
        let! expected = cartId1 |> actTrappingStateAsSaved
        let! actual = cartId2 |> actLoadingStateSeparately

        // Assert 1. Despite being on different streams (and being in-memory vs re-loaded) we expect the same outcome
        test <@ expected = actual @>

        // Assert 2. Verify that the Command got correctly reflected in the state, with no extraneous effects
        let verifyFoldedStateReflectsCommand = function
            | { Domain.Cart.Fold.State.items = [ item ] } ->
                let expectedItem : Domain.Cart.Fold.ItemInfo = { skuId = skuId; quantity = quantity; returnsWaived = false }
                test <@ expectedItem = item @>
            | x -> x |> failwithf "Expected to find item, got %A"
        verifyFoldedStateReflectsCommand expected
        verifyFoldedStateReflectsCommand actual
    }
