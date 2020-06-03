module Equinox.MemoryStore.Integration.MemoryStoreIntegration

open Equinox.MemoryStore
open Swensen.Unquote

let createMemoryStore () = VolatileStore<_>()
let createServiceMemory log store =
    let resolver = Resolver(store, FsCodec.Box.Codec.Create(), Domain.Cart.Fold.fold, Domain.Cart.Fold.initial)
    let resolve (id, opt) = resolver.Resolve(id, ?option=opt)
    Backend.Cart.create log resolve

#nowarn "1182" // From hereon in, we may have some 'unused' privates (the tests)

type Tests(testOutputHelper) =
    let testOutput = TestOutputAdapter testOutputHelper
    let createLog () = createLogger testOutput

    let (|NonZero|) = function
        | None -> Some 1
        | Some c -> Some (max 1 c)

    [<AutoData>]
    let ``Basic tracer bullet, sending a command and verifying the folded result directly and via a reload``
            cartId1 cartId2 (ctx,skuId,NonZero quantity,waive) = Async.RunSynchronously <| async {
        let log, store = createLog (), createMemoryStore ()
        let service = createServiceMemory log store
        let command = Domain.Cart.SyncItem (ctx,skuId,quantity,waive)

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
                let expectedItem : Domain.Cart.Fold.ItemInfo = { skuId = skuId; quantity = quantity.Value; returnsWaived = waive }
                test <@ expectedItem = item @>
            | x -> x |> failwithf "Expected to find item, got %A"
        verifyFoldedStateReflectsCommand expected
        verifyFoldedStateReflectsCommand actual
    }

let createFavoritesServiceMemory log store =
    let resolver = Resolver(store, FsCodec.Box.Codec.Create(), Domain.Favorites.Fold.fold, Domain.Favorites.Fold.initial)
    Backend.Favorites.create log resolver.Resolve

type ChangeFeed(testOutputHelper) =
    let testOutput = TestOutputAdapter testOutputHelper
    let createLog () = createLogger testOutput

    [<AutoData>]
    let ``Commits get reported`` (clientId, sku) = Async.RunSynchronously <| async {
        let log, store = createLog (), createMemoryStore ()
        let events = ResizeArray()
        let takeCaptured () =
            let xs = events.ToArray()
            events.Clear()
            List.ofArray xs
        use _ = store.Committed.Subscribe(fun (s, xs) -> events.Add((s, List.ofArray xs)))
        let service = createFavoritesServiceMemory log store
        let expectedStream = Domain.Favorites.streamName clientId

        do! service.Favorite(clientId, [sku])
        let written = takeCaptured ()
        test <@ let stream, xs = written |> List.exactlyOne
                let env = xs |> List.exactlyOne
                stream = expectedStream
                && env.Index = 0L
                && env.EventType = "Favorited"
                && env.Data |> unbox<Domain.Favorites.Events.Favorited> |> fun x -> x.skuId = sku @>
        do! service.Unfavorite(clientId, sku)
        let written = takeCaptured ()
        test <@ let stream, xs = written |> List.exactlyOne
                let env = xs |> List.exactlyOne
                stream = expectedStream
                && env.Index = 1L
                && env.EventType = "Unfavorited"
                && env.Data |> unbox<Domain.Favorites.Events.Unfavorited> |> fun x -> x.skuId = sku @>
}
