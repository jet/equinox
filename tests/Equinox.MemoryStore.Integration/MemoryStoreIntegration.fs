module Equinox.MemoryStore.Integration.MemoryStoreIntegration

open Domain
open Equinox.MemoryStore
open FsCheck.FSharp
open Swensen.Unquote

module ArbMap =
    let defGen<'t> = ArbMap.defaults |> ArbMap.generate<'t>

type FsCheckGenerators =
    static member SkuId = ArbMap.defGen |> Gen.map SkuId |> Arb.fromGen

type AutoDataAttribute() =
    inherit FsCheck.Xunit.PropertyAttribute(Arbitrary = [| typeof<FsCheckGenerators> |], MaxTest = 1, QuietOnSuccess = true)

let createMemoryStore () = VolatileStore<_>()

let createServiceMemory log store =
    let cat = MemoryStoreCategory(store, FsCodec.Box.Codec.Create(), Cart.Fold.fold, Cart.Fold.initial)
    cat |> Equinox.Decider.resolve log |> Cart.create

type Tests(testOutputHelper) =
    let log = TestOutput(testOutputHelper).CreateLogger()

    let (|NonZero|) = function
        | None -> Some 1
        | Some c -> Some (max 1 c)

    [<AutoData>]
    let ``Basic tracer bullet, sending a command and verifying the folded result directly and via a reload``
            cartId1 cartId2 (ctx,skuId,NonZero quantity,waive) = async {
        let store = createMemoryStore ()
        let service = createServiceMemory log store
        let command = Cart.SyncItem (ctx,skuId,quantity,waive)

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
            | { Cart.Fold.State.items = [ item ] } ->
                let expectedItem : Cart.Fold.ItemInfo = { skuId = skuId; quantity = quantity.Value; returnsWaived = waive }
                test <@ expectedItem = item @>
            | x -> x |> failwithf "Expected to find item, got %A"
        verifyFoldedStateReflectsCommand expected
        verifyFoldedStateReflectsCommand actual
    }

let createFavoritesServiceMemory store log : Favorites.Service =
    let cat = MemoryStoreCategory(store, FsCodec.Box.Codec.Create(), Favorites.Fold.fold, Favorites.Fold.initial)
    cat |> Equinox.Decider.resolve log |> Favorites.create

type ChangeFeed(testOutputHelper) =
    let log = TestOutput(testOutputHelper).CreateLogger()

    [<AutoData>]
    let ``Commits get reported`` (clientId, sku) = async {
        let store = createMemoryStore ()
        let events = ResizeArray()
        let takeCaptured () =
            let xs = events.ToArray()
            events.Clear()
            List.ofArray xs
        use _ = store.Committed.Subscribe(fun struct (sn, xs) -> events.Add(FsCodec.StreamName.toString sn, List.ofArray xs))
        let service = createFavoritesServiceMemory store log
        let expectedStream = Favorites.streamId clientId |> Equinox.Core.StreamId.renderStreamName Favorites.Category

        do! service.Favorite(clientId, [sku])
        let written = takeCaptured ()
        test <@ let stream, xs = written |> List.exactlyOne
                let env = xs |> List.exactlyOne
                stream = expectedStream
                && env.Index = 0L
                && env.EventType = "Favorited"
                && env.Data |> unbox<Favorites.Events.Favorited> |> fun x -> x.skuId = sku @>
        do! service.Unfavorite(clientId, sku)
        let written = takeCaptured ()
        test <@ let stream, xs = written |> List.exactlyOne
                let env = xs |> List.exactlyOne
                stream = expectedStream
                && env.Index = 1L
                && env.EventType = "Unfavorited"
                && env.Data |> unbox<Favorites.Events.Unfavorited> |> fun x -> x.skuId = sku @> }

type Versions(testOutputHelper) =
    let log = TestOutput(testOutputHelper).CreateLogger()

    [<AutoData>]
    let ``Post-Version is computed correctly`` (clientId, sku) = async {
        let store = createMemoryStore ()
        let service = createFavoritesServiceMemory store log

        do! service.Favorite(clientId, [sku])
        let! postVersion = service.UnfavoriteWithPostVersion(clientId, sku)
        test <@ 1L + 1L = postVersion @> }
