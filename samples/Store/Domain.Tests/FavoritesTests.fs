module Samples.Store.Domain.Tests.FavoritesTests

open Domain
open Domain.Favorites
open Domain.Favorites.Events
open Domain.Favorites.Fold
open Swensen.Unquote
open System

let mkFavorite skuId    = Favorited { date = DateTimeOffset.UtcNow; skuId = skuId }
let mkUnfavorite skuId  = Unfavorited { skuId = skuId }

/// Put the aggregate into the state where the command should trigger an event; verify correct events are yielded
let verifyCorrectEventGenerationWhenAppropriate command (originState: State) =
    let initialEvents = command |> function
        | Unfavorite skuId ->                       [ mkFavorite skuId ]
        | Favorite _ ->                             []
    let state = fold originState initialEvents
    let events = Commands.interpret command state
    let state' = fold state events

    let hadSkuId, hasSkuId =
        let stateHasSku (s : State) (skuId : SkuId) = s |> Array.exists (function { skuId = sSkuId } -> sSkuId = skuId)
        stateHasSku state, stateHasSku state'
    match command, events with
    | Unfavorite skuId, [ Unfavorited e] ->
        test <@ e = { skuId = skuId}
                && not (hasSkuId skuId) @>
    | Favorite (_date, skuIds), events ->
        let isFavoritingEventFor skuId = function
            | Favorited { skuId = eSkuId } -> eSkuId = skuId
            | _ -> false
        test <@ skuIds |> List.forall (fun skuId -> hadSkuId skuId || events |> List.exists (isFavoritingEventFor skuId))
                && skuIds |> List.forall (fun skuId -> hasSkuId skuId) @>
    | c,e -> failwithf "Invalid result - Command %A yielded Events %A in State %A" c e state

/// Processing should allow for any given Command to be retried at will
let verifyIdempotency (command: Command) (originState: State) =
    // Put the aggregate into the state where the command should not trigger an event
    let initialEvents: Event list = command |> function
        | Unfavorite _ ->                           []
        | Favorite (_,skuIds) ->                    [| for sku in skuIds -> mkFavorite sku |] |> knuthShuffle |> List.ofArray
    let state = fold originState initialEvents
    let events = Commands.interpret command state
    // Assert we decided nothing needs to happen
    test <@ List.isEmpty events @>

[<DomainProperty(MaxTest = 1000)>]
let ``interpret yields correct events, idempotently`` (cmd: Command) (state: State) =
    verifyCorrectEventGenerationWhenAppropriate cmd state
    verifyIdempotency cmd state