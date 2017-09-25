module Example.Domain.Tests.FavoritesTests

open Domain
open Domain.Favorites
open Domain.Favorites.Events
open Domain.Favorites.Folds
open Domain.Favorites.Commands
open Swensen.Unquote
open System

let mkSkuId () = Guid.NewGuid() |> SkuId
let mkFavorite skuId    = Favorited { Favorited.date = DateTimeOffset.UtcNow; skuId = skuId }
let mkUnfavorite skuId  = Unfavorited { skuId = skuId }

/// As a basic sanity check, verify the basic properties we'd expect per command if we were to run it on an empty stream
let verifyCanProcessInInitialState cmd =
    let events = interpret cmd initial
    match cmd with
    // Special case
    | Favorite (_,[]) ->
        test <@ List.isEmpty events @>
    // Should always generate something
    | Compact
    | Favorite _ ->
        test <@ (not << List.isEmpty) events @>
    // Should never generate anything
    | Unfavorite _ ->
        test <@ List.isEmpty events @>

/// Put the aggregate into the state where the command should trigger an event; verify correct events are yielded
let verifyCorrectEventGenerationWhenAppropriate cmd = 
    let generateEventsTriggeringNeedForChange: Command -> Event list = function
        | Compact ->                                [ (* Command is not designed to be idempotent *) ]
        | Favorite _ ->                             []
        | Unfavorite skuId ->                       [ mkFavorite skuId ]
    let verifyResultingEventsAreCorrect state state': Command * Event list -> unit = function
        | Compact,                                  [ Compacted { net = netItems } ] ->
            test <@ netItems = state @>
        | Favorite (_date, skuIds),                  events ->
            let hasSkuId skuId : Folds.State -> bool = Array.exists (function { skuId = sSkuId } when sSkuId = skuId -> true | _ -> false)
            let isFavoritingEventFor skuId = function
                | Favorited { skuId = eSkuId } -> eSkuId = skuId
                | _ -> false
            test <@ skuIds |> List.forall (fun skuId -> events |> List.exists (isFavoritingEventFor skuId))
                    && skuIds |> List.forall (fun skuId -> state' |> hasSkuId skuId) @>
        | Unfavorite skuId,                         [ Unfavorited e] ->
            let hasSkuId skuId : Folds.State -> bool = Array.exists (function { skuId = sSkuId } when sSkuId = skuId -> true | _ -> false)
            test <@ state |> hasSkuId skuId
                    && e = { skuId = skuId}
                    && not (state' |> hasSkuId skuId) @>
        | c,e -> failwithf "Invalid result - Command %A yielded Events %A in State %A" c e state
    let initialEvents = cmd |> generateEventsTriggeringNeedForChange
    let state = fold initial initialEvents
    let events = interpret cmd state
    let state' = fold state events
    (cmd, events) |> verifyResultingEventsAreCorrect state state'

/// Processing should allow for any given Command to be retried at will
let verifyIdempotency (cmd: Command) =
    // Put the aggregate into the state where the command should not trigger an event
    let mkRandomFavorites () = List.init (rnd.Next(1000)) (ignore >> mkSkuId >> mkFavorite)
    let establish: Event list = cmd |> function
        | Compact _ ->                              mkRandomFavorites ()
        | Favorite (_,skuIds) ->                    [| for sku in skuIds -> mkFavorite sku |] |> knuthShuffle |> List.ofArray 
        | Unfavorite _ ->                           mkRandomFavorites ()
    let state = fold initial establish
    let events = interpret cmd state
    let state' = fold state events
    match cmd with
    | Compact ->
        // Command should be unconditional
        test <@ not (List.isEmpty events)
                // and not have any side-effects on state
                && state' = state @>
    | _ ->
        // Assert we decided nothing needs to happen
        test <@ events |> List.isEmpty @>

[<Property(MaxTest = 1000)>]
let ``interpret yields correct events, idempotently`` (cmd: Command) =
    cmd |> verifyCanProcessInInitialState
    cmd |> verifyCorrectEventGenerationWhenAppropriate
    cmd |> verifyIdempotency