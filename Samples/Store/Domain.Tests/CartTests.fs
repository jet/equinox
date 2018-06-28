module Samples.Store.Domain.Tests.CartTests

open Domain
open Domain.Cart
open Domain.Cart.Events
open Domain.Cart.Folds
open Domain.Cart.Commands
open Swensen.Unquote
open System
open TypeShape.Empty

let mkAddQty skuId qty          = ItemAdded { empty<ItemAddInfo> with skuId = skuId; quantity = qty }
let mkAdd skuId                 = mkAddQty skuId 1
let mkRemove skuId              = ItemRemoved { empty<ItemRemoveInfo> with skuId = skuId }
let mkChangeWaived skuId value  = ItemWaiveReturnsChanged { empty<ItemWaiveReturnsInfo> with skuId = skuId; waived = value }

/// As a basic sanity check, verify the basic properties we'd expect per command if we were to run it on an empty stream
let verifyCanProcessInInitialState cmd (originState: State) =
    let events = interpret cmd originState
    match cmd with
    | Compact
    | AddItem _ ->
        test <@ (not << List.isEmpty) events @>
    | PatchItem _
    | RemoveItem _ ->
        test <@ List.isEmpty events @>

/// Put the aggregate into the state where the command should trigger an event; verify correct events are yielded
let verifyCorrectEventGenerationWhenAppropriate command (originState: State) =
    let initialEvents = command |> function
        | Compact ->                                [ (* Command is not designed to be idempotent *) ]
        | AddItem _ ->                              []
        | RemoveItem (_, skuId)
        | PatchItem (_, skuId, Some 0, _) ->        [ mkAdd skuId ]
        | PatchItem (_, skuId, quantity, None) ->   [ mkAddQty skuId (defaultArg quantity 0+1) ]
        | PatchItem (_, skuId, quantity, Some waive) ->
                                                    [ mkAddQty skuId (defaultArg quantity 0+1)
                                                      mkChangeWaived skuId (not waive)]
    let state = fold originState initialEvents
    let events = interpret command state
    let state' = fold state events

    let find skuId                                  = state'.items |> List.find (fun x -> x.skuId = skuId)
    match command, events with
    | Compact,                                      [ Compacted e ] ->
        let expectedState = State.ofCompacted e
        test <@ expectedState = state' @>
    | AddItem (_, csku, quantity),                  [ ItemAdded e ] ->
        test <@ { ItemAddInfo.context = e.context; skuId = csku; quantity = quantity } = e
                && quantity = (find csku).quantity @>
    | PatchItem (_, csku, Some 0, _),               [ ItemRemoved e ]
    | RemoveItem (_, csku),               [ ItemRemoved e ] ->
        test <@ { ItemRemoveInfo.context = e.context; skuId = csku } = e
                && not (state'.items |> List.exists (fun x -> x.skuId = csku)) @>
    | PatchItem (_, csku, quantity, waive),    es ->
        match quantity with
        | Some value ->
            test <@ es |> List.exists (function ItemQuantityChanged e -> e = { context = e.context; skuId = csku; quantity = value } | _ -> false)
                    && value = (find csku).quantity @>
        | None -> ()
        match waive with
        | None -> ()
        | Some value ->
            test <@ es |> List.exists (function ItemWaiveReturnsChanged e -> e = { context = e.context; skuId = csku; waived = value } | _ -> false)
                    && value = (find csku).returnsWaived @>
    | c,e -> failwithf "Invalid result - Command %A yielded Events %A in State %A" c e state

/// Processing should allow for any given Command to be retried at will
let verifyIdempotency (cmd: Command) (originState: State) =
    // Put the aggregate into the state where the command should not trigger an event
    let establish: Event list = cmd |> function
        | Compact _ ->                              let skuId = SkuId (Guid.NewGuid()) in [ mkAdd skuId; mkRemove skuId]
        | AddItem (_, skuId, qty) ->                [ mkAddQty skuId qty]
        | RemoveItem _
        | PatchItem (_, _, Some 0, _) ->            []
        | PatchItem (_, skuId, quantity, waived) -> [ mkAddQty skuId (defaultArg quantity 1)
                                                      mkChangeWaived skuId (defaultArg waived false) ]
    let state = fold originState establish
    let events = interpret cmd state
    match cmd, not (List.isEmpty events) with
    | Compact, hasEvents ->
        // Command should be unconditional
        test <@ hasEvents @>
    | _, hasEvents ->
        // Assert we decided nothing needs to happen
        test <@ not hasEvents @>

[<DomainProperty(MaxTest = 1000)>]
let ``interpret yields correct events, idempotently`` (cmd: Command) (originState: State) =
    verifyCanProcessInInitialState cmd originState
    verifyCorrectEventGenerationWhenAppropriate cmd originState
    verifyIdempotency cmd originState