module Samples.Store.Domain.Tests.CartTests

open Domain
open Domain.Cart
open Domain.Cart.Fold
open Swensen.Unquote
open TypeShape.Empty

let mkAddQty skuId qty waive    = Events.ItemAdded { empty<Events.ItemAddedInfo> with skuId = skuId; quantity = qty; waived = waive }
let mkAdd skuId                 = mkAddQty skuId 1 None
let mkRemove skuId              = Events.ItemRemoved { empty<Events.ItemRemovedInfo> with skuId = skuId }
let mkChangeWaived skuId value  = Events.ItemPropertiesChanged { empty<Events.ItemPropertiesChangedInfo> with skuId = skuId; waived = value }

/// Represents the high level primitives that can be expressed in a SyncItem Command
type Command =
    | AddItem of Context * SkuId * quantity : int * waiveStatus : bool option
    | PatchItem of Context * SkuId * quantity : int option * waiveStatus : bool option
    | RemoveItem of Context * SkuId

let interpret = function
    | AddItem (c, s, q, w) ->   SyncItem (c, s, Some q, w)    |> interpret
    | PatchItem (c, s, q, w) -> SyncItem (c, s, q, w)         |> interpret
    | RemoveItem (c, s) ->      SyncItem (c, s, Some 0, None) |> interpret

/// As a basic sanity check, verify the basic properties we'd expect per command if we were to run it on an empty stream
// Note validating basics like this is not normally that useful a property; in this instance (I think) it takes some
//   cases/logic out of the main property and is hence worth doing for this aggregate
let verifyCanProcessInOriginState cmd (originState: State) =
    let events = interpret cmd originState
    match cmd with
    | PatchItem (_, _, Some 0, _)
    | RemoveItem _ ->
        test <@ List.isEmpty events @>
    | _ ->
        test <@ (not << List.isEmpty) events @>

/// Put the aggregate into the state where the command should trigger an event; verify correct events are yielded
let verifyCorrectEventGenerationWhenAppropriate command (originState: State) =
    let initialEvents = command |> function
        | AddItem _
        | PatchItem (_, _, None, _) ->                  []
        | RemoveItem (_, skuId)
        | PatchItem (_, skuId, Some 0, _) ->            [ mkAdd skuId ]
        | PatchItem (_, skuId, Some quantity, Some waive) ->
                                                        [ mkAddQty skuId (quantity+1) (Some (not waive)) ]
        | PatchItem (_, skuId, Some quantity, waive) -> [ mkAddQty skuId (quantity+1) waive]
    let state = fold originState initialEvents
    let events = interpret command state
    let state' = fold state events

    let find skuId = state'.items |> List.find (fun x -> x.skuId = skuId)

    match command, events with
    | AddItem (_, csku, quantity, waive),               [ Events.ItemAdded e ] ->
        test <@ e = { context = e.context; skuId = csku; quantity = quantity; waived = waive }
                && quantity = (find csku).quantity @>
    | PatchItem (_, csku, Some 0, _),                   [ Events.ItemRemoved e ]
    | RemoveItem (_, csku),                             [ Events.ItemRemoved e ] ->
        test <@ e = { Events.ItemRemovedInfo.context = e.context; skuId = csku }
                && not (state'.items |> List.exists (fun x -> x.skuId = csku)) @>
    | PatchItem (_, csku, quantity, waive),    es ->
        match quantity with
        | Some value ->
            test <@ es
                    |> List.exists (function
                       | Events.ItemQuantityChanged e -> e = { context = e.context; skuId = csku; quantity = value }
                       | _ -> false)
                    && value = (find csku).quantity @>
        | None -> ()
        match waive with
        | None -> ()
        | Some value ->
            test <@ es
                    |> List.exists (function
                        | Events.ItemPropertiesChanged e -> e = { context = e.context; skuId = csku; waived = value }
                        | _ -> false)
                    && value = (find csku).returnsWaived.Value @>
    | c,e -> failwithf "Invalid result - Command %A yielded Events %A in State %A" c e state

/// Processing should allow for any given Command to be retried at will, without inducing redundant
/// (and hence potentially-conflicting) changes
let verifyIdempotency (cmd: Command) (originState: State) =
    // Put the aggregate into the state where the command should not trigger an event
    let establish: Events.Event list = cmd |> function
        | AddItem (_, skuId, qty, waive) ->             [ mkAddQty skuId qty waive]
        | RemoveItem _
        | PatchItem (_, _, Some 0, _)
        | PatchItem (_, _, None, _) ->                  []
        | PatchItem (_, skuId, Some quantity, waived) ->[ mkAddQty skuId quantity waived ]
    let state = fold originState establish
    let events = interpret cmd state

    // Assert we decided nothing needs to happen
    test <@ List.isEmpty events @>

/// These cases are assumed to be covered by external validation, so logic can treat them as hypotheticals rather than have to reject
let isValid = function
    // One can generate a null request consisting of quantity = None, waived = None, which has no concievable outcome
    // we don't guard or special case this condition
    | PatchItem (_, _, None, _) -> false
    | AddItem (_, _, quantity, _)
    | PatchItem (_, _, Some quantity, _) -> quantity > 0
    | _ -> true

// For the origin state, we only do basic filtering, which can provide good fuzz testing even if our implementation
// might not happen to ever trigger such a state (as opposed to neutering an entire scenario as we do with isValue)
let (|ValidOriginState|) : Fold.State -> Fold.State =
    let updateItems f = function { items = i } -> { items = f i }
    updateItems (List.choose (function { quantity = q } as x when q > 0 -> Some x | _ -> None))

[<DomainProperty>]
let ``interpret yields correct events, idempotently`` (cmd: Command) (ValidOriginState originState) =
    if not (isValid cmd) then () else
    verifyCanProcessInOriginState cmd originState
    verifyCorrectEventGenerationWhenAppropriate cmd originState
    verifyIdempotency cmd originState
