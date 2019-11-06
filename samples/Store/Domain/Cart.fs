﻿module Domain.Cart

// NB - these schemas reflect the actual storage formats and hence need to be versioned with care
module Events =
    type ContextInfo =              { time: System.DateTime; requestId: RequestId }

    type ItemInfo =                 { context: ContextInfo; item: ItemInfo }
    type ItemAddInfo =              { context: ContextInfo; skuId: SkuId; quantity: int }
    type ItemRemoveInfo =           { context: ContextInfo; skuId: SkuId }
    type ItemQuantityChangeInfo =   { context: ContextInfo; skuId: SkuId; quantity: int }
    type ItemWaiveReturnsInfo =     { context: ContextInfo; skuId: SkuId; waived: bool }

    module Compaction =
        type StateItemInfo =        { skuId: SkuId; quantity: int; returnsWaived: bool }
        type State =                { items: StateItemInfo[] }

    type Event =
        | Compacted                 of Compaction.State
        | ItemAdded                 of ItemAddInfo
        | ItemRemoved               of ItemRemoveInfo
        | ItemQuantityChanged       of ItemQuantityChangeInfo
        | ItemWaiveReturnsChanged   of ItemWaiveReturnsInfo
        interface TypeShape.UnionContract.IUnionContract
    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()

module Folds =
    type ItemInfo =                 { skuId: SkuId; quantity: int; returnsWaived: bool }
    type State =                    { items: ItemInfo list }
    module State =
        let toSnapshot (s: State) : Events.Compaction.State =
            { items = [| for i in s.items -> { skuId = i.skuId; quantity = i.quantity; returnsWaived = i.returnsWaived } |] }
        let ofCompacted (s: Events.Compaction.State) : State =
            { items = [ for i in s.items -> { skuId = i.skuId; quantity = i.quantity; returnsWaived = i.returnsWaived } ] }
    let initial = { items = [] }
    let evolve (state : State) event =
        let updateItems f = { state with items = f state.items }
        match event with
        | Events.Compacted s -> State.ofCompacted s
        | Events.ItemAdded e -> updateItems (fun current -> { skuId = e.skuId; quantity = e.quantity; returnsWaived = false  } :: current)
        | Events.ItemRemoved e -> updateItems (List.filter (fun x -> x.skuId <> e.skuId))
        | Events.ItemQuantityChanged e -> updateItems (List.map (function i when i.skuId = e.skuId -> { i with quantity = e.quantity } | i -> i))
        | Events.ItemWaiveReturnsChanged e -> updateItems (List.map (function i when i.skuId = e.skuId -> { i with returnsWaived = e.waived } | i -> i))
    let fold : State -> Events.Event seq -> State = Seq.fold evolve
    let isOrigin = function Events.Compacted _ -> true | _ -> false
    let compact = State.toSnapshot >> Events.Compacted
    /// This transmute impl a) removes events - we're not interested in storing the events b) packs the post-state into a Compacted unfold-event
    let transmute _events state = [],[compact state]
type Context =              { time: System.DateTime; requestId : RequestId }
type Command =
    | AddItem               of Context * SkuId * quantity: int
    | PatchItem             of Context * SkuId * quantity: int option * waived: bool option
    | RemoveItem            of Context * SkuId

module Commands =
    let interpret command (state : Folds.State) =
        let itemExists f                                    = state.items |> List.exists f
        let itemExistsWithDifferentWaiveStatus skuId waive  = itemExists (fun x -> x.skuId = skuId && x.returnsWaived <> waive)
        let itemExistsWithDifferentQuantity skuId quantity  = itemExists (fun x -> x.skuId = skuId && x.quantity <> quantity)
        let itemExistsWithSameQuantity skuId quantity       = itemExists (fun x -> x.skuId = skuId && x.quantity = quantity)
        let itemExistsWithSkuId skuId                       = itemExists (fun x -> x.skuId = skuId && x.quantity <> 0)
        let toEventContext (reqContext: Context)            = { requestId = reqContext.requestId; time = reqContext.time } : Events.ContextInfo
        let (|Context|) (context : Context)                 = toEventContext context
        match command with
        | AddItem (Context c, skuId, quantity) ->
            if itemExistsWithSameQuantity skuId quantity then [] else
            [ Events.ItemAdded { context = c; skuId = skuId; quantity = quantity } ]
        | RemoveItem (Context c, skuId)
        | PatchItem (Context c, skuId, Some 0, _) ->
            if not (itemExistsWithSkuId skuId) then [] else
            [ Events.ItemRemoved { context = c; skuId = skuId } ]
        | PatchItem (_, skuId, _, _) when not (itemExistsWithSkuId skuId) ->
            []
        | PatchItem (Context c, skuId, quantity, waived) ->
            [   match quantity  with
                | Some quantity when itemExistsWithDifferentQuantity skuId quantity ->
                    yield Events.ItemQuantityChanged { context = c; skuId = skuId; quantity = quantity }
                | _ -> ()
                match waived with
                | Some waived when itemExistsWithDifferentWaiveStatus skuId waived ->
                     yield Events.ItemWaiveReturnsChanged { context = c; skuId = skuId; waived = waived }
                | _ -> () ] 