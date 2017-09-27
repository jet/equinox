module Domain.Cart

// NB - these schemas reflect the actual storage formats and hence need to be versioned with care
module Events =
    type ContextInfo =              { time: System.DateTime; requestId: RequestId }

    type ItemInfo =                 { context: ContextInfo; item: ItemInfo }
    type ItemAddInfo =              { context: ContextInfo; skuId: SkuId; quantity: int }
    type ItemRemoveInfo =           { context: ContextInfo; skuId: SkuId }
    type ItemQuantityChangeInfo =   { context: ContextInfo; skuId: SkuId; quantity: int }
    type ItemWaiveReturnsInfo =     { context: ContextInfo; skuId: SkuId; waived: bool }

    type Event =
        | ItemAdded                 of ItemAddInfo
        | ItemRemoved               of ItemRemoveInfo
        | ItemQuantityChanged       of ItemQuantityChangeInfo
        | ItemWaiveReturnsChanged   of ItemWaiveReturnsInfo

module Folds =
    type ItemInfo =                 { skuId: SkuId; quantity: int; returnsWaived: bool }
    type State =                    { items: ItemInfo list }

    let initial = { items = [] }
    let evolve (state : State) event =
        let updateItems f = { state with items = f state.items }
        match event with
        | Events.ItemAdded e -> updateItems (fun current -> { skuId = e.skuId; quantity = e.quantity; returnsWaived = false  } :: current)
        | Events.ItemRemoved e -> updateItems (List.filter (fun x -> x.skuId <> e.skuId))
        | Events.ItemQuantityChanged e -> updateItems (List.map (function i when i.skuId = e.skuId -> { i with quantity = e.quantity } | i -> i))
        | Events.ItemWaiveReturnsChanged e -> updateItems (List.map (function i when i.skuId = e.skuId -> { i with returnsWaived = e.waived } | i -> i))
    let fold state = List.fold evolve state

module Commands =
    type Context =              { time: System.DateTime; requestId : RequestId }
    type Command =
        | AddItem               of Context * SkuId * quantity: int
        | ChangeItemQuantity    of Context * SkuId * quantity: int
        | ChangeWaiveReturns    of Context * SkuId * waived: bool
        | RemoveItem            of Context * SkuId

    let toEventContext (reqContext: Context) : Events.ContextInfo = { requestId = reqContext.requestId; time = reqContext.time }
    let interpret command (state : Folds.State) =
        let itemExists f                                    = state.items |> List.exists f
        let itemExistsWithDifferentWaiveStatus skuId waive  = itemExists (fun x -> x.skuId = skuId && x.returnsWaived <> waive)
        let itemExistsWithDifferentQuantity skuId quantity  = itemExists (fun x -> x.skuId = skuId && x.quantity <> quantity)
        let itemExistsWithSameQuantity skuId quantity       = itemExists (fun x -> x.skuId = skuId && x.quantity = quantity)
        let itemExistsWithSkuId skuId                       = itemExists (fun x -> x.skuId = skuId)
        let (|Context|) (context : Context)                 = toEventContext context
        match command with
        | AddItem (Context c, skuId, quantity) ->
            if itemExistsWithSameQuantity skuId quantity then [] else
            [ Events.ItemAdded { context = c; skuId = skuId; quantity = quantity } ]
        | ChangeItemQuantity (Context c, skuId, quantity) ->
            if not (itemExistsWithDifferentQuantity skuId quantity) then [] else
            [ Events.ItemQuantityChanged { context = c; skuId = skuId; quantity = quantity } ]
        | ChangeWaiveReturns (Context c, skuId, waived) ->
            if not (itemExistsWithDifferentWaiveStatus skuId waived) then [] else
            [ Events.ItemWaiveReturnsChanged { context = c; skuId = skuId; waived = waived } ]
        | RemoveItem (Context c, skuId) ->
            if not (itemExistsWithSkuId skuId) then [] else
            [ Events.ItemRemoved { context = c; skuId = skuId } ]

type Handler(stream) =
    let handler = Foldunk.Handler(Folds.fold, Folds.initial)
    member __.Decide log decide =
        handler.Decide decide log stream
    member __.Load log : Async<Folds.State> =
        handler.Query id log stream