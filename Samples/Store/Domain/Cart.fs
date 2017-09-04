module Domain.Cart

open System
open System.Runtime.Serialization

module Events =

    type ContextInfo =              {   time: DateTime; requestId: RequestId }

    type ItemInfo =                 {   context: ContextInfo; item: ItemInfo }
    type ItemAddInfo =              {   context: ContextInfo; skuId: SkuId; quantity: int }
    type ItemRemoveInfo =           {   context: ContextInfo; skuId: SkuId }
    type ItemQuantityChangeInfo =   {   context: ContextInfo; skuId: SkuId; quantity: int }
    type ItemWaiveReturnsInfo =     {   context: ContextInfo; skuId: SkuId; waived: bool }

    type Event =
        | ItemAdded                 of ItemAddInfo
        | [<DataMember(Name="itemRemoved")>] 
          ItemRemoved               of ItemRemoveInfo
        | ItemQuantityChanged       of ItemQuantityChangeInfo
        | ItemWaiveReturnsChanged   of ItemWaiveReturnsInfo
        
module Folds =
    type ItemInfo =
        {   skuId: SkuId; quantity: int; returnsWaived: bool }

    type State =
        {   items: ItemInfo list }
    module State =
        let hasItemWithDifferentWaiveStatus skuId waive state = state.items |> List.exists (fun x -> x.skuId = skuId && x.returnsWaived <> waive)
        let hasItemWithDifferentQuantity skuId quantity state = state.items |> List.exists (fun x -> x.skuId = skuId && x.quantity <> quantity)
        let hasItemWithSameQuantity skuId quantity state      = state.items |> List.exists (fun x -> x.skuId = skuId && x.quantity = quantity)
        let hasItem skuId state                               = state.items |> List.exists (fun x -> x.skuId = skuId)
        
    let evolve (state : State) = function
        | Events.ItemAdded e ->
            { state with items = { skuId = e.skuId; quantity = e.quantity; returnsWaived = false  } :: state.items}
        | Events.ItemRemoved e ->
            { state with items = state.items |> List.filter (fun x -> x.skuId = e.skuId) }
        | Events.ItemQuantityChanged e ->
            { state with items = state.items |> List.map (function i when i.skuId = e.skuId -> { i with quantity = e.quantity } | i -> i) }
        | Events.ItemWaiveReturnsChanged e ->
            { state with items = state.items |> List.map (function i when i.skuId = e.skuId -> { i with returnsWaived = e.waived } | i -> i) }

    let initial = { items = [] }
    let fold state = List.fold evolve state

module Commands =
    type Context = { time: DateTime; requestId : RequestId }
    let toEventContext (reqContext: Context) : Events.ContextInfo = { requestId = reqContext.requestId; time = reqContext.time }

    type Command =
        | AddItem of Context * SkuId * quantity: int
        | ChangeItemQuantity of Context * SkuId * quantity: int
        | ChangeWaiveReturns of Context * SkuId * waived: bool
        | RemoveItem of Context * SkuId

    // TODO make idempotent
    let interpret c (state : Folds.State) =
        let (|Context|) (context : Context) = toEventContext context
        match c with
        | AddItem (Context c, skuId, quantity) ->
            if state |> Folds.State.hasItemWithSameQuantity skuId quantity then [] else
            [ Events.ItemAdded { context = c; skuId = skuId; quantity = quantity } ]
        | ChangeItemQuantity (Context c, skuId, quantity) ->
            if not (state |> Folds.State.hasItemWithDifferentQuantity skuId quantity) then [] else
            [ Events.ItemQuantityChanged { context = c; skuId = skuId; quantity = quantity } ]
        | ChangeWaiveReturns (Context c, skuId, waived) ->
            if not (state |> Folds.State.hasItemWithDifferentWaiveStatus skuId waived) then [] else
            [ Events.ItemWaiveReturnsChanged { context = c; skuId = skuId; waived = waived } ]
        | RemoveItem (Context c, skuId) ->
            if not (state |> Folds.State.hasItem skuId) then [] else
            [ Events.ItemRemoved { context = c; skuId = skuId } ]

    let streamName (id: CartId) = sprintf "Cart-%s" id.Value