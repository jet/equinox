module Domain.Cart

// NB - these schemas reflect the actual storage formats and hence need to be versioned with care
module Events =
    type ContextInfo =              { time: System.DateTime; requestId: RequestId }

    type ItemInfo =                 { context: ContextInfo; item: ItemInfo }
    type ItemAddInfo =              { context: ContextInfo; skuId: SkuId; quantity: int }
    type ItemRemoveInfo =           { context: ContextInfo; skuId: SkuId }
    type ItemQuantityChangeInfo =   { context: ContextInfo; skuId: SkuId; quantity: int }
    type ItemWaiveReturnsInfo =     { context: ContextInfo; skuId: SkuId; waived: bool }

    module Compaction =
        let [<Literal>] EventType = "compact/1"
        type StateItemInfo =        { skuId: SkuId; quantity: int; returnsWaived: bool }
        type State =                { items: StateItemInfo[] }

    type Event =
        | [<System.Runtime.Serialization.DataMember(Name = Compaction.EventType)>]
          Compacted                 of Compaction.State
        | ItemAdded                 of ItemAddInfo
        | ItemRemoved               of ItemRemoveInfo
        | ItemQuantityChanged       of ItemQuantityChangeInfo
        | ItemWaiveReturnsChanged   of ItemWaiveReturnsInfo
        interface TypeShape.UnionContract.IUnionContract

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
    let fold state = Seq.fold evolve state
    let compact = Events.Compaction.EventType, fun state -> Events.Compacted (State.toSnapshot state)

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
        let itemExistsWithSkuId skuId                       = itemExists (fun x -> x.skuId = skuId)
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

type Handler(log, stream) =
    let inner = Equinox.Handler(Folds.fold, log, stream, maxAttempts = 3)
    member __.FlowAsync(flow, ?prepare) =
        inner.DecideAsync <| fun ctx -> async {
            let execute = Commands.interpret >> ctx.Execute
            match prepare with None -> () | Some prep -> do! prep
            return flow ctx execute }
    member __.Execute command =
        __.FlowAsync(fun _ctx execute -> execute command)
    member __.Read : Async<Folds.State> =
        inner.Query id