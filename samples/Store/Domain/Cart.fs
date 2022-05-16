module Domain.Cart

let streamName (id: CartId) = FsCodec.StreamName.create "Cart" (CartId.toString id)

// NOTE - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
[<RequireQualifiedAccess>]
module Events =

    type ContextInfo =              { time: System.DateTime; requestId: RequestId }

    type ItemAddedInfo =            { context: ContextInfo; skuId: SkuId; quantity: int; waived: bool option }
    type ItemRemovedInfo =          { context: ContextInfo; skuId: SkuId }
    type ItemQuantityChangedInfo =  { context: ContextInfo; skuId: SkuId; quantity: int }
    type ItemPropertiesChangedInfo ={ context: ContextInfo; skuId: SkuId; waived: bool }

    module Compaction =
        type StateItemInfo =        { skuId: SkuId; quantity: int; returnsWaived: bool option }
        type State =                { items: StateItemInfo[] }

    type Event =
        | Snapshotted               of Compaction.State
        | ItemAdded                 of ItemAddedInfo
        | ItemRemoved               of ItemRemovedInfo
        | ItemQuantityChanged       of ItemQuantityChangedInfo
        | ItemPropertiesChanged     of ItemPropertiesChangedInfo
        interface TypeShape.UnionContract.IUnionContract
    let codec = EventCodec.create<Event>()
    let codecJe = EventCodec.createJson<Event>()

module Fold =

    type ItemInfo =                 { skuId: SkuId; quantity: int; returnsWaived: bool option }
    type State =                    { items: ItemInfo list }
    module State =
        let toSnapshot (s: State) : Events.Compaction.State =
            { items = [| for i in s.items -> { skuId = i.skuId; quantity = i.quantity; returnsWaived = i.returnsWaived } |] }
        let ofSnapshot (s: Events.Compaction.State) : State =
            { items = [ for i in s.items -> { skuId = i.skuId; quantity = i.quantity; returnsWaived = i.returnsWaived } ] }
    let initial = { items = [] }
    let evolve (state : State) event =
        let updateItems f = { state with items = f state.items }
        match event with
        | Events.Snapshotted s ->
            State.ofSnapshot s
        | Events.ItemAdded e ->
            updateItems (fun current ->
                { skuId = e.skuId; quantity = e.quantity; returnsWaived = e.waived }
                :: current)
        | Events.ItemRemoved e ->
            updateItems (List.filter (fun x -> x.skuId <> e.skuId))
        | Events.ItemQuantityChanged e ->
            updateItems (List.map (function
                | i when i.skuId = e.skuId -> { i with quantity = e.quantity }
                | i -> i))
        | Events.ItemPropertiesChanged e ->
            updateItems (List.map (function
                | i when i.skuId = e.skuId -> { i with returnsWaived = Some e.waived }
                | i -> i))
    let fold : State -> Events.Event seq -> State = Seq.fold evolve
    let isOrigin = function Events.Snapshotted _ -> true | _ -> false
    let snapshot = State.toSnapshot >> Events.Snapshotted

type Context =              { time: System.DateTime; requestId : RequestId }
type Command =
    | SyncItem              of Context * SkuId * quantity: int option * waived: bool option

let interpret command (state : Fold.State) =
    let itemExists f                                    = state.items |> List.exists f
    let itemExistsWithDifferentWaiveStatus skuId waive  = itemExists (fun x -> x.skuId = skuId && x.returnsWaived <> Some waive)
    let itemExistsWithDifferentQuantity skuId quantity  = itemExists (fun x -> x.skuId = skuId && x.quantity <> quantity)
    let itemExistsWithSkuId skuId                       = itemExists (fun x -> x.skuId = skuId)
    let toEventContext (reqContext: Context)            = { requestId = reqContext.requestId; time = reqContext.time } : Events.ContextInfo
    let (|Context|) (context : Context)                 = toEventContext context
    let maybePropChanges c skuId = function
        | None -> []
        | Some waived ->
            if not (itemExistsWithDifferentWaiveStatus skuId waived) then []
            else [ Events.ItemPropertiesChanged { context = c; skuId = skuId; waived = waived } ]
    let maybeQuantityChanges c skuId quantity =
        if not (itemExistsWithDifferentQuantity skuId quantity) then [] else
        [ Events.ItemQuantityChanged { context = c; skuId = skuId; quantity = quantity } ]
    match command with
    // a request to set quantity of `0` represents a removal request
    | SyncItem (Context c, skuId, Some 0, _) ->
        if itemExistsWithSkuId skuId then [ Events.ItemRemoved { context = c; skuId = skuId } ]
        else []
    // Add/quantity change with potential waive change at same time
    | SyncItem (Context c, skuId, Some q, w) ->
        if itemExistsWithSkuId skuId then maybeQuantityChanges c skuId q @ maybePropChanges c skuId w
        else [ Events.ItemAdded { context = c; skuId = skuId; quantity = q; waived = w } ]
    // Waive return status change only
    | SyncItem (Context c, skuId, None, w) ->
        maybePropChanges c skuId w

#if ACCUMULATOR
// This was once part of the core Equinox functionality, but was removed in https://github.com/jet/equinox/pull/184
// it remains here solely to serve as an example; the PR details the considerations leading to this conclusion

/// Maintains a rolling folded State while Accumulating Events pended as part of a decision flow
type Accumulator<'event, 'state>(fold : 'state -> 'event seq -> 'state, originState : 'state) =
    let accumulated = ResizeArray<'event>()

    /// The Events that have thus far been pended via the `decide` functions `Execute`/`Decide`d during the course of this flow
    member _.Accumulated : 'event list =
        accumulated |> List.ofSeq

    /// The current folded State, based on the Stream's `originState` + any events that have been Accumulated during the the decision flow
    member _.State : 'state =
        accumulated |> fold originState

    /// Invoke a decision function, gathering the events (if any) that it decides are necessary into the `Accumulated` sequence
    member x.Transact(interpret : 'state -> 'event list) : unit =
        interpret x.State |> accumulated.AddRange
    /// Invoke an Async decision function, gathering the events (if any) that it decides are necessary into the `Accumulated` sequence
    member x.Transact(interpret : 'state -> Async<'event list>) : Async<unit> = async {
        let! events = interpret x.State
        accumulated.AddRange events }
    /// Invoke a decision function, while also propagating a result yielded as the fst of an (result, events) pair
    member x.Transact(decide : 'state -> 'result * 'event list) : 'result =
        let result, newEvents = decide x.State
        accumulated.AddRange newEvents
        result
    /// Invoke a decision function, while also propagating a result yielded as the fst of an (result, events) pair
    member x.Transact(decide : 'state -> Async<'result * 'event list>) : Async<'result> = async {
        let! result, newEvents = decide x.State
        accumulated.AddRange newEvents
        return result }
#else
let interpretMany fold interpreters (state : 'state) : 'state * 'event list =
    ((state,[]),interpreters)
    ||> Seq.fold (fun (state : 'state, acc : 'event list) interpret ->
        let events = interpret state
        let state' = fold state events
        state', acc @ events)
#endif

type Service internal (resolve : CartId -> Equinox.Decider<Events.Event, Fold.State>) =

    member _.Run(cartId, optimistic, commands : Command seq, ?prepare) : Async<Fold.State> =
        let interpret state = async {
            match prepare with None -> () | Some prep -> do! prep
#if ACCUMULATOR
            let acc = Accumulator(Fold.fold, state)
            for cmd in commands do
                acc.Transact(interpret cmd)
            return acc.State, acc.Accumulated }
#else
            return interpretMany Fold.fold (Seq.map interpret commands) state }
#endif
        let decider = resolve cartId
        let opt = if optimistic then Equinox.AllowStale else Equinox.RequireLoad
        decider.TransactAsync(interpret, opt)

    member x.ExecuteManyAsync(cartId, optimistic, commands : Command seq, ?prepare) : Async<unit> =
        x.Run(cartId, optimistic, commands, ?prepare=prepare) |> Async.Ignore

    member x.Execute(cartId, command) =
        x.ExecuteManyAsync(cartId, false, [command])

    member _.Read cartId =
        let decider = resolve cartId
        decider.Query id
    member _.ReadStale cartId =
        let decider = resolve cartId
        decider.Query(id, Equinox.LoadOption.AllowStale)

let create log resolveStream =
    let resolve id =
        let stream = resolveStream (streamName id)
        Equinox.Decider(log, stream, maxAttempts = 3)
    Service(resolve)
