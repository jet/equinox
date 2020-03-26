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
    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()

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
