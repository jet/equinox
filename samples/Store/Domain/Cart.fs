module Domain.Cart

module Stream =
    let [<Literal>] CategoryName = "Cart"
    let id = FsCodec.StreamId.gen CartId.toString
    let name = id >> FsCodec.StreamName.create CategoryName

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
    let codec = EventCodec.gen<Event>
    let codecJe = EventCodec.genJsonElement<Event>

module Fold =

    type ItemInfo =                 { skuId: SkuId; quantity: int; returnsWaived: bool option }
    type State =                    { items: ItemInfo list }
    let initial = { items = [] }

    module Snapshot =

        let generate (s: State) =
            Events.Snapshotted <|
            { items = [| for i in s.items -> { skuId = i.skuId; quantity = i.quantity; returnsWaived = i.returnsWaived } |] }
        let isOrigin = function Events.Snapshotted _ -> true | _ -> false
        let config = isOrigin, generate
        let hydrate (s: Events.Compaction.State): State =
            { items = [ for i in s.items -> { skuId = i.skuId; quantity = i.quantity; returnsWaived = i.returnsWaived } ] }

        // NOTE Should match the event type that is stored, which does not necessarily match the case name
        // e.g. if you override the name via [<DataMember(Name="snapshot-2")>], it needs to reflect that
        let eventCaseName = nameof Events.Snapshotted

    let private evolve (state: State) event =
        let updateItems f = { items = f state.items }
        match event with
        | Events.Snapshotted s ->
            Snapshot.hydrate s
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
    let fold = Array.fold evolve

type Context =              { time: System.DateTime; requestId: RequestId }
type ItemInfo =             Context * SkuId * (*quantity:*) int option * (*waived:*) bool option

let interpretSync (item: ItemInfo) (state: Fold.State) =
    let itemExists f                                    = state.items |> List.exists f
    let itemExistsWithDifferentWaiveStatus skuId waive  = itemExists (fun x -> x.skuId = skuId && x.returnsWaived <> Some waive)
    let itemExistsWithDifferentQuantity skuId quantity  = itemExists (fun x -> x.skuId = skuId && x.quantity <> quantity)
    let itemExistsWithSkuId skuId                       = itemExists (fun x -> x.skuId = skuId)
    let toEventContext (reqContext: Context)            = { requestId = reqContext.requestId; time = reqContext.time }: Events.ContextInfo
    let (|Context|) (context: Context)                  = toEventContext context
    let maybePropChanges c skuId maybeWaived = seq {
        match maybeWaived with
        | Some waived when itemExistsWithDifferentWaiveStatus skuId waived ->
            Events.ItemPropertiesChanged { context = c; skuId = skuId; waived = waived }
        | _ -> () }
    let maybeQuantityChanges c skuId quantity = seq {
        if itemExistsWithDifferentQuantity skuId quantity then
            Events.ItemQuantityChanged { context = c; skuId = skuId; quantity = quantity } }
    [| match item with
        // a request to set quantity of `0` represents a removal request
        | Context c, skuId, Some 0, _ ->
            if itemExistsWithSkuId skuId then
                yield Events.ItemRemoved { context = c; skuId = skuId }
        // Add/quantity change with potential waive change at same time
        | Context c, skuId, Some q, w ->
            if itemExistsWithSkuId skuId then yield! maybeQuantityChanges c skuId q; yield! maybePropChanges c skuId w
            else yield Events.ItemAdded { context = c; skuId = skuId; quantity = q; waived = w }
        // Waive return status change only
        | Context c, skuId, None, w ->
            yield! maybePropChanges c skuId w |]

// See DOCUMENTATION.md for an overview of this helper and why it exists
let interpretMany fold (interpreters: seq<'state -> 'event[]>) (state: 'state): 'event[] = [|
    let mutable state = state
    for interpret in interpreters do
        let events = interpret state
        state <- fold state events
        yield! events |]

type Service internal (resolve: CartId -> Equinox.Decider<Events.Event, Fold.State>) =

    // NOTE in a real app, you should NOT emit the post-state like this
    //      (it's passed out to validate that Read yields an identical result to what the writer wrote for the purposes of validating the Store)
    member _.RunInternal(cartId, optimistic, items: ItemInfo seq, ?prepare): Async<Fold.State> =
        let interpret state = async {
            match prepare with None -> () | Some prep -> do! prep
            return interpretMany Fold.fold (Seq.map interpretSync items) state }
        let decider = resolve cartId
        let opt = if optimistic then Equinox.LoadOption.AnyCachedValue else Equinox.LoadOption.RequireLoad
        decider.Transact(interpret, id, opt)

    member x.SyncItems(cartId, optimistic, items: ItemInfo seq, ?prepare): Async<unit> =
        x.RunInternal(cartId, optimistic, items, ?prepare = prepare) |> Async.Ignore

    member _.Read cartId =
        let decider = resolve cartId
        decider.Query id
    member _.ReadAnyCachedValue cartId =
        let decider = resolve cartId
        decider.Query(id, Equinox.LoadOption.AnyCachedValue)

let create resolve =
    Service(Stream.id >> resolve)
