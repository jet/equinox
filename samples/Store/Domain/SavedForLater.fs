module Domain.SavedForLater

open System
open System.Collections.Generic

let [<Literal>] CategoryName = "SavedForLater"
let private streamId = FsCodec.StreamId.gen ClientId.toString

// NOTE - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
module Events =

    type Item =             { skuId: SkuId; dateSaved: DateTimeOffset }

    type Added =            { skus: SkuId []; dateSaved: DateTimeOffset }
    type Removed =          { skus: SkuId [] }
    type Merged =           { items: Item [] }
    type Snapshotted =      { items: Item [] }

    type Event =
        /// Checkpoint with snapshot of entire preceding event fold, avoiding need for any further reads
        | [<System.Runtime.Serialization.DataMember(Name="Snapshotted")>] Snapshotted of Snapshotted

        /// Inclusion of another set of state in this one
        | Merged of Merged
        /// Removal of a set of skus
        | Removed of Removed
        /// Addition of a collection of skus to the list
        | Added of Added
        interface TypeShape.UnionContract.IUnionContract
    let codec = EventCodec.gen<Event>

module Fold =
    open Events
    let isSupersededAt effectiveDate (item: Item) = item.dateSaved < effectiveDate
    type private InternalState(externalState: seq<Item>) =
        let index = Dictionary(seq { for i in externalState -> KeyValuePair(i.skuId, i) })

        member _.Replace (skus: seq<Item>) =
            index.Clear(); for s in skus do index[s.skuId] <- s
        member _.Append(skus: seq<Item>) =
            for sku in skus do
                let ok,found = index.TryGetValue sku.skuId
                if not ok || found |> isSupersededAt sku.dateSaved then
                    index[sku.skuId] <- sku
        member _.Remove (skus: seq<SkuId>) =
            for sku in skus do index.Remove sku |> ignore
        member _.ToExternalState () =
            index.Values |> Seq.sortBy (fun s -> -s.dateSaved.Ticks, s.skuId) |> Seq.toArray

    type State = Item []
    let initial = Array.empty<Item>

    module Snapshot =

        let generate state = Snapshotted { items = state }
        let isOrigin = function Snapshotted _ -> true | _ -> false
        let config = isOrigin, generate

    let fold (state: State) (events: seq<Event>): State =
        let index = InternalState state
        for event in events do
            match event with
            | Snapshotted { items = skus } ->   index.Replace skus
            | Merged { items = skus} ->         index.Append skus
            | Removed { skus = skus } ->        index.Remove skus
            | Added { dateSaved = d; skus = skus } ->
                index.Append(seq { for sku in skus -> { skuId = sku; dateSaved = d }})
        index.ToExternalState()
    let containsSku: State -> SkuId -> bool = Seq.map _.skuId >> HashSet >> _.Contains
    let proposedEventsWouldExceedLimit maxSavedItems events state =
        let newState = fold state events
        Array.length newState > maxSavedItems

type private Index(state: Events.Item seq) =
    let index = Dictionary(seq { for i in state -> KeyValuePair(i.skuId, i) })

    member _.DoesNotAlreadyContainSameOrMoreRecent effectiveDate sku =
        match index.TryGetValue sku with
        | true, item when item.dateSaved >= effectiveDate -> false
        | _ -> true
    member this.DoesNotAlreadyContainItem(item: Events.Item) =
        this.DoesNotAlreadyContainSameOrMoreRecent item.dateSaved item.skuId

// true if the command was executed, false if it would have breached the invariants
module Decisions =

    let private validateAgainstInvariants maxSavedItems state events =
        if Fold.proposedEventsWouldExceedLimit maxSavedItems events state then false, [||]
        else true, events

    let add maxSavedItems (dateSaved, skus) state =
        let index = Index state
        let net = skus |> Array.distinct |> Array.filter (index.DoesNotAlreadyContainSameOrMoreRecent dateSaved)
        if Array.isEmpty net then true, [||]
        else validateAgainstInvariants maxSavedItems state [| Events.Added { skus = net ; dateSaved = dateSaved } |]

    let merge maxSavedItems (merges: Events.Item []) state =
        let net = merges |> Array.filter (Index state).DoesNotAlreadyContainItem
        if Array.isEmpty net then true, [||]
        else validateAgainstInvariants maxSavedItems state [| Events.Merged { items = net } |]

    let remove skuIds (state: Fold.State) = [|
        let hasSku = Fold.containsSku state
        match [| for x in Seq.distinct skuIds do if hasSku x then x |] with
        | [||] -> ()
        | net -> Events.Removed { skus = net } |]

type Service internal (resolve: ClientId -> Equinox.Decider<Events.Event, Fold.State>, maxSavedItems) =

    do if maxSavedItems < 0 then invalidArg "maxSavedItems" "must be non-negative value."

    member _.MaxSavedItems = maxSavedItems

    member _.List clientId: Async<Events.Item []> =
        let decider = resolve clientId
        decider.Query id

    member _.Save(clientId, skus: seq<SkuId>): Async<bool> =
        let decider = resolve clientId
        decider.Transact(Decisions.add maxSavedItems (DateTimeOffset.Now, Seq.toArray skus))

    member x.Merge(clientId, targetId): Async<bool> = async {
        let! sourceContent = x.List clientId
        let decider = resolve targetId
        return! decider.Transact(Decisions.merge maxSavedItems sourceContent) }

    member _.Remove(clientId, skuIds: SkuId[]): Async<unit> =
        let decider = resolve clientId
        decider.Transact(Decisions.remove skuIds)

    member _.RemoveAll(clientId): Async<unit> =
        let decider = resolve clientId
        decider.Transact(fun state -> Decisions.remove (state |> Seq.map _.skuId) state)

let create maxSavedItems resolve =
    Service(streamId >> resolve, maxSavedItems)
