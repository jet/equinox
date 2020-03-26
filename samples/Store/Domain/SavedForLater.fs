module Domain.SavedForLater

open System
open System.Collections.Generic

let streamName (id: ClientId) = FsCodec.StreamName.create "SavedForLater" (ClientId.toString id)

// NOTE - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
module Events =

    type Item =             { skuId : SkuId; dateSaved : DateTimeOffset }

    type Added =            { skus : SkuId []; dateSaved : DateTimeOffset }
    type Removed =          { skus : SkuId [] }
    type Merged =           { items : Item [] }
    module Compaction =
        type Compacted =    { items : Item [] }
        // NB need to revise this tag if you break the unfold schema
        let [<Literal>] EventType = "compacted"

    type Event =
        /// Checkpoint with snapshot of entire preceding event fold, avoiding need for any further reads
        | [<System.Runtime.Serialization.DataMember(Name=Compaction.EventType)>] Compacted of Compaction.Compacted

        /// Inclusion of another set of state in this one
        | Merged of Merged
        /// Removal of a set of skus
        | Removed of Removed
        /// Addition of a collection of skus to the list
        | Added of Added
        interface TypeShape.UnionContract.IUnionContract
    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()

module Fold =
    open Events
    let isSupersededAt effectiveDate (item : Item) = item.dateSaved < effectiveDate
    type private InternalState(externalState : seq<Item>) =
        let index = Dictionary<_,_>()
        do for i in externalState do index.[i.skuId] <- i

        member __.Replace (skus : seq<Item>) =
            index.Clear() ; for s in skus do index.[s.skuId] <- s
        member __.Append(skus : seq<Item>) =
            for sku in skus do
                let ok,found = index.TryGetValue sku.skuId
                if not ok || found |> isSupersededAt sku.dateSaved then
                    index.[sku.skuId] <- sku
        member __.Remove (skus : seq<SkuId>) =
            for sku in skus do index.Remove sku |> ignore
        member __.ToExernalState () =
            index.Values |> Seq.sortBy (fun s -> -s.dateSaved.Ticks, s.skuId) |> Seq.toArray

    type State = Item []
    let initial = Array.empty<Item>
    let fold (state : State) (events : seq<Event>) : State =
        let index = InternalState state
        for event in events do
            match event with
            | Compacted { items = skus } -> index.Replace skus
            | Merged { items = skus} ->     index.Append skus
            | Removed { skus = skus } ->    index.Remove skus
            | Added { dateSaved = d; skus = skus } ->
                index.Append(seq { for sku in skus -> { skuId = sku; dateSaved = d }})
        index.ToExernalState()
    let proposedEventsWouldExceedLimit maxSavedItems events state =
        let newState = fold state events
        Array.length newState > maxSavedItems
    let isOrigin = function Compacted _ -> true | _ -> false
    let compact state = Events.Compacted { items = state }

type Command =
    | Merge of merges : Events.Item []
    | Remove of skuIds : SkuId []
    | Add of dateSaved : DateTimeOffset * skuIds : SkuId []

type private Index(state : Events.Item seq) =
    let index = Dictionary<_,_>()
    do for i in state do do index.[i.skuId] <- i

    member __.DoesNotAlreadyContainSameOrMoreRecent effectiveDate sku =
        match index.TryGetValue sku with
        | true,item when item.dateSaved >= effectiveDate -> false
        | _ -> true
    member this.DoesNotAlreadyContainItem(item : Events.Item) =
        this.DoesNotAlreadyContainSameOrMoreRecent item.dateSaved item.skuId

// yields true if the command was executed, false if it would have breached the invariants
let decide (maxSavedItems : int) (cmd : Command) (state : Fold.State) : bool * Events.Event list =
    let validateAgainstInvariants events =
        if Fold.proposedEventsWouldExceedLimit maxSavedItems events state then false, []
        else true, events
    match cmd with
    | Merge merges ->
        let net = merges |> Array.filter (Index state).DoesNotAlreadyContainItem
        if Array.isEmpty net then true, []
        else validateAgainstInvariants [ Events.Merged { items = net } ]
    | Remove skuIds ->
        let content = seq { for item in state -> item.skuId } |> set
        let net = skuIds |> Array.filter content.Contains
        if Array.isEmpty net then true, []
        else true, [ Events.Removed { skus = net } ]
    | Add (dateSaved, skus) ->
        let index = Index state
        let net = skus |> Array.filter (index.DoesNotAlreadyContainSameOrMoreRecent dateSaved)
        if Array.isEmpty net then true, []
        else validateAgainstInvariants [ Events.Added { skus = net ; dateSaved = dateSaved } ]
