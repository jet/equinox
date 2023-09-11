/// Simple example of how one might have multiple instances of an uploader app agree/share a common UploadId for a given OrderId
module Upload

open FSharp.UMX
open System

type PurchaseOrderId = int<purchaseOrderId>
and [<Measure>] purchaseOrderId
module PurchaseOrderId =
    let toString (value : PurchaseOrderId) : string = string %value

type CompanyId = string<companyId>
and [<Measure>] companyId
module CompanyId =
    let toString (value : CompanyId) : string = %value

module Stream =
    let [<Literal>] Category = "Upload"
    let id = FsCodec.StreamId.gen2 CompanyId.toString PurchaseOrderId.toString

type UploadId = string<uploadId>
and [<Measure>] uploadId
module UploadId =
    let toString (value : UploadId) : string = %value

// NOTE - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
module Events =

    type IdAssigned = { value : UploadId }
    type Event =
        | IdAssigned of IdAssigned
        interface TypeShape.UnionContract.IUnionContract
    let codec = EventCodec.gen<Event>
    let codecJe = EventCodec.genJsonElement<Event>

module Fold =

    type State = UploadId option
    let initial = None
    let private evolve _ignoreState = function
        | Events.IdAssigned e -> Some e.value
    let fold (state: State) (events: seq<Events.Event>) : State =
        events |> Seq.tryLast |> Option.fold evolve state

let decide (value : UploadId) (state : Fold.State) : Choice<UploadId,UploadId> * Events.Event[] =
    match state with
    | None -> Choice1Of2 value, [| Events.IdAssigned { value = value} |]
    | Some value -> Choice2Of2 value, [||]

type Service internal (resolve: struct (CompanyId * PurchaseOrderId) -> Equinox.Decider<Events.Event, Fold.State>) =

    member _.Sync(companyId, purchaseOrderId, value) : Async<Choice<UploadId,UploadId>> =
        let decider = resolve (companyId, purchaseOrderId)
        decider.Transact(decide value)

let create cat = Service(Stream.id >> Equinox.Decider.forStream Serilog.Log.Logger cat)

module Cosmos =

    open Equinox.CosmosStore
    let category (context, cache) =
        let cacheStrategy = Equinox.CachingStrategy.SlidingWindow (cache, TimeSpan.FromMinutes 20.) // OR CachingStrategy.NoCaching
        CosmosStoreCategory(context, Stream.Category, Events.codecJe, Fold.fold, Fold.initial, AccessStrategy.LatestKnownEvent, cacheStrategy)

module EventStore =
    open Equinox.EventStoreDb
    let category context =
        EventStoreCategory(context, Stream.Category, Events.codec, Fold.fold, Fold.initial, AccessStrategy.LatestKnownEvent, Equinox.CachingStrategy.NoCaching)
