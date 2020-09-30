/// Simple example of how one might have multiple uploaders agree/share a common UploadId for a given OrderId
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

let [<Literal>] Category = "Upload"
let streamName (companyId, purchaseOrderId) = FsCodec.StreamName.compose Category [PurchaseOrderId.toString purchaseOrderId; CompanyId.toString companyId]

// shim for net461
module Seq =
    let tryLast (source : seq<_>) =
        use e = source.GetEnumerator()
        if e.MoveNext() then
            let mutable res = e.Current
            while (e.MoveNext()) do res <- e.Current
            Some res
        else
            None

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
    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()

module Fold =

    type State = UploadId option
    let initial = None
    let private evolve _ignoreState = function
        | Events.IdAssigned e -> Some e.value
    let fold (state: State) (events: seq<Events.Event>) : State =
        events |> Seq.tryLast |> Option.fold evolve state

let decide (value : UploadId) (state : Fold.State) : Choice<UploadId,UploadId> * Events.Event list =
    match state with
    | None -> Choice1Of2 value, [Events.IdAssigned { value = value}]
    | Some value -> Choice2Of2 value, []

type Service internal (resolve : CompanyId * PurchaseOrderId -> Equinox.Stream<Events.Event, Fold.State>) =

    member __.Sync(companyId, purchaseOrderId, value) : Async<Choice<UploadId,UploadId>> =
        let stream = resolve (companyId, purchaseOrderId)
        stream.Transact(decide value)

let create resolve =
    let resolve ids =
        let streamName = streamName ids
        Equinox.Stream(Serilog.Log.ForContext<Service>(), resolve streamName, maxAttempts = 3)
    Service(resolve)

module Cosmos =

    open Equinox.CosmosStore
    let create (context,cache) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, TimeSpan.FromMinutes 20.) // OR CachingStrategy.NoCaching
        let category = CosmosStoreCategory(context, Events.codec, Fold.fold, Fold.initial, cacheStrategy, AccessStrategy.LatestKnownEvent)
        create category.Resolve

module EventStore =
    open Equinox.EventStore
    let create context =
        let resolver = Resolver(context, Events.codec, Fold.fold, Fold.initial, access=AccessStrategy.LatestKnownEvent)
        create resolver.Resolve
