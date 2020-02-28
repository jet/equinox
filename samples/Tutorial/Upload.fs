/// Simple example of how one might have multiple uploaders agree/share a common UploadId for a given OrderId
module Upload

open System
open System.Text.Json
open FSharp.UMX

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

type PurchaseOrderId = int<purchaseOrderId>
and [<Measure>] purchaseOrderId
module PurchaseOrderId =
    let toString (value : PurchaseOrderId) : string = string %value

type CompanyId = string<companyId>
and [<Measure>] companyId
module CompanyId =
    let toString (value : CompanyId) : string = %value

type UploadId = string<uploadId>
and [<Measure>] uploadId
module UploadId =
    let toString (value : UploadId) : string = %value

// NOTE - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
module Events =

    let [<Literal>] categoryId = "Upload"
    let (|ForCompanyAndPurchaseOrder|) (companyId, purchaseOrderId) = FsCodec.StreamName.compose categoryId [PurchaseOrderId.toString purchaseOrderId; CompanyId.toString companyId]

    type IdAssigned = { value : UploadId }
    type Event =
        | IdAssigned of IdAssigned
        interface TypeShape.UnionContract.IUnionContract

    module Utf8ArrayCodec =
        let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()

    module JsonElementCodec =
        open FsCodec.SystemTextJson

        let encode (options: JsonSerializerOptions) = fun (evt: Event) ->
            match evt with
            | IdAssigned id -> "IdAssigned", JsonSerializer.SerializeToElement(id, options)

        let tryDecode (options: JsonSerializerOptions) = fun (eventType, data: JsonElement) ->
            match eventType with
            | "IdAssigned" -> Some (IdAssigned <| JsonSerializer.DeserializeElement<IdAssigned>(data, options))
            | _ -> None

        let codec options = FsCodec.Codec.Create<Event, JsonElement>(encode options, tryDecode options)

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

type Service internal (log, resolve, maxAttempts) =

    let resolve (Events.ForCompanyAndPurchaseOrder streamId) = Equinox.Stream(log, resolve streamId, maxAttempts)

    member __.Sync(companyId, purchaseOrderId, value) : Async<Choice<UploadId,UploadId>> =
        let stream = resolve (companyId, purchaseOrderId)
        stream.Transact(decide value)

let create resolve = Service(Serilog.Log.ForContext<Service>(), resolve, 3)

module Cosmos =

    open Equinox.Cosmos
    open FsCodec.SystemTextJson.Serialization

    let createService (context,cache) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, TimeSpan.FromMinutes 20.) // OR CachingStrategy.NoCaching
        let codec = Events.JsonElementCodec.codec JsonSerializer.defaultOptions
        let resolve = Resolver(context, codec, Fold.fold, Fold.initial, cacheStrategy, AccessStrategy.LatestKnownEvent).Resolve
        create resolve

module EventStore =
    open Equinox.EventStore
    let createService context =
        let resolve = Resolver(context, Events.Utf8ArrayCodec.codec, Fold.fold, Fold.initial, access=AccessStrategy.LatestKnownEvent).Resolve
        create resolve
