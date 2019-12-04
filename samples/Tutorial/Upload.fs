/// Simple example of how one might have multiple uploaders agree/share a common UploadId for a given OrderId
module Upload

open System
open FSharp.UMX

type OrderId = string<orderNo>
and [<Measure>] orderNo
module OrderId =
    let toString (value : OrderId) : string = %value

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
    let [<Literal>] categoryId = "Upload"
    let (|AggregateId|) id = Equinox.AggregateId(categoryId, OrderId.toString id)

module Folds =

    type State = UploadId option
    let initial = None
    let private evolve _ignoreState = function
        | Events.IdAssigned e -> Some e.value
    let fold (state: State) (events: seq<Events.Event>) : State =
        Seq.tryLast events |> Option.fold evolve state

let decide (value : UploadId) (state : Folds.State) : Choice<unit,UploadId> * Events.Event list =
    match state with
    | None -> Choice1Of2 (), [Events.IdAssigned { value = value}]
    | Some value -> Choice2Of2 value, []

type Service internal (resolveStream, ?maxAttempts) =

    let log = Serilog.Log.ForContext<Service>()
    let (|Stream|) (Events.AggregateId streamId) = Equinox.Stream(log, resolveStream streamId, defaultArg maxAttempts 3)

    member __.Sync(orderId,value) : Async<Choice<unit,UploadId>> =
        let (Stream stream) = orderId
        stream.Transact(decide value)

let create resolveStream = Service(resolveStream)

module Cosmos =

    open Equinox.Cosmos
    let createService (context,cache) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, TimeSpan.FromMinutes 20.) // OR CachingStrategy.NoCaching
        let resolve = Resolver(context, Events.codec, Folds.fold, Folds.initial, cacheStrategy, AccessStrategy.LatestKnownEvent).Resolve
        create resolve

module EventStore =
    open Equinox.EventStore
    let createService context =
        let resolve = Resolver(context, Events.codec, Folds.fold, Folds.initial, access=AccessStrategy.LatestKnownEvent).Resolve
        create resolve