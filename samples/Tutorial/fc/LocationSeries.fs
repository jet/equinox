module Fc.Location.Series

open Fc

// NB - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
[<RequireQualifiedAccess>]
module Events =

    type Started = { epochId : LocationEpochId }
    type Event =
        | Started of Started
        interface TypeShape.UnionContract.IUnionContract
    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()
    let [<Literal>] categoryId = "LocationSeries"

module Folds =

    type State = LocationEpochId
    let initial = 0<locationEpochId>
    let evolve _state = function
        | Events.Started e -> e.epochId
    let fold = Seq.fold evolve

let interpret epochId (state : Folds.State) =
    [if state < epochId then yield Events.Started { epochId = epochId }]

type Service internal (resolve, ?maxAttempts) =

    let log = Serilog.Log.ForContext<Service>()
    let (|AggregateId|) id = Equinox.AggregateId(Events.categoryId, LocationId.toString id)
    let (|Stream|) (AggregateId id) = Equinox.Stream<Events.Event,Folds.State>(log, resolve id, maxAttempts = defaultArg maxAttempts 2)
    let query (Stream stream) = stream.Query
    let execute (Stream stream) = interpret >> stream.Transact

    member __.Read(locationId) : Async<LocationEpochId> = query locationId id
    member __.MarkActive(locationId,epochId) : Async<unit> = execute locationId epochId

let createService resolve = Service(resolve)

module Cosmos =

    open Equinox.Cosmos
    let resolve (context,cache) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        let opt = Equinox.ResolveOption.AllowStale
        fun id -> Resolver(context, Events.codec, Folds.fold, Folds.initial, cacheStrategy, AccessStrategy.LatestKnownEvent).Resolve(id,opt)
    let createService (context,cache) =
        createService (resolve (context,cache))