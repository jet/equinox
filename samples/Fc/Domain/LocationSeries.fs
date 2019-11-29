module Location.Series

// NOTE - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
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
    let initial = LocationEpochId.parse -1
    let evolve _state = function
        | Events.Started e -> e.epochId
    let fold = Seq.fold evolve

let interpretActivateEpoch epochId (state : Folds.State) =
    [if state < epochId then yield Events.Started { epochId = epochId }]

let toActiveEpoch state =
    if state = Folds.initial then None else Some state

type Service internal (resolve, ?maxAttempts) =

    let log = Serilog.Log.ForContext<Service>()
    let (|AggregateId|) id = Equinox.AggregateId(Events.categoryId, LocationId.toString id)
    let (|Stream|) (AggregateId id) = Equinox.Stream<Events.Event,Folds.State>(log, resolve id, maxAttempts = defaultArg maxAttempts 2)

    member __.Read(locationId) : Async<LocationEpochId option> =
        let (Stream stream) = locationId
        stream.Query toActiveEpoch

    member __.ActivateEpoch(locationId,epochId) : Async<unit> =
        let (Stream stream) = locationId
        stream.Transact(interpretActivateEpoch epochId)

let create resolve maxAttempts = Service(resolve, maxAttempts)

module Cosmos =

    open Equinox.Cosmos
    let resolve (context,cache) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        let opt = Equinox.ResolveOption.AllowStale
        fun id -> Resolver(context, Events.codec, Folds.fold, Folds.initial, cacheStrategy, AccessStrategy.LatestKnownEvent).Resolve(id,opt)
    let createService (context, cache, maxAttempts) =
        create (resolve (context,cache)) maxAttempts