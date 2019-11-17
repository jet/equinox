module Fc.Location.Epoch

open Fc

// NB - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
[<RequireQualifiedAccess>]
module Events =

    type CarriedForward = { initial : int }
    type Delta = { value : int }
    type Event =
        | CarriedForward of CarriedForward
        | Closed
        | Delta of Delta
        interface TypeShape.UnionContract.IUnionContract
    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()
    let [<Literal>] categoryId = "LocationEpoch"

module Folds =

    type Balance = int
    type State = Initial | Open of Balance | Closed of Balance
    let initial = Initial
    let (|Balance|) = function
        | Initial -> 0 : Balance
        | Open bal -> bal
        | Closed bal -> bal
    let evolve state = function
        | Events.CarriedForward e -> state |> function
            | Initial -> Open e.initial
            | x -> failwithf "CarriedForward : Unexpected %A" x
        | Events.Delta e ->
            let (Balance bal) = state
            Open (bal + e.value)
        | Events.Closed ->
            let (Balance bal) = state
            Closed bal
    let fold = Seq.fold evolve

type Result =
    | Open of int
    | Closed of int

let render state =
    let (Folds.Balance bal) = state
    match state with
    | Folds.Initial | Folds.Open _ -> Open bal
    | Folds.Closed bal -> Closed bal

let decideSync balance state : Result*Events.Event list =
    failwith "TODO"

type Service internal (resolve, ?maxAttempts) =

    let log = Serilog.Log.ForContext<Service>()
    let (|AggregateId|) (locationId,epochId) =
        let id = sprintf "%s_%s" (LocationId.toString locationId) (LocationEpochId.toString epochId)
        Equinox.AggregateId(Events.categoryId, id)
    let (|Stream|) (AggregateId id) = Equinox.Stream<Events.Event,Folds.State>(log, resolve id, maxAttempts = defaultArg maxAttempts 2)
    let query (Stream stream) = stream.Query
    let decide (Stream stream) : (Folds.State -> 'r * Events.Event list) -> Async<'r> = stream.Transact

    member __.Read(locationId,epochId) : Async<Result> = query (locationId,epochId) render
    member __.Sync(locationId,epochId,balance) : Async<Result> = decide (locationId,epochId) (decideSync balance)

let createService resolve = Service(resolve)

module Cosmos =

    open Equinox.Cosmos
    let resolve (context,cache) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        let opt = Equinox.ResolveOption.AllowStale
        fun id -> Resolver(context, Events.codec, Folds.fold, Folds.initial, cacheStrategy, AccessStrategy.LatestKnownEvent).Resolve(id,opt)
    let createService (context,cache) =
        createService (resolve (context,cache))