module Fc.Location.Epoch

open Equinox.Cosmos.Store
open Fc

// NOTE - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
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
    let empty = 0 : Balance
    let (|Balance|) = function
        | Initial -> empty
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

let render = function
    | Folds.Initial | Folds.Open _ as state -> let (Folds.Balance bal) = state in Open bal
    | Folds.Closed bal -> Closed bal

type Command =
    | Sync
    | Execute of decide : (Folds.State -> Result*Events.Event list)

let decideSync (balanceCarriedForward : Folds.Balance) (command : Command) : Folds.State -> Result*Events.Event list = function
    | Folds.Initial -> Open balanceCarriedForward,[Events.CarriedForward { initial = balanceCarriedForward }]
    | Folds.Open bal -> Open bal,[]
    | Folds.Closed bal -> Closed bal,[]

let decide (command : Command) : Folds.State -> Result*Events.Event list =
    match state with
    | Folds.Initial -> Open balanceCarriedForward,[Events.CarriedForward { initial = Folds.zero }]
    | Folds.Open bal -> Open bal,[match command with Sync -> () | Execute decide -> yield! decide]
    | Folds.Closed bal -> Closed bal,[]

type Service internal (resolve, ?maxAttempts) =

    let log = Serilog.Log.ForContext<Service>()
    let (|AggregateId|) (locationId,epochId) =
        let id = sprintf "%s_%s" (LocationId.toString locationId) (LocationEpochId.toString epochId)
        Equinox.AggregateId(Events.categoryId, id)
    let (|Stream|) opt (AggregateId id) = Equinox.Stream<Events.Event,Folds.State>(log, resolve (id,opt), maxAttempts = defaultArg maxAttempts 2)
    let query (Stream None stream) = stream.Query
    let decide (Stream None stream) : (Folds.State -> 'r * Events.Event list) -> Async<'r> = stream.Transact

    /// Fetch the (current, not stale) value and open/closed state for the specified epoch
    member __.Read(locationId, epochId) : Async<Result> =
        query (locationId,epochId) render

    /// Given <c>prevEpochBalanceCarriedForward</c>, read the latest value or establish the new stream with its opening event
    member __.Execute(locationId, epochId, prevEpochBalanceCarriedForward, command) : Async<Result> =
        decide (locationId,epochId) (decideSync prevEpochBalanceCarriedForward command)
    member __.Execute(locationId, epochId, command) : Async<Result> =
        decide (locationId,epochId) (decide command)

let createService resolve = Service(resolve)

module Cosmos =

    open Equinox.Cosmos
    let resolve (context,cache) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        fun (id,opt) -> Resolver(context, Events.codec, Folds.fold, Folds.initial, cacheStrategy, AccessStrategy.LatestKnownEvent).Resolve(id,?option=opt)
    let createService (context,cache) =
        createService (resolve (context,cache))