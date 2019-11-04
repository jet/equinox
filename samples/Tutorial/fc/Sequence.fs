// Manages a sequence of ids, without provision for returning unused ones in cases where we're potentially leaving a gap
// see Gapless.fs for a potential approach for handling such a desire
module Fc.Sequence

// NB - these schemas reflect the actual storage formats and hence need to be versioned with care
module Events =

    type Item = { next : int64 }
    type Event =
        | Reserved of Item
        interface TypeShape.UnionContract.IUnionContract
    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()
    let [<Literal>] categoryId = "Sequence"

module Folds =

    type State = { next : int64 }
    let initial = { next = 0L }
    let private evolve _ignoreState = function
        | Events.Reserved e -> { next = e.next }
    let fold (state: State) (events: seq<Events.Event>) : State =
        Seq.tryLast events |> Option.fold evolve state
    let isOrigin _ = true
    let transmute events _state =
        [],List.tryLast events |> Option.toList

let decideReserve (count : int) (state : Folds.State) : int64 * Events.Event list =
    state.next,[Events.Reserved { next = state.next + int64 count }]

type Service(log, resolveStream, ?maxAttempts) =

    let (|AggregateId|) id = Equinox.AggregateId(Events.categoryId, id)
    let (|Stream|) (AggregateId aggregateId) = Equinox.Stream(log, resolveStream aggregateId, defaultArg maxAttempts 3)

    let decide (Stream stream) : (Folds.State -> 'r * Events.Event list) -> Async<'r> = stream.Transact

    /// Reserves an id, yielding the reserved value. Optional <c>count</c> enables reserving more than the default count of <c>1</c> in a single transaction
    member __.Reserve(series,?count) : Async<int64> =
        decide series (decideReserve (defaultArg count 1))

let appName = "equinox-tutorial-sequence"

module Cosmos =

    open Equinox.Cosmos
    open System

    let read key = System.Environment.GetEnvironmentVariable key |> Option.ofObj |> Option.get
    let connector = Connector(TimeSpan.FromSeconds 5., 2, TimeSpan.FromSeconds 5., log=Serilog.Log.Logger, mode=ConnectionMode.Gateway)
    let connection = connector.Connect(appName, Discovery.FromConnectionString (read "EQUINOX_COSMOS_CONNECTION")) |> Async.RunSynchronously
    let context = Context(connection, read "EQUINOX_COSMOS_DATABASE", read "EQUINOX_COSMOS_CONTAINER")

    let private createService (context,cache,accessStrategy) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, TimeSpan.FromMinutes 20.) // OR CachingStrategy.NoCaching
        let resolve = Resolver(context, Events.codec, Folds.fold, Folds.initial, cacheStrategy, accessStrategy).Resolve
        Service(Serilog.Log.Logger, resolve)

    module AnyKnownEventType =

        let createService (context,cache) =
            let accessStrategy = AccessStrategy.AnyKnownEventType
            createService (context,cache,accessStrategy)

    module RollingUnfolds =

        let createService (context,cache) =
            createService (context,cache,AccessStrategy.RollingUnfolds (Folds.isOrigin,Folds.transmute))