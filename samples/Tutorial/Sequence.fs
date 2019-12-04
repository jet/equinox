// Manages a sequence of ids, without provision for returning unused ones in cases where we're potentially leaving a gap
// see Gapless.fs for a potential approach for handling such a desire
module Sequence

open System

// NOTE - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
module Events =

    type Reserved = { next : int64 }
    type Event =
        | Reserved of Reserved
        interface TypeShape.UnionContract.IUnionContract
    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()
    let [<Literal>] categoryId = "Sequence"
    let (|AggregateId|) id = Equinox.AggregateId(categoryId, SequenceId.toString id)

module Folds =

    type State = { next : int64 }
    let initial = { next = 0L }
    let private evolve _ignoreState = function
        | Events.Reserved e -> { next = e.next }
    let fold (state: State) (events: seq<Events.Event>) : State =
        Seq.tryLast events |> Option.fold evolve state
    let snapshot (state : State) = Events.Reserved { next = state.next }

let decideReserve (count : int) (state : Folds.State) : int64 * Events.Event list =
    state.next,[Events.Reserved { next = state.next + int64 count }]

type Service internal (resolveStream, ?maxAttempts) =

    let log = Serilog.Log.ForContext<Service>()
    let (|Stream|) (Events.AggregateId aggregateId) = Equinox.Stream(log, resolveStream aggregateId, defaultArg maxAttempts 3)

    /// Reserves an id, yielding the reserved value. Optional <c>count</c> enables reserving more than the default count of <c>1</c> in a single transaction
    member __.Reserve(series,?count) : Async<int64> =
        let (Stream stream) = series
        stream.Transact(decideReserve (defaultArg count 1))

let create resolveStream = Service(resolveStream)

module Cosmos =

    open Equinox.Cosmos
    let private createService (context,cache,accessStrategy) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, TimeSpan.FromMinutes 20.) // OR CachingStrategy.NoCaching
        let resolve = Resolver(context, Events.codec, Folds.fold, Folds.initial, cacheStrategy, accessStrategy).Resolve
        create resolve

    module LatestKnownEvent =

        let createService (context,cache) =
            let accessStrategy = AccessStrategy.LatestKnownEvent
            createService (context,cache,accessStrategy)

    module RollingUnfolds =

        let createService (context,cache) =
            createService (context,cache,AccessStrategy.RollingState Folds.snapshot)