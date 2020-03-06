// Manages a sequence of ids, without provision for returning unused ones in cases where we're potentially leaving a gap
// see Gapless.fs for a potential approach for handling such a desire
module Sequence

open System

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

// NOTE - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
module Events =

    let [<Literal>] categoryId = "Sequence"
    let (|ForSequenceId|) id = FsCodec.StreamName.create categoryId (SequenceId.toString id)

    type Reserved = { next : int64 }
    type Event =
        | Reserved of Reserved
        interface TypeShape.UnionContract.IUnionContract

    let codecNewtonsoft = FsCodec.NewtonsoftJson.Codec.Create<Event>()
    let codecStj = FsCodec.SystemTextJson.Codec.Create<Event>()

module Fold =

    type State = { next : int64 }
    let initial = { next = 0L }
    let private evolve _ignoreState = function
        | Events.Reserved e -> { next = e.next }
    let fold (state: State) (events: seq<Events.Event>) : State =
        Seq.tryLast events |> Option.fold evolve state
    let snapshot (state : State) = Events.Reserved { next = state.next }

let decideReserve (count : int) (state : Fold.State) : int64 * Events.Event list =
    state.next,[Events.Reserved { next = state.next + int64 count }]

type Service internal (log, resolve, maxAttempts) =

    let resolve (Events.ForSequenceId streamId) = Equinox.Stream(log, resolve streamId, maxAttempts)

    /// Reserves an id, yielding the reserved value. Optional <c>count</c> enables reserving more than the default count of <c>1</c> in a single transaction
    member __.Reserve(series,?count) : Async<int64> =
        let stream = resolve series
        stream.Transact(decideReserve (defaultArg count 1))

let create resolve = Service(Serilog.Log.ForContext<Service>(), resolve, maxAttempts = 3)

module Cosmos =

    open Equinox.Cosmos

    let private createService (context,cache,accessStrategy) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, TimeSpan.FromMinutes 20.) // OR CachingStrategy.NoCaching
        let resolve = Resolver(context, Events.codecStj, Fold.fold, Fold.initial, cacheStrategy, accessStrategy).Resolve
        create resolve

    module LatestKnownEvent =

        let createService (context,cache) =
            let accessStrategy = AccessStrategy.LatestKnownEvent
            createService (context,cache,accessStrategy)

    module RollingUnfolds =

        let createService (context,cache) =
            createService (context,cache,AccessStrategy.RollingState Fold.snapshot)
