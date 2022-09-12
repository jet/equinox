// Manages a sequence of ids, without provision for returning unused ones in cases where we're potentially leaving a gap
// see Gapless.fs for a potential approach for handling such a desire
module Sequence

open System

let [<Literal>] Category = "Sequence"
let streamName id = struct (Category, SequenceId.toString id)

// NOTE - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
module Events =

    type Reserved = { next : int64 }
    type Event =
        | Reserved of Reserved
        interface TypeShape.UnionContract.IUnionContract
    let codec = FsCodec.SystemTextJson.CodecJsonElement.Create<Event>()

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

type Service internal (resolve : SequenceId -> Equinox.Decider<Events.Event, Fold.State>) =

    /// Reserves an id, yielding the reserved value. Optional <c>count</c> enables reserving more than the default count of <c>1</c> in a single transaction
    member _.Reserve(series,?count) : Async<int64> =
        let decider = resolve series
        decider.Transact(decideReserve (defaultArg count 1))

let create resolve = Service(streamName >> resolve)

module Cosmos =

    open Equinox.CosmosStore
    let private create (context, cache, accessStrategy) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, TimeSpan.FromMinutes 20.) // OR CachingStrategy.NoCaching
        CosmosStoreCategory(context, Events.codec, Fold.fold, Fold.initial, cacheStrategy, accessStrategy)
        |> Equinox.Decider.resolve (Serilog.Log.ForContext<Service>())
        |> create

    module LatestKnownEvent =

        let create (context,cache) =
            let accessStrategy = AccessStrategy.LatestKnownEvent
            create (context,cache,accessStrategy)

    module RollingUnfolds =

        let create (context,cache) =
            create (context,cache,AccessStrategy.RollingState Fold.snapshot)
