// PoC for managing a contiguous sequence of ids, with a Reserve -> Confirm OR Release flow allowing removal of gaps due to identifiers going unused
// See Sequence.fs, which represents a far simpler and saner form of this
module Gapless

open System

let [<Literal>] private CategoryName = "Gapless"
let private streamId = FsCodec.StreamId.gen SequenceId.toString

// NOTE - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
module Events =

    type Item = { id: int64 }
    type Snapshotted = { reservations: int64[];  nextId: int64 }
    type Event =
        | Reserved of Item
        | Confirmed of Item
        | Released of Item
        | Snapshotted of Snapshotted
        interface TypeShape.UnionContract.IUnionContract
    let codec = EventCodec.genJsonElement<Event>

module Fold =

    type State = { reserved: Set<int64>; next: int64 }
    let initial = { reserved = Set.empty; next = 0L }

    module Snapshot =
        let private isOrigin = function Events.Snapshotted _ -> true | _ -> false
        let generate state = Events.Snapshotted { reservations = Array.ofSeq state.reserved; nextId = state.next }
        let config = isOrigin, generate

    module State =
        let ofInternal (lowWatermark : int64) (reserved : int64 seq) (confirmed : int64 seq) (released : int64 seq) : State =
            failwith "TODO"
        type InternalState =
            { reserved: Set<int64>; confirmed: Set<int64>; released : Set<int64>; next: int64 }
            member x.Evolve = function
                | Events.Reserved e -> { x with reserved = x.reserved |> Set.add e.id }
                | Events.Confirmed e -> { x with confirmed = x.confirmed |> Set.add e.id }
                | Events.Released e -> { x with reserved = x.reserved |> Set.remove e.id  }
                | Events.Snapshotted e -> { reserved = set e.reservations; confirmed = Set.empty; released = Set.empty; next = e.nextId }
            member x.ToState() =
                ofInternal x.next x.reserved x.confirmed x.released
        let toInternal (state : State) : InternalState =
            { reserved = state.reserved; confirmed = Set.empty; released = Set.empty; next = state.next }
    let fold (state: State) (xs: Events.Event[]): State =
        let s = State.toInternal state
        let state' = (s, xs) ||> Array.fold _.Evolve
        state'.ToState()

let decideReserve count (state : Fold.State): int64[] * Events.Event[] =
    failwith "TODO"

let decideConfirm item (state : Fold.State) : Events.Event[] =
    failwith "TODO"

let decideRelease item (state : Fold.State) : Events.Event[] =
    failwith "TODO"

type Service internal (resolve: SequenceId -> Equinox.Decider<Events.Event, Fold.State>) =

    member _.ReserveMany(series, count) : Async<int64[]> =
        let decider = resolve series
        decider.Transact(decideReserve count)

    member x.Reserve(series) : Async<int64> = async {
        let! res = x.ReserveMany(series, 1)
        return Array.head res }

    member _.Confirm(series,item) : Async<unit> =
        let decider = resolve series
        decider.Transact(decideConfirm item)

    member _.Release(series,item) : Async<unit> =
        let decider = resolve series
        decider.Transact(decideRelease item)

let [<Literal>] appName = "equinox-tutorial-gapless"

let create cat = Service(streamId >> Equinox.Decider.forStream Serilog.Log.Logger cat)

module Cosmos =

    open Equinox.CosmosStore
    let private category (context, cache, accessStrategy) =
        let cacheStrategy = Equinox.CachingStrategy.SlidingWindow (cache, TimeSpan.FromMinutes 20.) // OR CachingStrategy.NoCaching
        CosmosStoreCategory(context, CategoryName, Events.codec, Fold.fold, Fold.initial, accessStrategy, cacheStrategy)

    module Snapshot =

        let category (context, cache) =
            category (context, cache, AccessStrategy.Snapshot Fold.Snapshot.config)

    module RollingUnfolds =

        let category (context, cache) =
            category (context, cache, AccessStrategy.RollingState Fold.Snapshot.generate)
