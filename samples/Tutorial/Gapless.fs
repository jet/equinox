// PoC for managing a contiguous sequence of ids, with a Reserve -> Confirm OR Release flow allowing removal of gaps due to identifiers going unused
// See Sequence.fs, which represents a far simpler and saner form of this
module Gapless

open System

// NOTE - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
module Events =
    type Item = { id : int64 }
    type Snapshotted = { reservations : int64[];  nextId : int64 }
    type Event =
        | Reserved of Item
        | Confirmed of Item
        | Released of Item
        | Snapshotted of Snapshotted
        interface TypeShape.UnionContract.IUnionContract
    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()
    let [<Literal>] categoryId = "Gapless"
    let (|ForSequenceId|) id = Equinox.AggregateId(categoryId, SequenceId.toString id)

module Fold =

    type State = { reserved : Set<int64>; next : int64 }
    let initial = { reserved = Set.empty; next = 0L }
    module State =
        let ofInternal (lowWatermark : int64) (reserved : int64 seq) (confirmed : int64 seq) (released : int64 seq) : State =
            failwith "TODO"
        type InternalState =
            { reserved : Set<int64>; confirmed : Set<int64>; released : Set<int64>; next : int64 }
            member x.Evolve = function
                | Events.Reserved e -> { x with reserved = x.reserved |> Set.add e.id }
                | Events.Confirmed e -> { x with confirmed = x.confirmed |> Set.add e.id }
                | Events.Released e -> { x with reserved = x.reserved |> Set.remove e.id  }
                | Events.Snapshotted e -> { reserved = set e.reservations; confirmed = Set.empty; released = Set.empty; next = e.nextId }
            member x.ToState() =
                ofInternal x.next x.reserved x.confirmed x.released
        let toInternal (state : State) : InternalState =
            { reserved = state.reserved; confirmed = Set.empty; released = Set.empty; next = state.next }
    let fold (state : State) (xs : Events.Event seq) : State =
        let s = State.toInternal state
        let state' = (s,xs) ||> Seq.fold (fun s -> s.Evolve)
        state'.ToState()
    let isOrigin = function Events.Snapshotted _ -> true | _ -> false
    let snapshot state = Events.Snapshotted { reservations = Array.ofSeq state.reserved; nextId = state.next }

let decideReserve count (state : Fold.State) : int64 list*Events.Event list =
    failwith "TODO"

let decideConfirm item (state : Fold.State) : Events.Event list =
    failwith "TODO"

let decideRelease item (state : Fold.State) : Events.Event list =
    failwith "TODO"

type Service(log, resolve, ?maxAttempts) =

    let resolve (Events.ForSequenceId streamId) = Equinox.Stream(log, resolve streamId, defaultArg maxAttempts 3)

    member __.ReserveMany(series,count) : Async<int64 list> =
        let stream = resolve series
        stream.Transact(decideReserve count)

    member __.Reserve(series) : Async<int64> = async {
        let! res = __.ReserveMany(series,1)
        return List.head res }

    member __.Confirm(series,item) : Async<unit> =
        let stream = resolve series
        stream.Transact(decideConfirm item)

    member __.Release(series,item) : Async<unit> =
        let stream = resolve series
        stream.Transact(decideRelease item)

let [<Literal>] appName = "equinox-tutorial-gapless"

module Cosmos =

    open Equinox.Cosmos
    let private createService (context,cache,accessStrategy) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, TimeSpan.FromMinutes 20.) // OR CachingStrategy.NoCaching
        let resolve = Resolver(context, Events.codec, Fold.fold, Fold.initial, cacheStrategy, accessStrategy).Resolve
        Service(Serilog.Log.Logger, resolve)

    module Snapshot =

        let createService (context,cache) =
            let accessStrategy = AccessStrategy.Snapshot (Fold.isOrigin,Fold.snapshot)
            createService(context,cache,accessStrategy)

    module RollingUnfolds =

        let createService (context,cache) =
            let accessStrategy = AccessStrategy.RollingState Fold.snapshot
            createService(context,cache,accessStrategy)