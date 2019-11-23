module TicketAllocator

open System

// NOTE - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
module Events =

    type Commenced =    { transId : TicketTransId; cutoff : DateTimeOffset }
    type Completed =    { transId : TicketTransId; reason : Reason }
    and  [<Newtonsoft.Json.JsonConverter(typeof<FsCodec.NewtonsoftJson.TypeSafeEnumConverter>)>]
         Reason = Ok | TimedOut | Cancelled
    type Snapshotted =  { active : Commenced option }
    type Event =
        | Commenced     of Commenced
        | Completed     of Completed
        interface TypeShape.UnionContract.IUnionContract
    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()
    let [<Literal>] categoryId = "TicketAllocator"

module Folds =

    type State = Events.Commenced option
    let initial = None
    let evolve _state = function
        | Events.Commenced e -> Some e
        | Events.Completed e -> None
    let fold : State -> Events.Event seq -> State = Seq.fold evolve

type Command =
    | Commence  of transId : TicketTransId * cutoffTime : DateTimeOffset
    | Complete  of transId : TicketTransId * reason     : Events.Reason

let decide command (state : Folds.State) =
    match command, state with
    | Commence (transId, cutoff), None ->
        true, [Events.Commenced { transId = transId; cutoff = cutoff }]
    | Commence (transId, cutoff), Some { transId = tid } when transId = tid ->
        true, [] // Accept replay idempotently
    | Commence (transId, cutoff), Some _ ->
        false, [] // Reject attempts at commencing overlapping transactions
    | Complete (transId, reason), Some { transId = tid } when transId = tid ->
        true, [Events.Completed { transId = transId; reason = reason }]
    | Complete _, (Some _|None) ->
        true, [] // Assume relay; accept idempotently

type EntryPoint internal (resolve, ?maxAttempts) =

    let log = Serilog.Log.ForContext<EntryPoint>()
    let (|AggregateId|) id = Equinox.AggregateId(Events.categoryId, TicketAllocatorId.toString id)
    let (|Stream|) (AggregateId id) = Equinox.Stream<Events.Event,Folds.State>(log, resolve id, maxAttempts = defaultArg maxAttempts 3)

    member __.Execute(allocatorId, command) : Async<bool> =
        let (Stream agg) = allocatorId
        agg.Transact(decide command)

module EventStore =

    open Equinox.EventStore
    let resolve (context,cache) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        // while there are competing writers [which might cause us to have to retry a Transact], this should be infrequent
        let opt = Equinox.ResolveOption.AllowStale
        fun id -> Resolver(context, Events.codec, Folds.fold, Folds.initial, cacheStrategy, AccessStrategy.LatestKnownEvent).Resolve(id,opt)
    let create (context,cache) =
        EntryPoint(resolve (context,cache))

module Cosmos =

    open Equinox.Cosmos
    let resolve (context,cache) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        // while there are competing writers [which might cause us to have to retry a Transact], this should be infrequent
        let opt = Equinox.ResolveOption.AllowStale
        fun id -> Resolver(context, Events.codec, Folds.fold, Folds.initial, cacheStrategy, AccessStrategy.LatestKnownEvent).Resolve(id,opt)
    let create (context,cache) =
        EntryPoint(resolve (context,cache))

type Request =
    /// Caller requesting continuation of in-flight work
    | Continue
    /// User said cancel request
    | Cancel
    /// Watchdog said this transaction has timed out
    | Abort

type ProcessManager(transactionTimeout, service : EntryPoint, listService : TicketList.EntryPoint, ticketService : Ticket.EntryPoint, ?timeout) =

    let timeout = defaultArg timeout System.TimeSpan.FromMinutes 1.

    member __.Start(allocator,transactionTimeout,timeSlice,tickets) : Async<bool*Result> =

        failwith "TODO"

    member __.Execute(allocator,timeSlice,command : Request) : Async<Result> =

        failwith "TODO"

//    static let maxDop = new SemaphoreSlim 10
//
//    let release (transactionId,tickets) = async {
//        let! results = seq { for x in tickets do yield ticketService.Sync(x,transactionId,None) |> maxDop.Throttle } |> Async.Parallel
//        return results |> Seq.filter id |> Seq.toList }
//
//    let acquire (transactionId,listId,tickets) = async {
//        let! results = seq { for x in tickets do yield ticketService.Sync(x,transactionId,Some listId) |> maxDop.Throttle } |> Async.Parallel
//        return results |> Seq.filter id |> Seq.toList }
//
//    let rec sync attempts (transactionId, listId, tickets, removed, acquired) : Async<Result> = async {
//        match! listService.Sync(listId, transactionId, tickets, removed, acquired) with
//        | PickList.Result.Conflict state ->
//            return Conflict state
//        | PickList.Result.Ok state ->
//            if attempts = 0 then
//                return Ok state.owned
//            else
//                let! removed =
//                    match state.toRelease with
//                    | [] -> async { return [] }
//                    | rel -> release (transactionId, rel)
//                let! acquired =
//                    match state.toAcquire with
//                    | [] -> async { return [] }
//                    | acq -> acquire (transactionId, listId, acq)
//                match removed, acquired with
//                | [], [] -> return Ok state.owned
//                | rel, acq -> return! sync (attempts-1) (transactionId, listId, tickets, removed, acquired)
//    }
//
//    member __.Allocate(transactionId, listId, tickets) =
//        sync 2 (transactionId, listId, tickets, [], [])
//
//    /// Triggered by a reactor when there's been a Commenced without a Completed or Aborted
//    member __.Deallocate(transactionId, listId) =
//        sync 0 (transactionId, listId, [], [], [])
//        // TODO think

module EventStore =

    open Equinox.EventStore
    let resolve (context,cache) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        // while there are competing writers [which might cause us to have to retry a Transact], this should be infrequent
        let opt = Equinox.ResolveOption.AllowStale
        // We should be reaching Completed state frequently so no actual Snapshots should get written
        // TOCONSIDER implement an explicit Equinox.EventStore.AccessStrategy to codify this
        let makeEmptySnapshot _state = Events.Snapshotted
        let accessStrategy = AccessStrategy.RollingSnapshots (Folds.isOrigin,makeEmptySnapshot)
        fun id -> Resolver(context, Events.codec, Folds.fold, Folds.initial, cacheStrategy, accessStrategy).Resolve(id,opt)
    let createService (context,cache) =
        Service(resolve (context,cache))

module Cosmos =

    open Equinox.Cosmos
    let resolve (context,cache) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        // while there are competing writers [which might cause us to have to retry a Transact], this should be infrequent
        let opt = Equinox.ResolveOption.AllowStale
        let makeEmptyUnfolds events _state = events,[]
        let accessStrategy = AccessStrategy.Custom (Folds.isOrigin,makeEmptyUnfolds)
        fun id -> Resolver(context, Events.codec, Folds.fold, Folds.initial, cacheStrategy, accessStrategy).Resolve(id,opt)
    let createService (context,cache) =
        Service(resolve (context,cache))