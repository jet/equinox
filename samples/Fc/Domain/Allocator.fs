module Allocator

open System

// NOTE - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
module Events =

    type Commenced =    { allocationId : AllocationId; cutoff : DateTimeOffset }
    type Completed =    { allocationId : AllocationId; reason : Reason }
    and  [<Newtonsoft.Json.JsonConverter(typeof<FsCodec.NewtonsoftJson.TypeSafeEnumConverter>)>]
         Reason = Ok | TimedOut | Cancelled
    type Snapshotted =  { active : Commenced option }
    type Event =
        | Commenced     of Commenced
        | Completed     of Completed
        interface TypeShape.UnionContract.IUnionContract
    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()
    let [<Literal>] categoryId = "Allocator"

module Folds =

    type State = Events.Commenced option
    let initial = None
    let evolve _state = function
        | Events.Commenced e -> Some e
        | Events.Completed _ -> None
    let fold : State -> Events.Event seq -> State = Seq.fold evolve

type CommenceResult = Accepted | Conflict of AllocationId

let decideCommence allocationId cutoff : Folds.State -> CommenceResult*Events.Event list = function
    | None -> Accepted, [Events.Commenced { allocationId = allocationId; cutoff = cutoff }]
    | Some { allocationId = tid } when allocationId = tid -> Accepted, [] // Accept replay idempotently
    | Some curr -> Conflict curr.allocationId, [] // Reject attempts at commencing overlapping transactions

let decideComplete allocationId reason : Folds.State -> Events.Event list = function
    | Some { allocationId = tid } when allocationId = tid -> [Events.Completed { allocationId = allocationId; reason = reason }]
    | Some _ | None -> [] // Assume replay; accept but don't write

type Service internal (resolve, ?maxAttempts) =

    let log = Serilog.Log.ForContext<Service>()
    let (|AggregateId|) id = Equinox.AggregateId(Events.categoryId, AllocatorId.toString id)
    let (|Stream|) (AggregateId id) = Equinox.Stream<Events.Event,Folds.State>(log, resolve id, maxAttempts = defaultArg maxAttempts 3)

    member __.Commence(allocatorId, allocationId, cutoff) : Async<CommenceResult> =
        let (Stream agg) = allocatorId
        agg.Transact(decideCommence allocationId cutoff)

    member __.Complete(allocatorId, allocationId, reason) : Async<unit> =
        let (Stream agg) = allocatorId
        agg.Transact(decideComplete allocationId reason)

module EventStore =

    open Equinox.EventStore
    let resolve (context,cache) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        // while there are competing writers [which might cause us to have to retry a Transact], this should be infrequent
        let opt = Equinox.ResolveOption.AllowStale
        fun id -> Resolver(context, Events.codec, Folds.fold, Folds.initial, cacheStrategy, AccessStrategy.LatestKnownEvent).Resolve(id,opt)
    let create (context,cache) =
        Service(resolve (context,cache))

module Cosmos =

    open Equinox.Cosmos
    let resolve (context,cache) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        // while there are competing writers [which might cause us to have to retry a Transact], this should be infrequent
        let opt = Equinox.ResolveOption.AllowStale
        fun id -> Resolver(context, Events.codec, Folds.fold, Folds.initial, cacheStrategy, AccessStrategy.LatestKnownEvent).Resolve(id,opt)
    let create (context,cache) =
        Service(resolve (context,cache))

type Result =
    | Incomplete of Allocation.Folds.Stats
    | Completed of Allocation.Folds.Stats
(*
    | Running       of reserved : TicketId list * toAssign : Events.Allocated list * toRelease : TicketId list * toReserve : TicketId list
    | Idle          of reserved : TicketId list
    | Cancelling    of                            toAssign : Events.Allocated list * toRelease : TicketId list
    | Completed
*)
type ProcessManager(maxListLen, allocators : Service, allocations : Allocation.Service, lists : TicketList.Service, tickets : Ticket.Service) =

    let run timeSlice (state : Allocation.ProcessState) =
        failwith "TODO"

    let cont timeSlice allocatorId allocationId = async {
        let! ok,state = allocations.Sync(allocationId,Seq.empty,Allocation.Command.Apply ([],[]))
        return! run timeSlice state }

    member __.Commence(allocatorId,allocationId,timeSlice,transactionTimeout,tickets) : Async<Result> = async {
        let cutoff = let now = DateTimeOffset.UtcNow in now.Add transactionTimeout
        let! res = allocators.Commence(allocatorId, allocationId, cutoff)
        match res with
        | Accepted ->
            let! ok,state = allocations.Sync(allocationId,Seq.empty,Allocation.Command.Commence tickets)
            return! run timeSlice state
        | Conflict otherAllocationId ->
            return! cont timeSlice allocatorId otherAllocationId
        }

    member __.Continue(allocatorId,allocationId,timeSlice) : Async<Result> = async {
        return! cont timeSlice allocatorId allocationId }

    member __.Cancel(allocatorId,allocationId,timeSlice,reason) : Async<Result> = async {
        let! ok,state = allocations.Sync(allocationId,Seq.empty,Allocation.Command.Cancel)
        return! run timeSlice state }