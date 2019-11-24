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

type CommenceResult = Accepted | Conflict of TicketTransId

let decideCommence transId cutoff : Folds.State -> CommenceResult*Events.Event list = function
    | None -> Accepted, [Events.Commenced { transId = transId; cutoff = cutoff }]
    | Some { transId = tid } when transId = tid -> Accepted, [] // Accept replay idempotently
    | Some curr -> Conflict curr.transId, [] // Reject attempts at commencing overlapping transactions

let decideComplete transId reason : Folds.State -> Events.Event list = function
    | Some { transId = tid } when transId = tid -> [Events.Completed { transId = transId; reason = reason }]
    | Some _|None -> [] // Assume relay; accept but don't write

type EntryPoint internal (resolve, ?maxAttempts) =

    let log = Serilog.Log.ForContext<EntryPoint>()
    let (|AggregateId|) id = Equinox.AggregateId(Events.categoryId, TicketAllocatorId.toString id)
    let (|Stream|) (AggregateId id) = Equinox.Stream<Events.Event,Folds.State>(log, resolve id, maxAttempts = defaultArg maxAttempts 3)

    member __.Commence(allocatorId, transId, cutoff) : Async<CommenceResult> =
        let (Stream agg) = allocatorId
        agg.Transact(decideCommence transId cutoff)

    member __.Complete(allocatorId, transId, reason) : Async<unit> =
        let (Stream agg) = allocatorId
        agg.Transact(decideComplete transId reason)

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

type Result =
    | Incomplete of TicketTrans.Folds.Stats
    | Completed of TicketTrans.Folds.Stats
(*
    | Running       of reserved : TicketId list * toAssign : Events.Allocated list * toRelease : TicketId list * toReserve : TicketId list
    | Idle          of reserved : TicketId list
    | Cancelling    of                            toAssign : Events.Allocated list * toRelease : TicketId list
    | Completed
*)
type ProcessManager(maxListLen, allocators : EntryPoint, transactions : TicketTrans.EntryPoint, lists : TicketList.EntryPoint, tickets : Ticket.EntryPoint) =

    let run timeSlice (state : TicketTrans.ProcessState) =
        failwith "TODO"

    let continue timeSlice allocatorId transId = async {
        let! ok,state = transactions.Sync(transId,Seq.empty,TicketTrans.Command.Apply ([],[]))
        return! run timeSlice state }

    member __.Commence(allocatorId,transId,timeSlice,transactionTimeout,tickets) : Async<Result> = async {
        let! res = allocators.Commence(allocatorId, transId, DateTimeOffset.UtcNow.Add transactionTimeout)
        match res with
        | Accepted ->
            let! ok,state = transactions.Sync(transId,Seq.empty,TicketTrans.Command.Commence tickets)
            return! run timeSlice state
        | Conflict otherTransId ->
            return! continue timeSlice allocatorId otherTransId
        }

    member __.Continue(allocatorId,transId,timeSlice) : Async<Result> = async {
        return! continue timeSlice allocatorId transId }

    member __.Cancel(allocatorId,transId,timeSlice,reason) : Async<Result> = async {
        let! ok,state = transactions.Sync(transId,Seq.empty,TicketTrans.Command.Cancel)
        return! run timeSlice state }