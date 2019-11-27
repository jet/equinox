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