module Fc.TicketAllocator
open System

// NOTE - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
module Events =

    type Commenced =    { cutoff : System.DateTimeOffset; ticketIds : TicketId[] }
    type Tickets =      { ticketIds : TicketId[] }
    type Allocating =   { listId : TicketListId; ticketIds : TicketId[] }
    type Allocated =    { listId : TicketListId }
    type Snapshotted =  { ticketIds : TicketId[] }
    type Event =
        /// Records full set of targets (so Abort can Revoke all potential in flight Reservations)
        | Commenced     of Commenced
        /// Tickets verified as not being attainable (Allocated, not just Reserved) // TODO does this matter or can Ticket.Reserved state go?
        | Failed        of Tickets
        /// Tickets verified as having been marked Reserved
        | Reserved      of Tickets
        /// Confirming cited tickets are to be allocated to the cited list
        | Allocating    of Allocating
        /// Transitioning to phase where (Commenced-Allocated) get Returned by performing Releases on the Tickets due to user request
        | Cancelled
        /// Watchdog or self-policing triggering a rollback due to cutoff expiration
        | Aborted
        /// Confirming cited tickets have been assigned to the list
        | Allocated     of Allocated
        /// Records confirmed Releases of cited Tickets
        | Released      of Tickets
        /// Allocated + Returned = Commenced ==> Open for a new Commenced to happen
        | Completed
        // Dummy event to make Equinox.EventStore happy (see `module EventStore`)
        | Snapshotted
        interface TypeShape.UnionContract.IUnionContract
    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()
    let [<Literal>] categoryId = "TicketAllocator"

module Folds =

    type State = Idle | Running of States | Reverting of States
    and States =
        {   unknown     : Set<TicketId>
            failed      : Set<TicketId>
            reserved    : Set<TicketId>
            allocating  : Events.Allocating list }
    module States =
        let (|ToSet|) = set
        let withKnown (ToSet xs) x =    { x with unknown = Set.difference x.unknown xs }
        let withFailed (ToSet xs) x =   { withKnown xs x with failed = x.failed |> Set.union xs }
        let withReserved (ToSet xs) x = { withKnown xs x with reserved = x.reserved |> Set.union xs }
        let release (ToSet xs) x =      { withKnown xs x with reserved = Set.difference x.reserved xs }
        let withAllocated listId x =
            let decided,remaining = x.allocating |> List.partition (fun x -> x.listId = listId)
            let xs = seq { for x in decided do yield! x.ticketIds }
            { release xs x with allocating = remaining }
    let initial = Idle
    let evolve state = function
        | Events.Commenced e -> state |> function
            | Idle -> Running { unknown = set e.ticketIds; failed = Set.empty; reserved = Set.empty; allocating = [] }
            | x -> failwithf "Can only Commence when Idle, not %A" x
        | Events.Failed e -> state |> function
            | Idle -> failwith "Cannot have Failed if Idle"
            | Running s -> Running (s |> States.withFailed e.ticketIds)
            | Reverting s -> Reverting (s |> States.withFailed e.ticketIds)
        | Events.Reserved e -> state |> function
            | Idle -> failwith "Cannot have Reserved if Idle"
            | Running s -> Running (s |> States.withReserved e.ticketIds)
            | Reverting s -> Reverting (s |> States.withReserved e.ticketIds)
        | Events.Allocating e -> state |> function
            | Idle -> failwith "Cannot have Allocating if Idle"
            | Running s -> Running { s with allocating = e :: s.allocating}
            | Reverting s -> Reverting { s with allocating = e :: s.allocating}
        | Events.Cancelled | Events.Aborted -> state |> function
            | Running s -> Reverting s
            | x -> failwithf "Can only Abort when Running, not %A" x
        | Events.Allocated e -> state |> function
            | Idle -> failwith "Cannot have Allocated if Idle"
            | Running s -> Running (s |> States.withAllocated e.listId)
            | Reverting s -> Reverting (s |> States.withAllocated e.listId)
        | Events.Released e -> state |> function
            | Idle -> failwith "Cannot have Released if Idle"
            | Running s -> Running (s |> States.release e.ticketIds)
            | Reverting s -> Reverting (s |> States.release e.ticketIds)
        | Events.Completed -> state |> function
            | Running s | Reverting s when Set.isEmpty s.unknown && Set.isEmpty s.reserved && List.isEmpty s.allocating -> Idle
            | x -> failwithf "Can only Complete when reservations and unknowns resolved, not %A" x
        | Events.Snapshotted -> state // Dummy event, see EventStore bindings
    let fold : State -> Events.Event seq -> State = Seq.fold evolve
    let isOrigin = function Events.Completed -> true | Events.Snapshotted | _ -> false

/// Impetus provided to the Aggregate Service from the Process Manager
type Command =
    | Commence      of TicketId list * timeout : TimeSpan
    | Progress      of allocate : Events.Allocating list * release : TicketId list
    | Cancel
    | Abort

/// Current state of the workflow based on the events noted in the Aggregate
type State =
    | Running       of reserved : TicketId list * toReserve : TicketId list * toAllocate : Events.Allocating
    | Idle          of reserved : TicketId list
    | Cancelling    of toRelease : TicketId list * toAssign : Events.Allocating
    | Completed

let decide (effectiveTimeStamp, failed, reserved, assigned, released, command : Command) (state : Folds.State) : (bool*State) * Events.Event list =
    failwith "TODO"
//    match state, co§mmand with
//    | Idle, Commence tickets -> true*,[Events.Commenced { ticketIds = Array.ofList tickets }]

type Service internal (resolve, ?maxAttempts, ?timeout) =

    let timeout = defaultArg timeout System.TimeSpan.FromMinutes 1.

    let log = Serilog.Log.ForContext<Service>()
    let (|AggregateId|) id = Equinox.AggregateId(Events.categoryId, AllocatorId.toString id)
    let (|Stream|) (AggregateId id) = Equinox.Stream<Events.Event,Folds.State>(log, resolve id, maxAttempts = defaultArg maxAttempts 3)

    let decide (Stream stream) = decide >> stream.Transact

    let sync (allocatorId,startTimestamp,failed,reserved,assigned,released,command) : Async<bool*State> =
        decide allocatorId (startTimestamp,failed,reserved,assigned,released,command)

type Request =
    /// Caller requesting continuation of in-flight work
    | Continue
    /// User said cancel request
    | Cancel
    /// Watchdog said this transaction has timed out
    | Abort

type Stats = { ticketsReserved : int; listsAllocated : int }
type Running = { toAcquire : int; percentComplete : int }
type Cancelling = { toRelease : int; percentComplete : int }

type Result =
    | Running of Running
    | Cancelling of Cancelling
    | Completed of Stats

type ProcessManager(transactionTimeout, service : Service, listService : TicketList.Service, ticketService : Ticket.Service) =

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