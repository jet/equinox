module TicketAllocator

open System
open System.Collections.Generic

// NOTE - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
module Events =

    type Commenced =    { ticketIds : TicketId[]; cutoff : System.DateTimeOffset }
    type Tickets =      { ticketIds : TicketId[] }
    type Allocated =    { ticketIds : TicketId[]; listId : TicketListId }
    type Assigned =     { listId    : TicketListId }
    type Snapshotted =  { ticketIds : TicketId[] }
    type Event =
        /// Records full set of targets (so Abort can Revoke all potential in flight Reservations)
        | Commenced     of Commenced
        /// Tickets verified as not being attainable (Allocated, not just Reserved)
        | Failed        of Tickets
        /// Tickets verified as having been marked Reserved
        | Reserved      of Tickets
        /// Confirming cited tickets are to be allocated to the cited list
        | Allocated     of Allocated
        /// Records intention to release cited tickets (while Running, not implicitly via Aborted)
        | Released      of Tickets
        /// Transitioning to phase where (Commenced-Allocated) get Returned by performing Releases on the Tickets due to user request
        | Cancelled
        /// Watchdog or self-policing triggering a rollback due to cutoff expiration
        | Aborted
        /// Confirming cited tickets have been assigned to the list
        | Assigned      of Assigned
        /// Records confirmed Revokes of cited Tickets
        | Revoked       of Tickets
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
            assigning   : Events.Allocated list
            releasing   : Set<TicketId> }
    module States =
        let (|ToSet|) = set
        let private withKnown xs x =    { x with unknown = Set.difference x.unknown xs }
        let withFailed (ToSet xs) x =   { withKnown xs x with failed = x.failed |> Set.union xs }
        let withReserved (ToSet xs) x = { withKnown xs x with reserved = x.reserved |> Set.union xs }
        let withRevoked (ToSet xs) x =  { withKnown xs x with reserved = Set.difference x.reserved xs }
        let withReleasing (ToSet xs) x ={ withKnown xs x with releasing = x.releasing |> Set.union xs } // TODO
        let withAssigned listId x = // TODO
            let decided,remaining = x.assigning |> List.partition (fun x -> x.listId = listId)
            let xs = seq { for x in decided do yield! x.ticketIds }
            { withRevoked xs x with assigning = remaining }
    let initial = Idle
    let evolve state = function
        | Events.Commenced e ->
            match state with
            | Idle ->           Running { unknown = set e.ticketIds; failed = Set.empty; reserved = Set.empty; assigning = []; releasing = Set.empty }
            | x ->              failwithf "Can only Commence when Idle, not %A" x
        | Events.Failed e ->
            match state with
            | Idle ->           failwith "Cannot have Failed if Idle"
            | Running s ->      Running (s |> States.withFailed e.ticketIds)
            | Reverting s ->    Reverting (s |> States.withFailed e.ticketIds)
        | Events.Reserved e ->
            match state with
            | Idle ->           failwith "Cannot have Reserved if Idle"
            | Running s ->      Running (s |> States.withReserved e.ticketIds)
            | Reverting s ->    Reverting (s |> States.withReserved e.ticketIds)
        | Events.Allocated e ->
            match state with
            | Idle ->           failwith "Cannot have Allocating if Idle"
            | Running s ->      Running { s with assigning = e :: s.assigning}
            | Reverting s ->    Reverting { s with assigning = e :: s.assigning}
        | Events.Released e ->
            match state with
            | Idle ->           failwith "Cannot have Releasing if Idle"
            | Running s ->      Running (s |> States.withReleasing e.ticketIds)
            | Reverting s ->    Reverting (s |> States.withReleasing e.ticketIds)
        | Events.Cancelled
        | Events.Aborted ->
            match state with
            | Running s ->      Reverting s
            | x ->              failwithf "Can only Abort when Running, not %A" x
        | Events.Assigned e ->
            match state with
            | Idle ->           failwith "Cannot have Allocated if Idle"
            | Running s ->      Running (s |> States.withAssigned e.listId)
            | Reverting s ->    Reverting (s |> States.withAssigned e.listId)
        | Events.Revoked e ->
            match state with
            | Idle ->           failwith "Cannot have Released if Idle"
            | Running s ->      Running (s |> States.withRevoked e.ticketIds)
            | Reverting s ->    Reverting (s |> States.withRevoked e.ticketIds)
        | Events.Completed ->
            match state with
            | Running s
            | Reverting s when Set.isEmpty s.unknown && Set.isEmpty s.reserved && List.isEmpty s.assigning ->
                                Idle
            | x ->              failwithf "Can only Complete when reservations and unknowns resolved, not %A" x
        | Events.Snapshotted -> state // Dummy event, see EventStore bindings
    let fold : State -> Events.Event seq -> State = Seq.fold evolve
    let isOrigin = function Events.Completed -> true | Events.Snapshotted | _ -> false

/// Current state of the workflow based on the present state of the Aggregate
type ProcessState =
    | Running       of reserved : TicketId list * toAssign : Events.Allocated list * toRelease : TicketId list * toReserve : TicketId list
    | Idle          of reserved : TicketId list
    | Cancelling    of                            toAssign : Events.Allocated list * toRelease : TicketId list
    | Completed
    static member FromFoldState = function
        | Folds.Running e ->
            match Set.toList e.reserved, e.assigning, Set.toList e.releasing, Set.toList e.unknown with
            | res, [], [], [] ->
                Idle (reserved = res)
            | res, ass, rel, tor ->
                Running (reserved = res, toAssign = ass, toRelease = rel, toReserve = tor)
        | Folds.Reverting e ->
            Cancelling (toAssign = e.assigning, toRelease = [yield! e.reserved; yield! e.unknown; yield! e.releasing])
        | Folds.Idle ->
            Completed

/// Updates on achieved progress
type Update =
    | Failed        of tickets : TicketId list
    | Reserved      of tickets : TicketId list
    | Assigned      of listId  : TicketListId
    | Revoked       of tickets : TicketId list

let (|ToSet|) xs = set xs
let (|SetEmpty|_|) s = if Set.isEmpty s then Some () else None

/// Map processed work to associated events that are to be recorded in the stream
let decideUpdate update state =
    let owned (s : Folds.States) = Set.union s.releasing (set <| seq { yield! s.unknown; yield! s.reserved })
    match state, update with
    | Folds.Idle, (Failed _|Reserved _|Assigned _|Revoked _) as x ->
        failwithf "Folds.Idle cannot handle (Failed|Revoked|Assigned) %A" x
    | (Folds.Running s|Folds.Reverting s), Reserved (ToSet xs) ->
        match set s.unknown |> Set.intersect xs with SetEmpty -> [] | changed -> [Events.Reserved { ticketIds = Set.toArray changed }]
    | (Folds.Running s|Folds.Reverting s), Failed (ToSet xs) ->
        match owned s |> Set.intersect xs with SetEmpty -> [] | changed -> [Events.Failed { ticketIds = Set.toArray changed }]
    | (Folds.Running s|Folds.Reverting s), Revoked (ToSet xs) ->
        match owned s |> Set.intersect xs with SetEmpty -> [] | changed -> [Events.Revoked { ticketIds = Set.toArray changed }]
    | (Folds.Running s|Folds.Reverting s), Assigned listId ->
        if s.assigning |> List.exists (fun x -> x.listId = listId) then [Events.Assigned { listId = listId }] else []

/// Holds events accumulated from a series of decisions while also evolving the presented `state` to reflect the pended events
type private Accumulator() =
    let acc = ResizeArray()
    member __.Ingest state : 'res * Events.Event list -> 'res * Folds.State = function
        | res, [] ->                   res,state
        | res, [e] -> acc.Add e;       res,Folds.evolve state e
        | res, xs ->  acc.AddRange xs; res,Folds.fold state (Seq.ofList xs)
    member __.Accumulated = List.ofSeq acc

/// Impetus provided to the Aggregate Service from the Process Manager
type Command =
    | Commence      of tickets : TicketId list         * timeout : TimeSpan
    | Apply         of assign  : Events.Allocated list * release : TicketId list
    | Cancel
    | Abort

/// Apply updates, decide whether Command is applicable, emit state reflecting work to be completed to conclude the in-progress workflow (if any)
let sync (effectiveTimestamp : DateTimeOffset, updates : Update seq, command : Command) (state : Folds.State) : (bool*ProcessState) * Events.Event list =
    let acc = Accumulator()

    (* Apply any updates *)
    let mutable state = state
    for x in updates do
        let (),state' = acc.Ingest state ((),decideUpdate x state)
        state <- state'

    (* Decide whether the Command is now acceptable *)
    let accepted,state =
        acc.Ingest state <|
            match state, command with
            (* Symptomatic of a problem - fail request *)
            // TOCONSIDER how to represent that a request is being denied e.g. due to timeout vs due to being complete
            | (Folds.Idle|Folds.Reverting _), Apply _ -> false, []
            (* Defer; Need to allow current request to progress before it can be considered *)
            | (Folds.Running _|Folds.Reverting _), Commence _ -> false, []
            (* Ok on the basis of idempotency *)
            | (Folds.Idle|Folds.Reverting _), (Cancel|Abort) -> true, []
            (* Ok; Currently idle, normal Commence request*)
            | Folds.Idle, Commence (tickets,timeout) ->
                true,[Events.Commenced { ticketIds = Array.ofList tickets; cutoff = effectiveTimestamp + timeout }]
            (* Ok; normal apply to distribute held tickets *)
            | Folds.Running s, Apply (assign,release) ->
                let avail = HashSet s.reserved
                let toAssign = [for a in assign -> { a with ticketIds = a.ticketIds |> Array.where avail.Remove }]
                let toRelease = (Set.empty,release) ||> List.fold (fun s x -> if avail.Remove x then Set.add x s else s)
                true, [
                    for x in toAssign do if (not << Array.isEmpty) x.ticketIds then yield Events.Allocated x
                    match toRelease with SetEmpty -> () | toRelease -> yield Events.Released { ticketIds = Set.toArray toRelease }]
            (* Ok, normal Cancel *)
            | Folds.Running _, Cancel -> true, [Events.Cancelled]
            (* Ok, normal Abort *)
            | Folds.Running _, Abort -> true, [Events.Aborted]

    (* Yield outstanding processing requirements (if any), together with events accumulated based on the `updates` *)
    (accepted, ProcessState.FromFoldState state), acc.Accumulated

type Service internal (resolve, ?maxAttempts, ?timeout) =

    let timeout = defaultArg timeout System.TimeSpan.FromMinutes 1.

    let log = Serilog.Log.ForContext<Service>()
    let (|AggregateId|) id = Equinox.AggregateId(Events.categoryId, TicketAllocatorId.toString id)
    let (|Stream|) (AggregateId id) = Equinox.Stream<Events.Event,Folds.State>(log, resolve id, maxAttempts = defaultArg maxAttempts 3)

    let decide (Stream stream) = sync >> stream.Transact

    let sync (allocatorId,effectiveTimestamp,updates,command) : Async<bool*ProcessState> =
        decide allocatorId (effectiveTimestamp,updates,command)

type Request =
    /// Caller requesting continuation of in-flight work
    | Continue
    /// User said cancel request
    | Cancel
    /// Watchdog said this transaction has timed out
    | Abort

type Stats =        { ticketsReserved : int; listsAllocated : int }
type Running =      { toAcquire : int; percentComplete : int }
type Cancelling =   { toRelease : int; percentComplete : int }

type Result =
    | Running       of Running
    | Cancelling    of Cancelling
    | Completed     of Stats

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