module Fc.PickTicket

// NOTE - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
module Events =

    type Assigned = { allocatorId : AllocatorId; listId : PickListId }

    type Event =
        | Assigned of Assigned
        | Revoked
        interface TypeShape.UnionContract.IUnionContract
    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()
    let [<Literal>] categoryId = "PickTicket"

module Folds =

    type Allocation = { allocator : AllocatorId; list : PickListId }
    type State = Allocation option
    let initial = None
    let evolve _state = function
        | Events.Assigned e -> Some { allocator = e.allocatorId; list = e.listId }
        | Events.Revoked -> None
    // because each event supersedes the previous one, we only ever need to fold the last event
    let fold state events =
        Seq.tryLast events |> Option.fold evolve state

let decideSync (allocator : AllocatorId, desiredAssignment : PickListId option) (state : Folds.State) : bool * Events.Event list =
    match desiredAssignment, state with
    // ALLOCATE: not already allocated; i.e. normal case -> Ok
    | Some reqListId, None -> true,[Events.Assigned { allocatorId = allocator; listId = reqListId }]
    // ALLOCATE: already the owner -> idempotently Ok
    | Some reqListId, Some { allocator = a; list = l } when l = reqListId && a = allocator -> true,[]
    // ALLOCATE: conflicting request
    | Some _, Some _ -> false,[]
    // REVOKE: by the owner, i.e. normal case
    | None, Some { allocator = a } when a = allocator -> true,[]
    // REVOKE: Attempt to deallocate, but no longer the owner => REPORT SUCCESS (but leave it to the actual owner to manage any actual revoke)
    | None, Some _ -> true,[]
    // REVOKE: already deallocated -> idempotently Ok
    | None, None -> true,[]

type Service(resolve, ?maxAttempts) =

    let log = Serilog.Log.ForContext<Service>()
    let (|AggregateId|) id = Equinox.AggregateId(Events.categoryId, PickTicketId.toString id)
    let (|Stream|) (AggregateId id) = Equinox.Stream<Events.Event,Folds.State>(log, resolve id, maxAttempts = defaultArg maxAttempts 3)
    let execute (Stream stream) = decideSync >> stream.Transact

    /// Attempts to achieve the `desiredAssignment`. High level semantics are: (see decideSync for the lowdown)
    /// - reallocation is permitted if the supplied `allocator` is the current 'owner'
    /// - deallocation must be performed by the owner; attempts by non-owners to deallocate get ignored (but are not failures from an Allocator's perspective)
    /// - `false` is returned iff it's under the control of another allocator and hence cannot become owned by this allocator
    member __.Sync(pickTicketId,allocator,desiredAssignment : PickListId option) : Async<bool> =
        execute pickTicketId (allocator,desiredAssignment)

module EventStore =

    open Equinox.EventStore
    let resolve cache context =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        // because we only ever need the last event, we use the Equinox.EventStore mode that optimizes around that
        // (with Equinox.Cosmos, the equivalent is to use AccessStrategy.Snapshot or AccessStrategy.RollingUnfolds)
        fun id -> Resolver(context, Events.codec, Folds.fold, Folds.initial, cacheStrategy, AccessStrategy.EventsAreState).Resolve(id)
    let createService cache context =
        Service(resolve cache context)