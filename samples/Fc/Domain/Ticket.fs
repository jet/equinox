module Ticket

// NOTE - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
module Events =

    type Reserved =     { allocatorId : AllocatorId }
    type Allocated =    { allocatorId : AllocatorId; listId : TicketListId }

    type Event =
        | Reserved      of Reserved
        | Allocated     of Allocated
        | Revoked
        interface TypeShape.UnionContract.IUnionContract
    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()
    let [<Literal>] categoryId = "Ticket"

module Folds =

    type State = Unallocated | Reserved of by : AllocatorId | Allocated of by : AllocatorId * on : TicketListId
    let initial = Unallocated
    let private evolve _state = function
        | Events.Reserved e -> Reserved e.allocatorId
        | Events.Allocated e -> Allocated (e.allocatorId, e.listId)
        | Events.Revoked -> Unallocated
    // because each event supersedes the previous one, we only ever need to fold the last event
    let fold state events =
        Seq.tryLast events |> Option.fold evolve state

type Command =
    /// permitted if nobody owns it (or idempotently ok if we are the owner)
    | Reserve
    /// permitted if the allocator has it reserved (or idempotently ok if already on list)
    | Allocate of on : TicketListId
    /// must be performed by the owner; attempts by non-owner to deallocate get ignored as a new owner now has that responsibility
    /// (but are not failures from an Allocator's perspective)
    | Revoke

let decide (allocator : AllocatorId) (command : Command) (state : Folds.State) : bool * Events.Event list =
    match command, state with
    | Reserve, Folds.Unallocated -> true,[Events.Reserved { allocatorId = allocator }] // normal case -> allow+record
    | Reserve, Folds.Reserved by when by = allocator -> true,[] // idempotently permit
    | Reserve, (Folds.Reserved _ | Folds.Allocated _) -> false,[] // report failure, nothing to write
    | Allocate list, Folds.Allocated (by,l) when by = allocator && l = list -> true,[] // idempotent processing
    | Allocate list, Folds.Reserved by when by = allocator -> true,[Events.Allocated { allocatorId = allocator; listId = list }] // normal
    | Allocate _, (Folds.Allocated _ | Folds.Unallocated | Folds.Reserved _) -> false,[] // Fail if someone else has reserved or allocated, or we are jumping straight to Allocated without Reserving first
    | Revoke, Folds.Unallocated -> true,[] // idempotent handling
    | Revoke, (Folds.Reserved by | Folds.Allocated (by,_)) when by = allocator -> true,[Events.Revoked] // release Reservation or Allocation
    | Revoke, (Folds.Reserved _ | Folds.Allocated _ ) -> true,[] // NOTE we report success of achieving the intent (but, critically, we leave it to the actual owner to manage any actual revoke)

type Service internal (resolve, ?maxAttempts) =

    let log = Serilog.Log.ForContext<Service>()
    let (|AggregateId|) id = Equinox.AggregateId(Events.categoryId, TicketId.toString id)
    let (|Stream|) (AggregateId id) = Equinox.Stream<Events.Event,Folds.State>(log, resolve id, maxAttempts = defaultArg maxAttempts 3)

    /// Attempts to achieve the intent represented by `desired`. High level semantics as per comments on Desired (see decideSync for lowdown)
    /// `false` is returned if a competing allocator holds it (or we're attempting to jump straight to Allocated without first Reserving)
    member __.Sync(pickTicketId, allocator, command : Command) : Async<bool> =
        let (Stream agg) = pickTicketId
        agg.Transact(decide allocator command)

module EventStore =

    open Equinox.EventStore
    let resolve (context,cache) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        // because we only ever need the last event, we use the Equinox.EventStore access strategy that optimizes around that
        Resolver(context, Events.codec, Folds.fold, Folds.initial, cacheStrategy, AccessStrategy.LatestKnownEvent).Resolve
    let create (context,cache)=
        Service(resolve (context,cache))

module Cosmos =

    open Equinox.Cosmos
    let resolve (context,cache) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        // because we only ever need the last event to build the state, we feed the events we are writing
        // (there's always exactly one if we are writing), into the unfolds slot so a single point read with etag check gets us state in one trip
        Resolver(context, Events.codec, Folds.fold, Folds.initial, cacheStrategy, AccessStrategy.LatestKnownEvent).Resolve
    let create (context,cache) =
        Service(resolve (context,cache))