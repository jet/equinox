module TicketList

// NOTE - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
module Events =

    type Allocated =    { allocatorId : AllocatorId; ticketIds : TicketId[] }
    type Snapshotted =  { ticketIds : TicketId[] }
    type Event =
        | Allocated     of Allocated
        | Snapshotted   of Snapshotted
        interface TypeShape.UnionContract.IUnionContract
    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()
    let [<Literal>] categoryId = "TicketList"

module Folds =

    type State = Set<TicketId>
    let initial = Set.empty
    let evolve state = function
        | Events.Allocated e -> (state,e.ticketIds) ||> Array.fold (fun m x -> Set.add x m)
        | Events.Snapshotted e -> Set.ofArray e.ticketIds
    let fold : State -> Events.Event seq -> State = Seq.fold evolve
    let isOrigin = function Events.Snapshotted _ -> true | Events.Allocated _ -> false
    let snapshot state = Events.Snapshotted { ticketIds = Set.toArray state }

let interpret (allocatorId : AllocatorId, allocated : TicketId list) (state : Folds.State) : Events.Event list =
    match allocated |> Seq.except state |> Seq.distinct |> Seq.toArray with
    | [||] -> []
    | news -> [Events.Allocated { allocatorId = allocatorId; ticketIds = news }]

type Service internal (resolve, ?maxAttempts) =

    let log = Serilog.Log.ForContext<Service>()
    let (|AggregateId|) id = Equinox.AggregateId(Events.categoryId, PickListId.toString id)
    let (|Stream|) (AggregateId id) = Equinox.Stream<Events.Event,Folds.State>(log, resolve id, maxAttempts = defaultArg maxAttempts 3)

    member __.Sync(pickListId,allocatorId,assignedTickets) : Async<unit> =
        let (Stream agg) = pickListId
        agg.Transact(interpret (allocatorId,assignedTickets))

module EventStore =

    open Equinox.EventStore
    let resolve (context,cache) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        // while there are competing writers (which might cause us to have to retry a Transact and discover it is redundant), there is never a cost to being wrong
        let opt = Equinox.ResolveOption.AllowStale
        // we _could_ use this Access Strategy, but because we are only generally doing a single shot write, its unwarranted
        // let accessStrategy = AccessStrategy.RollingSnapshots (Folds.isOrigin,Folds.snapshot)
        fun id -> Resolver(context, Events.codec, Folds.fold, Folds.initial, cacheStrategy).Resolve(id,opt)
    let create (context,cache) =
        Service(resolve (context,cache))

module Cosmos =

    open Equinox.Cosmos
    let resolve (context,cache) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        // while there are competing writers (which might cause us to have to retry a Transact and discover it is redundant), there is never a cost to being wrong
        let opt = Equinox.ResolveOption.AllowStale
        // we want reads and writes (esp idempotent ones) to have optimal RU efficiency so we go the extra mile to do snapshotting into the Tip
        let accessStrategy = AccessStrategy.Snapshot (Folds.isOrigin,Folds.snapshot)
        fun id -> Resolver(context, Events.codec, Folds.fold, Folds.initial, cacheStrategy, accessStrategy).Resolve(id,opt)
    let create (context,cache)=
        Service(resolve (context,cache))