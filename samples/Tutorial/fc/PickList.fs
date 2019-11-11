module Fc.PickList

// NOTE - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
module Events =

    type Allocated = { allocatorId : AllocatorId; tickets : PickTicketId[] }
    type Snapshotted = { tickets : PickTicketId[] }
    type Event =
        | Allocated of Allocated
        | Snapshotted of Snapshotted
        interface TypeShape.UnionContract.IUnionContract
    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()
    let [<Literal>] categoryId = "PickList"

module Folds =

    type State = { tickets : Set<PickTicketId> }
    let initial = { tickets = Set.empty }
    let evolve state = function
        | Events.Allocated e -> { tickets = (state.tickets,e.tickets) ||> Array.fold (fun m x -> Set.add x m) }
        | Events.Snapshotted e -> { tickets = Set.ofArray e.tickets }
    let fold : State -> Events.Event seq -> State = Seq.fold evolve
    let isOrigin = function Events.Snapshotted _ -> true | _ -> false
    let snapshot state = Events.Snapshotted { tickets = Set.toArray state.tickets }
    let accessStrategy = Equinox.EventStore.AccessStrategy.RollingSnapshots (isOrigin,snapshot)

let interpretAllocated (allocatorId : AllocatorId, desired : PickTicketId list) (state : Folds.State) : Events.Event list =
    match desired |> Seq.except state.tickets |> Seq.distinct |> Seq.toArray with
    | [||] -> []
    | news -> [ Events.Allocated { allocatorId = allocatorId; tickets = news } ]

type Service internal (resolve, ?maxAttempts) =

    let log = Serilog.Log.ForContext<Service>()
    let (|AggregateId|) id = Equinox.AggregateId(Events.categoryId, PickListId.toString id)
    let (|Stream|) (AggregateId id) = Equinox.Stream<Events.Event,Folds.State>(log, resolve id, maxAttempts = defaultArg maxAttempts 3)
    let execute (Stream stream) = interpretAllocated >> stream.Transact

    /// Ensures the `assignedTickets` are recorded on the list
    member __.Sync(pickListId,allocatorId,assignedTickets) : Async<unit> =
        execute pickListId (allocatorId,assignedTickets)

module EventStore =

    open Equinox.EventStore
    let resolve cache context =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        // while there are competing writers [which might cause us to have to retry a Transact], this should be infrequent
        let opt = Equinox.ResolveOption.AllowStale
        fun id -> Resolver(context, Events.codec, Folds.fold, Folds.initial, cacheStrategy, Folds.accessStrategy).Resolve(id,opt)
    let createService cache context =
        Service(resolve cache context)