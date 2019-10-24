module Fc.PickTicket

// NB - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
module Events =

    type Assigned = { transactionId : TransactionId; listId : PickListId }

    type Event =
        | Assigned of Assigned
        | Revoked
        interface TypeShape.UnionContract.IUnionContract
    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()
    let [<Literal>] categoryId = "PickTicket"

module Folds =

    type Allocation = { transaction : TransactionId; list : PickListId }
    type State = { allocation : Allocation option }
    let initial = { allocation = None }
    let evolve _state = function
        | Events.Assigned e -> { allocation = Some { transaction = e.transactionId; list = e.listId } }
        | Events.Revoked -> { allocation = None }
    let fold : State -> Events.Event seq -> State = Seq.fold evolve
    let accessStrategy = Equinox.EventStore.AccessStrategy.EventsAreState

type [<RequireQualifiedAccess>] Result = Ok | Conflict of TransactionId * PickListId

let decideSync (reqTransId : TransactionId, desired : PickListId option) (state : Folds.State) : Result * Events.Event list =
    match desired, state with
    // ALLOCATE: not already allocated; i.e. normal case -> Ok
    | Some reqListId, { allocation = None } -> Result.Ok,[ Events.Assigned { transactionId = reqTransId; listId = reqListId }]
    // ALLOCATE: already the owner -> Idempotently Ok
    | Some reqListId, { allocation = Some a } when a.list = reqListId && a.transaction = reqTransId -> Result.Ok,[]
    // ALLOCATE: conflicting request
    | Some _, { allocation = Some a } -> Result.Conflict (a.transaction,a.list),[]
    // REVOKE: by the owner, i.e. normal case
    | None, { allocation = Some a } when a.transaction = reqTransId -> Result.Ok,[]
    // REVOKE: already deallocated -> Idempotently Ok
    | None, { allocation = None } -> Result.Ok,[]
    // REVOKE: Not by the owner; report success, but let owner formally execute the revoke
    | None, { allocation = Some a } -> Result.Ok,[]

type Service(resolve, ?maxAttempts) =

    let log = Serilog.Log.ForContext<Service>()
    let (|AggregateId|) id = Equinox.AggregateId(Events.categoryId, PickTicketId.toString id)
    let (|Stream|) (AggregateId id) = Equinox.Stream<Events.Event,Folds.State>(log, resolve id, maxAttempts = defaultArg maxAttempts 3)
    let execute (Stream stream) = decideSync >> stream.Transact

    member __.Sync(pickTicketId,transactionId,assignment : PickListId option) : Async<Result> =
        execute pickTicketId (transactionId,assignment)

module EventStore =

    open Equinox.EventStore
    let resolve cache context =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        // while there are competing writers [which might cause us to have to retry a Transact], this should be infrequent
        let opt = Equinox.ResolveOption.AllowStale
        fun id -> Resolver(context, Events.codec, Folds.fold, Folds.initial, cacheStrategy, Folds.accessStrategy).Resolve(id,opt)
    let createService cache context =
        Service(resolve cache context)
