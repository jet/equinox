module Fc.PickAllocator

// NOTE - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
[<RequireQualifiedAccess>]
module Events =

    type Commenced = { transId : AllocatorId; desired : PickTicketId[]; acquired : PickTicketId[] }
    type Items = { tickets : PickTicketId[] }
    type Event =
        | Commenced of Commenced
        | Acquired of Items
        | Failed of Items
        | Released of Items
        | Completed
        | Aborted
        | Snapshotted
        interface TypeShape.UnionContract.IUnionContract
    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()
    let [<Literal>] categoryId = "PickList"

module Folds =

    let mkId () = let g = System.Guid.NewGuid() in g.ToString "N" |> FSharp.UMX.UMX.tag
    let private merge (xs : 't seq) (x : 't list) = (xs |> Seq.except x,x) ||> Seq.foldBack (fun x l -> x :: l)
    type Running = { trans : AllocatorId; owned : PickTicketId list; pending : PickTicketId list; acquired : PickTicketId list; failed : PickTicketId list }
    type Reverting = { trans : AllocatorId; owned : PickTicketId list; outstanding : PickTicketId list }
    type State = Open of owned : PickTicketId list | Running of Running | Reverting of Reverting
    let initial = Open []
    let evolve state = function
        | Events.Commenced e -> state |> function
            | Open owned -> Running { owned = owned; trans = e.transId; pending = List.ofArray e.desired; acquired = List.ofArray e.acquired; failed = [] }
            | x -> failwithf "Can only Commence when Reverting, not %A" x
        | Events.Acquired e -> state |> function
            | Running s -> Running { s with pending = s.pending |> List.except e.tickets; acquired = merge e.tickets s.acquired }
            | Reverting s -> Reverting { s with outstanding = merge e.tickets s.outstanding }
            | Open _ -> failwith "Cannot have Acquired if not Open"
        | Events.Failed e -> state |> function
            | Open _ as state -> failwith "Cannot have Failed if not Open"
            | Running s -> Running { s with pending = s.pending |> List.except e.tickets; failed = merge e.tickets s.failed }
            | Reverting s -> Reverting { s with outstanding = s.outstanding |> List.except e.tickets }
        | Events.Released e -> state |> function
            | Open _ as state -> state
            | Running s as state -> state // we would be Reverting or Closed
            | Reverting s -> Reverting { s with outstanding = s.outstanding |> List.except e.tickets }
        | Events.Completed -> state |> function
            | Running s as state -> Open (merge s.owned s.acquired)
            | x -> failwithf "Can only Complete when Running, not %A" x
        | Events.Aborted -> state |> function
            | Reverting s -> Open s.owned
            | x -> failwithf "Can only Complete when Reverting, not %A" x
        | Events.Snapshotted -> state // only a dummy event so impl needed
    let fold : State -> Events.Event seq -> State = Seq.fold evolve

    let isOrigin = function Events.Aborted | Events.Commenced _ -> true | _ -> false
    let snapshot _state = Events.Snapshotted // Dummy event; we don't stop reading (we'd use a JsonIsomorphism to code/decode if there was benefit)
    let accessStrategy = Equinox.EventStore.AccessStrategy.RollingSnapshots (isOrigin,snapshot)

type State = { owned : PickTicketId list; toRelease : PickTicketId list; toAcquire : PickTicketId list }
type BusyState = { transactionId : AllocatorId; state : State }
type Result = Ok of State | Conflict of BusyState

let decideSync (transId : AllocatorId, desired : PickTicketId list, removed, acquired) (state : Folds.State) : Result * Events.Event list =
    failwith "TODO"

type Service(resolve, ?maxAttempts) =

    let log = Serilog.Log.ForContext<Service>()
    let (|AggregateId|) id = Equinox.AggregateId(Events.categoryId, PickListId.toString id)
    let (|Stream|) (AggregateId id) = Equinox.Stream<Events.Event,Folds.State>(log, resolve id, maxAttempts = defaultArg maxAttempts 3)
    let execute (Stream stream) = decideSync >> stream.Transact

    member __.Sync(pickListId,transactionId,desired, removed, acquired) : Async<Result> =
        execute pickListId (transactionId,desired,removed, acquired)

module EventStore =

    open Equinox.EventStore
    let resolve cache context =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        // while there are competing writers [which might cause us to have to retry a Transact], this should be infrequent
        let opt = Equinox.ResolveOption.AllowStale
        fun id -> Resolver(context, Events.codec, Folds.fold, Folds.initial, cacheStrategy, Folds.accessStrategy).Resolve(id,opt)
    let createService cache context =
        Service(resolve cache context)
