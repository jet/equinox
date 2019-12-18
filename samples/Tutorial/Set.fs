module Set

// NOTE - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
module Events =

    type Items = { items : string[] }
    type Event =
        | Added of Items
        | Deleted of Items
        | Snapshotted of Items
        interface TypeShape.UnionContract.IUnionContract
    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()
    let [<Literal>] categoryId = "Set"
    let (|ForSetId|) id = Equinox.AggregateId(categoryId, SetId.toString id)

module Fold =

    type State = Set<string>
    let initial : State = Set.empty
    let private evolve state = function
        | Events.Deleted { items = xs } ->
            (state,xs) ||> Array.fold (fun state x -> Set.remove x state)
        | Events.Added { items = xs }
        | Events.Snapshotted { items = xs } ->
            (state,xs) ||> Array.fold (fun state x -> Set.add x state)
    let fold (state : State) = Seq.fold evolve state
    let snapshot state = Events.Snapshotted { items = Set.toArray state }

let interpret add remove (state : Fold.State) =
    // no need to deduplicate adds as we'll be folding these into the state imminently in any case
    let fresh = [| for i in add do if not (state.Contains i) then yield i |]
    let dead = [| for i in remove do if state.Contains i then yield i |]
    match fresh,dead with
    | [||],[||] -> (0,0),[]
    | adds,removes ->
        (adds.Length,removes.Length),
        [   if adds.Length <> 0 then yield Events.Added { items = adds }
            if removes.Length <> 0 then yield Events.Deleted { items = removes } ]

type Service internal (log, setId, resolve, maxAttempts) =

    let resolve (Events.ForSetId streamId) = Equinox.Stream(log, resolve streamId, maxAttempts)
    let stream = resolve setId

    member __.Add(add : string seq,remove : string seq) : Async<int*int> =
        stream.Transact(interpret add remove)

    member __.Read() : Async<Set<string>> =
        stream.Query id

let create resolve setId = Service(Serilog.Log.ForContext<Service>(), setId, resolve, maxAttempts = 3)

module Cosmos =

    open Equinox.Cosmos
    let createService (context,cache) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        let accessStrategy = AccessStrategy.RollingState Fold.snapshot
        let resolve = Resolver(context, Events.codec, Fold.fold, Fold.initial, cacheStrategy, accessStrategy).Resolve
        create resolve

module MemoryStore =

    let createService store =
        let resolve = Equinox.MemoryStore.Resolver(store, Events.codec, Fold.fold, Fold.initial).Resolve
        create resolve