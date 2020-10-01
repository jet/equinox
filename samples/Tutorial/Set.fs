module Set

let [<Literal>] Category = "Set"
let streamName id = FsCodec.StreamName.create Category (SetId.toString id)

// NOTE - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
module Events =

    type Items = { items : string[] }
    type Event =
        | Added of Items
        | Deleted of Items
        | Snapshotted of Items
        interface TypeShape.UnionContract.IUnionContract
    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()

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

type Service internal (stream : Equinox.Stream<Events.Event, Fold.State>) =

    member __.Add(add : string seq, remove : string seq) : Async<int*int> =
        stream.Transact(interpret add remove)

    member __.Read() : Async<Set<string>> =
        stream.Query id

let create resolve setId =
    let streamName = streamName setId
    let stream = Equinox.Stream(Serilog.Log.ForContext<Service>(), resolve streamName, maxAttempts = 3)
    Service(stream)

module Cosmos =

    open Equinox.CosmosStore
    let create (context, cache) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        let accessStrategy = AccessStrategy.RollingState Fold.snapshot
        let category = CosmosStoreCategory(context, Events.codec, Fold.fold, Fold.initial, cacheStrategy, accessStrategy)
        create category.Resolve

module MemoryStore =

    let create store =
        let resolver = Equinox.MemoryStore.Resolver(store, Events.codec, Fold.fold, Fold.initial)
        create resolver.Resolve
