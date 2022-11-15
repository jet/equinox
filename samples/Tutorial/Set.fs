module Set

let [<Literal>] Category = "Set"
let target = Equinox.Target.gen Category SetId.toString

// NOTE - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
module Events =

    type Items = { items : string[] }
    type Event =
        | Added of Items
        | Deleted of Items
        | Snapshotted of Items
        interface TypeShape.UnionContract.IUnionContract
    let codec = FsCodec.SystemTextJson.CodecJsonElement.Create<Event>()

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

type Service internal (decider : Equinox.Decider<Events.Event, Fold.State>) =

    member _.Add(add : string seq, remove : string seq) : Async<int*int> =
        decider.Transact(interpret add remove)

    member _.Read() : Async<Set<string>> =
        decider.Query id

let create resolve setId =
    Service(target setId |> resolve)

module Cosmos =

    open Equinox.CosmosStore
    let create (context, cache) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        let accessStrategy = AccessStrategy.RollingState Fold.snapshot
        CosmosStoreCategory(context, Events.codec, Fold.fold, Fold.initial, cacheStrategy, accessStrategy)
        |> Equinox.Decider.resolve Serilog.Log.Logger |> create

module MemoryStore =

    let create store =
        Equinox.MemoryStore.MemoryStoreCategory(store, Events.codec, Fold.fold, Fold.initial)
        |> Equinox.Decider.resolve Serilog.Log.Logger |> create
