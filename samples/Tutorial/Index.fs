module Index

let [<Literal>] Category = "Index"
let streamId = Equinox.StreamId.gen IndexId.toString

// NOTE - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
module Events =

    type ItemIds = { items : string[] }
    type Items<'v> = { items : Map<string,'v> }
    type Event<'v> =
        | Added of Items<'v>
        | Deleted of ItemIds
        | Snapshotted of Items<'v>
        interface TypeShape.UnionContract.IUnionContract
    let codec<'v> = FsCodec.SystemTextJson.CodecJsonElement.Create<Event<'v>>()

module Fold =

    type State<'v> = Map<string,'v>
    let initial = Map.empty
    let private evolve state = function
        | Events.Deleted { items = xs } ->
            (state,xs) ||> Array.fold (fun state k -> Map.remove k state)
        | Events.Added { items = xs }
        | Events.Snapshotted { items = xs } ->
            (state,xs) ||> Map.fold (fun state k v -> Map.add k v state)
    let fold state = Seq.fold evolve state
    let snapshot state = Events.Snapshotted { items = state }

let interpret add remove (state : Fold.State<'v>) =
    let fresh = [| for k,v in add do if not (state |> Map.containsKey k) then yield k,v |]
    let dead = [| for k in remove do if state |> Map.containsKey k then yield k |]
    match fresh,dead with
    | [||],[||] -> (0,0),[]
    | adds,removes ->
        (adds.Length,removes.Length),
        [   if adds.Length <> 0 then yield Events.Added { items = Map.ofSeq adds }
            if removes.Length <> 0 then yield Events.Deleted { items = removes } ]

type Service<'t> internal (decider : Equinox.Decider<Events.Event<'t>, Fold.State<'t>>) =

    member _.Ingest(adds : seq<string*'t>, removes : string seq) : Async<int*int> =
        decider.Transact(interpret adds removes)
    member _.Read() : Async<Map<string,'t>> =
        decider.Query id

let create<'t> indexId cat =
    Service(streamId indexId |> Equinox.Decider.forStream Serilog.Log.Logger cat)

module Cosmos =

    open Equinox.CosmosStore
    let category (context,cache) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        let accessStrategy = AccessStrategy.RollingState Fold.snapshot
        CosmosStoreCategory(context, Category, Events.codec, Fold.fold, Fold.initial, accessStrategy, cacheStrategy)

module MemoryStore =

    let category store =
        Equinox.MemoryStore.MemoryStoreCategory(store, Category, Events.codec, Fold.fold, Fold.initial)
