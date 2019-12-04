module Index

// NOTE - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
module Events =

    type ItemIds = { items : string[] }
    type Items<'v> = { items : Map<string,'v> }
    type Event<'v> =
        | Added of Items<'v>
        | Deleted of ItemIds
        | Snapshotted of Items<'v>
        interface TypeShape.UnionContract.IUnionContract
    let codec<'v> = FsCodec.NewtonsoftJson.Codec.Create<Event<'v>>()
    let [<Literal>] categoryId = "Index"
    let (|AggregateId|) id = Equinox.AggregateId(categoryId, IndexId.toString id)

module Folds =

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

let interpret add remove (state : Folds.State<'v>) =
    let fresh = [| for k,v in add do if not (state |> Map.containsKey k) then yield k,v |]
    let dead = [| for k in remove do if state |> Map.containsKey k then yield k |]
    match fresh,dead with
    | [||],[||] -> (0,0),[]
    | adds,removes ->
        (adds.Length,removes.Length),
        [   if adds.Length <> 0 then yield Events.Added { items = Map.ofSeq adds }
            if removes.Length <> 0 then yield Events.Deleted { items = removes } ]

type Service<'t> internal (indexId, resolveStream, ?maxAttempts) =

    let log = Serilog.Log.ForContext<Service<'t>>()
    let (|Stream|) (Events.AggregateId aggregateId) = Equinox.Stream(log, resolveStream aggregateId, defaultArg maxAttempts 3)
    let (Stream stream) = indexId

    member __.Ingest(adds : seq<string*'t>, removes : string seq) : Async<int*int> =
        stream.Transact(interpret adds removes)
    member __.Read() : Async<Map<string,'t>> =
        stream.Query id

let create resolveStream indexId = Service(indexId, resolveStream)

module Cosmos =

    open Equinox.Cosmos
    let createService<'v> (context,cache) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        let accessStrategy = AccessStrategy.RollingState Folds.snapshot
        let resolve = Resolver(context, Events.codec, Folds.fold, Folds.initial, cacheStrategy, accessStrategy).Resolve
        create resolve

module MemoryStore =

    let createService store =
        let resolve = Equinox.MemoryStore.Resolver(store, Events.codec, Folds.fold, Folds.initial).Resolve
        create resolve