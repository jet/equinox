module Fc.Set

// NOTE - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
module Events =

    type Items = { items : string[] }
    type Event =
        | Added of Items
        | Snapshotted of Items
        interface TypeShape.UnionContract.IUnionContract
    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()
    let [<Literal>] categoryId = "Set"

module Folds =

    type State = Set<string>
    let initial : State = Set.empty
    let private evolve state = function
        | Events.Added { items = xs }
        | Events.Snapshotted { items = xs } ->
            (state,xs) ||> Array.fold (fun state x -> Set.add x state)
    let fold (state : State) = Seq.fold evolve state
    let snapshot state = Events.Snapshotted { items = Set.toArray state }

let interpret items (state : Folds.State) =
    // no need to deduplicate adds as we'll be folding these into the state imminently in any case
    let fresh = [| for i in items do if not (state.Contains i) then yield i|]
    match fresh with
    | [||] -> 0,[]
    | xs -> xs.Length,[Events.Added { items = xs }]

type Service(resolveStream, ?maxAttempts) =

    let log = Serilog.Log.ForContext<Service>()
    let (|AggregateId|) id = Equinox.AggregateId(Events.categoryId, SetId.toString id)
    let (|Stream|) (AggregateId aggregateId) = Equinox.Stream(log, resolveStream aggregateId, defaultArg maxAttempts 3)

    member __.Ingest(Stream index,items : string seq) : Async<int> = index.Transact(interpret items)
    member __.Read(Stream index) : Async<Set<string>> = index.Query id

module Cosmos =

    open Equinox.Cosmos
    let private createService (context,cache) =
        let accessStrategy = AccessStrategy.RollingState Folds.snapshot
        let resolve (context,cache) =
            let cacheStrategy = CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
            Resolver(context, Events.codec, Folds.fold, Folds.initial, cacheStrategy, accessStrategy).Resolve
        Service(resolve (context,cache))

module MemoryStore =

    let private createService store =
        let resolver = Equinox.MemoryStore.Resolver(store,Events.codec,Folds.fold,Folds.initial).Resolve
        Service(resolver)