/// TL;DR There is no secret plan to make a package out of this;
/// it's infrastructure that makes sense solely in the context of this test rig
/// ...depending on 3 stores is not a goal or achievement

/// Yes, they live together here - that's because it's infrastructure that makes sense in the context of this test rig
/// It also proves the point that the Domain and backend are not bound to the Store, which is important too
/// IRL, it's unlikely you'd want to do storage switching in this manner

module Store.CompositionRoot

open System

let serializationSettings = Newtonsoft.Json.Converters.FSharp.Settings.CreateCorrect()
let inline genCodec<'Union when 'Union :> TypeShape.UnionContract.IUnionContract>() = Equinox.UnionCodec.JsonUtf8.Create<'Union>(serializationSettings)

type private EsResolver(useCache) =
    member val Cache =
        if useCache then
            let c = Equinox.EventStore.Caching.Cache("Cli", sizeMb = 50)
            Equinox.EventStore.CachingStrategy.SlidingWindow (c, TimeSpan.FromMinutes 20.) |> Some
        else None
    member __.CreateAccessStrategy snapshot =
        match snapshot with
        | None -> None
        | Some snapshot -> Equinox.EventStore.AccessStrategy.RollingSnapshots snapshot |> Some
type private CosmosResolver(useCache) =
    member val Cache =
        if useCache then
            let c = Equinox.Cosmos.Caching.Cache("Cli", sizeMb = 50)
            Equinox.Cosmos.CachingStrategy.SlidingWindow (c, TimeSpan.FromMinutes 20.) |> Some
        else None
    member __.CreateAccessStrategy snapshot =
        match snapshot with
        | None -> None
        | Some snapshot -> Equinox.Cosmos.AccessStrategy.Snapshot snapshot |> Some

[<RequireQualifiedAccess; NoEquality; NoComparison>]
type Store =
    | Mem of Equinox.MemoryStore.VolatileStore
    | Es of Equinox.EventStore.GesGateway
    | Cosmos of Equinox.Cosmos.EqxGateway * databaseId: string * collectionId: string

type Builder(store, useCache, useUnfolds) =
    member __.ResolveStream
        (   codec : Equinox.UnionCodec.IUnionEncoder<'event,byte[]>,
            fold: ('state -> 'event seq -> 'state),
            initial: 'state,
            snapshot: (('event -> bool) * ('state -> 'event))) =
        let snapshot = if useUnfolds then Some snapshot else None
        match store with
        | Store.Mem store ->
            Equinox.MemoryStore.MemResolver(store, fold, initial).Resolve
        | Store.Es gateway ->
            let resolver = EsResolver(useCache)
            Equinox.EventStore.GesResolver<'event,'state>(gateway, codec, fold, initial, ?access = resolver.CreateAccessStrategy(snapshot), ?caching = resolver.Cache).Resolve
        | Store.Cosmos (gateway, databaseId, connectionId) ->
            let resolver = CosmosResolver(useCache)
            let store = Equinox.Cosmos.EqxStore(gateway, Equinox.Cosmos.EqxCollections(databaseId, connectionId))
            Equinox.Cosmos.EqxResolver<'event,'state>(store, codec, fold, initial, ?access = resolver.CreateAccessStrategy snapshot, ?caching = resolver.Cache).Resolve