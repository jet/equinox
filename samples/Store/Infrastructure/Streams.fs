/// TL;DR There is no secret plan to make a package out of this;
/// it's infrastructure that makes sense solely in the context of this test rig
/// ...depending on 3 stores is not a goal or achievement

/// Yes, they live together here - that's because it's infrastructure that makes sense in the context of this test rig
/// It also proves the point that the Domain and backend are not bound to the Store, which is important too
/// IRL, it's unlikely you'd want to do storage switching in this manner

module Samples.Streams

let serializationSettings = Newtonsoft.Json.Converters.FSharp.Settings.CreateCorrect()
let inline genCodec<'Union when 'Union :> TypeShape.UnionContract.IUnionContract>() = Equinox.UnionCodec.JsonUtf8.Create<'Union>(serializationSettings)

type Resolver(storage) =
    member __.Resolve
        (   codec : Equinox.UnionCodec.IUnionEncoder<'event,byte[]>,
            fold: ('state -> 'event seq -> 'state),
            initial: 'state,
            snapshot: (('event -> bool) * ('state -> 'event))) =
        match storage with
        | Config.StorageConfig.Memory store ->
            Equinox.MemoryStore.MemResolver(store, fold, initial).Resolve
        | Config.StorageConfig.Es (gateway, cache, unfolds) ->
            let accessStrategy = if unfolds then Equinox.EventStore.AccessStrategy.RollingSnapshots snapshot |> Some else None
            Equinox.EventStore.GesResolver<'event,'state>(gateway, codec, fold, initial, ?access = accessStrategy, ?caching = cache).Resolve
        | Config.StorageConfig.Cosmos (gateway, cache, unfolds, databaseId, connectionId) ->
            let store = Equinox.Cosmos.EqxStore(gateway, Equinox.Cosmos.EqxCollections(databaseId, connectionId))
            let accessStrategy = if unfolds then Equinox.Cosmos.AccessStrategy.Snapshot snapshot |> Some else None
            Equinox.Cosmos.EqxResolver<'event,'state>(store, codec, fold, initial, ?access = accessStrategy, ?caching = cache).Resolve