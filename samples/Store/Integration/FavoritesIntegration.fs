module Samples.Store.Integration.FavoritesIntegration

open Equinox
open Equinox.Cosmos.Integration
open Swensen.Unquote

#nowarn "1182" // From hereon in, we may have some 'unused' privates (the tests)

let fold, initial = Domain.Favorites.Fold.fold, Domain.Favorites.Fold.initial
let snapshot = Domain.Favorites.Fold.isOrigin, Domain.Favorites.Fold.snapshot

let createMemoryStore () = MemoryStore.VolatileStore<_>()
let createServiceMemory log store =
    Backend.Favorites.Service(log, MemoryStore.Resolver(store, FsCodec.Box.Codec.Create(), fold, initial).Resolve)

let codec = Domain.Favorites.Events.codec
let createServiceGes gateway log =
    let resolve = EventStore.Resolver(gateway, codec, fold, initial, access = EventStore.AccessStrategy.RollingSnapshots snapshot).Resolve
    Backend.Favorites.Service(log, resolve)

let createServiceCosmosSnapshotsUncached gateway log =
    let resolve = Cosmos.Resolver(gateway, codec, fold, initial, Cosmos.CachingStrategy.NoCaching, Cosmos.AccessStrategy.Snapshot snapshot).Resolve
    Backend.Favorites.Service(log, resolve)

let createServiceCosmosRollingStateUncached gateway log =
    let access = Cosmos.AccessStrategy.RollingState Domain.Favorites.Fold.snapshot
    let resolve = Cosmos.Resolver(gateway, codec, fold, initial, Cosmos.CachingStrategy.NoCaching, access).Resolve
    Backend.Favorites.Service(log, resolve)

let createServiceCosmosUnoptimizedButCached gateway log =
    let access = Cosmos.AccessStrategy.Unoptimized
    let caching =
        let cache = Equinox.Cache ("name", 10)
        Cosmos.CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
    let resolve = Cosmos.Resolver(gateway, codec, fold, initial, caching, access).Resolve
    Backend.Favorites.Service(log, resolve)

type Tests(testOutputHelper) =
    let testOutput = TestOutputAdapter testOutputHelper
    let createLog () = createLogger testOutput

    let act (service : Backend.Favorites.Service) (clientId, command) = async {
        do! service.Execute(clientId, command)
        let! version, items = service.ListWithVersion clientId

        match command with
        | Domain.Favorites.Favorite (_, skuIds) ->
            test <@ skuIds |> List.forall (fun skuId -> items |> Array.exists (function { skuId = itemSkuId} -> itemSkuId = skuId)) @>
        | Domain.Favorites.Unfavorite _ ->
            test <@ Array.isEmpty items @>
        return version, items }

    [<AutoData>]
    let ``Can roundtrip in Memory, correctly folding the events`` args = Async.RunSynchronously <| async {
        let log, store = createLog (), createMemoryStore ()
        let service = createServiceMemory log store
        let! version, items = act service args
        version =! items.LongLength
    }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_EVENTSTORE")>]
    let ``Can roundtrip against EventStore, correctly folding the events`` args = Async.RunSynchronously <| async {
        let log = createLog ()
        let! conn = connectToLocalEventStoreNode log
        let gateway = createGesGateway conn defaultBatchSize
        let service = createServiceGes gateway log
        let! version, items = act service args
        version =! items.LongLength
    }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``Can roundtrip against Cosmos, correctly folding the events`` args = Async.RunSynchronously <| async {
        let log = createLog ()
        let! conn = connectToSpecifiedCosmosOrSimulator log
        let gateway = createCosmosContext conn defaultBatchSize
        let service = createServiceCosmosSnapshotsUncached gateway log
        let! version, items = act service args
        version =! items.LongLength
    }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``Can roundtrip against Cosmos, correctly folding the events with caching`` (clientId, cmd) = Async.RunSynchronously <| async {
        let log = createLog ()
        let! conn = connectToSpecifiedCosmosOrSimulator log
        let gateway = createCosmosContext conn defaultBatchSize
        let service = createServiceCosmosUnoptimizedButCached gateway log
        let clientId = clientId () // generate a fresh one per test so repeated runs start from a stable base
        let! version, items = act service (clientId, cmd)
        version =! items.LongLength

        // Validate consecutive Unoptimized Cached reads yield the correct Version
        // TODO represent this more directly as a Cosmos Integration test
        let service2 = createServiceCosmosUnoptimizedButCached gateway log
        let! rereadVersion, items = service2.ListWithVersion clientId
        rereadVersion =! version
        rereadVersion =! items.LongLength
        let! rereadVersion2, items = service2.ListWithVersion clientId
        rereadVersion2 =! version
        rereadVersion2 =! items.LongLength
    }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``Can roundtrip against Cosmos, correctly folding the events with rolling unfolds`` args = Async.RunSynchronously <| async {
        let log = createLog ()
        let! conn = connectToSpecifiedCosmosOrSimulator log
        let gateway = createCosmosContext conn defaultBatchSize
        let service = createServiceCosmosRollingStateUncached gateway log
        let! version, _items = act service args
        version =! 0L
    }
