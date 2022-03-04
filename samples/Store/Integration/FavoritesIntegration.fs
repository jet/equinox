module Samples.Store.Integration.FavoritesIntegration

open Domain
open Equinox
open Equinox.CosmosStore.Integration
open Swensen.Unquote

let fold, initial = Favorites.Fold.fold, Favorites.Fold.initial
let snapshot = Favorites.Fold.isOrigin, Favorites.Fold.snapshot

let createMemoryStore () = MemoryStore.VolatileStore<_>()
let createServiceMemory log store =
    Favorites.create log (MemoryStore.MemoryStoreCategory(store, FsCodec.Box.Codec.Create(), fold, initial).Resolve)

let codec = Favorites.Events.codec
let codecStj = Favorites.Events.codecStj
let createServiceGes log context =
    let cat = EventStore.EventStoreCategory(context, codec, fold, initial, access = EventStore.AccessStrategy.RollingSnapshots snapshot)
    Favorites.create log cat.Resolve

let createServiceCosmosSnapshotsUncached log context =
    let cat = CosmosStore.CosmosStoreCategory(context, codecStj, fold, initial, CosmosStore.CachingStrategy.NoCaching, CosmosStore.AccessStrategy.Snapshot snapshot)
    Favorites.create log cat.Resolve

let createServiceCosmosRollingStateUncached log context =
    let access = CosmosStore.AccessStrategy.RollingState Favorites.Fold.snapshot
    let cat = CosmosStore.CosmosStoreCategory(context, codecStj, fold, initial, CosmosStore.CachingStrategy.NoCaching, access)
    Favorites.create log cat.Resolve

let createServiceCosmosUnoptimizedButCached log context =
    let access = CosmosStore.AccessStrategy.Unoptimized
    let caching =
        let cache = Equinox.Cache ("name", 10)
        CosmosStore.CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
    let cat = CosmosStore.CosmosStoreCategory(context, codecStj, fold, initial, caching, access)
    Favorites.create log cat.Resolve

type Command =
    | Favorite      of date : System.DateTimeOffset * skuIds : SkuId list
    | Unfavorite    of skuId : SkuId

let execute (service : Favorites.Service) clientId = function
    | Favorite (date, skus) ->  service.Favorite(clientId, skus, at = date)
    | Unfavorite sku ->         service.Unfavorite(clientId, sku)

type Tests(testOutputHelper) =
    let testOutput = TestOutputAdapter testOutputHelper
    let createLog () = createLogger testOutput

    let act (service : Favorites.Service) (clientId, command) = async {
        do! execute service clientId command
        let! version, items = service.ListWithVersion clientId

        match command with
        | Favorite (_, skuIds) ->
            test <@ skuIds |> List.forall (fun skuId -> items |> Array.exists (function { skuId = itemSkuId} -> itemSkuId = skuId)) @>
        | Unfavorite _ ->
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
        let! client = connectToLocalEventStoreNode log
        let context = createContext client defaultBatchSize
        let service = createServiceGes log context
        let! version, items = act service args
        version =! items.LongLength
    }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``Can roundtrip against Cosmos, correctly folding the events with caching`` (clientId, cmd) = Async.RunSynchronously <| async {
        let log = createLog ()
        let context = createPrimaryContext log defaultQueryMaxItems
        let service = createServiceCosmosUnoptimizedButCached log context
        let clientId = clientId () // generate a fresh one per test so repeated runs start from a stable base
        let! version, items = act service (clientId, cmd)
        version =! items.LongLength

        // Validate consecutive Unoptimized Cached reads yield the correct Version
        // TODO represent this more directly as a Cosmos Integration test
        let service2 = createServiceCosmosUnoptimizedButCached log context
        let! rereadVersion, items = service2.ListWithVersion clientId
        rereadVersion =! version
        rereadVersion =! items.LongLength
        let! rereadVersion2, items = service2.ListWithVersion clientId
        rereadVersion2 =! version
        rereadVersion2 =! items.LongLength
    }
    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``Can roundtrip against Cosmos, correctly folding the events`` args = Async.RunSynchronously <| async {
        let log = createLog ()
        let context = createPrimaryContext log defaultQueryMaxItems
        let service = createServiceCosmosSnapshotsUncached log context
        let! version, items = act service args
        version =! items.LongLength
    }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``Can roundtrip against Cosmos, correctly folding the events with rolling unfolds`` args = Async.RunSynchronously <| async {
        let log = createLog ()
        let context = createPrimaryContext log defaultQueryMaxItems
        let service = createServiceCosmosRollingStateUncached log context
        let! version, _items = act service args
        version =! 0L
    }
