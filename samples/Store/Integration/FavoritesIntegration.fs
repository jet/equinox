module Samples.Store.Integration.FavoritesIntegration

open Domain
open Equinox
open Equinox.CosmosStore.Integration.CosmosFixtures
open Swensen.Unquote

let [<Literal>] Category = Favorites.Category
let fold, initial = Favorites.Fold.fold, Favorites.Fold.initial
let snapshot = Favorites.Fold.isOrigin, Favorites.Fold.snapshot

let createMemoryStore () = MemoryStore.VolatileStore<_>()
let createServiceMemory log store =
    MemoryStore.MemoryStoreCategory(store, Category, FsCodec.Box.Codec.Create(), fold, initial)
    |> Decider.forStream log
    |> Favorites.create

let codec = Favorites.Events.codec
let codecJe = Favorites.Events.codecJe
let createServiceGes log context =
    EventStoreDb.EventStoreCategory(context, Category, codec, fold, initial, access = EventStoreDb.AccessStrategy.RollingSnapshots snapshot)
    |> Decider.forStream log
    |> Favorites.create

let createServiceCosmosSnapshotsUncached log context =
    CosmosStore.CosmosStoreCategory(context, Category, codecJe, fold, initial, CosmosStore.AccessStrategy.Snapshot snapshot, CosmosStore.CachingStrategy.NoCaching)
    |> Decider.forStream log
    |> Favorites.create

let createServiceCosmosRollingStateUncached log context =
    let access = CosmosStore.AccessStrategy.RollingState Favorites.Fold.snapshot
    CosmosStore.CosmosStoreCategory(context, Category, codecJe, fold, initial, access, CosmosStore.CachingStrategy.NoCaching)
    |> Decider.forStream log
    |> Favorites.create

let createServiceCosmosUnoptimizedButCached log context =
    let access = CosmosStore.AccessStrategy.Unoptimized
    let caching =
        let cache = Cache ("name", 10)
        CosmosStore.CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
    CosmosStore.CosmosStoreCategory(context, Category, codecJe, fold, initial, access, caching)
    |> Decider.forStream log
    |> Favorites.create

type Command =
    | Favorite      of date : System.DateTimeOffset * skuIds : SkuId list
    | Unfavorite    of skuId : SkuId

let execute (service : Favorites.Service) clientId = function
    | Favorite (date, skus) ->  service.Favorite(clientId, skus, at = date)
    | Unfavorite sku ->         service.Unfavorite(clientId, sku)

type Tests(testOutputHelper) =
    let testOutput = TestOutput testOutputHelper
    let log = testOutput.CreateLogger()

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
    let ``Can roundtrip in Memory, correctly folding the events`` args = async {
        let store = createMemoryStore ()
        let service = createServiceMemory log store
        let! version, items = act service args
        version =! items.LongLength
    }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_EVENTSTORE")>]
    let ``Can roundtrip against EventStore, correctly folding the events`` args = async {
        let client = connectToLocalEventStoreNode log
        let context = createContext client defaultBatchSize
        let service = createServiceGes log context
        let! version, items = act service args
        version =! items.LongLength
    }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``Can roundtrip against Cosmos, correctly folding the events with caching`` (clientId, cmd) = async {
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
    let ``Can roundtrip against Cosmos, correctly folding the events`` args = async {
        let context = createPrimaryContext log defaultQueryMaxItems
        let service = createServiceCosmosSnapshotsUncached log context
        let! version, items = act service args
        version =! items.LongLength
    }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``Can roundtrip against Cosmos, correctly folding the events with rolling unfolds`` args = async {
        let context = createPrimaryContext log defaultQueryMaxItems
        let service = createServiceCosmosRollingStateUncached log context
        let! version, _items = act service args
        version =! 0L
    }
