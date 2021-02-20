module Samples.Store.Integration.FavoritesIntegration

open Domain
open Equinox
open Equinox.CosmosStore.Integration
open Swensen.Unquote

#nowarn "1182" // From hereon in, we may have some 'unused' privates (the tests)

let fold, initial = Domain.Favorites.Fold.fold, Domain.Favorites.Fold.initial
let snapshot = Domain.Favorites.Fold.isOrigin, Domain.Favorites.Fold.snapshot

let createMemoryStore () = MemoryStore.VolatileStore<_>()
let createServiceMemory log store =
    Favorites.create log (MemoryStore.MemoryStoreCategory(store, FsCodec.Box.Codec.Create(), fold, initial).Resolve)

let codec = Domain.Favorites.Events.codec
let createServiceGes log context =
    let cat = EventStore.EventStoreCategory(context, codec, fold, initial, access = EventStore.AccessStrategy.RollingSnapshots snapshot)
    Favorites.create log cat.Resolve

let createServiceCosmos log context =
    let cat = CosmosStore.CosmosStoreCategory(context, codec, fold, initial, CosmosStore.CachingStrategy.NoCaching, CosmosStore.AccessStrategy.Snapshot snapshot)
    Favorites.create log cat.Resolve

let createServiceCosmosRollingState log context =
    let access = CosmosStore.AccessStrategy.RollingState Domain.Favorites.Fold.snapshot
    let cat = CosmosStore.CosmosStoreCategory(context, codec, fold, initial, CosmosStore.CachingStrategy.NoCaching, access)
    Favorites.create log cat.Resolve

type Tests(testOutputHelper) =
    let testOutput = TestOutputAdapter testOutputHelper
    let createLog () = createLogger testOutput

    let act (service : Favorites.Service) (clientId, command) = async {
        do! service.Execute(clientId, command)
        let! items = service.List clientId

        match command with
        | Domain.Favorites.Favorite (_,skuIds) ->
            test <@ skuIds |> List.forall (fun skuId -> items |> Array.exists (function { skuId = itemSkuId} -> itemSkuId = skuId)) @>
        | _ ->
            test <@ Array.isEmpty items @> }

    [<AutoData>]
    let ``Can roundtrip in Memory, correctly folding the events`` args = Async.RunSynchronously <| async {
        let log, store = createLog (), createMemoryStore ()
        let service = createServiceMemory log store
        do! act service args
    }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_EVENTSTORE")>]
    let ``Can roundtrip against EventStore, correctly folding the events`` args = Async.RunSynchronously <| async {
        let log = createLog ()
        let! client = connectToLocalEventStoreNode log
        let context = createGesContext client defaultBatchSize
        let service = createServiceGes log context
        do! act service args
    }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``Can roundtrip against Cosmos, correctly folding the events`` args = Async.RunSynchronously <| async {
        let log = createLog ()
        let context = createPrimaryContext log defaultQueryMaxItems
        let service = createServiceCosmos log context
        do! act service args
    }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``Can roundtrip against Cosmos, correctly folding the events with rolling unfolds`` args = Async.RunSynchronously <| async {
        let log = createLog ()
        let context = createPrimaryContext log defaultQueryMaxItems
        let service = createServiceCosmosRollingState log context
        do! act service args
    }
