module Samples.Store.Integration.FavoritesIntegration

open Equinox
open Equinox.Cosmos.Integration
open Swensen.Unquote

#nowarn "1182" // From hereon in, we may have some 'unused' privates (the tests)

let fold, initial = Domain.Favorites.Fold.fold, Domain.Favorites.Fold.initial
let snapshot = Domain.Favorites.Fold.isOrigin, Domain.Favorites.Fold.snapshot

let createMemoryStore () = MemoryStore.VolatileStore<_>()
let createServiceMemory log store =
    Backend.Favorites.create log (MemoryStore.Resolver(store, FsCodec.Box.Codec.Create(), fold, initial).Resolve)

let codec = Domain.Favorites.Events.codec
let createServiceGes gateway log =
    let resolver = EventStore.Resolver(gateway, codec, fold, initial, access = EventStore.AccessStrategy.RollingSnapshots snapshot)
    Backend.Favorites.create log resolver.Resolve

let createServiceCosmos gateway log =
    let resolver = Cosmos.Resolver(gateway, codec, fold, initial, Cosmos.CachingStrategy.NoCaching, Cosmos.AccessStrategy.Snapshot snapshot)
    Backend.Favorites.create log resolver.Resolve

let createServiceCosmosRollingState gateway log =
    let access = Cosmos.AccessStrategy.RollingState Domain.Favorites.Fold.snapshot
    let resolver = Cosmos.Resolver(gateway, codec, fold, initial, Cosmos.CachingStrategy.NoCaching, access)
    Backend.Favorites.create log resolver.Resolve

type Tests(testOutputHelper) =
    let testOutput = TestOutputAdapter testOutputHelper
    let createLog () = createLogger testOutput

    let act (service : Backend.Favorites.Service) (clientId, command) = async {
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
        let! conn = connectToLocalEventStoreNode log
        let gateway = createGesGateway conn defaultBatchSize
        let service = createServiceGes gateway log
        do! act service args
    }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``Can roundtrip against Cosmos, correctly folding the events`` args = Async.RunSynchronously <| async {
        let log = createLog ()
        let! conn = connectToSpecifiedCosmosOrSimulator log
        let gateway = createCosmosContext conn defaultBatchSize
        let service = createServiceCosmos gateway log
        do! act service args
    }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``Can roundtrip against Cosmos, correctly folding the events with rolling unfolds`` args = Async.RunSynchronously <| async {
        let log = createLog ()
        let! conn = connectToSpecifiedCosmosOrSimulator log
        let gateway = createCosmosContext conn defaultBatchSize
        let service = createServiceCosmosRollingState gateway log
        do! act service args
    }
