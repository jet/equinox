module Samples.Store.Integration.FavoritesIntegration

open Equinox.Cosmos.Builder
open Equinox.Cosmos.Integration
open Equinox.EventStore
open Equinox.MemoryStore
open Swensen.Unquote

#nowarn "1182" // From hereon in, we may have some 'unused' privates (the tests)

let fold, initial, snapshot = Domain.Favorites.Folds.fold, Domain.Favorites.Folds.initial, Domain.Favorites.Folds.snapshot

let createMemoryStore () =
    new VolatileStore()
let createServiceMem log store =
    Backend.Favorites.Service(log, MemResolver(store, fold, initial).Resolve)

let codec = genCodec<Domain.Favorites.Events.Event>()
let createServiceGes gateway log =
    let resolveStream = GesResolver(gateway, codec, fold, initial, AccessStrategy.RollingSnapshots snapshot).Resolve
    Backend.Favorites.Service(log, resolveStream)

let createServiceEqx gateway log =
    let resolveStream = EqxStreamBuilder(gateway, codec, fold, initial, AccessStrategy.Projection compact).Create
    Backend.Favorites.Service(log, resolveStream)

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
        let store = createMemoryStore ()
        let service = let log = createLog () in createServiceMem log store
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
        let gateway = createEqxStore conn defaultBatchSize
        let service = createServiceEqx gateway log
        do! act service args
    }