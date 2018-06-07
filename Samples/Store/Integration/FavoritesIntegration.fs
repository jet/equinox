module Samples.Store.Integration.FavoritesIntegration

open Foldunk.EventStore
open Foldunk.MemoryStore
open Swensen.Unquote

#nowarn "1182" // From hereon in, we may have some 'unused' privates (the tests)

let fold, initial = Domain.Favorites.Folds.fold, Domain.Favorites.Folds.initial

let createMemoryStore () =
    new VolatileStore()
let createServiceMem store =
    Backend.Favorites.Service(fun _cet -> MemoryStreamBuilder(store, fold, initial).Create)

let codec = genCodec<Domain.Favorites.Events.Event>
let createServiceGes connection =
    let gateway = createGesGateway connection defaultBatchSize
    Backend.Favorites.Service(fun cet -> GesStreamBuilder(gateway, codec, fold, initial, CompactionStrategy.EventType cet).Create)

type Tests(testOutputHelper) =
    let testOutput = TestOutputAdapter testOutputHelper
    let createLog () = createLogger testOutput

    [<AutoData>]
    let ``Can roundtrip in Memory, correctly folding the events`` clientId command = Async.RunSynchronously <| async {
        let store = createMemoryStore ()
        let log, service = createLog (), createServiceMem store

        do! service.Execute log clientId command
        let! items = service.Read log clientId

        match command with
        | Domain.Favorites.Favorite (_,skuIds) ->
            test <@ skuIds |> List.forall (fun skuId -> items |> Array.exists (function { skuId = itemSkuId} -> itemSkuId = skuId)) @>
        | _ ->
            test <@ Array.isEmpty items@>
    }

    [<AutoData>]
    let ``Can roundtrip against EventStore, correctly folding the events`` clientId command = Async.RunSynchronously <| async {
        let log = createLog ()
        let! conn = connectToLocalEventStoreNode log
        let log, service = createLog (), createServiceGes conn

        do! service.Execute log clientId command
        let! items = service.Read log clientId

        match command with
        | Domain.Favorites.Favorite (_,skuIds) ->
            test <@ skuIds |> List.forall (fun skuId -> items |> Array.exists (function { skuId = itemSkuId} -> itemSkuId = skuId)) @>
        | _ ->
            test <@ Array.isEmpty items@>
    }