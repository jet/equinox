module Samples.Store.Integration.FavoritesIntegration

open Swensen.Unquote

#nowarn "1182" // From hereon in, we may have some 'unused' privates (the tests)

let createServiceMem () =
    let store = createMemStore ()
    Backend.Favorites.Service(fun _codec -> createMemStream store)

let createServiceGes eventStoreConnection =
    let gateway = createGesGateway eventStoreConnection 500
    Backend.Favorites.Service(createGesStream gateway)

type Tests(testOutputHelper) =
    let testOutput = TestOutputAdapter testOutputHelper
    let createLog () = createLogger (testOutput.Subscribe >> ignore)

    [<AutoData>]
    let ``Can roundtrip in Memory, correctly folding the events`` clientId command = Async.RunSynchronously <| async {
        let! conn = connectToLocalEventStoreNode ()
        let log, service = createLog (), createServiceGes conn

        do! service.Run log clientId command
        let! items = service.Load log clientId

        match command with
        | Domain.Favorites.Commands.Favorite (_,skuIds) ->
            test <@ skuIds |> List.forall (fun skuId -> items |> Array.exists (function { skuId = itemSkuId} -> itemSkuId = skuId)) @>
        | _ ->
            test <@ Array.isEmpty items@>
    }

    [<AutoData>]
    let ``Can roundtrip against EventStore, correctly folding the events`` clientId command = Async.RunSynchronously <| async {
        let! conn = connectToLocalEventStoreNode ()
        let log, service = createLog (), createServiceGes conn

        do! service.Run log clientId command
        let! items = service.Load log clientId

        match command with
        | Domain.Favorites.Commands.Favorite (_,skuIds) ->
            test <@ skuIds |> List.forall (fun skuId -> items |> Array.exists (function { skuId = itemSkuId} -> itemSkuId = skuId)) @>
        | _ ->
            test <@ Array.isEmpty items@>
    }