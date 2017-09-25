module Example.Integration.FavoritesIntegration

open Domain
open Backend
open Swensen.Unquote

#nowarn "1182" // From hereon in, we may have some 'unused' privates (the tests)

let createServiceWithEventStore eventStoreConnection = Favorites.Service(createGesStreamer eventStoreConnection 500, 3)

type Tests(testOutputHelper) =
    let testOutput = TestOutputAdapter testOutputHelper
    let createLog () = createLogger (testOutput.Subscribe >> ignore)

    [<AutoData>]
    let ``Can roundtrip Favorites, correctly folding the events`` clientId command = Async.RunSynchronously <| async {
        let! conn = connectToLocalEventStoreNode ()
        let log, service = createLog (), createServiceWithEventStore conn

        do! service.Run log clientId command
        let! items = service.Load log clientId

        match command with
        | Favorites.Commands.Favorite (_,skuIds) ->
            test <@ skuIds |> List.forall (fun skuId -> items |> Array.exists (function { skuId = itemSkuId} -> itemSkuId = skuId)) @>
        | _ ->
            test <@ Array.isEmpty items@>
    }