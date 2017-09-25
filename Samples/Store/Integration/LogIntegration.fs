module Example.Integration.LogIntegration

open Domain
open Backend
open Swensen.Unquote
open System
open System.Diagnostics

#nowarn "1182" // From hereon in, we may have some 'unused' privates (the tests)

let createServiceWithEventStore eventStoreConnection = Favorites.Service(createGesStreamer eventStoreConnection 500, 3)

type Tests() =
    let createLoggerWithCapture () =
        let capture = SerilogTracerAdapter(System.Diagnostics.Trace.WriteLine)
        let subscribeLogListeners obs =
            obs |> capture.Subscribe |> ignore
        createLogger subscribeLogListeners, capture

    [<AutoData>]
    let ``Can trap and emit log entries`` clientId command = Async.RunSynchronously <| async {
        let! conn = connectToLocalEventStoreNode ()
        let log, _ = createLoggerWithCapture ()
        let service = createServiceWithEventStore conn

        do! service.Run log clientId command
        let! items = service.Load log clientId

        match command with
        | Favorites.Commands.Favorite (_,skuIds) ->
            test <@ skuIds |> List.forall (fun skuId -> items |> Array.exists (function { skuId = itemSkuId} -> itemSkuId = skuId)) @>
        | _ ->
            test <@ Array.isEmpty items@>
    }