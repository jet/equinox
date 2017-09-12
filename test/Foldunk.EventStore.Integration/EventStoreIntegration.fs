module Foldunk.EventStore.Integration.EventStoreIntegration

open Backend.Favorites
open Domain
open Foldunk
open Swensen.Unquote

#nowarn "1182" // From hereon in, we may have some 'unused' privates (the tests)

type Tests() =
    /// Needs an ES instance with default settings
    /// TL;DR: At an elevated command prompt: choco install eventstore-oss; \ProgramData\chocolatey\bin\EventStore.ClusterNode.exe
    let connectToLocalEventStoreNode () = async {
        let localhost = System.Net.IPEndPoint(System.Net.IPAddress.Loopback, 1113)
        let conn = EventStore.ClientAPI.EventStoreConnection.Create(localhost)
        do! conn.ConnectAsync() |> Async.AwaitTask
        return conn }

    let createService eventStoreConnection =
        let connection          = Foldunk.Stores.EventStore.GesConnection(eventStoreConnection)
        let gateway             = Foldunk.Stores.EventStore.GesGateway(connection, Foldunk.Stores.EventStore.GesStreamPolicy(maxBatchSize = 500))
        let codec               = Foldunk.EventSum.generateJsonUtf8SumEncoder<_>
        let eventStream         = Foldunk.Stores.EventStore.GesEventStream(gateway, codec)
        FavoritesService(eventStream)

    let createLog () = createLogger ignore

    [<AutoData>]
    let ``Can roundtrip, correctly folding the events`` clientId command = Async.RunSynchronously <| async {
        let! conn = connectToLocalEventStoreNode ()
        let log, service = createLog (), createService conn

        do! service.Run log clientId command
        let! items = service.Load log clientId

        match command with
        | Favorites.Commands.Favorite (_,skuIds) ->
            test <@ skuIds |> List.forall (fun skuId -> items |> Array.exists (function { skuId = itemSkuId} -> itemSkuId = skuId)) @>
        | _ ->
            test <@ Array.isEmpty items@>
    }