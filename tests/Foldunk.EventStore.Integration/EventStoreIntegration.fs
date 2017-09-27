module Foldunk.EventStore.Integration.EventStoreIntegration

open Swensen.Unquote

#nowarn "1182" // From hereon in, we may have some 'unused' privates (the tests)

/// Needs an ES instance with default settings
/// TL;DR: At an elevated command prompt: choco install eventstore-oss; \ProgramData\chocolatey\bin\EventStore.ClusterNode.exe
let connectToLocalEventStoreNode () = async {
    let localhost = System.Net.IPEndPoint(System.Net.IPAddress.Loopback, 1113)
    let conn = EventStore.ClientAPI.EventStoreConnection.Create(localhost)
    do! conn.ConnectAsync() |> Async.AwaitTask
    return conn }
let createGesGateway  maxBatchSize eventStoreConnection =
    let connection = Foldunk.EventStore.GesConnection(eventStoreConnection)
    Foldunk.EventStore.GesGateway(connection, Foldunk.EventStore.GesStreamPolicy(maxBatchSize = maxBatchSize))
let createStream eventStoreConnection maxBatchSize codec streamName: Foldunk.IStream<_,_> =
    let gateway = createGesGateway eventStoreConnection maxBatchSize
    let store = Foldunk.EventStore.GesStreamStore(gateway, codec)
    Foldunk.EventStore.GesStream(store, streamName) :> _
let createCartServiceEx batchSize eventStoreConnection = Backend.Cart.Service(createStream eventStoreConnection batchSize)
let createCartServiceGesWithBatchSize batchSize eventStoreConnection = createCartServiceEx eventStoreConnection batchSize
let createCartServiceGes eventStoreConnection = createCartServiceGesWithBatchSize 500 eventStoreConnection

type Tests() =
    let addAndThenRemoveItems exceptTheLastOne context cartId skuId log (service: Backend.Cart.Service) count =
        let decide (ctx : Foldunk.DecisionContext<_,_>) = async {
            let run cmd = ctx.Execute(Domain.Cart.Commands.interpret cmd)
            for i in 1..count do
                run <| Domain.Cart.Commands.AddItem (context, skuId, i)
                if not exceptTheLastOne || i <> count then
                    run <| Domain.Cart.Commands.RemoveItem (context, skuId)
            return ctx.Complete() }
        service.Decide log cartId decide

    let addAndThenRemoveItemsManyTimes context cartId skuId log service count =
        addAndThenRemoveItems false context cartId skuId log service count
    let addAndThenRemoveItemsManyTimesExceptTheLastOne context cartId skuId log service count =
        addAndThenRemoveItems true context cartId skuId log service count

    let createLoggerWithCapture () =
        let capture = LogCaptureBuffer()
        let subscribeLogListeners obs =
            obs |> capture.Subscribe |> ignore
        createLogger subscribeLogListeners, capture

    [<AutoData()>]
    let ``Can roundtrip against EventStore, correctly batching the reads without compaction`` context cartId skuId = Async.RunSynchronously <| async {
        let log, capture = createLoggerWithCapture ()
        let! conn = connectToLocalEventStoreNode ()
        let batchSize = 3
        let service = createCartServiceGesWithBatchSize batchSize conn

        // The command processing should trigger only a single read and a single write call
        let addRemoveCount = 6
        do! addAndThenRemoveItemsManyTimesExceptTheLastOne context cartId skuId log service addRemoveCount
        let expectedEventCount = 2 * addRemoveCount - 1
        test <@ [ "ReadStreamEventsForwardAsync"; "AppendToStreamAsync" ] = capture.ExternalCalls @>

        // Restart the counting
        capture.Clear()

        // Validate basic operation; Key side effect: Log entries will be emitted to `capture`
        let! state = service.Load log cartId
        test <@ 6 = match state with { items = [{ quantity = quantity }] } -> quantity | _ -> failwith "nope" @>

        // Need to read 4 batches to read 11 events in batches of 3
        let expectedBatches = ceil(float expectedEventCount/float batchSize) |> int
        test <@ List.replicate expectedBatches "ReadStreamEventsForwardAsync" = capture.ExternalCalls @>
    }