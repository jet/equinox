module Foldunk.EventStore.Integration.EventStoreIntegration

open Swensen.Unquote

/// Needs an ES instance with default settings
/// TL;DR: At an elevated command prompt: choco install eventstore-oss; \ProgramData\chocolatey\bin\EventStore.ClusterNode.exe
let connectToLocalEventStoreNode () = async {
    let localhost = System.Net.IPEndPoint(System.Net.IPAddress.Loopback, 1113)
    let conn = EventStore.ClientAPI.EventStoreConnection.Create(localhost)
    do! conn.ConnectAsync() |> Async.AwaitTask
    return conn }

let private createGesGateway eventStoreConnection maxBatchSize =
    let connection = Foldunk.EventStore.GesConnection(eventStoreConnection)
    Foldunk.EventStore.GesGateway(connection, Foldunk.EventStore.GesStreamPolicy(maxBatchSize = maxBatchSize))

let createGesStream<'event, 'state> eventStoreConnection batchSize (codec : Foldunk.EventSum.IEventSumEncoder<'event,byte[]>) streamName : Foldunk.IStream<_,_> =
    let gateway = createGesGateway eventStoreConnection batchSize
    let streamState = Foldunk.EventStore.GesStreamState<'event, 'state>(gateway, codec)
    Foldunk.EventStore.GesStream<'event, 'state>(streamState, streamName) :> _

let createGesStreamWithCompactionEventTypeOption<'event, 'state> eventStoreConnection batchSize compactionEventTypeOption (codec : Foldunk.EventSum.IEventSumEncoder<'event,byte[]>) streamName
    : Foldunk.IStream<'event, 'state> =
    let gateway = createGesGateway eventStoreConnection batchSize
    let streamState = Foldunk.EventStore.GesStreamState<'event, 'state>(gateway, codec, ?compactionEventType = compactionEventTypeOption)
    Foldunk.EventStore.GesStream<'event, 'state>(streamState, streamName) :> _

let createCartServiceGesWithoutCompaction eventStoreConnection batchSize =
    Backend.Cart.Service(fun _ignoreCompactionEventTypeOption -> createGesStream eventStoreConnection batchSize)

let createCartServiceGesWithCompaction eventStoreConnection batchSize =
    Backend.Cart.Service(createGesStreamWithCompactionEventTypeOption eventStoreConnection batchSize)

let createCartServiceGes = createCartServiceGesWithCompaction

#nowarn "1182" // From hereon in, we may have some 'unused' privates (the tests)

type Tests() =
    let addAndThenRemoveItems exceptTheLastOne context cartId skuId log (service: Backend.Cart.Service) count =
        service.Flow log cartId <| fun _ctx execute ->
            for i in 1..count do
                execute <| Domain.Cart.AddItem (context, skuId, i)
                if not exceptTheLastOne || i <> count then
                    execute <| Domain.Cart.RemoveItem (context, skuId)
    let addAndThenRemoveItemsManyTimes context cartId skuId log service count =
        addAndThenRemoveItems false context cartId skuId log service count
    let addAndThenRemoveItemsManyTimesExceptTheLastOne context cartId skuId log service count =
        addAndThenRemoveItems true context cartId skuId log service count

    let createLoggerWithCapture () =
        let capture = LogCaptureBuffer()
        let subscribeLogListeners observable =
            capture.Subscribe observable |> ignore
        createLogger subscribeLogListeners, capture

    let singleSliceForward = "ReadStreamEventsForwardAsync"
    let singleBatchForward = [singleSliceForward; "LoadF"]
    let batchForwardAndAppend = singleBatchForward @ ["AppendToStreamAsync"]

    [<AutoData>]
    let ``Can roundtrip against EventStore, correctly batching the reads without compaction`` context cartId skuId = Async.RunSynchronously <| async {
        let log, capture = createLoggerWithCapture ()
        let! conn = connectToLocalEventStoreNode ()
        let batchSize = 3
        let service = createCartServiceGesWithoutCompaction conn batchSize

        // The command processing should trigger only a single read and a single write call
        let addRemoveCount = 6
        do! addAndThenRemoveItemsManyTimesExceptTheLastOne context cartId skuId log service addRemoveCount
        let expectedEventCount = 2 * addRemoveCount - 1
        test <@ batchForwardAndAppend = capture.ExternalCalls @>

        // Restart the counting
        capture.Clear()

        // Validate basic operation; Key side effect: Log entries will be emitted to `capture`
        let! state = service.Read log cartId
        test <@ 6 = match state with { items = [{ quantity = quantity }] } -> quantity | _ -> failwith "nope" @>

        // Need to read 4 batches to read 11 events in batches of 3
        let expectedBatches = ceil(float expectedEventCount/float batchSize) |> int
        test <@ List.replicate (expectedBatches-1) singleSliceForward @ singleBatchForward = capture.ExternalCalls @>
    }

    [<AutoData>]
    let ``Can roundtrip against EventStore, correctly compacting to avoid redundant reads`` context skuId cartId = Async.RunSynchronously <| async {
        let log, capture = createLoggerWithCapture ()
        let! conn = connectToLocalEventStoreNode ()
        let batchSize = 10
        let service = createCartServiceGesWithCompaction conn batchSize

        // Trigger 10 events, then reload
        do! addAndThenRemoveItemsManyTimes context cartId skuId log service 5
        let! _ = service.Read log cartId

        let singleBatch = ["ReadStreamEventsBackwardAsync"; "BatchBackward"]
        let batchAndAppend = singleBatch @ ["AppendToStreamAsync"]
        // ... should see a single read as we are inside the batch threshold
        test <@ batchAndAppend @ singleBatch = capture.ExternalCalls @>

        // Add two more, which should push it over the threshold and hence trigger inclusion of a snapshot event (but not incurr extra roundtrips)
        capture.Clear()
        do! addAndThenRemoveItemsManyTimes context cartId skuId log service 1
        test <@ batchAndAppend = capture.ExternalCalls @>

        // While we now have 13 events, we should be able to read them with a single call
        capture.Clear()
        let! _ = service.Read log cartId
        test <@ singleBatch = capture.ExternalCalls @>

        // Add 8 more; total of 21 should not trigger snapshotting as Event Number 12 (the 13th one) is a shapshot
        capture.Clear()
        do! addAndThenRemoveItemsManyTimes context cartId skuId log service 4
        test <@ batchAndAppend = capture.ExternalCalls @>

        // While we now have 21 events, we should be able to read them with a single call
        capture.Clear()
        let! _ = service.Read log cartId
        // ... and trigger a second snapshotting (inducing a single additional read + write)
        do! addAndThenRemoveItemsManyTimes context cartId skuId log service 1
        // and reload the 24 events with a single read
        let! _ = service.Read log cartId
        test <@ singleBatch @ batchAndAppend @ singleBatch = capture.ExternalCalls @>
    }