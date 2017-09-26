module Example.Integration.LogIntegration

open Domain
open Backend
open Swensen.Unquote
open System
open System.Diagnostics

#nowarn "1182" // From hereon in, we may have some 'unused' privates (the tests)

let batchSize = 500
let createCartServiceWithEventStore eventStoreConnection = Carts.Service(createGesStreamer eventStoreConnection batchSize)

let createLoggerWithCapture emit =
    let capture = SerilogTracerAdapter emit
    let subscribeLogListeners obs =
        obs |> capture.Subscribe |> ignore
    createLogger subscribeLogListeners, capture

type Tests(testOutputHelper) =
    let testOutput = TestOutputAdapter testOutputHelper
    let createLog () = createLogger (testOutput.Subscribe >> ignore)

    [<AutoData>]
    let ``Can roundtrip against EventStore, correctly batching the reads and folding the events`` context cartId skuId = Async.RunSynchronously <| async {
        let! conn = connectToLocalEventStoreNode ()
        let buffer = ResizeArray<string>()
        let emit msg = System.Diagnostics.Trace.WriteLine msg; buffer.Add msg
        let (log,_), service = createLoggerWithCapture emit, createCartServiceWithEventStore conn

        let itemCount = batchSize / 2 + 1
        do! CartIntegration.addAndThenRemoveItemsManyTimesExceptTheLastOne context cartId skuId log service itemCount

        let! state = service.Load log cartId
        test <@ itemCount = match state with { items = [{ quantity = quantity }] } -> quantity | _ -> failwith "nope" @>

        // Because we've gone over a page, we need two reads to load the state, making a total of three
        let contains (s : string) (x : string) = x.IndexOf s <> -1
        test <@ let reads = buffer |> Seq.filter (fun s -> s |> contains "ReadStreamEventsForwardAsync-Elapsed")
                3 = Seq.length reads @>
    }