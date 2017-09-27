module Samples.Store.Integration.LogIntegration

open Swensen.Unquote
open System

type SerilogTracerAdapter(emit) =
    let formatter = Serilog.Formatting.Display.MessageTemplateTextFormatter("{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level}] {Message}{NewLine}{Exception}", null)
    let render logEvent =
        use writer = new System.IO.StringWriter()
        formatter.Format(logEvent, writer)
        string writer
    let handleLogEvent (logEvent : Serilog.Events.LogEvent) =
        try match logEvent with
            | SerilogProperty Foldunk.EventStore.Metrics.ExternalTag (Some (SerilogScalar (:? Foldunk.EventStore.Metrics.Metric as metric))) ->
                logEvent.RemovePropertyIfPresent Foldunk.EventStore.Metrics.ExternalTag
                render logEvent |> sprintf "%s-Elapsed=%O %s" metric.action metric.interval.Elapsed |> emit
            | _ -> render logEvent |> emit
        with _ -> render logEvent |> sprintf "Cannnot log event: %s" |> emit
    member __.Subscribe(source: IObservable<Serilog.Events.LogEvent>) =
        source.Subscribe handleLogEvent

let batchSize = 500
let createCartServiceWithEventStore eventStoreConnection =
    let gateway = createGesGateway eventStoreConnection batchSize
    Backend.Cart.Service(createGesStream gateway)

let createLoggerWithCapture emit =
    let capture = SerilogTracerAdapter emit
    let subscribeLogListeners obs =
        obs |> capture.Subscribe |> ignore
    createLogger subscribeLogListeners, capture

#nowarn "1182" // From hereon in, we may have some 'unused' privates (the tests)

type Tests() =
    [<AutoData>]
    let ``Can roundtrip against EventStore, with control over logging, correctly batching the reads and folding the events`` context cartId skuId = Async.RunSynchronously <| async {
        let! conn = connectToLocalEventStoreNode ()
        let buffer = ResizeArray<string>()
        let emit msg = System.Diagnostics.Trace.WriteLine msg; buffer.Add msg
        let (log,capture), service = createLoggerWithCapture emit, createCartServiceWithEventStore conn

        let itemCount = batchSize / 2 + 1
        do! CartIntegration.addAndThenRemoveItemsManyTimesExceptTheLastOne context cartId skuId log service itemCount

        let! state = service.Load log cartId
        test <@ itemCount = match state with { items = [{ quantity = quantity }] } -> quantity | _ -> failwith "nope" @>

        // Because we've gone over a page, we need two reads to load the state, making a total of three
        let contains (s : string) (x : string) = x.IndexOf s <> -1
        test <@ let reads = buffer |> Seq.filter (fun s -> s |> contains "ReadStreamEventsForwardAsync-Elapsed")
                3 <= Seq.length reads
                && not (obj.ReferenceEquals(capture, null)) @>
    }