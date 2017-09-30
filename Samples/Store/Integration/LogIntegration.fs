module Samples.Store.Integration.LogIntegration

open Swensen.Unquote
open System

type SerilogTracerAdapter(emit : string -> unit) =
    let formatter = Serilog.Formatting.Display.MessageTemplateTextFormatter("{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level}] {Message}{NewLine}{Exception}", null)
    let emitEvent (logEvent : Serilog.Events.LogEvent) =
        use writer = new System.IO.StringWriter()
        formatter.Format(logEvent, writer)
        writer |> string |> System.Diagnostics.Trace.WriteLine
        logEvent.RenderMessage() |> emit
    let handleLogEvent = function
        | HasLogEventProperty Foldunk.EventStore.Metrics.ExternalTag (Some (SerilogScalar (:? Foldunk.EventStore.Metrics.Metric as metric))) as logEvent ->
            let renderedMetrics = sprintf "%s-Elapsed=%O" metric.action metric.interval.Elapsed |> Serilog.Events.ScalarValue
            // Serilog provides lots of ways of configuring custom rendering -  solely tweaking the rendering is doable using the configuration syntax
            // (the goal here is to illustrate how a given value can be extracted (as required in some cases) and/or stubbed yet retain the rest of the message)
            logEvent.AddOrUpdateProperty(Serilog.Events.LogEventProperty(Foldunk.EventStore.Metrics.ExternalTag, renderedMetrics))
            emitEvent logEvent
        | logEvent -> emitEvent logEvent
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
    let ``Can roundtrip against EventStore, hooking, extracting and substuting metrics in the logging information`` context cartId skuId = Async.RunSynchronously <| async {
        let! conn = connectToLocalEventStoreNode ()
        let buffer = ResizeArray<string>()
        let (log,capture), service = createLoggerWithCapture buffer.Add, createCartServiceWithEventStore conn

        let itemCount = batchSize / 2 + 1
        do! CartIntegration.addAndThenRemoveItemsManyTimesExceptTheLastOne context cartId skuId log service itemCount

        let! state = service.Load log cartId
        test <@ itemCount = match state with { items = [{ quantity = quantity }] } -> quantity | _ -> failwith "nope" @>

        // Because we've gone over a page, we need two reads to load the state, making a total of three
        let contains (s : string) (x : string) = x.IndexOf s <> -1
        test <@ let reads = buffer |> Seq.filter (fun s -> s |> contains "ReadStreamEventsForwxardAsync-Elapsed")
                3 <= Seq.length reads
                && not (obj.ReferenceEquals(capture, null)) @>
    }