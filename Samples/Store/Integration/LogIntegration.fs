module Samples.Store.Integration.LogIntegration

open Swensen.Unquote
open System

type SerilogMetricsExtractor(emit : string -> unit) =
    let formatter = Serilog.Formatting.Display.MessageTemplateTextFormatter("{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level}] {Message}{NewLine}{Exception}", null)
    let emitEvent (logEvent : Serilog.Events.LogEvent) =
        use writer = new System.IO.StringWriter()
        formatter.Format(logEvent, writer)
        writer |> string |> System.Diagnostics.Trace.WriteLine
        logEvent.RenderMessage() |> emit
    let (|MetricProperty|_|) (logEvent : Serilog.Events.LogEvent) : (string * Foldunk.EventStore.Metrics.Metric) option =
        logEvent.Properties |> Seq.tryPick (function KeyValue (k, SerilogScalar (:? Foldunk.EventStore.Metrics.Metric as m)) -> Some (k,m) | _ -> None)
    let handleLogEvent = function
        | MetricProperty (name, metric) as logEvent ->
            let renderedMetrics = sprintf "%s-Duration=%O" metric.action metric.interval.Elapsed |> Serilog.Events.ScalarValue
            // Serilog provides lots of ways of configuring custom rendering -  solely tweaking the rendering is doable using the configuration syntax
            // (the goal here is to illustrate how a given value can be extracted (as required in some cases) and/or stubbed yet retain the rest of the message)
            // Other example approaches:
            // 1. logEvent.RemovePropertyIfExists name
            // 2. let rendered = logEvent.RenderMessage() in rendered.Replace("{metric} ","")
            logEvent.AddOrUpdateProperty(Serilog.Events.LogEventProperty(name, renderedMetrics))
            emitEvent logEvent
        | logEvent -> emitEvent logEvent
    member __.Subscribe(source: IObservable<Serilog.Events.LogEvent>) =
        source.Subscribe handleLogEvent

let createLoggerWithMetricsExtraction emit =
    let capture = SerilogMetricsExtractor emit
    let subscribeLogListeners observable =
        capture.Subscribe observable |> ignore
    createLogger subscribeLogListeners, capture

#nowarn "1182" // From hereon in, we may have some 'unused' privates (the tests)

type Tests() =
    // Protip: Debug this test to view standard metrics rendering
    [<AutoData>]
    let ``Can roundtrip against EventStore, hooking, extracting and substituting metrics in the logging information`` context cartId skuId = Async.RunSynchronously <| async {
        let! conn = connectToLocalEventStoreNode ()
        let buffer = ResizeArray<string>()
        let batchSize = defaultBatchSize
        let (log,capture), service = createLoggerWithMetricsExtraction buffer.Add, CartIntegration.createServiceGes conn batchSize

        let itemCount, cartId = batchSize / 2 + 1, cartId ()
        do! CartIntegration.addAndThenRemoveItemsManyTimesExceptTheLastOne context cartId skuId log service itemCount

        let! state = service.Read log cartId
        test <@ itemCount = match state with { items = [{ quantity = quantity }] } -> quantity | _ -> failwith "nope" @>

        // Because we've gone over a page, we need two reads to load the state, making a total of three
        let contains (s : string) (x : string) = x.IndexOf s <> -1
        test <@ let reads = buffer |> Seq.filter (fun s -> s |> contains "ReadStreamEventsForwardAsync-Duration")
                3 = Seq.length reads
                && not (obj.ReferenceEquals(capture, null)) @>
    }