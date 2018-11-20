﻿module Samples.Store.Integration.LogIntegration

open Equinox.Store
open Equinox.Cosmos.Integration
open Swensen.Unquote

module EquinoxEsInterop =
    open Equinox.EventStore
    [<NoEquality; NoComparison>]
    type FlatMetric = { action: string; stream: string; interval: StopwatchInterval; bytes: int; count: int; batches: int option } with
        override __.ToString() = sprintf "%s-Stream=%s %s-Elapsed=%O" __.action __.stream __.action __.interval.Elapsed
    let flatten (evt : Log.Event) : FlatMetric =
        let action, metric, batches =
            match evt with
            | Log.WriteSuccess m -> "AppendToStreamAsync", m, None
            | Log.WriteConflict m -> "AppendToStreamAsync", m, None
            | Log.Slice (Direction.Forward,m) -> "ReadStreamEventsForwardAsync", m, None
            | Log.Slice (Direction.Backward,m) -> "ReadStreamEventsBackwardAsync", m, None
            | Log.Batch (Direction.Forward,c,m) -> "LoadF", m, Some c
            | Log.Batch (Direction.Backward,c,m) -> "LoadB", m, Some c
        { action = action; stream = metric.stream; interval = metric.interval; bytes = metric.bytes; count = metric.count; batches = batches }
module EquinoxCosmosInterop =
    open Equinox.Cosmos
    [<NoEquality; NoComparison>]
    type FlatMetric = { action: string; stream: string; interval: StopwatchInterval; bytes: int; count: int; batches: int option; ru: float } with
        override __.ToString() = sprintf "%s-Stream=%s %s-Elapsed=%O Ru=%O" __.action __.stream __.action __.interval.Elapsed __.ru
    let flatten (evt : Log.Event) : FlatMetric =
        let action, metric, batches, ru =
            match evt with
            | Log.WriteSuccess m -> "EqxAppendToStreamAsync", m, None, m.ru
            | Log.WriteConflict m -> "EqxAppendToStreamAsync", m, None, m.ru
            | Log.Slice (Direction.Forward,m) -> "EqxReadStreamEventsForwardAsync", m, None, m.ru
            | Log.Slice (Direction.Backward,m) -> "EqxReadStreamEventsBackwardAsync", m, None, m.ru
            | Log.Batch (Direction.Forward,c,m) -> "EqxLoadF", m, Some c, m.ru
            | Log.Batch (Direction.Backward,c,m) -> "EqxLoadB", m, Some c, m.ru
        {   action = action; stream = metric.stream; bytes = metric.bytes; count = metric.count; batches = batches
            interval = StopwatchInterval(metric.interval.StartTicks,metric.interval.EndTicks); ru = ru }

type SerilogMetricsExtractor(emit : string -> unit) =
    let render template =
        let formatter = Serilog.Formatting.Display.MessageTemplateTextFormatter(template, null)
        fun logEvent ->
            use writer = new System.IO.StringWriter()
            formatter.Format(logEvent, writer)
            writer |> string
    let renderFull = render "{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level}] {Message} {Properties} {NewLine}{Exception}"
    let renderSummary = render "{Message} {Properties}"
    let emitEvent (logEvent : Serilog.Events.LogEvent) =
        logEvent |> renderFull |> System.Diagnostics.Trace.Write
        logEvent |> renderSummary |> emit
    let (|SerilogScalar|_|) : Serilog.Events.LogEventPropertyValue -> obj option = function
        | (:? Serilog.Events.ScalarValue as x) -> Some x.Value
        | _ -> None
    let (|EsMetric|CosmosMetric|GenericMessage|) (logEvent : Serilog.Events.LogEvent) =
        logEvent.Properties
        |> Seq.tryPick (function
            | KeyValue (k, SerilogScalar (:? Equinox.EventStore.Log.Event as m)) -> Some <| Choice1Of3 (k,m)
            | KeyValue (k, SerilogScalar (:? Equinox.Cosmos.Log.Event as m)) -> Some <| Choice2Of3 (k,m)
            | _ -> None)
        |> Option.defaultValue (Choice3Of3 ())
    let handleLogEvent logEvent =
        match logEvent with
        | EsMetric (name, evt) as logEvent ->
            let flat = EquinoxEsInterop.flatten evt
            let renderedMetrics = sprintf "%s-Duration=%O" flat.action flat.interval.Elapsed |> Serilog.Events.ScalarValue
            // Serilog provides lots of ways of configuring custom rendering -  solely tweaking the rendering is doable using the configuration syntax
            // (the goal here is to illustrate how a given value can be extracted (as required in some cases) and/or stubbed yet retain the rest of the message)
            // Other example approaches:
            // 1. logEvent.RemovePropertyIfExists name
            // 2. let rendered = logEvent.RenderMessage() in rendered.Replace("{esEvt} ","")
            logEvent.AddOrUpdateProperty(Serilog.Events.LogEventProperty(name, renderedMetrics))
            emitEvent logEvent
        | CosmosMetric (name, evt) as logEvent ->
            let flat = EquinoxCosmosInterop.flatten evt
            let renderedMetrics = sprintf "%s-Duration=%O" flat.action flat.interval.Elapsed |> Serilog.Events.ScalarValue
            logEvent.AddOrUpdateProperty(Serilog.Events.LogEventProperty(name, renderedMetrics))
            emitEvent logEvent
        | GenericMessage () as logEvent ->
            emitEvent logEvent
    interface Serilog.Core.ILogEventSink with member __.Emit logEvent = handleLogEvent logEvent

let createLoggerWithMetricsExtraction emit =
    let capture = SerilogMetricsExtractor emit
    createLogger capture, capture

#nowarn "1182" // From hereon in, we may have some 'unused' privates (the tests)

type Tests() =
    let act buffer capture (service : Backend.Cart.Service) itemCount context cartId skuId resultTag = async {
        do! CartIntegration.addAndThenRemoveItemsManyTimesExceptTheLastOne context cartId skuId service itemCount
        let! state = service.Read cartId
        test <@ itemCount = match state with { items = [{ quantity = quantity }] } -> quantity | _ -> failwith "nope" @>

        // Even though we've gone over a page, we only need a single read to read the state (plus the one from the execute)
        let contains (s : string) (x : string) = x.IndexOf s <> -1
        test <@ let reads = buffer |> Seq.filter (fun s -> s |> contains resultTag)
                2 = Seq.length reads
                && not (obj.ReferenceEquals(capture, null)) @> }

    // Protip: Debug this test to view standard metrics rendering
    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_EVENTSTORE")>]
    let ``Can roundtrip against EventStore, hooking, extracting and substituting metrics in the logging information`` context cartId skuId = Async.RunSynchronously <| async {
        let buffer = ResizeArray<string>()
        let batchSize = defaultBatchSize
        let (log,capture) = createLoggerWithMetricsExtraction buffer.Add
        let! conn = connectToLocalEventStoreNode log
        let gateway = createGesGateway conn batchSize
        let service = Backend.Cart.Service(log, CartIntegration.resolveGesStreamWithCompactionEventType gateway)
        let itemCount, cartId = batchSize / 2 + 1, cartId ()
        do! act buffer capture service itemCount context cartId skuId "ReadStreamEventsBackwardAsync-Duration"
    }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``Can roundtrip against Cosmos, hooking, extracting and substituting metrics in the logging information`` context cartId skuId = Async.RunSynchronously <| async {
        let buffer = ResizeArray<string>()
        let batchSize = defaultBatchSize
        let (log,capture) = createLoggerWithMetricsExtraction buffer.Add
        let! conn = connectToSpecifiedCosmosOrSimulator log
        let gateway = createEqxGateway conn batchSize
        let service = Backend.Cart.Service(log, CartIntegration.resolveEqxStreamWithCompactionEventType gateway)
        let itemCount, cartId = batchSize / 2 + 1, cartId ()
        do! act buffer capture service itemCount context cartId skuId "ReadStreamEventsBackwardAsync-Duration"
    }