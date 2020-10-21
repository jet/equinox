module Samples.Store.Integration.LogIntegration

open Equinox.Core
open Equinox.CosmosStore.Integration
open FSharp.UMX
open Swensen.Unquote
open System
open System.Collections.Concurrent

module EquinoxEsInterop =
    open Equinox.EventStore
    [<NoEquality; NoComparison>]
    type FlatMetric = { action: string; stream : string; interval: StopwatchInterval; bytes: int; count: int; batches: int option } with
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
    open Equinox.CosmosStore.Core
    [<NoEquality; NoComparison>]
    type FlatMetric = { action: string; stream : string; interval: StopwatchInterval; bytes: int; count: int; responses: int option; ru: float } with
        override __.ToString() = sprintf "%s-Stream=%s %s-Elapsed=%O Ru=%O" __.action __.stream __.action __.interval.Elapsed __.ru
    let flatten (evt : Log.Event) : FlatMetric =
        let action, metric, batches, ru =
            match evt with
            | Log.Tip m -> "CosmosTip", m, None, m.ru
            | Log.TipNotFound m -> "CosmosTip404", m, None, m.ru
            | Log.TipNotModified m -> "CosmosTip302", m, None, m.ru
            | Log.Query (Direction.Forward,c,m) -> "CosmosQueryF", m, Some c, m.ru
            | Log.Query (Direction.Backward,c,m) -> "CosmosQueryB", m, Some c, m.ru
            | Log.Response (Direction.Forward,m) -> "CosmosResponseF", m, None, m.ru
            | Log.Response (Direction.Backward,m) -> "CosmosResponseB", m, None, m.ru
            | Log.SyncSuccess m -> "CosmosSync200", m, None, m.ru
            | Log.SyncConflict m -> "CosmosSync409", m, None, m.ru
            | Log.SyncResync m -> "CosmosSyncResync", m, None, m.ru
            | Log.PruneResponse m -> "CosmosPruneResponse", m, None, m.ru
            | Log.Delete m -> "CosmosDelete", m, None, m.ru
            | Log.Prune (events, m) -> "CosmosPrune", m, Some events, m.ru
        {   action = action; stream = metric.stream; bytes = metric.bytes; count = metric.count; responses = batches
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
            | KeyValue (k, SerilogScalar (:? Equinox.CosmosStore.Core.Log.Event as m)) -> Some <| Choice2Of3 (k,m)
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
    createLogger capture

#nowarn "1182" // From hereon in, we may have some 'unused' privates (the tests)

type Tests() =
    let act buffer (service : Backend.Cart.Service) itemCount context cartId skuId resultTag = async {
        do! CartIntegration.addAndThenRemoveItemsManyTimesExceptTheLastOne context cartId skuId service itemCount
        let! state = service.Read cartId
        test <@ itemCount = match state with { items = [{ quantity = quantity }] } -> quantity | _ -> failwith "nope" @>
        // Because we're using Access Strategies that enable us to read our state in a single roundtrip...
        // (even though we've gone over a page), we only need a single read to read the state (plus the one from the execute)
        let contains (s : string) (x : string) = x.Contains s
        test <@ let reads = buffer |> Seq.filter (contains resultTag)
                2 = Seq.length reads @> }

    // Protip: Debug this test to view standard metrics rendering
    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_EVENTSTORE")>]
    let ``Can roundtrip against EventStore, hooking, extracting and substituting metrics in the logging information`` context skuId = Async.RunSynchronously <| async {
        let batchSize = defaultBatchSize
        let buffer = ConcurrentQueue<string>()
        let log = createLoggerWithMetricsExtraction buffer.Enqueue
        let! conn = connectToLocalEventStoreNode log
        let gateway = createGesGateway conn batchSize
        let service = Backend.Cart.create log (CartIntegration.resolveGesStreamWithRollingSnapshots gateway)
        let itemCount = batchSize / 2 + 1
        let cartId = % Guid.NewGuid()
        do! act buffer service itemCount context cartId skuId "ReadStreamEventsBackwardAsync-Duration"
    }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``Can roundtrip against Cosmos, hooking, extracting and substituting metrics in the logging information`` cartContext skuId = Async.RunSynchronously <| async {
        let queryMaxItems = defaultQueryMaxItems
        let buffer = ConcurrentQueue<string>()
        let log = createLoggerWithMetricsExtraction buffer.Enqueue
        let context = createPrimaryContext log queryMaxItems
        let service = Backend.Cart.create log (CartIntegration.resolveCosmosStreamWithSnapshotStrategy context)
        let itemCount = queryMaxItems / 2 + 1
        let cartId = % Guid.NewGuid()
        do! act buffer service itemCount cartContext cartId skuId "EqxCosmos Tip " // one is a 404, one is a 200
    }
