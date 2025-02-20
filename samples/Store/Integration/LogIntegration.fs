﻿module Samples.Store.Integration.LogIntegration

open Domain
open Equinox.Core
open Equinox.CosmosStore.Integration.CosmosFixtures
open Swensen.Unquote
open System.Collections.Concurrent

module EquinoxEsInterop =
    open Equinox.EventStoreDb
    [<NoEquality; NoComparison>]
    type FlatMetric = { action: string; stream: string; interval: StopwatchInterval; bytes: int; count: int; batches: int option } with
        override x.ToString() = $"%s{x.action}-Stream=%s{x.stream} %s{x.action}-Elapsed={x.interval.Elapsed}"
    let flatten (evt: Log.Metric) : FlatMetric =
        let action, metric, batches =
            match evt with
            | Log.WriteSuccess m -> "AppendToStreamAsync", m, None
            | Log.WriteConflict m -> "AppendToStreamAsync", m, None
// For the gRPC edition, no slice information is available
//          | Log.Slice (Direction.Forward,m) -> "ReadStreamEventsForwardAsync", m, None
//          | Log.Slice (Direction.Backward,m) -> "ReadStreamEventsBackwardAsync", m, None
            | Log.Batch (Direction.Forward,c,m) -> "ReadStreamAsyncF", m, Some c
            | Log.Batch (Direction.Backward,c,m) -> "ReadStreamAsyncB", m, Some c
        { action = action; stream = metric.stream; interval = metric.interval; bytes = metric.bytes; count = metric.count; batches = batches }

module EquinoxCosmosInterop =
    open Equinox.CosmosStore.Core
    [<NoEquality; NoComparison>]
    type FlatMetric = { action: string; stream: string; interval: StopwatchInterval; bytes: int; count: int; responses: int option; ru: float } with
        override x.ToString() = $"%s{x.action}-Stream=%s{x.stream} %s{x.action}-Elapsed={x.interval.Elapsed} Ru={x.ru}"
    let flatten (evt: Log.Metric) : FlatMetric =
        let action, metric, batches, ru =
            match evt with
            | Log.Metric.Tip m -> "CosmosTip", m, None, m.ru
            | Log.Metric.TipNotFound m -> "CosmosTip404", m, None, m.ru
            | Log.Metric.TipNotModified m -> "CosmosTip302", m, None, m.ru
            | Log.Metric.Query (Direction.Forward,c,m) -> "CosmosQueryF", m, Some c, m.ru
            | Log.Metric.Query (Direction.Backward,c,m) -> "CosmosQueryB", m, Some c, m.ru
            | Log.Metric.QueryResponse (Direction.Forward,m) -> "CosmosResponseF", m, None, m.ru
            | Log.Metric.QueryResponse (Direction.Backward,m) -> "CosmosResponseB", m, None, m.ru
            | Log.Metric.SyncSuccess m -> "CosmosSync200", m, None, m.ru
            | Log.Metric.SyncConflict m -> "CosmosSync409", m, None, m.ru
            | Log.Metric.SyncResync m -> "CosmosSyncResync", m, None, m.ru
            | Log.Metric.Prune (events, m) -> "CosmosPrune", m, Some events, m.ru
            | Log.Metric.PruneResponse m -> "CosmosPruneResponse", m, None, m.ru
            | Log.Metric.Delete m -> "CosmosDelete", m, None, m.ru
            | Log.Metric.Trim m -> "CosmosTrim", m, None, m.ru
        {   action = action; stream = metric.stream; bytes = metric.bytes; count = metric.count; responses = batches
            interval = metric.interval; ru = ru }

type SerilogMetricsExtractor(emit: string -> unit) =
    let renderSummary = TestOutputRenderer.render "{Message} {Properties}"
    let emitEvent (logEvent: Serilog.Events.LogEvent) =
        logEvent |> TestOutputRenderer.full |> System.Diagnostics.Trace.Write
        logEvent |> renderSummary |> emit
    let (|SerilogScalar|_|) : Serilog.Events.LogEventPropertyValue -> obj option = function
        | :? Serilog.Events.ScalarValue as x -> Some x.Value
        | _ -> None
    let (|EsMetric|CosmosMetric|GenericMessage|) (logEvent: Serilog.Events.LogEvent) =
        logEvent.Properties
        |> Seq.tryPick (function
            | KeyValue (k, SerilogScalar (:? Equinox.EventStoreDb.Log.Metric as m)) -> Some <| Choice1Of3 (k,m)
            | KeyValue (k, SerilogScalar (:? Equinox.CosmosStore.Core.Log.Metric as m)) -> Some <| Choice2Of3 (k,m)
            | _ -> None)
        |> Option.defaultValue (Choice3Of3 ())
    let handleLogEvent logEvent =
        match logEvent with
        | EsMetric (name, evt) as logEvent ->
            let flat = EquinoxEsInterop.flatten evt
            let renderedMetrics = $"%s{flat.action}-Duration={flat.interval.Elapsed}" |> Serilog.Events.ScalarValue
            // Serilog provides lots of ways of configuring custom rendering -  solely tweaking the rendering is doable using the configuration syntax
            // (the goal here is to illustrate how a given value can be extracted (as required in some cases) and/or stubbed yet retain the rest of the message)
            // Other example approaches:
            // 1. logEvent.RemovePropertyIfExists name
            // 2. let rendered = logEvent.RenderMessage() in rendered.Replace("{esEvt} ","")
            logEvent.AddOrUpdateProperty(Serilog.Events.LogEventProperty(name, renderedMetrics))
            emitEvent logEvent
        | CosmosMetric (name, evt) as logEvent ->
            let flat = EquinoxCosmosInterop.flatten evt
            let renderedMetrics = $"%s{flat.action}-Duration={flat.interval.Elapsed}" |> Serilog.Events.ScalarValue
            logEvent.AddOrUpdateProperty(Serilog.Events.LogEventProperty(name, renderedMetrics))
            emitEvent logEvent
        | GenericMessage () as logEvent ->
            emitEvent logEvent
    interface Serilog.Core.ILogEventSink with member _.Emit logEvent = handleLogEvent logEvent

type Tests(testOutputHelper) =
    let output = TestOutput testOutputHelper
    let createLoggerWithMetricsExtraction emit =
        let capture = SerilogMetricsExtractor emit
        output.CreateLogger capture
    let act buffer (service : Cart.Service) itemCount context cartId skuId resultTag = async {
        do! CartIntegration.addAndThenRemoveItemsManyTimesExceptTheLastOne context cartId skuId service itemCount
        let! state = service.Read cartId
        test <@ itemCount = match state with { items = [{ quantity = quantity }] } -> quantity | _ -> failwith "nope" @>
        // Because we're using Access Strategies that enable us to read our state in a single roundtrip...
        // (even though we've gone over a page), we only need a single read to read the state (plus the one from the execute)
        let contains (s: string) (x: string) = x.Contains s
        test <@ let reads = buffer |> Seq.filter (contains resultTag)
                2 = Seq.length reads @> }

    // Protip: Debug this test to view standard metrics rendering
    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_EVENTSTORE")>]
    let ``Can roundtrip against EventStore, hooking, extracting and substituting metrics in the logging information`` (ctx, skuId) = async {
        let batchSize = defaultBatchSize
        let buffer = ConcurrentQueue<string>()
        let log = createLoggerWithMetricsExtraction buffer.Enqueue
        let client = connectToLocalEventStoreNode log
        let context = createContext client batchSize
        let service = Cart.create (CartIntegration.categoryGesStreamWithRollingSnapshots context |> Equinox.Decider.forStream log)
        let itemCount = batchSize / 2 + 1
        let cartId = CartId.gen ()
        do! act buffer service itemCount ctx cartId skuId "ReadStreamAsyncB-Duration" }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``Can roundtrip against Cosmos, hooking, extracting and substituting metrics in the logging information`` (ctx, skuId) = async {
        let queryMaxItems = defaultQueryMaxItems
        let buffer = ConcurrentQueue<string>()
        let log = createLoggerWithMetricsExtraction buffer.Enqueue
        let context = createPrimaryContext log queryMaxItems
        let service = Cart.create (CartIntegration.categoryCosmosStreamWithSnapshotStrategy context |> Equinox.Decider.forStream log)
        let itemCount = queryMaxItems / 2 + 1
        let cartId = CartId.gen ()
        do! act buffer service itemCount ctx cartId skuId "EqxCosmos Tip " } // one is a 404, one is a 200
