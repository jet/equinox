namespace global

open System
open System.Diagnostics

/// Maps completed store-specific Activities to equivalent Serilog log writes,
/// providing a migration path for consumers with existing Serilog-based metric pipelines.
/// Usage: Instantiate with a Serilog.ILogger, and all store Activities will be logged with
/// structured properties matching the original Serilog-based Log.Metric output.
type OtelToSerilogBridge(log: Serilog.ILogger) =
    let listener = new ActivityListener(
        ShouldListenTo = (fun source -> source.Name.StartsWith("Equinox.", StringComparison.Ordinal)),
        Sample = (fun _ -> ActivitySamplingResult.AllDataAndRecorded),
        ActivityStopped = (fun act ->
            let tag name = act.GetTagItem(name)
            let tagStrArr name = match tag name :?> string[] with null -> null | s -> s
            let tagInt name = match tag name with :? int as i -> i | _ -> 0
            let count = tagInt "eqx.count"
            let bytes = tagInt "eqx.bytes"
            let op = act.OperationName
            match op with
            | "Append" ->
                log.Information("Otel {Source} {Action:l} count={Count} bytes={Bytes}",
                    act.Source.Name, op, count, bytes)
            | "AppendConflict" ->
                let eventTypes = tagStrArr "eqx.event_types"
                log.Information("Otel {Source} {Action:l} conflict eventTypes={EventTypes}",
                    act.Source.Name, op, eventTypes)
            | "SliceForward" | "SliceBackward" ->
                log.Information("Otel {Source} {Action:l} count={Count} bytes={Bytes}",
                    act.Source.Name, op, count, bytes)
            | "BatchForward" | "BatchBackward" ->
                let batches = tagInt "eqx.batches"
                log.Information("Otel {Source} {Action:l} count={Count} bytes={Bytes} batches={Batches}",
                    act.Source.Name, op, count, bytes, batches)
            | "ReadLast" ->
                log.Information("Otel {Source} {Action:l} count={Count} bytes={Bytes}",
                    act.Source.Name, op, count, bytes)
            | _ -> ()))
    do ActivitySource.AddActivityListener(listener)
    interface IDisposable with member _.Dispose() = listener.Dispose()
