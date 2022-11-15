[<AutoOpen>]
module internal Equinox.Core.Internal

open System.Diagnostics

module Log =

    // Sidestep Log.ForContext converting to a string; see https://github.com/serilog/serilog/issues/1124
    let withScalarProperty (key: string) (value : 'T) (log : Serilog.ILogger) =
        let enrich (e : Serilog.Events.LogEvent) =
            e.AddPropertyIfAbsent(Serilog.Events.LogEventProperty(key, Serilog.Events.ScalarValue(value)))
        log.ForContext({ new Serilog.Core.ILogEventEnricher with member _.Enrich(evt,_) = enrich evt })
    let [<return: Struct>] (|ScalarValue|_|) : Serilog.Events.LogEventPropertyValue -> obj voption = function
        | :? Serilog.Events.ScalarValue as x -> ValueSome x.Value
        | _ -> ValueNone


[<System.Runtime.CompilerServices.Extension>]
type ActivityExtensions =

    [<System.Runtime.CompilerServices.Extension>]
    static member AddStreamName(act: Activity, streamName: string) = act.AddTag("eqx.stream_name", streamName)

    [<System.Runtime.CompilerServices.Extension>]
    static member AddExpectedVersion(act: Activity, version: int64) = act.AddTag("eqx.expected_version", version)

    [<System.Runtime.CompilerServices.Extension>]
    static member AddVersion(act: Activity, version: int64) = act.AddTag("eqx.last_version", version)

    [<System.Runtime.CompilerServices.Extension>]
    static member AddMetric(act: Activity, count: int, bytes: int) = act.AddTag("eqx.count", count).AddTag("eqx.bytes", bytes)

    [<System.Runtime.CompilerServices.Extension>]
    static member AddBatchSize(act: Activity, size: int64) = act.AddTag("eqx.batch_size", size)

    [<System.Runtime.CompilerServices.Extension>]
    static member AddBatchInformation(act: Activity, size: int64, index: int) = act.AddBatchSize(size).AddTag("eqx.batch_index", index)

    [<System.Runtime.CompilerServices.Extension>]
    static member AddStartPosition(act: Activity, pos: int64) = act.AddTag("eqx.start_position", pos)

    [<System.Runtime.CompilerServices.Extension>]
    static member AddLeader(act: Activity, requiresLeader) = if requiresLeader then act.AddTag("eqx.requires_leader", true) else act

    [<System.Runtime.CompilerServices.Extension>]
    static member RecordConflict(act: Activity) = act.AddTag("eqx.conflict", true).AddEvent(ActivityEvent("WrongExpectedVersion"))

