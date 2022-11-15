[<AutoOpen>]
module internal Equinox.MessageDb.Tracing


open System.Diagnostics

let source = new ActivitySource("Equinox.MessageDb")

let addRetryAttempt (attempt: int) (act: Activity) = if act <> null then act.AddTag("eqx.op_attempt", attempt) |> ignore

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
    static member AddBatch(act: Activity, size: int64, index: int) = act.AddBatchSize(size).AddTag("eqx.batch_index", index)

    [<System.Runtime.CompilerServices.Extension>]
    static member AddBatches(act: Activity, batches: int, count: int) = act.AddTag("eqx.batches", batches).AddTag("eqx.event_count", count)

    [<System.Runtime.CompilerServices.Extension>]
    static member AddStartPosition(act: Activity, pos: int64) = act.AddTag("eqx.start_position", pos)

    [<System.Runtime.CompilerServices.Extension>]
    static member AddLeader(act: Activity, requiresLeader) = if requiresLeader then act.AddTag("eqx.requires_leader", true) else act

    [<System.Runtime.CompilerServices.Extension>]
    static member RecordConflict(act: Activity) = act.AddTag("eqx.conflict", true).AddEvent(ActivityEvent("WrongExpectedVersion"))



