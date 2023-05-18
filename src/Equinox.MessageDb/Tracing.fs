[<AutoOpen>]
module internal Equinox.MessageDb.Tracing

open Equinox.Core.Tracing
open System.Diagnostics
open Equinox.MessageDb.Core

let source = new ActivitySource("Equinox.MessageDb")

[<System.Runtime.CompilerServices.Extension>]
type ActivityExtensions =

    [<System.Runtime.CompilerServices.Extension>]
    static member AddStreamFromParent(act: Activity, parent: Activity) =
        if parent <> null then
            let streamName = parent.GetTagItem("eqx.stream_name") |> unbox<string>
            let streamId = parent.GetTagItem("eqx.stream_id") |> unbox<string>
            let category = parent.GetTagItem("eqx.category") |> unbox<string>
            act.AddStream(streamName, streamId, category) |> ignore
        act

    [<System.Runtime.CompilerServices.Extension>]
    static member AddExpectedVersion(act: Activity, version) =
        match version with StreamVersion v -> act.AddTag("eqx.expected_version", v) | Any -> act

    [<System.Runtime.CompilerServices.Extension>]
    static member AddLastVersion(act: Activity, version: int64) =
        act.AddTag("eqx.last_version", version)

    [<System.Runtime.CompilerServices.Extension>]
    static member AddBatchSize(act: Activity, size: int64) =
        act.AddTag("eqx.batch_size", size)

    [<System.Runtime.CompilerServices.Extension>]
    static member AddBatch(act: Activity, size: int64, index: int) =
        act.AddBatchSize(size).AddTag("eqx.batch_index", index)

    [<System.Runtime.CompilerServices.Extension>]
    static member AddBatches(act: Activity, batches: int) =
        act.AddTag("eqx.batches", batches)

    [<System.Runtime.CompilerServices.Extension>]
    static member AddStartPosition(act: Activity, pos: int64) =
        act.AddTag("eqx.start_position", pos)

    [<System.Runtime.CompilerServices.Extension>]
    static member AddLoadMethod(act: Activity, method: string) =
        act.AddTag("eqx.load_method", method)

    [<System.Runtime.CompilerServices.Extension>]
    static member RecordConflict(act: Activity) =
        act.AddTag("eqx.conflict", true).SetStatus(ActivityStatusCode.Error, "WrongExpectedVersion")
