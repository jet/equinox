module Equinox.Core.Tracing

open System.Diagnostics

let source = new ActivitySource("Equinox")

[<System.Runtime.CompilerServices.Extension>]
type ActivityExtensions =

    [<System.Runtime.CompilerServices.Extension>]
    static member AddAttempt(act: Activity, attempt: int) = act.AddTag("eqx.op_attempt", attempt)

    [<System.Runtime.CompilerServices.Extension>]
    static member AddLeader(act: Activity, requiresLeader) = if requiresLeader then act.AddTag("eqx.requires_leader", true) else act

    [<System.Runtime.CompilerServices.Extension>]
    static member AddMetric(act: Activity, count: int, bytes: int) =
        let currentCount = act.GetTagItem("eqx.count") |> ValueOption.ofObj |> ValueOption.map unbox<int> |> ValueOption.defaultValue 0
        let currentBytes = act.GetTagItem("eqx.bytes") |> ValueOption.ofObj |> ValueOption.map unbox<int> |> ValueOption.defaultValue 0
        let count = count + currentCount
        let bytes = bytes + currentBytes
        act.SetTag("eqx.count", count).SetTag("eqx.bytes", bytes)

    [<System.Runtime.CompilerServices.Extension>]
    static member AddStale(act: Activity, allowStale: bool) = act.AddTag("eqx.allow_stale", allowStale)

    [<System.Runtime.CompilerServices.Extension>]
    static member AddStream(act: Activity, category: string, streamId: string, streamName: string) =
        act.AddTag("eqx.stream_name", streamName).AddTag("eqx.stream_id", streamId).AddTag("eqx.category", category)

