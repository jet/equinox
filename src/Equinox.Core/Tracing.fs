module Equinox.Core.Tracing

open System.Diagnostics

let source = new ActivitySource("Equinox")

[<System.Runtime.CompilerServices.Extension>]
type ActivityExtensions =

    [<System.Runtime.CompilerServices.Extension>]
    static member AddLeader(act: Activity, requiresLeader) =
        if requiresLeader then act.AddTag("eqx.requires_leader", true) else act

    [<System.Runtime.CompilerServices.Extension>]
    static member AddRetryAttempt(act: Activity, attempt: int) =
        if attempt > 1 then act.AddTag("eqx.retry_count", attempt - 1) else act

    [<System.Runtime.CompilerServices.Extension>]
    static member AddSyncAttempt(act: Activity, attempt: int) =
        if attempt > 1 then act.AddTag("eqx.resync_count", attempt - 1) else act

    [<System.Runtime.CompilerServices.Extension>]
    static member AddStale(act: Activity, allowStale) =
        if allowStale then act.AddTag("eqx.allow_stale", true) else act

    [<System.Runtime.CompilerServices.Extension>]
    static member AddStream(act: Activity, category: string, streamId: string, streamName: string) =
        act.AddTag("eqx.stream_name", streamName).AddTag("eqx.stream_id", streamId).AddTag("eqx.category", category)

    [<System.Runtime.CompilerServices.Extension>]
    static member IncMetric(act: Activity, count: int, bytes: int) =
        let currentCount = act.GetTagItem("eqx.count") |> ValueOption.ofObj |> ValueOption.map unbox<int> |> ValueOption.defaultValue 0
        let currentBytes = act.GetTagItem("eqx.bytes") |> ValueOption.ofObj |> ValueOption.map unbox<int> |> ValueOption.defaultValue 0
        let count = count + currentCount
        let bytes = bytes + currentBytes
        act.SetTag("eqx.count", count).SetTag("eqx.bytes", bytes)
