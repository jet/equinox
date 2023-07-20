module Equinox.Core.Tracing

open System.Diagnostics

let source = new ActivitySource("Equinox")

type private Activity with

    member act.AddLeader(requiresLeader) =
        if requiresLeader then act.SetTag("eqx.requires_leader", true) else act

    member act.AddRetryAttempt(attempt: int) =
        if attempt > 1 then act.SetTag("eqx.retry_count", attempt - 1) else act

    member act.AddSyncAttempt(attempt: int) =
        if attempt > 1 then act.SetTag("eqx.resync_count", attempt - 1) else act

    member act.AddStale(maxAge: System.TimeSpan) =
        if maxAge.Ticks <> 0L then act.SetTag("eqx.allow_stale", true) else act

    member act.AddStream(category: string, streamId: string, streamName: string) =
        act.SetTag("eqx.stream_name", streamName).SetTag("eqx.stream_id", streamId).SetTag("eqx.category", category)

    member act.AddCacheHit(hit: bool) = act.SetTag("eqx.cache_hit", hit) |> ignore

    member act.AddAppendCount(count: int) = act.SetTag("eqx.append_count", count)

    member act.IncMetric(count: int, bytes: int) =
        let currentCount = act.GetTagItem("eqx.count") |> ValueOption.ofObj |> ValueOption.map unbox<int> |> ValueOption.defaultValue 0
        let currentBytes = act.GetTagItem("eqx.bytes") |> ValueOption.ofObj |> ValueOption.map unbox<int> |> ValueOption.defaultValue 0
        let count = count + currentCount
        let bytes = bytes + currentBytes
        act.SetTag("eqx.count", count).SetTag("eqx.bytes", bytes)
