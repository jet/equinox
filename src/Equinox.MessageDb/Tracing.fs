[<AutoOpen>]
module internal Equinox.MessageDb.Tracing

open Equinox.MessageDb.Core
open System.Diagnostics

type private Activity with

    member act.AddExpectedVersion(version) =
        match version with StreamVersion v -> act.SetTag("eqx.expected_version", v) | Any -> act

    member act.AddLastVersion(version: int64) = act.SetTag("eqx.last_version", version)

    member act.AddBatchSize(size: int64) = act.SetTag("eqx.batch_size", size)

    member act.AddBatches(batches: int) = act.SetTag("eqx.batches", batches)

    member act.AddStartPosition(pos: int64) = act.SetTag("eqx.start_position", pos)

    member act.AddLoadMethod(method: string) = act.SetTag("eqx.load_method", method)

    member act.RecordConflict() =
        act.SetTag("eqx.conflict", true).SetStatus(ActivityStatusCode.Error, "WrongExpectedVersion")
