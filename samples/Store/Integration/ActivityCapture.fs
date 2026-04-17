namespace global

open System
open System.Diagnostics

/// Captures completed Activities from store-specific ActivitySources for test assertions
type ActivityCapture(sourceNamePrefix: string) =
    let captured = ResizeArray<Activity>()
    let listener = new ActivityListener(
        ShouldListenTo = (fun source -> source.Name.StartsWith(sourceNamePrefix, StringComparison.Ordinal)),
        Sample = (fun _ -> ActivitySamplingResult.AllDataAndRecorded),
        ActivityStopped = (fun act -> captured.Add(act)))
    do ActivitySource.AddActivityListener(listener)
    member _.Clear() = captured.Clear()
    member _.Activities = captured |> Seq.toList
    interface IDisposable with member _.Dispose() = listener.Dispose()
