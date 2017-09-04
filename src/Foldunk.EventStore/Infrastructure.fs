[<AutoOpen>]
module Foldunk.Infrastructure

open FSharp.Control
open System
open System.Diagnostics

type Async with
    /// <summary>
    ///     Gets the result of given task so that in the event of exception
    ///     the actual user exception is raised as opposed to being wrapped
    ///     in a System.AggregateException.
    /// </summary>
    /// <param name="task">Task to be awaited.</param>
    static member AwaitTaskCorrect(task : System.Threading.Tasks.Task<'T>) : Async<'T> =
        Async.FromContinuations(fun (sc,ec,_) ->
            task.ContinueWith(fun (t : System.Threading.Tasks.Task<'T>) -> 
                if t.IsFaulted then 
                    let e = t.Exception
                    if e.InnerExceptions.Count = 1 then ec e.InnerExceptions.[0]
                    else ec e
                elif t.IsCanceled then ec(new System.Threading.Tasks.TaskCanceledException())
                else sc t.Result)
            |> ignore)

module AsyncSeq =
    /// Same as takeWhileAsync, but returns the final element too
    let takeWhileInclusiveAsync p (source : AsyncSeq<'T>) : AsyncSeq<_> = asyncSeq {
        use ie = source.GetEnumerator() 
        let! move = ie.MoveNext()
        let b = ref move
        while b.Value.IsSome do
            let v = b.Value.Value 
            yield v
            let! res = p v
            if res then 
                let! moven = ie.MoveNext()
                b := moven
            else
                b := None }

    /// Same as takeWhile, but returns the final element too
    let takeWhileInclusive p (source : AsyncSeq<'T>) = 
        takeWhileInclusiveAsync (p >> async.Return) source  

type TimeSpan with
    /// Converts a tick count as measured by stopwatch into a TimeSpan value
    static member FromStopwatchTicks(ticks : int64) =
        let ticksPerSecond = double Stopwatch.Frequency
        let totalSeconds = double ticks / ticksPerSecond
        TimeSpan.FromSeconds totalSeconds

/// Represents a time measurement of a computation that includes stopwatch tick metadata
[<NoEquality; NoComparison>]
type StopwatchInterval (startTicks : int64, endTicks : int64) =
    do if startTicks < 0L || startTicks > endTicks then invalidArg "ticks" "tick arguments do not form a valid interval."
    member __.StartTicks = startTicks
    member __.EndTicks = endTicks
    member __.Elapsed = TimeSpan.FromStopwatchTicks(endTicks - startTicks)

type Stopwatch =
    /// <summary>
    ///     Times a computation, returning the result with a time range measurement.
    /// </summary>
    /// <param name="f">Function to execute & time.</param>
    static member Time(f : unit -> 'T) : StopwatchInterval * 'T =
        let startTicks = System.Diagnostics.Stopwatch.GetTimestamp()
        let result = f ()
        let endTicks = System.Diagnostics.Stopwatch.GetTimestamp()
        let tr = StopwatchInterval(startTicks, endTicks)
        tr, result

    /// <summary>
    ///     Times an async computation, returning the result with a time range measurement.
    /// </summary>
    /// <param name="f">Function to execute & time.</param>
    static member Time(f : Async<'T>) : Async<StopwatchInterval * 'T> = async {
        let startTicks = System.Diagnostics.Stopwatch.GetTimestamp()
        let! result = f
        let endTicks = System.Diagnostics.Stopwatch.GetTimestamp()
        let tr = StopwatchInterval(startTicks, endTicks)
        return tr, result
    }