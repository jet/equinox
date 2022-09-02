namespace Equinox.Core

open System
open System.Diagnostics
open System.Runtime.CompilerServices

type Stopwatch =

    /// <summary>Times an async computation, returning the result with a time range measurement.</summary>
    /// <param name="f">Function to execute / time.</param>
    [<DebuggerStepThrough>]
    static member Time(f : Async<'T>) : Async<struct (StopwatchInterval * 'T)> = async {
        let startTicks = Stopwatch.GetTimestamp()
        let! result = f
        let endTicks = Stopwatch.GetTimestamp()
        return StopwatchInterval(startTicks, endTicks), result }

    /// Converts a tick count (derived from two Stopwatch.GetTimeStamp() Tick Counters) into a number of seconds
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    static member TicksToSeconds(ticks : int64) : double =
        let ticksPerSecond = double Stopwatch.Frequency
        double ticks / ticksPerSecond

/// Represents a time measurement of a computation that includes the Stopwatch Timestamp metadata
and [<Struct; NoEquality; NoComparison>] StopwatchInterval(startTicks : int64, endTicks : int64) =
    // do if startTicks < 0L || startTicks > endTicks then invalidArg "ticks" "tick arguments do not form a valid interval."
    member _.StartTicks = startTicks
    member _.EndTicks = endTicks
    member _.Elapsed = Stopwatch.TicksToSeconds(endTicks - startTicks) |> TimeSpan.FromSeconds
    member _.ElapsedMilliseconds = Stopwatch.TicksToSeconds(endTicks - startTicks) * 1000.
    override x.ToString () = sprintf "%g ms" x.ElapsedMilliseconds
