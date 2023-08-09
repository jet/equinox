namespace Equinox.Core

open System.Diagnostics

type Stopwatch =

    /// Converts a tick count (derived from two Stopwatch.GetTimeStamp() Tick Counters) into a number of seconds
    [<System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)>]
    static member TicksToSeconds(ticks: int64): double =
        let ticksPerSecond = double Stopwatch.Frequency
        double ticks / ticksPerSecond

/// Represents a time measurement of a computation that includes the Stopwatch Timestamp metadata
type [<Struct; NoEquality; NoComparison>] StopwatchInterval(startTicks: int64, endTicks: int64) =
    // do if startTicks < 0L || startTicks > endTicks then invalidArg "ticks" "tick arguments do not form a valid interval."
    member _.StartTicks = startTicks
    member _.EndTicks = endTicks
    member _.Elapsed = Stopwatch.TicksToSeconds(endTicks - startTicks) |> System.TimeSpan.FromSeconds
    member _.ElapsedMilliseconds = Stopwatch.TicksToSeconds(endTicks - startTicks) * 1000.
    override x.ToString () = $"%g{x.ElapsedMilliseconds} ms"

module Stopwatch =

    [<DebuggerStepThrough>]
    let time (ct: CancellationToken) (f: CancellationToken -> Task<'T>): Task<struct (StopwatchInterval * 'T)> = task {
        let startTicks = Stopwatch.GetTimestamp()
        let! result = f ct
        let endTicks = Stopwatch.GetTimestamp()
        return struct (StopwatchInterval(startTicks, endTicks), result) }
