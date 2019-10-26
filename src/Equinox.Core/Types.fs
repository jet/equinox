namespace Equinox.Core

open Equinox
open Serilog
open System
open System.Diagnostics

/// Store-agnostic interface representing interactions an Application can have with a set of streams with a common event type
type ICategory<'event, 'state, 'streamId, 'context> =
    /// Obtain the state from the target stream
    abstract Load : log: ILogger * 'streamId * ResolveOption option -> Async<StreamToken * 'state>
    /// Given the supplied `token`, attempt to sync to the proposed updated `state'` by appending the supplied `events` to the underlying stream, yielding:
    /// - Written: signifies synchronization has succeeded, implying the included StreamState should now be assumed to be the state of the stream
    /// - Conflict: signifies the sync failed, and the proposed decision hence needs to be reconsidered in light of the supplied conflicting Stream State
    /// NB the central precondition upon which the sync is predicated is that the stream has not diverged from the `originState` represented by `token`
    ///    where the precondition is not met, the SyncResult.Conflict bears a [lazy] async result (in a specific manner optimal for the store)
    abstract TrySync : log: ILogger * StreamToken * 'state * events: 'event list * 'context option -> Async<SyncResult<'state>>

/// Represents a time measurement of a computation that includes stopwatch tick metadata
[<NoEquality; NoComparison>]
type StopwatchInterval (startTicks : int64, endTicks : int64) =
    do if startTicks < 0L || startTicks > endTicks then invalidArg "ticks" "tick arguments do not form a valid interval."

    // Converts a tick count as measured by stopwatch into a TimeSpan value
    let timeSpanFromStopwatchTicks (ticks : int64) =
        let ticksPerSecond = double System.Diagnostics.Stopwatch.Frequency
        let totalSeconds = double ticks / ticksPerSecond
        TimeSpan.FromSeconds totalSeconds

    member __.StartTicks = startTicks
    member __.EndTicks = endTicks
    member __.Elapsed = timeSpanFromStopwatchTicks(endTicks - startTicks)
    override __.ToString () = let e = __.Elapsed in sprintf "%g ms" e.TotalMilliseconds

type Stopwatch =
    /// <summary>
    ///     Times a computation, returning the result with a time range measurement.
    /// </summary>
    /// <param name="f">Function to execute & time.</param>
    [<DebuggerStepThrough>]
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
    [<DebuggerStepThrough>]
    static member Time(f : Async<'T>) : Async<StopwatchInterval * 'T> = async {
        let startTicks = System.Diagnostics.Stopwatch.GetTimestamp()
        let! result = f
        let endTicks = System.Diagnostics.Stopwatch.GetTimestamp()
        let tr = StopwatchInterval(startTicks, endTicks)
        return tr, result
    }