[<AutoOpen>]
module Equinox.Store.Infrastructure

open FSharp.Control
open System
open System.Diagnostics

#if NET461
module Seq =
    let tryLast (source : seq<_>) =
        use e = source.GetEnumerator()
        if e.MoveNext() then
            let mutable res = e.Current
            while (e.MoveNext()) do res <- e.Current
            Some res
        else
            None
    let arrMapFold f acc (array: _[]) =
        match array.Length with
        | 0 -> [| |], acc
        | len ->
            let f = OptimizedClosures.FSharpFunc<_,_,_>.Adapt(f)
            let mutable acc = acc
            let res = Array.zeroCreate len
            for i = 0 to array.Length-1 do
                let h',s' = f.Invoke(acc,array.[i])
                res.[i] <- h'
                acc <- s'
            res, acc
    let mapFold<'T,'State,'Result> (mapping: 'State -> 'T -> 'Result * 'State) state source =
        let arr,state = source |> Seq.toArray |> arrMapFold mapping state
        Seq.readonly arr, state

module Array =
    let tryHead (array : 'T[]) =
        if array.Length = 0 then None
        else Some array.[0]
    let tryFindBack predicate (array: _[]) =
        let rec loop i =
            if i < 0 then None
            elif predicate array.[i] then Some array.[i]
            else loop (i - 1)
        loop (array.Length - 1)
    let tryFindIndexBack predicate (array: _[]) =
        let rec loop i =
            if i < 0 then None
            elif predicate array.[i] then Some i
            else loop (i - 1)
        loop (array.Length - 1)

module Option =
    let filter predicate option = match option with None -> None | Some x -> if predicate x then Some x else None
#endif

type Async with
    /// <summary>
    ///     Gets the result of given task so that in the event of exception
    ///     the actual user exception is raised as opposed to being wrapped
    ///     in a System.AggregateException.
    /// </summary>
    /// <param name="task">Task to be awaited.</param>
    [<DebuggerStepThrough>]
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
    [<DebuggerStepThrough>]
    static member AwaitTaskCorrect(task : System.Threading.Tasks.Task) : Async<unit> =
        Async.FromContinuations(fun (sc,ec,_) ->
            task.ContinueWith(fun (task : System.Threading.Tasks.Task) ->
                if task.IsFaulted then
                    let e = task.Exception
                    if e.InnerExceptions.Count = 1 then ec e.InnerExceptions.[0]
                    else ec e
                elif task.IsCanceled then
                    ec(System.Threading.Tasks.TaskCanceledException())
                else
                    sc ())
            |> ignore)
    static member inline bind (f:'a -> Async<'b>) (a:Async<'a>) : Async<'b> = async.Bind(a, f)

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

[<RequireQualifiedAccess>]
module Regex =
    open System.Text.RegularExpressions

    let DefaultTimeout = TimeSpan.FromMilliseconds 250.
    let private mkRegex p = Regex(p, RegexOptions.None, DefaultTimeout)

    /// Active pattern for branching on successful regex matches
    let (|Match|_|) (pattern : string) (input : string) =
        let m = (mkRegex pattern).Match input
        if m.Success then Some m else None