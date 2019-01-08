[<AutoOpen>]
module Equinox.Store.Infrastructure

open FSharp.Control
open System
open System.Diagnostics


type OAttribute = System.Runtime.InteropServices.OptionalAttribute
type DAttribute = System.Runtime.InteropServices.DefaultParameterValueAttribute
    
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
    let toNullable option = match option with Some x -> Nullable x | None -> Nullable ()
    let ofObj obj = match obj with null -> None | x -> Some x
    let toObj option = match option with None -> null | Some x -> x
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

/// Asynchronous Lazy<'T> that guarantees workflow will be executed at most once.
type AsyncLazy<'T>(workflow : Async<'T>) =
    let task = lazy(Async.StartAsTask workflow)
    member __.AwaitValue() = Async.AwaitTaskCorrect task.Value
    member internal __.PeekInternalTask = task

/// Generic async lazy caching implementation that admits expiration/recomputation semantics
/// If `workflow` fails, all readers entering while the load/refresh is in progress will share the failure
type AsyncCacheCell<'T>(workflow : Async<'T>, ?isExpired : 'T -> bool) =
    let mutable currentCell = AsyncLazy workflow

    let initializationFailed (value: System.Threading.Tasks.Task<_>) =
        // for TMI on this, see https://stackoverflow.com/a/33946166/11635
        value.IsCompleted && value.Status <> System.Threading.Tasks.TaskStatus.RanToCompletion

    let update cell = async {
        // avoid unneccessary recomputation in cases where competing threads detect expiry;
        // the first write attempt wins, and everybody else reads off that value
        let _ = System.Threading.Interlocked.CompareExchange(&currentCell, AsyncLazy workflow, cell)
        return! currentCell.AwaitValue()
    }

    /// Enables callers to short-circuit the gate by checking whether a value has been computed
    member __.PeekIsValid() =
        let cell = currentCell
        let currentState = cell.PeekInternalTask
        if not currentState.IsValueCreated then false else

        let value = currentState.Value
        not (initializationFailed value) 
        && (match isExpired with Some f -> not (f value.Result) | _ -> false)
    
    /// Gets or asynchronously recomputes a cached value depending on expiry and availability
    member __.AwaitValue() = async {
        let cell = currentCell
        let currentState = cell.PeekInternalTask
        // If the last attempt completed, but failed, we need to treat it as expired
        if currentState.IsValueCreated && initializationFailed currentState.Value then
            return! update cell
        else
            let! current = cell.AwaitValue()
            match isExpired with
            | Some f when f current -> return! update cell
            | _ -> return current
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