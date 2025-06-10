/// The types in here (being shims which ultimately will be either removed or gain proper homes in other libraries and/or can cause intellisense confusion for consumers) are intentionally not public and compiled into each module requiring it directly via a `Link`
[<AutoOpen>]
module internal Equinox.Core.Infrastructure

open System.Diagnostics

type internal CancellationToken = System.Threading.CancellationToken
type internal Task<'t> = System.Threading.Tasks.Task<'t>
type internal Task = System.Threading.Tasks.Task
type internal TaskCanceledException = System.Threading.Tasks.TaskCanceledException
type internal Interlocked = System.Threading.Interlocked

type OAttribute = System.Runtime.InteropServices.OptionalAttribute
type DAttribute = System.Runtime.InteropServices.DefaultParameterValueAttribute

// Direct copy of canonical implementation at http://www.fssnip.net/7Rc/title/AsyncAwaitTaskCorrect
// pending that being officially packaged somewhere or integrated into FSharp.Core https://github.com/fsharp/fslang-suggestions/issues/840
type Async with

    /// <summary>
    ///     Gets the result of given task so that in the event of exception
    ///     the actual user exception is raised as opposed to being wrapped
    ///     in a System.AggregateException.
    /// </summary>
    /// <param name="task">Task to be awaited.</param>
    [<DebuggerStepThrough>]
    static member AwaitTaskCorrect(task: Task<'T>): Async<'T> =
        Async.FromContinuations(fun (sc, ec, _cc) ->
            task.ContinueWith(fun (t: Task<'T>) ->
                if t.IsFaulted then
                    let e = t.Exception
                    if e.InnerExceptions.Count = 1 then ec e.InnerExceptions[0]
                    else ec e
                elif t.IsCanceled then ec (TaskCanceledException())
                else sc t.Result)
            |> ignore)

    /// <summary>
    ///     Gets the result of given task so that in the event of exception
    ///     the actual user exception is raised as opposed to being wrapped
    ///     in a System.AggregateException.
    /// </summary>
    /// <param name="task">Task to be awaited.</param>
    [<DebuggerStepThrough>]
    static member AwaitTaskCorrect(task: Task): Async<unit> =
        Async.FromContinuations(fun (sc, ec, _cc) ->
            task.ContinueWith(fun (task: Task) ->
                if task.IsFaulted then
                    let e = task.Exception
                    if e.InnerExceptions.Count = 1 then ec e.InnerExceptions[0]
                    else ec e
                elif task.IsCanceled then
                    ec (TaskCanceledException())
                else
                    sc ())
            |> ignore)

module Async =

    let inline call (start: System.Threading.CancellationToken -> Task<'T>) = async {
        let! ct = Async.CancellationToken
        return! start ct |> Async.AwaitTaskCorrect }

module ValueTuple =

    let inline fst struct (f, _s) = f
    let inline snd struct (_f, s) = s

module Array =

    let inline chooseV f xs = [| for item in xs do match f item with ValueSome v -> yield v | ValueNone -> () |]
    let inline tryFirstV (xs: _[]) = if xs.Length = 0 then ValueNone else ValueSome xs[0]
    let takeWhileInclusive (predicate: 'T -> bool) (array: 'T[]) : 'T[] =
        let result = ResizeArray<'T>()
        let mutable i, continue' = 0, true

        while i < array.Length && continue' do
            let current = array[i]
            result.Add(current)
            if not (predicate current) then
                continue' <- false
            i <- i + 1

        result.ToArray()
