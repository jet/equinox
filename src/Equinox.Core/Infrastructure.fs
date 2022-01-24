/// The types in here (being shims which ultimately will be either removed or gain proper homes in other libraries and/or can cause intellisense confusion for consumers) are intentionally not public and compiled into each module requiring it directly via a `Link`
[<AutoOpen>]
module internal Equinox.Core.Infrastructure

open FSharp.Control
open System.Diagnostics
open System.Threading.Tasks

type OAttribute = System.Runtime.InteropServices.OptionalAttribute
type DAttribute = System.Runtime.InteropServices.DefaultParameterValueAttribute

type Async with
    /// <summary>
    ///     Gets the result of given task so that in the event of exception
    ///     the actual user exception is raised as opposed to being wrapped
    ///     in a System.AggregateException.
    /// </summary>
    /// <param name="task">Task to be awaited.</param>
    [<DebuggerStepThrough>]
    static member AwaitTaskCorrect(task : Task<'T>) : Async<'T> =
        Async.FromContinuations(fun (sc, ec, _) ->
            task.ContinueWith(fun (t : Task<'T>) ->
                if t.IsFaulted then
                    let e = t.Exception
                    if e.InnerExceptions.Count = 1 then ec e.InnerExceptions.[0]
                    else ec e
                elif t.IsCanceled then ec(new TaskCanceledException())
                else sc t.Result)
            |> ignore)

    [<DebuggerStepThrough>]
    static member AwaitTaskCorrect(task : Task) : Async<unit> =
        Async.FromContinuations(fun (sc, ec, _) ->
            task.ContinueWith(fun (task : Task) ->
                if task.IsFaulted then
                    let e = task.Exception
                    if e.InnerExceptions.Count = 1 then ec e.InnerExceptions.[0]
                    else ec e
                elif task.IsCanceled then
                    ec(TaskCanceledException())
                else
                    sc ())
            |> ignore)
