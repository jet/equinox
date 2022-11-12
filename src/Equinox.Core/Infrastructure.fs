/// The types in here (being shims which ultimately will be either removed or gain proper homes in other libraries and/or can cause intellisense confusion for consumers) are intentionally not public and compiled into each module requiring it directly via a `Link`
[<AutoOpen>]
module internal Equinox.Core.Infrastructure

open FSharp.Control
open System.Diagnostics
open System.Threading.Tasks

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
    static member AwaitTaskCorrect(task : Task<'T>) : Async<'T> =
        Async.FromContinuations(fun (sc, ec, _cc) ->
            task.ContinueWith(fun (t : Task<'T>) ->
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
    static member AwaitTaskCorrect(task : Task) : Async<unit> =
        Async.FromContinuations(fun (sc, ec, _cc) ->
            task.ContinueWith(fun (task : Task) ->
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

    let startAsTask ct computation = Async.StartAsTask(computation, cancellationToken = ct)

module ValueTuple =

    let inline fst struct (f, _s) = f
    let inline snd struct (_f, s) = s

module ValueOption =

    let inline toOption x = match x with ValueSome x -> Some x | ValueNone -> None
    let inline toArray x = match x with ValueSome e -> [| e |] | ValueNone -> [||]

module Seq =

    let inline chooseV f = Seq.choose (f >> ValueOption.toOption)

module Array =

    let inline chooseV f arr = [| for item in arr do match f item with ValueSome v -> yield v | ValueNone -> () |]
