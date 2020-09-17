﻿/// The types in here (being shims which ultimately will be either removed or gain proper homes in other libraries and/or can cause intellisense confusion for consumers) are intentionally not public and compiled into each module requiring it directly via a `Link`
[<AutoOpen>]
module internal Equinox.Core.Infrastructure

open FSharp.Control
open System
open System.Diagnostics
open System.Threading.Tasks
open System.Threading

type OAttribute = System.Runtime.InteropServices.OptionalAttribute
type DAttribute = System.Runtime.InteropServices.DefaultParameterValueAttribute

#if NET461
let isNull v = v = null

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
    let singleton v = Array.create 1 v

module Option =
    let filter predicate option = match option with None -> None | Some x -> if predicate x then Some x else None
    let toNullable option = match option with Some x -> Nullable x | None -> Nullable ()
    let ofObj obj = match obj with null -> None | x -> Some x
    let toObj option = match option with None -> null | Some x -> x
    let defaultWith f = function | Some v -> v | _ -> f()
#endif

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

#if NETSTANDARD2_1
    static member inline AwaitValueTask (vtask: ValueTask<'T>) : Async<'T> = vtask.AsTask() |> Async.AwaitTaskCorrect
#endif

[<RequireQualifiedAccess>]
module Regex =
    open System.Text.RegularExpressions

    let DefaultTimeout = TimeSpan.FromMilliseconds 250.
    let private mkRegex p = Regex(p, RegexOptions.None, DefaultTimeout)

    /// Active pattern for branching on successful regex matches
    let (|Match|_|) (pattern : string) (input : string) =
        let m = (mkRegex pattern).Match input
        if m.Success then Some m else None
