[<AutoOpen>]
module Equinox.Projection.Infrastructure

open System
open System.Collections.Concurrent
open System.Threading
open System.Threading.Tasks

module ConcurrentQueue =
    let drain handle (xs : ConcurrentQueue<_>) = 
        let rec aux () =
            match xs.TryDequeue() with
            | false, _ -> ()
            | true, x -> handle x; aux ()
        aux ()

#if NET461
module Array =
    let tryHead (array : 'T[]) =
        if array.Length = 0 then None
        else Some array.[0]

module Option = 
    let toNullable option = match option with None -> System.Nullable() | Some v -> System.Nullable(v)
#endif

type Async with
    static member AwaitTaskCorrect (task : Task<'T>) : Async<'T> =
        Async.FromContinuations <| fun (k,ek,_) ->
            task.ContinueWith (fun (t:Task<'T>) ->
                if t.IsFaulted then
                    let e = t.Exception
                    if e.InnerExceptions.Count = 1 then ek e.InnerExceptions.[0]
                    else ek e
                elif t.IsCanceled then ek (TaskCanceledException("Task wrapped with Async has been cancelled."))
                elif t.IsCompleted then k t.Result
                else ek(Exception "invalid Task state!"))
            |> ignore

type SemaphoreSlim with
    /// F# friendly semaphore await function
    member semaphore.Await(?timeout : TimeSpan) = async {
        let! ct = Async.CancellationToken
        let timeout = defaultArg timeout Timeout.InfiniteTimeSpan
        let task = semaphore.WaitAsync(timeout, ct) 
        return! Async.AwaitTaskCorrect task
    } 