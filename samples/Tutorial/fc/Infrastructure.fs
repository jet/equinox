namespace Fc

[<AutoOpen>]
module AsyncExtensions =
    type Async with
       /// <summary>
        ///     Gets the result of given task so that in the event of exception
        ///     the actual user exception is raised as opposed to being wrapped
        ///     in a System.AggregateException.
        /// </summary>
        /// <param name="task">Task to be awaited.</param>
        [<System.Diagnostics.DebuggerStepThrough>]
        static member AwaitTaskCorrect(task : System.Threading.Tasks.Task<'T>) : Async<'T> =
            Async.FromContinuations(fun (sc, ec, _) ->
                task.ContinueWith(fun (t : System.Threading.Tasks.Task<'T>) ->
                    if t.IsFaulted then
                        let e = t.Exception
                        if e.InnerExceptions.Count = 1 then ec e.InnerExceptions.[0]
                        else ec e
                    elif t.IsCanceled then ec (new System.Threading.Tasks.TaskCanceledException())
                    else sc t.Result)
                |> ignore)

[<AutoOpen>]
module SemaphoreExtensions =

    type System.Threading.SemaphoreSlim with
        /// F# friendly semaphore await function
        member semaphore.Await(?timeout : System.TimeSpan) = async {
            let! ct = Async.CancellationToken
            let timeout = defaultArg timeout System.Threading.Timeout.InfiniteTimeSpan
            let task = semaphore.WaitAsync(timeout, ct)
            return! Async.AwaitTaskCorrect task
        }
        /// Throttling wrapper that waits asynchronously until the semaphore has available capacity
        member semaphore.Throttle(workflow : Async<'T>) : Async<'T> = async {
            let! _ = semaphore.Await()
            try return! workflow
            finally semaphore.Release() |> ignore }

// Shim for FSharp.Core 4.8 feature
[<AutoOpen>]
module AsyncExtensions2 =
    type Async with
        static member Sequential(computations : seq<Async<'T>>) : Async<'T []> =
            let sequential = new System.Threading.SemaphoreSlim 1
            computations |> Seq.map sequential.Throttle |> Async.Parallel

open FSharp.UMX // see https://github.com/fsprojects/FSharp.UMX - % operator and ability to apply units of measure to Guids+strings

type [<Measure>] fcId
type FcId = string<fcId>
module FcId =
    let toString (value : FcId) : string = %value
    let parse (value : string) : FcId = let raw = value in %raw

//type SkuId = string<skuId>
//and  [<Measure>] skuId
//module SkuId =
//    let toString (value : SkuId) : string = %value
//    let parse (value : string) : SkuId = let raw = value in %raw

type TicketId = string<ticketId>
and [<Measure>] ticketId
module TicketId =
    let toString (value : TicketId) : string = %value
    let parse (value : string) : TicketId = let raw = value in %raw

type TicketListId = string<ticketListId>
and [<Measure>] ticketListId
module PickListId =
    let toString (value : TicketListId) : string = %value
    let parse (value : string) : TicketListId = let raw = value in %raw

type AllocatorId = string<allocatorId>
and [<Measure>] allocatorId
module AllocatorId =
    let toString (value : AllocatorId) : string = %value

type SequenceId = string<sequenceId>
and [<Measure>] sequenceId
module SequenceId =
    let toString (value : SequenceId) : string = %value