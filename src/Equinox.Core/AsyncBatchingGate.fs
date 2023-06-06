namespace Equinox.Core

open System
open System.Threading
open System.Threading.Tasks

/// Thread-safe coordinator that batches concurrent requests for a single <c>dispatch</> invocation such that:
/// - requests arriving together can be coalesced into the batch during the linger period via TryAdd
/// - callers that have had items admitted can concurrently await the shared fate of the dispatch via Await
/// - callers whose TryAdd has been denied can await the completion of the in-flight batch via AwaitCompletion
type internal AsyncBatch<'Req, 'Res>() =
    let queue = new System.Collections.Concurrent.BlockingCollection<'Req>()
    let tryEnqueue item =
        if queue.IsAddingCompleted then false
        else
            // there's a race between the IsAddingCompleted check outcome and the CompleteAdding
            // sadly there's no way to detect without a try/catch
            try queue.TryAdd(item)
            with :? InvalidOperationException -> false
    let mutable attempt = Unchecked.defaultof<Lazy<Task<'Res[]>>>

    /// Attempt to add a request to the flight
    /// Succeeds during linger interval (which commences when the first caller triggers the workflow via AwaitResult)
    /// Fails if this flight has closed (caller should initialize a fresh Batch, potentially holding off until the current attempt completes)
    member _.TryAdd(req, dispatch: Func<'Req[], CancellationToken, Task<'Res[]>>, lingerMs: int, ct) =
        if not (tryEnqueue req) then false else

        // Prepare a new instance, with cancellation under our control (it won't start until the Force triggers it though)
        let newInstance : Lazy<Task<'Res[]>> = lazy task {
            do! Task.Delay(lingerMs, ct)
            queue.CompleteAdding()
            return! dispatch.Invoke(queue.ToArray(), ct) }
        // If there are concurrent executions, the first through the gate wins; everybody else awaits the attempt the winner wrote
        let _ = Interlocked.CompareExchange(&attempt, newInstance, null)
        true

    /// Await the outcome of dispatching the batch (on the basis that the caller has a stake due to a successful tryEnqueue)
    member _.Await() = attempt.Value

/// Manages concurrent work such that requests arriving while a batch is in flight converge to wait for the next window
type AsyncBatchingGate<'Req, 'Res>(dispatch: Func<'Req[], CancellationToken, Task<'Res[]>>, [<O; D null>]?linger: TimeSpan) =
    let lingerMs = match linger with None -> 1 | Some x -> int x.TotalMilliseconds
    let mutable cell = AsyncBatch<'Req, 'Res>()

    new (dispatch: 'Req[] -> Async<'Res[]>, ?linger) = AsyncBatchingGate((fun reqs ct -> dispatch reqs |> Async.startImmediateAsTask ct), ?linger = linger)

    /// Include an item in the batch; await the collective dispatch (subject to the configured linger time)
    member x.ExecuteAsync(req, ct) = task {
        let current = cell
        // If current has not yet been dispatched, hop on and join
        if current.TryAdd(req, dispatch, lingerMs, ct) then return! current.Await()
        else // Any thread that discovers a batch in flight, needs to wait for it to conclude first
            do! current.Await().ContinueWith<unit>(fun (_: Task) -> ()) // wait for, but don't observe the exception or result from the in-flight batch
            // where competing threads discover a closed flight, we only want a single one to regenerate it
            let _ = Interlocked.CompareExchange(&cell, AsyncBatch(), current)
            return! x.ExecuteAsync(req, ct) } // but everyone attempts to merge their requests into the batch during the linger period

    /// Include an item in the batch; await the collective dispatch (subject to the configured linger time)
    member x.Execute(req) = async {
        let! ct = Async.CancellationToken
        return! x.ExecuteAsync(req, ct) |> Async.AwaitTaskCorrect }
