namespace Equinox.Core

/// Thread-safe coordinator that batches concurrent requests for a single <c>dispatch</> invocation such that:
/// - requests arriving together can be coalesced into the batch during the linger period via TryAdd
/// - callers that have had items admitted can concurrently await the shared fate of the dispatch via AwaitResult
/// - callers whose TryAdd has been denied can await the completion of the in-flight batch via AwaitCompletion
type internal AsyncBatch<'Req, 'Res>(dispatch : 'Req[] -> Async<'Res>, linger : System.TimeSpan) =
    let lingerMs = int linger.TotalMilliseconds
    // Yes, naive impl in the absence of a cleaner way to have callers sharing the AwaitCompletion coordinate the adding
    do if lingerMs < 1 then invalidArg "linger" "must be >= 1ms"
    let queue = new System.Collections.Concurrent.BlockingCollection<'Req>()
    let workflow = async {
        do! Async.Sleep lingerMs
        queue.CompleteAdding()
        let reqs = queue.ToArray()
        return! dispatch reqs
    }
    let task = lazy (Async.StartAsTask workflow)

    /// Attempt to add a request to the flight
    /// Succeeds during linger interval (which commences when the first caller triggers the workflow via AwaitResult)
    /// Fails if this flight has closed (caller should generate a fresh, potentially after awaiting this.AwaitCompletion)
    member __.TryAdd(item) =
        if queue.IsAddingCompleted then false else

        // there's a race between the IsAddingCompleted check outcome and the CompleteAdding
        // sadly there's no way to detect without a try/catch
        try queue.TryAdd(item)
        with :? System.InvalidOperationException -> false

    /// Await the outcome of dispatching the batch (on the basis that the caller has a stake due to a successful TryAdd)
    member __.AwaitResult() = Async.AwaitTaskCorrect task.Value

    /// Wait for dispatch to conclude (for any reason: ok/exn/cancel; we only care about the channel being clear)
    member __.AwaitCompletion() =
        Async.FromContinuations(fun (cont, _, _) ->
            task.Value.ContinueWith(fun (_ : System.Threading.Tasks.Task<'Res>) -> cont ())
            |> ignore)

/// Manages concurrent work such that requests arriving while a batch is in flight converge to wait for the next window
type AsyncBatchingGate<'Req, 'Res>(dispatch : 'Req[] -> Async<'Res>, ?linger) =
    let mkBatch () = AsyncBatch(dispatch, defaultArg linger (System.TimeSpan.FromMilliseconds 5.))
    let mutable cell = mkBatch()

    member __.Execute req = async {
        let current = cell
        // If current has not yet been dispatched, hop on and join
        if current.TryAdd req then
            return! current.AwaitResult()
        else // Any thread that discovers a batch in flight, needs to wait for it to conclude first
            do! current.AwaitCompletion()
            // where competing threads discover a closed flight, we only want a single one to regenerate it
            let _ = System.Threading.Interlocked.CompareExchange(&cell, mkBatch (), current)
            return! __.Execute req
    }
