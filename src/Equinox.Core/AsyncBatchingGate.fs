namespace Equinox.Core

open System.Threading

[<AllowNullLiteral>]
type Node<'T>(data: 'T, next: Node<'T>) =
    member val Data = data with get, set
    member val Next = next with get, set

/// Lock free queue used to avoid blocking. Due to semantics of AsyncBatch, it accumulates requests until the batch
/// linger expires, and then is marked done.
type LockFreeQueue<'T> () =
    let Done = Node<'T>(Unchecked.defaultof<'T>, null) // On we queue as done no more entries can be accumulated
    let mutable head: Node<'T> = null

    member __.TryAdd(item) =
        let mutable prevHead = head
        if prevHead = Done then
            false
        else
            let n = Node<'T>(item, prevHead)
            let mutable swapHead = Interlocked.CompareExchange(&head, n, prevHead)
            let mutable cont = true

            // have to do this twice since F# has no do-while loop
            while (cont && prevHead <> swapHead) do
                prevHead <- head
                if prevHead = Done then
                    cont <- false
                else
                    n.Next <- prevHead
                    swapHead <- Interlocked.CompareExchange(&head, n, prevHead)

            prevHead <> Done

    member __.GetAll() =
        let mutable prevHead = head
        let mutable swapHead = Interlocked.CompareExchange(&head, Done, prevHead)

        while (prevHead <> swapHead) do
            prevHead <- head
            swapHead <- Interlocked.CompareExchange(&head, Done, prevHead)

        // will accumulate linked list in reverse order
        let mutable cur = prevHead
        let arr = new ResizeArray<'T>()
        while cur <> null do
            arr.Add(cur.Data)
            cur <- cur.Next

        arr.ToArray()

/// Thread-safe coordinator that batches concurrent requests for a single <c>dispatch</> invocation such that:
/// - requests arriving together can be coalesced into the batch during the linger period via TryAdd
/// - callers that have had items admitted can concurrently await the shared fate of the dispatch via AwaitResult
/// - callers whose TryAdd has been denied can await the completion of the in-flight batch via AwaitCompletion
type internal AsyncBatch<'Req, 'Res>(dispatch : 'Req[] -> Async<'Res>, linger : System.TimeSpan) =
    let lingerMs = int linger.TotalMilliseconds
    // Yes, naive impl in the absence of a cleaner way to have callers sharing the AwaitCompletion coordinate the adding
    do if lingerMs < 1 then invalidArg "linger" "must be >= 1ms"
    let queue = new LockFreeQueue<'Req>()
    let workflow = async {
        do! Async.Sleep lingerMs
        let reqs = queue.GetAll()
        return! dispatch reqs
    }
    let task = lazy (Async.StartAsTask workflow)

    /// Attempt to add a request to the flight
    /// Succeeds during linger interval (which commences when the first caller triggers the workflow via AwaitResult)
    /// Fails if this flight has closed (caller should generate a fresh, potentially after awaiting this.AwaitCompletion)
    member __.TryAdd(item) = queue.TryAdd(item)

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
