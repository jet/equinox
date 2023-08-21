// Manages grouping of concurrent requests (typically within a projection scenario) into batches
// Typically to reduce contention on a target resource
namespace Equinox.Core.Batching

open Equinox.Core
open System

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
    member _.TryAdd(req, dispatch: Func<'Req[], CancellationToken, Task<'Res[]>>, lingerMs: int, limiter: System.Threading.SemaphoreSlim voption, ct) =
        if not (tryEnqueue req) then false else

        // Prepare a new instance, with cancellation under our control (it won't start until the Force triggers it though)
        let newInstance: Lazy<Task<'Res[]>> = lazy task {
            do! Task.Delay(lingerMs, ct)
            match limiter with ValueNone -> () | ValueSome s -> do! s.WaitAsync(ct)
            try queue.CompleteAdding()
                return! dispatch.Invoke(queue.ToArray(), ct)
            finally match limiter with ValueNone -> () | ValueSome s -> s.Release() |> ignore }
        // If there are concurrent executions, the first through the gate wins; everybody else awaits the attempt the winner wrote
        let _ = Interlocked.CompareExchange(&attempt, newInstance, null)
        true

    /// Await the outcome of dispatching the batch (on the basis that the caller has a stake due to a successful tryEnqueue)
    member _.Await() = attempt.Value

/// Manages concurrent work such that requests arriving while a batch is in flight converge to wait for the next window
type Batcher<'Req, 'Res> private (tryInclude: Func<AsyncBatch<_, _>, 'Req, CancellationToken, bool>) =
    let mutable cell = AsyncBatch<'Req, 'Res>()
    new(dispatch: Func<'Req[], CancellationToken, Task<'Res[]>>, lingerMs, limiter) =
        Batcher(fun cell req ct -> cell.TryAdd(req, dispatch, lingerMs, limiter, ct = ct))
    new(dispatch: 'Req[] -> Async<'Res[]>, ?linger : TimeSpan,
        // Extends the linger phase to include a period during which we await capacity on an externally managed Semaphore
        // The Batcher doesn't care, but a typical use is to enable limiting the number of concurrent in-flight dispatches
        ?limiter) =
        Batcher((fun items ct -> Async.StartImmediateAsTask(dispatch items, ct)),
                (match linger  with Some x -> int x.TotalMilliseconds | None -> 1),
                (match limiter with Some x -> ValueSome x | None -> ValueNone))

    /// Include an item in the batch; await the collective dispatch (subject to the configured linger time)
    member x.ExecuteAsync(req, ct) = task {
        let current = cell
        // If current has not yet been dispatched, hop on and join
        if tryInclude.Invoke(current, req, ct) then return! current.Await()
        else // Any thread that discovers a batch in flight, needs to wait for it to conclude first
            do! current.Await().ContinueWith<unit>(fun (_: Task) -> ()) // wait for, but don't observe the exception or result from the in-flight batch
            // where competing threads discover a closed flight, we only want a single one to regenerate it
            let _ = Interlocked.CompareExchange(&cell, AsyncBatch(), current)
            return! x.ExecuteAsync(req, ct) } // but everyone attempts to merge their requests into the batch during the linger period

    /// Include an item in the batch; await the collective dispatch (subject to the configured linger time)
    member x.Execute(req) = Async.call (fun ct -> x.ExecuteAsync(req, ct))

/// <summary>Thread Safe collection intended to manage a collection of <c>Batchers</c> (or instances of an equivalent type)
/// NOTE the memory usage is unbounded; if there are not a small stable number of entries, it's advised to employ a <c>BatcherCache</c></summary>
type BatcherDictionary<'Id, 'Entry>(create: Func<'Id, 'Entry>) =

    // Its important we don't risk >1 instance https://andrewlock.net/making-getoradd-on-concurrentdictionary-thread-safe-using-lazy/
    // while it would be safe, there would be a risk of incurring the cost of multiple initialization loops
    let entries = System.Collections.Concurrent.ConcurrentDictionary<'Id, Lazy<'Entry>>()
    let build id = lazy create.Invoke id

    member _.GetOrAdd(id: 'Id): 'Entry =
        entries.GetOrAdd(id, build).Value

/// <summary>Thread Safe helper that maintains a set of <c>Batchers</c> (or instances of an equivalent type) within a MemoryCache
/// NOTE if the number of items is bounded, <c>BatcherDictionary</c> is significantly more efficient</summary>
type BatcherCache<'Id, 'Entry>(cache: Cache<'Entry>, toKey: Func<'Id, string>, create: Func<'Id, 'Entry>, ?cacheWindow) =
    let cacheWindow = defaultArg cacheWindow (TimeSpan.FromMinutes 1)
    let cachePolicy = System.Runtime.Caching.CacheItemPolicy(SlidingExpiration = cacheWindow)

    /// Maintains the entries in an internal cache limited to the specified size, with entries identified by "{id}"
    new(name, create: Func<'Id, 'Entry>, sizeMb: int, ?cacheWindow) =
        BatcherCache(Cache<'Entry>.Create(name, sizeMb), Func<'Id, string>(string), create, ?cacheWindow = cacheWindow)

    /// Stores entries in the supplied cache, with entries identified by keys of the form "$Batcher-{id}"
    static member Prefixed(cache: System.Runtime.Caching.MemoryCache, createEntry: Func<'Id, 'Entry>, ?cacheWindow) =
        let mapKey = Func<'Id, string>(fun id -> "$Batcher-" + string id)
        BatcherCache(Cache cache, mapKey, createEntry, ?cacheWindow = cacheWindow)

    member _.GetOrAdd(id : 'Id) : 'Entry =
        // Optimise for low allocations on happy path
        let key = toKey.Invoke(id)
        match cache.TryGet key with
        | ValueSome entry -> entry
        | ValueNone ->

        match cache.AddOrGet(key, create.Invoke id, cachePolicy) with
        | Ok entry -> entry
        | Error entry -> entry

and Cache<'Entry>(target: System.Runtime.Caching.MemoryCache) =

    static member Create<'Entry>(name, sizeMb) =
        let config = System.Collections.Specialized.NameValueCollection(1)
        config.Add("cacheMemoryLimitMegabytes", string sizeMb)
        Cache(new System.Runtime.Caching.MemoryCache(name, config))

    member internal _.TryGet key: 'Entry voption =
        match target.Get key with
        | null -> ValueNone
        | existingEntry -> ValueSome (existingEntry :?> 'Entry)
    member internal _.AddOrGet(key, entry, policy) =
        match target.AddOrGetExisting(key, entry, policy = policy) with
        | null -> Ok entry
        | existingEntry -> Error (existingEntry :?> 'Entry)
