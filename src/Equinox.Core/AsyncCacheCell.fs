namespace Equinox.Core

open System.Threading
open System.Threading.Tasks

/// Asynchronous Lazy<'T> used to gate a workflow to ensure at most once execution of a computation.
type AsyncLazy<'T>(workflow : unit -> Task<'T>) =

    let task = Lazy.Create workflow

    /// Await the outcome of the computation.
    /// NOTE due to `Lazy<T>` semantics, failed attempts will cache any exception; AsyncCacheCell compensates for this
    member _.Await() = task.Value

    /// Used to rule out values where the computation yielded an exception or the result has now expired
    member _.TryAwaitValid(isExpired) : 'T voption =
        if not task.IsValueCreated then ValueNone else

        let t = task.Value

        // Determines if the last attempt completed, but failed; For TMI see https://stackoverflow.com/a/33946166/11635
        if t.Status <> TaskStatus.RanToCompletion then ValueNone // net6.0 brings an IsCompletedSuccessfully, but we're still netstandard
        else match isExpired with
             | ValueSome check when not (check t.Result) -> ValueNone
             | _ -> ValueSome t.Result

    /// Synchronously check whether the value has been computed (and/or remains valid)
    member x.IsValid(isExpired) = x.TryAwaitValid isExpired |> ValueOption.isSome

/// Generic async lazy caching implementation that admits expiration/recomputation/retry on exception semantics.
/// If `workflow` fails, all readers entering while the load/refresh is in progress will share the failure
/// The first caller through the gate triggers a recomputation attempt if the previous attempt ended in failure
type AsyncCacheCell<'T>(workflow : CancellationToken -> Task<'T>, ?isExpired : 'T -> bool) =

    let isExpired = match isExpired with Some x -> ValueSome x | None -> ValueNone
    let mutable cell = AsyncLazy(fun () -> Task.FromCanceled<_>(CancellationToken.None))

    /// Synchronously check the value remains valid (to short-circuit an Async AwaitValue step where value not required)
    member _.IsValid() = cell.IsValid(isExpired)
    /// Gets or asynchronously recomputes a cached value depending on expiry and availability
    member _.Await(ct) =
        // First, take a local copy of the current state
        let current = cell
        match current.TryAwaitValid(isExpired) with
        | ValueSome res -> Task.FromResult res // ... if it's already / still valid, we're done
        | ValueNone ->
            // Prepare to do the work, with cancellation under out control
            let attemptLoad () = workflow ct
            // we want the actual workflow (if it is ultimately to run) to be on the thread pool
            let dispatch () = Task.Run<'T>(attemptLoad)
            // avoid unnecessary recomputation in cases where competing threads detect expiry;
            // the first write attempt wins, and everybody else reads off that value
            let _ = Interlocked.CompareExchange(&cell, AsyncLazy(dispatch), current)
            cell.Await()
