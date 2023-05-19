namespace Equinox.Core

open System.Threading
open System.Threading.Tasks

/// Asynchronous Lazy<'T> used to gate a workflow to ensure at most once execution of a computation.
type AsyncLazy<'T>(workflow: unit -> Task<'T>) =

    // NOTE due to `Lazy<T>` semantics, failed attempts will cache any exception; AsyncCacheCell compensates for this by rolling over to a new instance
    let workflow = lazy workflow ()

    /// Synchronously peek at what's been previously computed (if it's not Empty, or the last attempt Faulted).
    member _.TryCompleted() =
        if not workflow.IsValueCreated then ValueNone else

        let t = workflow.Value
        if t.Status <> TaskStatus.RanToCompletion then ValueNone
        else ValueSome t.Result

    /// Used to rule out values where the computation yielded an exception or the result has now expired
    member _.TryAwaitValid() = task {
        let t = workflow.Value

        // Determines if the last attempt completed, but failed; For TMI see https://stackoverflow.com/a/33946166/11635
        if t.IsFaulted then return ValueNone
        else
            let! res = t
            return ValueSome res }

    /// Await the outcome of the computation.
    member _.Await() = workflow.Value

    /// Singleton Empty value
    /// TODO (assuming this turns out to be the case) add "NOTE the fact that this is a singleton instance is critical to the impl of CacheEntry.ReadThrough"
    ///      and tests to ensure it remains the case
    static member val Empty = AsyncLazy(fun () -> Task.FromException<'T>(System.InvalidOperationException "Uninitialized AsyncLazy"))

/// Generic async lazy caching implementation that admits expiration/recomputation/retry on exception semantics.
/// If `workflow` fails, all readers entering while the load/refresh is in progress will share the failure
/// The first caller through the gate triggers a recomputation attempt if the previous attempt ended in failure
type AsyncCacheCell<'T>(workflow, ?isExpired: 'T -> bool) =

    let isValid = match isExpired with Some f -> f >> not | None -> fun _ -> true
    let mutable cell = AsyncLazy<'T>.Empty

    /// Synchronously check the value remains valid (to enable short-circuiting an Await step where value not required)
    member _.IsValid() =
        cell.TryCompleted() |> ValueOption.exists isValid

    /// Gets or asynchronously recomputes value depending on whether value passes the validity check
    member _.Await(ct : CancellationToken) = task {
        // Each concurrent execution takes a copy of the cell, and attempts to reuse the value; later used to ensure only one triggers the workflow
        let current = cell
        match! current.TryAwaitValid() with
        | ValueSome res when isValid res -> return res
        | _ ->
            // Prepare a new instance, with cancellation under our control (it won't start until the first Await on the LazyTask triggers it though)
            let newInstance = AsyncLazy<'T>(fun () -> workflow ct)
            // If there are concurrent executions, the first through the gate wins; everybody else awaits the instance the winner wrote
            let _ = Interlocked.CompareExchange(&cell, newInstance, current)
            return! cell.Await() }
