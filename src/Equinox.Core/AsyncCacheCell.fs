namespace Equinox.Core

open System.Threading
open System.Threading.Tasks

/// Asynchronous Lazy<'T> used to gate a workflow to ensure at most once execution of a computation.
type AsyncLazy<'T>(workflow: unit -> Task<'T>) =

    /// NOTE due to `Lazy<T>` semantics, failed attempts will cache any exception; AsyncCacheCell compensates for this by rolling over to a new instance
    let workflow = lazy workflow ()

    /// Synchronously check whether the value has been computed (and/or remains valid)
    member _.IsValid(isExpired) =
        if not workflow.IsValueCreated then
            false
        else

            let t = workflow.Value

            if t.Status <> TaskStatus.RanToCompletion then
                false
            else

                match isExpired with
                | ValueSome isExpired -> not (isExpired t.Result)
                | _ -> true

    /// Used to rule out values where the computation yielded an exception or the result has now expired
    member _.TryAwaitValid(isExpired) : Task<'T voption> =
        let t = workflow.Value

        // Determines if the last attempt completed, but failed; For TMI see https://stackoverflow.com/a/33946166/11635
        if t.IsFaulted then
            Task.FromResult ValueNone
        else
            task {
                let! (res: 'T) = t

                match isExpired with
                | ValueSome isExpired when isExpired res -> return ValueNone
                | _ -> return ValueSome res
            }

    /// Await the outcome of the computation.
    member _.Await() = workflow.Value

/// Generic async lazy caching implementation that admits expiration/recomputation/retry on exception semantics.
/// If `workflow` fails, all readers entering while the load/refresh is in progress will share the failure
/// The first caller through the gate triggers a recomputation attempt if the previous attempt ended in failure
type AsyncCacheCell<'T>(workflow: CancellationToken -> Task<'T>, ?isExpired: 'T -> bool) =

    let isExpired =
        match isExpired with
        | Some x -> ValueSome x
        | None -> ValueNone
    // we can't pre-initialize as we need the invocation to be tied to a CancellationToken
    let mutable cell =
        AsyncLazy(fun () -> Task.FromException<'T>(System.InvalidOperationException "AsyncCacheCell Not Yet initialized"))

    /// Synchronously check the value remains valid (to short-circuit an Await step where value not required)
    member _.IsValid() = cell.IsValid(isExpired)

    /// Gets or asynchronously recomputes a cached value depending on expiry and availability
    member _.Await(ct) = task {
        // Each concurrent execution takes a copy of the cell, and attempts to reuse the value; later used to ensure only one triggers the workflow
        let current = cell

        match! current.TryAwaitValid(isExpired) with
        | ValueSome res -> return res // ... if it's already / still valid, we're done
        | ValueNone ->
            // Prepare a new instance, with cancellation under our control (it won't start until the first Await on the LazyTask triggers it though)
            let newInstance = AsyncLazy(fun () -> workflow ct)
            // If there are concurrent executions, the first through the gate wins; everybody else awaits the instance the winner wrote
            let _ = Interlocked.CompareExchange(&cell, newInstance, current)
            return! cell.Await()
    }
