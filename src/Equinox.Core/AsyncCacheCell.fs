namespace Equinox.Core

/// Generic async lazy caching implementation that admits expiration/recomputation/retry on exception semantics.
/// If `workflow` fails, all readers entering while the load/refresh is in progress will share the failure
/// The first caller through the gate triggers a recomputation attempt if the previous attempt ended in failure
type
#if !EQUINOX_CORE
    // NOT PUBLIC in Equinox.CosmosStore library - used internally to ensure the stored proc init is checked on first append once per container per process
    // PUBLIC in Equinox.Core (it can also be used independent of Equinox)
    internal
#endif
    AsyncCacheCell<'T>(startWorkflow : System.Func<CancellationToken, Task<'T>>, [<O; D null>]?isExpired: System.Func<'T, bool>) =

    let isValid = match isExpired with Some f -> not << f.Invoke | None -> fun _ -> true
    let mutable cell = AsyncLazy<'T>.Empty

    /// Synchronously check the value remains valid (to enable short-circuiting an Await step where value not required)
    member _.IsValid() =
        cell.TryCompleted() |> ValueOption.exists isValid

    /// Gets or asynchronously recomputes value depending on whether value passes the validity check
    member _.Await(ct: CancellationToken) = task {
        // Each concurrent execution takes a copy of the cell, and attempts to reuse the value; later used to ensure only one triggers the workflow
        let current = cell
        match! current.TryAwaitValid() with
        | ValueSome res when isValid res -> return res
        | _ ->
            // Prepare a new instance, with cancellation under our control (it won't start until the first Await on the LazyTask triggers it though)
            let newInstance = AsyncLazy<'T>(fun () -> startWorkflow.Invoke ct)
            // If there are concurrent executions, the first through the gate wins; everybody else awaits the instance the winner wrote
            let _ = Interlocked.CompareExchange(&cell, newInstance, current)
            return! cell.Await() }
    member x.Await() = Async.call x.Await
