namespace Equinox.Core

/// Asynchronous Lazy<'T> used to gate a workflow to ensure at most one in-flight computation.
type AsyncLazy<'T>(workflow : Async<'T>) =
    let task = lazy (Async.StartAsTask workflow)

    member __.AwaitValue() = Async.AwaitTaskCorrect task.Value
    // Used to rule out values where the computation yielded an exception or the result has now expired
    member internal this.TryValidate(?isExpired) : Async<'T option> = async {
        // Determines if the last attempt completed, but failed; For TMI see https://stackoverflow.com/a/33946166/11635
        let value = task.Value
        if value.IsCompleted && value.Status <> System.Threading.Tasks.TaskStatus.RanToCompletion then return None else

        let! result = this.AwaitValue()
        match isExpired with
        | Some f when f result -> return None
        | _ -> return Some result
    }

/// Generic async lazy caching implementation that admits expiration/recomputation semantics
/// If `workflow` fails, all readers entering while the load/refresh is in progress will share the failure
type AsyncCacheCell<'T>(workflow : Async<'T>, ?isExpired : 'T -> bool) =
    let mutable cell = AsyncLazy workflow

    /// Gets or asynchronously recomputes a cached value depending on expiry and availability
    member __.AwaitValue() = async {
        let current = cell
        match! current.TryValidate(?isExpired=isExpired) with
        | Some res -> return res
        | None ->
            // avoid unnecessary recomputation in cases where competing threads detect expiry;
            // the first write attempt wins, and everybody else reads off that value
            let _ = System.Threading.Interlocked.CompareExchange(&cell, AsyncLazy workflow, current)
            return! cell.AwaitValue()
    }
