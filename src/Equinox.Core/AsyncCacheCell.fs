namespace Equinox.Core

/// Asynchronous Lazy<'T> that guarantees workflow will be executed at most once.
type AsyncLazy<'T>(workflow : Async<'T>) =
    let task = lazy(Async.StartAsTask workflow)
    member __.AwaitValue() = Async.AwaitTaskCorrect task.Value
    member internal __.PeekInternalTask = task

/// Generic async lazy caching implementation that admits expiration/recomputation semantics
/// If `workflow` fails, all readers entering while the load/refresh is in progress will share the failure
type AsyncCacheCell<'T>(workflow : Async<'T>, ?isExpired : 'T -> bool) =
    let mutable currentCell = AsyncLazy workflow

    let initializationFailed (value : System.Threading.Tasks.Task<_>) =
        // for TMI on this, see https://stackoverflow.com/a/33946166/11635
        value.IsCompleted && value.Status <> System.Threading.Tasks.TaskStatus.RanToCompletion

    let update cell = async {
        // avoid unnecessary recomputation in cases where competing threads detect expiry;
        // the first write attempt wins, and everybody else reads off that value
        let _ = System.Threading.Interlocked.CompareExchange(&currentCell, AsyncLazy workflow, cell)
        return! currentCell.AwaitValue()
    }

    /// Enables callers to short-circuit the gate by checking whether a value has been computed
    member __.PeekIsValid() =
        let cell = currentCell
        let currentState = cell.PeekInternalTask
        if not currentState.IsValueCreated then false else

        let value = currentState.Value
        not (initializationFailed value)
        && (match isExpired with Some f -> not (f value.Result) | _ -> false)

    /// Gets or asynchronously recomputes a cached value depending on expiry and availability
    member __.AwaitValue() = async {
        let cell = currentCell
        let currentState = cell.PeekInternalTask
        // If the last attempt completed, but failed, we need to treat it as expired
        if currentState.IsValueCreated && initializationFailed currentState.Value then
            return! update cell
        else
            let! current = cell.AwaitValue()
            match isExpired with
            | Some f when f current -> return! update cell
            | _ -> return current
    }
