namespace Equinox.Core

/// Asynchronous Lazy<'T> used to gate a workflow to ensure at most once execution of a computation.
type AsyncLazy<'T>(workflow : Async<'T>) =
    let task = lazy (Async.StartAsTask workflow)

    /// Await the outcome of the computation.
    /// NOTE due to `Lazy<T>` semantics, failed attempts will cache any exception; AsyncCacheCell compensates for this
    member __.AwaitValue() = Async.AwaitTaskCorrect task.Value

    /// Synchronously check whether the value has been computed (and/or remains valid)
    member this.IsValid(?isExpired) =
        if not task.IsValueCreated then false else

        let value = task.Value
        if not value.IsCompleted || value.IsFaulted then false else

        match isExpired with
        | Some f -> not (f value.Result)
        | _ -> true

    /// Used to rule out values where the computation yielded an exception or the result has now expired
    member this.TryAwaitValid(?isExpired) : Async<'T option> = async {
        // Determines if the last attempt completed, but failed; For TMI see https://stackoverflow.com/a/33946166/11635
        if task.Value.IsFaulted then return None else

        let! result = this.AwaitValue()
        match isExpired with
        | Some f when f result -> return None
        | _ -> return Some result
    }

/// Generic async lazy caching implementation that admits expiration/recomputation/retry on exception semantics.
/// If `workflow` fails, all readers entering while the load/refresh is in progress will share the failure
/// The first caller through the gate triggers a recomputation attempt if the previous attempt ended in failure
type AsyncCacheCell<'T>(workflow : Async<'T>, ?isExpired : 'T -> bool) =
    let mutable cell = AsyncLazy workflow

    /// Synchronously check the value remains valid (to short-circuit an Async AwaitValue step where value not required)
    member __.IsValid() = cell.IsValid(?isExpired=isExpired)
    /// Gets or asynchronously recomputes a cached value depending on expiry and availability
    member __.AwaitValue() = async {
        let current = cell
        match! current.TryAwaitValid(?isExpired=isExpired) with
        | Some res -> return res
        | None ->
            // avoid unnecessary recomputation in cases where competing threads detect expiry;
            // the first write attempt wins, and everybody else reads off that value
            let _ = System.Threading.Interlocked.CompareExchange(&cell, AsyncLazy workflow, current)
            return! cell.AwaitValue()
    }
