namespace Equinox.Core

/// Asynchronous Lazy<'T> used to gate a workflow to ensure at most once execution of a computation.
type
#if !EQUINOX_CORE
    // NOT PUBLIC in Equinox library - used internally in the impl of CacheEntry
    // PUBLIC in Equinox.Core (which also uses it in the impl of AsyncCacheCell)
    internal
#endif
      AsyncLazy<'T>(startTask: System.Func<Task<'T>>) =
    // NOTE due to `Lazy<T>` semantics, failed attempts will cache any exception; AsyncCacheCell compensates for this by rolling over to a new instance
    let workflow = lazy startTask.Invoke()

    /// Synchronously peek at what's been previously computed (if it's not Empty, or the last attempt Faulted).
    member _.TryCompleted() =
        if not workflow.IsValueCreated then ValueNone else

        let t = workflow.Value
        if t.Status <> System.Threading.Tasks.TaskStatus.RanToCompletion then ValueNone
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
    static member val Empty = AsyncLazy(fun () -> Task.FromException<'T>(System.InvalidOperationException "Uninitialized AsyncLazy"))
