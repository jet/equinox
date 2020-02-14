namespace Equinox

open Equinox.Core
open System.Runtime.InteropServices

/// Exception yielded by Stream.Transact after `count` attempts have yielded conflicts at the point of syncing with the Store
type MaxResyncsExhaustedException(count) =
   inherit exn(sprintf "Concurrency violation; aborting after %i attempts." count)

/// Central Application-facing API. Wraps the handling of decision or query flows in a manner that is store agnostic
type Stream<'event, 'state>
    (   log, stream : IStream<'event, 'state>, maxAttempts : int,
        [<Optional; DefaultParameterValue(null)>] ?createAttemptsExhaustedException,
        [<Optional; DefaultParameterValue(null)>] ?resyncPolicy) =

    let transact decide mapResult =
        let resyncPolicy = defaultArg resyncPolicy (fun _log _attemptNumber resyncF -> async { return! resyncF })
        let throwMaxResyncsExhaustedException attempts = MaxResyncsExhaustedException attempts
        let handleResyncsExceeded = defaultArg createAttemptsExhaustedException throwMaxResyncsExhaustedException
        Flow.transact (maxAttempts, resyncPolicy, handleResyncsExceeded) (stream, log) decide mapResult

    /// 0.  Invoke the supplied <c>interpret</c> function with the present state
    /// 1a. (if events yielded) Attempt to sync the yielded events events to the stream
    /// 1b. Tries up to <c>maxAttempts</c> times in the case of a conflict, throwing <c>MaxResyncsExhaustedException</c> to signal failure.
    member __.Transact(interpret : 'state -> 'event list) : Async<unit> =
        transact (fun state -> async { return (), interpret state }) (fun () _context -> ())

    /// 0.  Invoke the supplied <c>decide</c> function with the present state, holding the <c>'result</c>
    /// 1a. (if events yielded) Attempt to sync the yielded events events to the stream
    /// 1b. Tries up to <c>maxAttempts</c> times in the case of a conflict, throwing <c>MaxResyncsExhaustedException</c> to signal failure.
    /// 2.  Yield result
    member __.Transact(decide : 'state -> 'result * 'event list) : Async<'result> =
        transact (fun state -> async { return decide state }) (fun result _context -> result)

    /// 0.  Invoke the supplied <c>_Async_</c> <c>decide</c> function with the present state, holding the <c>'result</c>
    /// 1a. (if events yielded) Attempt to sync the yielded events events to the stream
    /// 1b. Tries up to <c>maxAttempts</c> times in the case of a conflict, throwing <c>MaxResyncsExhaustedException</c> to signal failure.
    /// 2.  Yield result
    member __.TransactAsync(decide : 'state -> Async<'result * 'event list>) : Async<'result> =
        transact decide (fun result _context -> result)

    /// 0.  Invoke the supplied <c>_Async_</c> <c>decide</c> function with the present state, holding the <c>'result</c>
    /// 1a. (if events yielded) Attempt to sync the yielded events events to the stream
    /// 1b. Tries up to <c>maxAttempts</c> times in the case of a conflict, throwing <c>MaxResyncsExhaustedException</c> to signal failure.
    /// 2.  Uses <c>mapResult</c> to render the final outcome from the <c>'result</c> and/or the final <c>ISyncContext</c>
    /// 3.  Yields the outcome
    member __.TransactAsyncEx(decide : 'state -> Async<'result * 'event list>, mapResult : 'result -> ISyncContext<'state> -> 'resultEx) : Async<'resultEx> =
        transact decide mapResult

    /// Project from the folded <c>'state</c>, without executing a decision flow as <c>Transact</c> does
    member __.Query(projection : 'state -> 'view) : Async<'view> =
        Flow.query (stream, log, fun syncState -> projection (syncState :> ISyncContext<'state>).State)

    /// Project from the stream's <c>'state<c> (including extended context), without executing a decision flow as <c>Transact<c> does
    member __.QueryEx(projection : ISyncContext<'state> -> 'view) : Async<'view> =
        Flow.query (stream, log, projection)

/// Store-agnostic <c>Context.Resolve</c> Options
type ResolveOption =
    /// Without consulting Cache or any other source, assume the Stream to be empty for the initial Query or Transact
    | AssumeEmpty
    /// If the Cache holds a value, use that without checking the backing store for updates, implying:
    /// - maximizing use of OCC for `Stream.Transact`
    /// - enabling potentially stale reads [in the face of multiple writers)] (for `Stream.Query`)
    | AllowStale
