namespace Equinox

open Equinox.Core
open System.Runtime.InteropServices

/// Store-agnostic <c>Category.Resolve</c> Options
type ResolveOption =
    /// Without consulting Cache or any other source, assume the Stream to be empty for the initial Query or Transact
    | AssumeEmpty
    /// If the Cache holds any state, use that without checking the backing store for updates, implying:
    /// - maximizing how much we lean on Optimistic Concurrency Control when doing a `Transact` (you're still guaranteed a consistent outcome)
    /// - enabling stale reads [in the face of multiple writers (either in this process or in other processes)] when doing a `Query`
    | AllowStale

/// Exception yielded by Decider.Transact after `count` attempts have yielded conflicts at the point of syncing with the Store
type MaxResyncsExhaustedException(count) =
   inherit exn(sprintf "Concurrency violation; aborting after %i attempts." count)

/// Central Application-facing API. Wraps the handling of decision or query flows in a manner that is store agnostic
type Decider<'event, 'state>
    (   log, stream : IStream<'event, 'state>, maxAttempts : int,
        [<Optional; DefaultParameterValue(null)>] ?createAttemptsExhaustedException : int -> exn,
        [<Optional; DefaultParameterValue(null)>] ?resyncPolicy) =

    let transact decide mapResult =
        let resyncPolicy = defaultArg resyncPolicy (fun _log _attemptNumber resyncF -> async { return! resyncF })
        let createDefaultAttemptsExhaustedException attempts : exn = MaxResyncsExhaustedException attempts :> exn
        let createAttemptsExhaustedException = defaultArg createAttemptsExhaustedException createDefaultAttemptsExhaustedException
        Flow.transact (maxAttempts, resyncPolicy, createAttemptsExhaustedException) (stream, log) decide mapResult

    /// 0.  Invoke the supplied <c>interpret</c> function with the present state
    /// 1a. (if events yielded) Attempt to sync the yielded events events to the stream
    /// 1b. Tries up to <c>maxAttempts</c> times in the case of a conflict, throwing <c>MaxResyncsExhaustedException</c> to signal failure.
    member _.Transact(interpret : 'state -> 'event list) : Async<unit> =
        transact (fun context -> async { return (), interpret context.State }) (fun () _context -> ())

    /// 0.  Invoke the supplied <c>decide</c> function with the present state, holding the <c>'result</c>
    /// 1a. (if events yielded) Attempt to sync the yielded events events to the stream
    /// 1b. Tries up to <c>maxAttempts</c> times in the case of a conflict, throwing <c>MaxResyncsExhaustedException</c> to signal failure.
    /// 2.  Yield result
    member _.Transact(decide : 'state -> 'result * 'event list) : Async<'result> =
        transact (fun context -> async { return decide context.State }) (fun result _context -> result)

    /// 0.  Invoke the supplied <c>_Async_</c> <c>decide</c> function with the present state, holding the <c>'result</c>
    /// 1a. (if events yielded) Attempt to sync the yielded events events to the stream
    /// 1b. Tries up to <c>maxAttempts</c> times in the case of a conflict, throwing <c>MaxResyncsExhaustedException</c> to signal failure.
    /// 2.  Yield result
    member _.Transact(decide : 'state -> Async<'result * 'event list>) : Async<'result> =
        transact (fun context -> decide context.State) (fun result _context -> result)

    /// 0.  Invoke the supplied <c>_Async_</c> <c>decide</c> function with the present state (including extended context), holding the <c>'result</c>
    /// 1a. (if events yielded) Attempt to sync the yielded events events to the stream
    /// 1b. Tries up to <c>maxAttempts</c> times in the case of a conflict, throwing <c>MaxResyncsExhaustedException</c> to signal failure.
    /// 2.  Uses <c>mapResult</c> to render the final 'view from the <c>'result</c> and/or the final <c>ISyncContext</c>
    /// 3.  Yields the 'view
    member _.TransactEx(decide : ISyncContext<'state> -> Async<'result * 'event list>, mapResult : 'result -> ISyncContext<'state> -> 'view) : Async<'view> =
        transact decide mapResult

    /// Project from the folded <c>'state</c>, without executing a decision flow as <c>Transact</c> does
    member _.Query(projection : 'state -> 'view) : Async<'view> =
        Flow.query (stream, log, fun context -> projection (context :> ISyncContext<'state>).State)

    /// Project from the stream's <c>'state<c> (including extended context), without executing a decision flow as <c>Transact<c> does
    member _.QueryEx(projection : ISyncContext<'state> -> 'view) : Async<'view> =
        Flow.query (stream, log, projection)
