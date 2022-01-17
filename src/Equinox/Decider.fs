namespace Equinox

open Equinox.Core
open System.Runtime.InteropServices

/// Exception yielded by Decider.Transact after `count` attempts have yielded conflicts at the point of syncing with the Store
type MaxResyncsExhaustedException(count) =
   inherit exn(sprintf "Concurrency violation; aborting after %i attempts." count)

/// Central Application-facing API. Wraps the handling of decision or query flows in a manner that is store agnostic
type Decider<'event, 'state>
    (   log, stream : IStream<'event, 'state>, maxAttempts : int,
        [<Optional; DefaultParameterValue(null)>] ?createAttemptsExhaustedException : int -> exn,
        [<Optional; DefaultParameterValue(null)>] ?resyncPolicy,
        ?defaultOption) =

    let options : LoadOption option -> LoadOption = function
        | None -> defaultArg defaultOption LoadOption.Load
        | Some o -> o

    let transact maybeOverride decide mapResult =
        let resyncPolicy = defaultArg resyncPolicy (fun _log _attemptNumber resyncF -> async { return! resyncF })
        let createDefaultAttemptsExhaustedException attempts : exn = MaxResyncsExhaustedException attempts :> exn
        let createAttemptsExhaustedException = defaultArg createAttemptsExhaustedException createDefaultAttemptsExhaustedException
        Flow.transact (options maybeOverride) (maxAttempts, resyncPolicy, createAttemptsExhaustedException) (stream, log) decide mapResult

    let query option args = Flow.query (options option) args

    /// 0.  Invoke the supplied <c>interpret</c> function with the present state
    /// 1a. (if events yielded) Attempt to sync the yielded events events to the stream
    /// 1b. Tries up to <c>maxAttempts</c> times in the case of a conflict, throwing <c>MaxResyncsExhaustedException</c> to signal failure.
    member _.Transact(interpret : 'state -> 'event list, ?option) : Async<unit> =
        transact option (fun context -> async { return (), interpret context.State }) (fun () _context -> ())

    /// 0.  Invoke the supplied <c>decide</c> function with the present state, holding the <c>'result</c>
    /// 1a. (if events yielded) Attempt to sync the yielded events events to the stream
    /// 1b. Tries up to <c>maxAttempts</c> times in the case of a conflict, throwing <c>MaxResyncsExhaustedException</c> to signal failure.
    /// 2.  Yield result
    member _.Transact(decide : 'state -> 'result * 'event list, ?option) : Async<'result> =
        transact option (fun context -> async { return decide context.State }) (fun result _context -> result)

    /// 0.  Invoke the supplied <c>_Async_</c> <c>decide</c> function with the present state, holding the <c>'result</c>
    /// 1a. (if events yielded) Attempt to sync the yielded events events to the stream
    /// 1b. Tries up to <c>maxAttempts</c> times in the case of a conflict, throwing <c>MaxResyncsExhaustedException</c> to signal failure.
    /// 2.  Yield result
    member _.Transact(decide : 'state -> Async<'result * 'event list>, ?option) : Async<'result> =
        transact option (fun context -> decide context.State) (fun result _context -> result)

    /// 0.  Invoke the supplied <c>_Async_</c> <c>decide</c> function with the present state (including extended context), holding the <c>'result</c>
    /// 1a. (if events yielded) Attempt to sync the yielded events events to the stream
    /// 1b. Tries up to <c>maxAttempts</c> times in the case of a conflict, throwing <c>MaxResyncsExhaustedException</c> to signal failure.
    /// 2.  Uses <c>mapResult</c> to render the final 'view from the <c>'result</c> and/or the final <c>ISyncContext</c>
    /// 3.  Yields the 'view
    member _.TransactEx(decide : ISyncContext<'state> -> Async<'result * 'event list>, mapResult : 'result -> ISyncContext<'state> -> 'view, ?option) : Async<'view> =
        transact option decide mapResult

    /// Project from the folded <c>'state</c>, without executing a decision flow as <c>Transact</c> does
    member _.Query(projection : 'state -> 'view, ?option) : Async<'view> =
        query option (stream, log, fun context -> projection (context :> ISyncContext<'state>).State)

    /// Project from the stream's <c>'state<c> (including extended context), without executing a decision flow as <c>Transact<c> does
    member _.QueryEx(projection : ISyncContext<'state> -> 'view, ?option) : Async<'view> =
        query option (stream, log, projection)
