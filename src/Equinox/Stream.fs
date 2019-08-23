namespace Equinox

open System.Runtime.InteropServices

// Exception yielded by Stream.Transact after `count` attempts have yielded conflicts at the point of syncing with the Store
type MaxResyncsExhaustedException(count) =
   inherit exn(sprintf "Concurrency violation; aborting after %i attempts." count)

/// Central Application-facing API. Wraps the handling of decision or query flows in a manner that is store agnostic
type Stream<'event, 'state>
    (   log, stream : Store.IStream<'event, 'state>, maxAttempts : int,
        [<Optional; DefaultParameterValue(null)>]?mkAttemptsExhaustedException,
        [<Optional; DefaultParameterValue(null)>]?resyncPolicy) =
    let transact f =
        let resyncPolicy = defaultArg resyncPolicy (fun _log _attemptNumber f -> async { return! f })
        let throwMaxResyncsExhaustedException attempts = MaxResyncsExhaustedException attempts
        let handleResyncsExceeded = defaultArg mkAttemptsExhaustedException throwMaxResyncsExhaustedException
        Flow.transact (maxAttempts,resyncPolicy,handleResyncsExceeded) (stream, log) f

    /// 0. Invoke the supplied `interpret` function with the present state 1. attempt to sync the accumulated events to the stream
    /// Tries up to `maxAttempts` times in the case of a conflict, throwing MaxResyncsExhaustedException` to signal failure.
    member __.Transact(interpret : 'state -> 'event list) : Async<unit> = transact (fun state -> async { return (), interpret state })
    /// 0. Invoke the supplied `decide` function with the present state 1. attempt to sync the accumulated events to the stream 2. yield result
    /// Tries up to `maxAttempts` times in the case of a conflict, throwing MaxResyncsExhaustedException` to signal failure.
    member __.Transact(decide : 'state -> 'result*'event list) : Async<'result> = transact (fun state -> async { return decide state })
    /// 0. Invoke the supplied _Async_ `decide` function with the present state 1. attempt to sync the accumulated events to the stream 2. yield result
    /// Tries up to `maxAttempts` times in the case of a conflict, throwing MaxResyncsExhaustedException` to signal failure.
    member __.TransactAsync(decide : 'state -> Async<'result*'event list>) : Async<'result> = transact decide

    /// Project from the folded `State` without executing a decision flow as `Decide` does
    member __.Query(projection : 'state -> 'view) : Async<'view> = Flow.query(stream, log, fun syncState -> projection syncState.State)
    /// Project from the folded `State` (with the current version of the stream supplied for context) without executing a decision flow as `Decide` does
    member __.QueryEx(projection : int64 -> 'state -> 'view) : Async<'view> = Flow.query(stream, log, fun syncState -> projection syncState.Version syncState.State)

    /// Low-level helper to allow one to obtain a reference to a stream and state pair (including the position) in order to pass it as a continuation within the application
    /// Such a memento is then held within the application and passed in lieue of a StreamId to the StreamResolver in order to avoid having to reload state
    member __.CreateMemento(): Async<Store.StreamToken * 'state> = Flow.query(stream, log, fun syncState -> syncState.Memento)

/// Store-agnostic way to specify a target Stream (with optional known State) to pass to a Resolver
[<NoComparison; NoEquality>]
type Target =
    /// Recommended way to specify a stream identifier; a category identifier and an aggregate identity
    | AggregateId of category: string * id: string
    /// Resolve the stream, but stub the attempt to Load based on a strong likelihood that a stream is empty and hence it's reasonable to optimistically avoid
    /// a Load roundtrip; if that turns out not to be the case, the price is to have to do a re-run after the resync
    | AggregateIdEmpty of category: string * id: string
    /// Specify the full stream name. NB use of AggregateId is recommended for simplicity and consistency.
    | StreamName of streamName: string