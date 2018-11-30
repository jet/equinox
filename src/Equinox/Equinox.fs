namespace Equinox

/// Maintains a rolling folded State while Accumulating Events decided upon as part of a decision flow
type Context<'event, 'state>(fold, originState : 'state) =
    let accumulated = ResizeArray<'event>()

    /// The Events that have thus far been pended via the `decide` functions `Execute`/`Decide`d during the course of this flow
    member __.Accumulated =
        accumulated |> List.ofSeq

    /// The current folded State, based on the Stream's `originState` + any events that have been Accumulated during the the decision flow
    member __.State =
        __.Accumulated |> fold originState

    /// Invoke a decision function, gathering the events (if any) that it decides are necessary into the `Accumulated` sequence
    member __.Execute(decide : 'state -> 'event list) : unit =
        decide __.State |> accumulated.AddRange
    /// Invoke an Async decision function, gathering the events (if any) that it decides are necessary into the `Accumulated` sequence
    member __.ExecuteAsync(decide : 'state -> Async<'event list>) : Async<unit> = async {
        let! events = decide __.State
        accumulated.AddRange events }
    /// As per `Execute`, invoke a decision function, while also propagating a result yielded as the fst of an (outcome, events) pair
    member __.Decide(decide : 'state -> 'result * 'event list) : 'result =
        let result, newEvents = decide __.State
        accumulated.AddRange newEvents
        result
    /// As per `ExecuteAsync`, invoke a decision function, while also propagating a result yielded as the fst of an (outcome, events) pair
    member __.DecideAsync(decide : 'state -> Async<'result * 'event list>) : Async<'result> = async {
        let! result, newEvents = decide __.State
        accumulated.AddRange newEvents
        return result }

// Exception yielded by Handler.Decide* after `count` attempts have yielded conflicts at the point of syncing with the Store
type MaxResyncsExhaustedException(count) =
   inherit exn(sprintf "Retry failed after %i attempts." count)

/// Core Application-facing API. Wraps the handling of decision or query flow in a manner that is store agnostic
type Handler<'event, 'state>(fold, log, stream : Store.IStream<'event, 'state>, maxAttempts : int, ?mkAttemptsExhaustedException, ?resyncPolicy) =
    let contextArgs =
        let mkContext state : Context<'event,'state> = Context<'event,'state>(fold, state)
        let getEvents (ctx: Context<'event,'state>) = ctx.Accumulated
        mkContext,getEvents
    let resyncArgs =
        let resyncPolicy = defaultArg resyncPolicy (fun _log _attemptNumber f -> async { return! f })
        let throwMaxResyncsExhaustedException attempts = MaxResyncsExhaustedException attempts
        let handleResyncsExceeded = defaultArg mkAttemptsExhaustedException throwMaxResyncsExhaustedException
        maxAttempts,resyncPolicy,handleResyncsExceeded

    /// 0. Invoke the supplied `decide` function 1. attempt to sync the accumulated events to the stream 2. (contigent on success of 1) yield the outcome.
    /// Tries up to `maxAttempts` times in the case of a conflict, throwing MaxResyncsExhaustedException` to signal failure.
    member __.Decide(flow) =
        Flow.decide contextArgs resyncArgs (stream, log, flow)
    /// 0. Invoke the supplied _Async_ `decide` function 1. attempt to sync the accumulated events to the stream 2. (contigent on success of 1) yield the outcome
    /// Tries up to `maxAttempts` times in the case of a conflict, throwing MaxResyncsExhaustedException` to signal failure.
    member __.DecideAsync(flowAsync) =
        Flow.decideAsync contextArgs resyncArgs (stream, log, flowAsync)
    /// Low Level helper to allow one to obtain the complete state of a stream (including the position) in order to pass it as a continuation within the application
    member __.Raw: Async<Store.StreamToken * 'state> =
        Flow.query(stream, log, fun syncState -> syncState.Memento)
    /// Project from the folded `State` without executing a decision flow as `Decide` does
    member __.Query(projection : 'state -> 'view) : Async<'view> =
        Flow.query(stream, log, fun syncState -> projection syncState.State)

/// Exception yielded by after `count` attempts to complete an operation have taken place
type OperationRetriesExceededException(count : int, innerException : exn) =
   inherit exn(sprintf "Retry failed after %i attempts." count, innerException)

/// Helper for defining backoffs within the definition of a retry policy for a store.
module Retry =
    /// Wraps an async computation in a retry loop, passing the (1-based) count into the computation and,
    ///   (until `attempts` exhausted) on an exception matching the `filter`, waiting for the timespan chosen by `backoff` before retrying
    let withBackoff (maxAttempts : int) (backoff : int -> System.TimeSpan option) (f : int -> Async<'a>) : Async<'a> =
        if maxAttempts < 1 then raise (invalidArg "maxAttempts" "Should be >= 1")
        let rec go attempt = async {
            try
                let! res = f attempt
                return res
            with ex ->
                if attempt = maxAttempts then return raise (OperationRetriesExceededException(maxAttempts, ex))
                else
                    match backoff attempt with
                    | Some timespan -> do! Async.Sleep (int timespan.TotalMilliseconds)
                    | None -> ()
                    return! go (attempt + 1) }
        go 1