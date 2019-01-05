namespace Equinox

/// Maintains a rolling folded State while Accumulating Events decided upon as part of a decision flow
type Context<'event, 'state>(fold : 'state -> 'event seq -> 'state, originState : 'state) =
    let accumulated = ResizeArray<'event>()

    /// The Events that have thus far been pended via the `decide` functions `Execute`/`Decide`d during the course of this flow
    member __.Accumulated : 'event list =
        accumulated |> List.ofSeq

    /// The current folded State, based on the Stream's `originState` + any events that have been Accumulated during the the decision flow
    member __.State : 'state =
        accumulated |> fold originState

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

/// Central Application-facing API. Wraps the handling of decision or query flows in a manner that is store agnostic
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

    /// 0. Invoke the supplied `flow` function 1. attempt to sync the accumulated events to the stream 2. (contigent on success of 1) yield the outcome.
    /// Tries up to `maxAttempts` times in the case of a conflict, throwing MaxResyncsExhaustedException` to signal failure.
    member __.Decide(flow : Context<'event,'state> -> 'result) : Async<'result> =
        Flow.decide contextArgs resyncArgs (stream, log, flow)
    /// 0. Invoke the supplied _Async_ `flow` function 1. attempt to sync the accumulated events to the stream 2. (contigent on success of 1) yield the outcome
    /// Tries up to `maxAttempts` times in the case of a conflict, throwing MaxResyncsExhaustedException` to signal failure.
    member __.DecideAsync(flowAsync : Context<'event,'state> -> Async<'result>) : Async<'result> =
        Flow.decideAsync contextArgs resyncArgs (stream, log, flowAsync)
    /// Project from the folded `State` without executing a decision flow as `Decide` does
    member __.Query(projection : 'state -> 'view) : Async<'view> =
        Flow.query(stream, log, fun syncState -> projection syncState.State)
    /// Low Level helper to allow one to obtain a reference to a stream and state pair (including the position) in order to pass it as a continuation within the application
    /// Such a memento is then held within the application and passed in lieue of a StreamId to the StreamResolver in order to avoid having to reload state
    member __.CreateMemento(): Async<Store.StreamToken * 'state> =
        Flow.query(stream, log, fun syncState -> syncState.Memento)

/// Store-agnostic way to specify a target Stream (with optional known State) to pass to a Resolver
[<NoComparison; NoEquality>]
type Target =
    /// Recommended way to specify a stream name
    | CatId of category: string * id: string
    /// Resolve the stream, but stub the attempt to Load based on a strong likelihood that a stream is empty and hence it's reasonable to optimistically avoid
    /// a Load roundtrip; if that turns out not to be the case, the price is to have to do a re-run after the resync
    | CatIdEmpty of category: string * id: string
    /// prefer CatId
    | DeprecatedRawName of streamName: string