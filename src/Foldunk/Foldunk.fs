namespace Foldunk

open Serilog

/// Exposes State and Accumulates events associated with a Handler.Decide-driven decision flow
type DecisionContext<'event, 'state>(fold, originState : 'state) =
    let accumulated = ResizeArray<'event>()
    /// Execute an interpret function, gathering the events (if any) that it decides are necessary into the `Accummulated` sequence
    member __.Execute (interpret : 'state -> 'event list) =
        interpret __.State |> accumulated.AddRange
    /// Execute an `interpret` function as `Execute` does, while also propagating a result output thats is yielded as the fst of (result,events) pair
    member __.Decide (interpret : 'state -> 'result * 'event list) =
        let result, newEvents = interpret __.State
        accumulated.AddRange newEvents
        result
    /// Execute an _Async_ `interpret` function as `Execute` does, while also propagating a result output thats is yielded as the fst of (result,events) pair
    // TODO add test coverage
    member __.DecideAsync (interpret : 'state -> Async<'outcome * 'event list>) = async {
        let! result, newEvents = interpret __.State
        accumulated.AddRange newEvents
        return result }
    /// The events that have been pended into this Context as decided by the interpret functions that have been run in the course of the Decision
    member __.Accumulated = accumulated |> List.ofSeq
    /// The current folded State, based on the initial Stream State + any events that have been added in the course of this Decision
    member __.State = __.Accumulated |> fold originState
    /// Complete the Decision, yielding the supplied `outcome` (which may be unit)
    member __.Complete outcome =
        outcome, __.Accumulated

/// Internal data stuctures. While these are intended to be legible, understand the abstractions involved is only necessary if you are implemening a Store or a decorator thereof.
[<RequireQualifiedAccess>]
module Storage =
    /// Store-specific opaque token to be used for synchronization purposes
    [<NoComparison>]
    type StreamToken = { value : obj }

    /// Representation of a current known state of the stream, including the concurrency token (e.g. Stream Version number)
    type StreamState<'state,'event> = StreamToken * 'state option * 'event list

    /// Helpers for derivation of a StreamState - shared between EventStore and MemoryStore
    module StreamState =
        /// Represent a [possibly compacted] array of events with a known token from a store.
        let ofTokenAndEvents (token : StreamToken) (events: 'event seq) = token, None, List.ofSeq events
        /// Represent a state known to have been persisted to the store
        let ofTokenAndKnownState token state = token, Some state, []
        /// Represent a state to be composed from a snapshot together with the successor events
        let ofTokenSnapshotAndEvents token (snapshotState : 'state) (successorEvents : 'event list) =
            token, Some snapshotState, successorEvents

    [<NoEquality; NoComparison; RequireQualifiedAccess>]
    type SyncResult<'state, 'event> =
        | Written of StreamState<'state, 'event>
        | Conflict of Async<StreamState<'state, 'event>>

/// Store-agnostic interface to the state of any given stream of events. Not intended for direct use by consumer code; this is an abstract interface to be implemented by a Store in order for the Application to be able to execute a standarized Flow via the Handler type
type IStream<'state,'event> =
    /// Obtain the state from the stream named `streamName`
    abstract Load: log: ILogger
        -> Async<Storage.StreamState<'state, 'event>>
    /// Synchronize the state of the stream named `streamName`, appending the supplied `events`, in order to establish the supplied `state` value
    /// NB the central precondition is that the Stream has not diverged from the state represented by `token`;  where this is not met, the response signals the
    ///    need to reprocess by yielding an Error bearing the conflicting StreamState with which to retry (loaded in a specific manner optimal for the store)
    abstract TrySync: log: ILogger -> token: Storage.StreamToken * originState: 'state -> events: 'event list * state' : 'state
        // - Written: to signify Synchronization has succeeded, taking the yielded Stream State as the representation of what is understood to be the state
        // - Conflict: to signify the synch failed and the deicsion hence needs to be rerun based on the updated Stream State with which the changes conflicted
        -> Async<Storage.SyncResult<'state, 'event>>

// Exception yielded by Handler.Decide after `count` attempts have yielded conflicts at the point of syncing with the Store
exception FlowAttemptsExceededException of count: int

/// Internal implementation of the Store agnostic load + run/render. See Handler for App-facing APIs.
module private Flow =
    /// Represents stream and folding state between the load and run/render phases
    type SyncState<'state, 'event>
        (   fold, initial, originState : Storage.StreamState<'state, 'event>,
            trySync : ILogger -> Storage.StreamToken * 'state -> 'event list * 'state -> Async<Storage.SyncResult<'state, 'event>>) =
        let toTokenAndState fold initial ((token, stateOption, events) : Storage.StreamState<'state,'event>) : Storage.StreamToken * 'state =
            let baseState =
                match stateOption with
                | Some state when List.isEmpty events -> state
                | Some state -> fold state events
                | None when List.isEmpty events -> fold initial events
                | None -> fold initial events
            token, baseState
        let tokenAndState = ref (toTokenAndState fold initial originState)
        let tryOr log events handleFailure = async {
            let proposedState = fold (snd !tokenAndState) events
            let! res = trySync log !tokenAndState (events, proposedState)
            match res with
            | Storage.SyncResult.Conflict resync ->
                return! handleFailure resync
            | Storage.SyncResult.Written streamState' ->
                tokenAndState := toTokenAndState fold initial streamState'
                return true }

        member __.State = snd !tokenAndState
        member __.Token = fst !tokenAndState
        member __.CreateDecisionContext(): DecisionContext<'event, 'state> =
            DecisionContext<'event, 'state>(fold, __.State)
        member __.TryOrResync log events =
            let resyncInPreparationForRetry resync = async {
                let! streamState' = resync
                tokenAndState := toTokenAndState fold initial streamState'
                return false }
            tryOr log events resyncInPreparationForRetry
        member __.TryOrThrow log events attempt =
            let throw _ = async { return raise <| FlowAttemptsExceededException attempt }
            tryOr log events throw |> Async.Ignore

    /// Load the state of a stream from the given stream
    let load(stream : IStream<'state,'event>)
            (log : Serilog.ILogger)
            (fold : 'state -> 'event list -> 'state)
            (initial : 'state)
            : Async<SyncState<'state, 'event>> = async {
        let! streamState = stream.Load log
        return SyncState(fold, initial, streamState, stream.TrySync) }

    /// Render a view of the loaded stare for the purposes of querying the folded State
    let render (query : 'state -> 'view) (syncState : SyncState<'state, 'event>) : 'view =
        syncState.State |> query

    /// Process a command, ensuring a consistent final state is established on the stream.
    /// 1.  make a decision given the known state
    /// 2a. if no changes required, exit with known state
    /// 2b. if saved without conflict, exit with updated state
    /// 2b. if conflicting changes, loop to retry against updated state
    let run (sync : SyncState<'state, 'event>)
            (log : ILogger)
            (maxAttempts : int)
            (decide : DecisionContext<'event, 'state> -> Async<'output * 'event list>)
            : Async<'output> =
        if maxAttempts < 1 then raise <| System.ArgumentOutOfRangeException("maxAttempts", maxAttempts, "should be >= 1")
        /// Run a decision cycle - decide what events should be appended given the presented state
        let rec loop attempt: Async<'output> = async {
            //let token, currentState = interpreter.Fold currentState
            let log = log.ForContext("Attempt", attempt)
            let ctx = sync.CreateDecisionContext()
            let! outcome, events = decide ctx
            if List.isEmpty events then
                return outcome
            elif attempt = maxAttempts then
                do! sync.TryOrThrow log events attempt
                return outcome
            else
                let! committed = sync.TryOrResync log events
                if not committed then
                    return! loop (attempt + 1)
                else
                    return outcome }
        /// Commence, processing based on the incoming state
        loop 1

/// Core Application-facing API. Wraps the handling of decision or query flow in a manner that is store agnostic; general across *Store implementations
type Handler<'state,'event>(fold, initial, ?maxAttempts) =
    /// Execute the `decide` decision flow, potentially looping `maxAttempts` times. If not syncing, use Query.
    /// Throws `FlowAttemptsExceededException` if when attempts exhausted 
    member __.Decide decide log (stream : IStream<'state,'event>) = async {
        let! syncState = Flow.load stream log fold initial
        return! Flow.run syncState log (defaultArg maxAttempts 3) decide }
    /// Project from the folded `State` without executing a decision flow as `Decide` does
    member __.Query query log (stream : IStream<'state,'event>) : Async<'state> = async {
        let! syncState = Flow.load stream log fold initial
        return syncState |> Flow.render query }

/// Helper for define backoff policies as part of the retry policy for a store.
module Retry =
    /// Wraps an async computation in a retry loop, passing the (1-based) count into the computation and,
    ///   (until `attempts` exhausted) on an exception matching the `filter`, waiting for the timespan chosen by `backoff` before retrying
    let withBackoff (maxAttempts : int) (backoff : int -> System.TimeSpan option) (f : int -> Async<'a>) =
        if maxAttempts < 1 then raise (invalidArg "maxAttempts" "Should be >= 1")
        let rec go attempt = async {
            try
                let! res = f attempt
                return res
            with ex ->
                if attempt = maxAttempts then return raise (exn(sprintf "Retry failed after %i attempts." maxAttempts, ex))
                else
                    match backoff attempt with
                    | Some timespan -> do! Async.Sleep (int timespan.TotalMilliseconds)
                    | None -> ()
                    return! go (attempt + 1) }
        go 1