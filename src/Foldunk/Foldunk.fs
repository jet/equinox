namespace Foldunk

open Serilog

/// Maintains state associated with a Command Handling flow
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

// Exception yielded by command handing function after `count` attempts have yielded conflicts at the point of syncing the result into the stream store
exception CommandAttemptsExceededException of count: int

[<RequireQualifiedAccess>]
module Internal =
    /// Store-specific opaque token to be used for synchronization purposes
    [<NoComparison>]
    type StreamToken = { value : obj }

    /// Foldunk-internal representation of a current known state of the stream, including the concurrency token (e.g. Stream Version number)
    type StreamState<'state,'event> = StreamToken * 'state option * 'event list

    /// Helpers for derivation of a StreamState - shared between EventStore and InMemoryStore
    module StreamState =
        /// Represent a [possibly compacted] array of events with a known token from a store.
        let ofTokenAndEvents (token : StreamToken) (events: 'event seq) = token, None, List.ofSeq events
        /// Represent a state known to have been persisted to the store
        let ofTokenAndKnownState token state = token, Some state, []
        /// Represent a state to be composed from a snapshot together with the successor events
        let ofTokenSnapshotAndEvents token (snapshotState : 'state) (successorEvents : 'event list) =
            token, Some snapshotState, successorEvents

    /// Internal implementation
    type SyncState<'state, 'event>
        (   fold, initial, originState : StreamState<'state, 'event>,
            trySync : ILogger -> StreamToken * 'state -> 'event list * 'state -> Async<Result<StreamState<'state, 'event>, Async<StreamState<'state, 'event>>>>) =
        let toTokenAndState fold initial ((token, stateOption, events) : StreamState<'state,'event>) : StreamToken * 'state =
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
            | Error resync ->
                return! handleFailure resync
            | Ok streamState' ->
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
            let throw _resync = async { return raise <| CommandAttemptsExceededException attempt }
            tryOr log events throw |> Async.Ignore

/// Store-agnostic interface to the state of any given event stream, as dictated by requirements of Foldunk's Handler
type IEventStream<'state,'event> =
    /// Obtain the state from the stream named `streamName`
    abstract Load: streamName: string -> log: ILogger
        -> Async<Internal.StreamState<'state, 'event>>
    /// Synchronize the state of the stream named `streamName`, appending the supplied `events`, in order to establish the supplied `state` value
    /// NB the central precondition is that the Stream has not diverged from the state represented by `token`;  where this is not met, the response signals the
    ///    need to reprocess by yielding an Error bearing the conflicting StreamState with which to retry (loaded in a specific manner optimal for the store)
    abstract TrySync: streamName: string -> log: ILogger -> token: Internal.StreamToken * originState: 'state -> events: 'event list * state' : 'state
        // Response is one of:
        // - Ok: to signify Synchronization has succeeded, taking the yielded Stream State as the representation of what is understood to be the state
        // - Error: to signify the synch failed and the deicsion hence needs to be rerun based on the updated Stream State with which the changes conflicted
        -> Async<Result<Internal.StreamState<'state, 'event>, Async<Internal.StreamState<'state, 'event>>>>

/// For use in application level code; general across store implementations under the Foldunk.Stores namespace
module Handler =
    /// Load the state of a stream from the given stream
    let load(fold : 'state -> 'event list -> 'state)
            (initial : 'state)
            (streamName : 'id -> string)
            (stream : IEventStream<'state,'event>)
            (log : Serilog.ILogger)
            (id : 'id)
            : Async<Internal.SyncState<'state, 'event>> = async {
        let streamName = streamName id
        let! streamState = stream.Load streamName log
        return Internal.SyncState(fold, initial, streamState, stream.TrySync streamName) }

    /// Process a command, ensuring a consistent final state is established on the stream.
    /// 1.  make a decision given the known state
    /// 2a. if no changes required, exit with known state
    /// 2b. if saved without conflict, exit with updated state
    /// 2b. if conflicting changes, loop to retry against updated state
    let run (log : ILogger)
            (maxAttempts : int)
            (sync : Internal.SyncState<'state, 'event>)
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