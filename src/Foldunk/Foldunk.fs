namespace Foldunk

open Serilog

/// Representation of a current known state of the stream, including the concurrency token (e.g. Stream Version number)
type StreamState<'token,'state,'event> = 'token * 'state option * 'event list

/// Complete interface to the state of an event stream, as dictated by requirements of Foldunk' Handler
type IEventStream<'token,'state,'event> =
    abstract Load: streamName: string -> log: ILogger
        -> Async<StreamState<'token, 'state, 'event>>
    abstract TrySync: streamName: string -> log: ILogger -> token: 'token * originState: 'state -> events: 'event list * state' : 'state
        -> Async<Result<StreamState<'token, 'state, 'event>, Async<StreamState<'token, 'state, 'event>>>>

/// Helpers for derivation of a StreamState
module StreamState =
    /// Represent a stream known to be empty
    let ofEmpty = -1, None, []
    /// Represent a [possibly compacted] array of events with a known token from a store.
    let ofTokenAndEvents token (events: 'event seq) = token, None, List.ofSeq events
    /// Represent a state known to have been persisted to the store
    let ofTokenAndKnownState token state = token, Some state, []
    /// Represent a state to be composed from a snapshot together with the successor events
    let ofTokenSnapshotAndEvents token stateSnapshot (successorEvents : 'event list) =
        token, Some stateSnapshot, successorEvents
    let toTokenAndState fold initial ((token, stateOption, events) : StreamState<'token,'state,'event>) : 'token * 'state =
        let baseState = 
            match stateOption with
            | Some state when List.isEmpty events -> state
            | Some state -> fold state events
            | None when List.isEmpty events -> fold initial events
            | None -> fold initial events
        token, baseState

/// Maintains state associated with a Command Handling flow
type DecisionState<'event, 'state>(fold, originState : 'state) =
    let accumulated = ResizeArray<'event>()

    member __.Execute (interpret : 'state -> 'event list) =
        interpret __.State |> accumulated.AddRange 
    member __.Decide (interpret : 'state -> 'outcome * 'event list) =
        let result, newEvents = interpret __.State
        accumulated.AddRange newEvents 
        result
    member __.Accumulated = accumulated |> List.ofSeq 
    member __.State = __.Accumulated |> fold originState 
    member __.Complete result =
        result, __.Accumulated

// Exception yielded by command handing function after `count` attempts have yielded conflicts at the point of syncing the result into the stream store
exception CommandAttemptsExceededException of count: int

type SyncState<'token, 'state, 'event>
    (   fold, initial, originState : StreamState<'token, 'state, 'event>,
        trySync : ILogger -> 'token * 'state -> 'event list * 'state
            -> Async<   Result< StreamState<'token, 'state, 'event>,
                                Async<StreamState<'token, 'state, 'event>>>>) =
    let tokenAndState = ref (StreamState.toTokenAndState fold initial originState)
    let tryOr log events handleFailure = async {
        let proposedState = fold (snd !tokenAndState) events
        let! res = trySync log !tokenAndState (events, proposedState)
        match res with
        | Error resync ->
            return! handleFailure resync
        | Ok streamState' ->
            tokenAndState := StreamState.toTokenAndState fold initial streamState'
            return true }

    member __.State = snd !tokenAndState
    member __.Token = fst !tokenAndState
    member __.CreateDecisionState(): DecisionState<'event, 'state> = 
        DecisionState<'event, 'state>(fold, __.State)
    member __.TryOrResync log events =
        let resyncInPreparationForRetry resync = async {
            let! streamState' = resync
            tokenAndState := StreamState.toTokenAndState fold initial streamState'
            return false }
        tryOr log events resyncInPreparationForRetry
    member __.TryOrThrow log events attempt =
        let throw _resync = async { return raise <| CommandAttemptsExceededException attempt }
        tryOr log events throw |> Async.Ignore
        
module Handler =
    /// Load the state of a stream from the given store
    let load fold initial streamName (stream : IEventStream<_,_,_>) log id = async {
        let streamName = streamName id
        let! streamState = stream.Load streamName log
        return SyncState(fold, initial, streamState, stream.TrySync streamName) }

    /// Process a command, ensuring a consistent final state is established on the stream.
    /// 1.  make a decision given the known state
    /// 2a. if no changes required, exit with known state
    /// 2b. if saved without conflict, exit with updated state
    /// 2b. if conflicting changes, loop to retry against updated state 
    let run (log : ILogger)
            (maxAttempts : int)
            (sync : SyncState<'token, 'state, 'event>)
            (decide : DecisionState<'event, 'state> -> Async<'output * 'event list>)
            : Async<'output> =
        if maxAttempts < 1 then raise <| System.ArgumentOutOfRangeException("maxAttempts", maxAttempts, "should be >= 1")
        /// Run a decision cycle - decide what events should be appended given the presented state
        let rec loop attempt: Async<'output> = async {
            //let token, currentState = interpreter.Fold currentState
            let log = log.ForContext("Attempt", attempt)
            let ctx = sync.CreateDecisionState()
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