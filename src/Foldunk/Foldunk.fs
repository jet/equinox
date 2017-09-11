namespace Foldunk

open Serilog

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

/// For use in application level code; general across store implementations under the Foldunk.Stores namespace
module Handler =
    [<RequireQualifiedAccess>]
    module Impl =
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
            member __.CreateDecisionState(): DecisionState<'event, 'state> = 
                DecisionState<'event, 'state>(fold, __.State)
            member __.TryOrResync log events =
                let resyncInPreparationForRetry resync = async {
                    let! streamState' = resync
                    tokenAndState := toTokenAndState fold initial streamState'
                    return false }
                tryOr log events resyncInPreparationForRetry
            member __.TryOrThrow log events attempt =
                let throw _resync = async { return raise <| CommandAttemptsExceededException attempt }
                tryOr log events throw |> Async.Ignore

    /// Complete interface to the state of any given event stream, as dictated by requirements of Foldunk' Handler
    type IEventStream<'state,'event> =
        abstract Load: streamName: string -> log: ILogger
            -> Async<StreamState<'state, 'event>>
        abstract TrySync: streamName: string -> log: ILogger -> token: StreamToken * originState: 'state -> events: 'event list * state' : 'state
            -> Async<Result<StreamState<'state, 'event>, Async<StreamState<'state, 'event>>>>

    /// Load the state of a stream from the given store
    let load fold initial streamName (stream : IEventStream<_,_>) log id = async {
        let streamName = streamName id
        let! streamState = stream.Load streamName log
        return Impl.SyncState(fold, initial, streamState, stream.TrySync streamName) }

    /// Process a command, ensuring a consistent final state is established on the stream.
    /// 1.  make a decision given the known state
    /// 2a. if no changes required, exit with known state
    /// 2b. if saved without conflict, exit with updated state
    /// 2b. if conflicting changes, loop to retry against updated state 
    let run (log : ILogger)
            (maxAttempts : int)
            (sync : Impl.SyncState<'state, 'event>)
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