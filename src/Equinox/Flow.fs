/// Internal implementation of the Store agnostic load + run/render. See Handler.fs for App-facing APIs.
namespace Equinox

open Equinox.Store
open Serilog

/// Represents stream and folding state between the load and run/render phases
type internal SyncState<'event, 'state>
    (   originState : StreamToken * 'state,
        trySync : ILogger -> StreamToken * 'state -> 'event list -> Async<SyncResult<'state>>) =
    let mutable tokenAndState = originState

    member __.Memento = tokenAndState
    member __.State = snd __.Memento

    member __.TryOr log events (handleFailureResync : Async<StreamToken*'state> -> Async<bool>) : Async<bool> = async {
        let! res = trySync log tokenAndState events
        match res with
        | SyncResult.Conflict resync ->
            return! handleFailureResync resync
        | SyncResult.Written (token', streamState') ->
            tokenAndState <- token', streamState'
            return true }
    member __.TryOrResync runResync (attemptNumber: int) (log : ILogger) events : Async<bool> =
        let resyncInPreparationForRetry resync = async {
            let! streamState' = runResync log attemptNumber resync
            tokenAndState <- streamState'
            return false }
        __.TryOr log events resyncInPreparationForRetry

module internal Flow =
    /// Obtain a representation of the current state and metadata from the underlying storage stream
    let load (log : ILogger) (stream : IStream<'event, 'state>)
        : Async<SyncState<'event, 'state>> = async {
        let! streamState = stream.Load log
        return SyncState(streamState, stream.TrySync) }

    /// Process a command, ensuring a consistent final state is established on the stream.
    /// 1.  make a decision predicated on the known state
    /// 2a. if no changes required, exit with known state
    /// 2b. if saved without conflict, exit with updated state
    /// 2b. if conflicting changes, retry by recommencing at step 1 with the updated state
    let inline run (log : ILogger) (maxSyncAttempts : int, resyncRetryPolicy, createMaxAttemptsExhaustedException)
        (syncState : SyncState<'event, 'state>)
        (decide : 'state -> Async<'result * 'event list>)
        : Async<'result> =
        if maxSyncAttempts < 1 then raise <| System.ArgumentOutOfRangeException("maxSyncAttempts", maxSyncAttempts, "should be >= 1")
        /// Run a decision cycle - decide what events should be appended given the presented state
        let rec loop attempt: Async<'result> = async {
            let log = if attempt = 1 then log else log.ForContext("syncAttempt", attempt)
            let! result, events = decide syncState.State
            if List.isEmpty events then
                log.Debug "No events generated"
                return result
            elif attempt = maxSyncAttempts then
                log.Debug "Max Sync Attempts exceeded"
                let! comitted = syncState.TryOr log events (fun _resync -> async { return false })

                if not comitted then
                    return raise (createMaxAttemptsExhaustedException attempt)
                else
                    return result
            else
                let! committed = syncState.TryOrResync resyncRetryPolicy attempt log events
                if not committed then
                    log.Debug "Resyncing and retrying"
                    return! loop (attempt + 1)
                else
                    return result }
        /// Commence, processing based on the incoming state
        loop 1

    let private execAsync stream log f = async {
        let! syncState = load log stream
        return! f syncState }

    let decide (createContext,extractEvents) (maxAttempts,resyncRetryPolicy,createMaxAttemptsExhaustedException) (stream, log, flow) : Async<'result> =
        let runAsync state = async {
            let context = createContext state
            let result = flow context
            return result, extractEvents context }
        let runWithSyncState syncState = run log (maxAttempts,resyncRetryPolicy,createMaxAttemptsExhaustedException) syncState runAsync
        execAsync stream log runWithSyncState

    let decideAsync (createContext,extractEvents) (maxAttempts,resyncRetryPolicy,createMaxAttemptsExhaustedException) (stream, log, flowAsync) : Async<'result> =
        let runAsync state = async {
            let context = createContext state
            let! result = flowAsync context
            return result, extractEvents context }
        let runWithSyncState syncState = run log (maxAttempts,resyncRetryPolicy,createMaxAttemptsExhaustedException) syncState runAsync
        execAsync stream log runWithSyncState

    let query(stream : IStream<'event, 'state>, log : ILogger, project: SyncState<'event,'state> -> 'result): Async<'result> =
        execAsync stream log (fun syncState -> async { return project syncState })