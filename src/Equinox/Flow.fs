/// Internal data structures/impl. While these are intended to be legible, understanding the abstractions involved is only necessary if you are implementing a Store or a decorator thereof.
/// i.e., if you're seeking to understand the main usage flows of the Equinox library, that's in Decider.fs, not here
namespace Equinox.Core

open Serilog

/// Store-specific opaque token to be used for synchronization purposes
[<NoComparison>]
type StreamToken = { value : obj; version: int64 }

/// Internal type used to represent the outcome of a TrySync operation
[<NoEquality; NoComparison; RequireQualifiedAccess>]
type SyncResult<'state> =
    /// The write succeeded (the supplied token and state can be used to efficiently continue the processing if, and only if, desired)
    | Written of StreamToken * 'state
    /// The set of changes supplied to TrySync conflict with the present state of the underlying stream based on the configured policy for that store
    /// The inner is Async as some stores (and/or states) are such that determining the conflicting state (if, and only if, required) needs an extra trip to obtain
    | Conflict of Async<StreamToken * 'state>

/// Store-agnostic interface representing interactions a Flow can have with the state of a given event stream. Not intended for direct use by consumer code.
type IStream<'event, 'state> =

    /// Obtain the state from the target stream
    abstract Load : log: ILogger -> Async<StreamToken * 'state>

    /// Given the supplied `token` [and related `originState`], attempt to move to state `state'` by appending the supplied `events` to the underlying stream
    /// SyncResult.Written: implies the state is now the value represented by the Result's value
    /// SyncResult.Conflict: implies the `events` were not synced; if desired the consumer can use the included resync workflow in order to retry
    abstract TrySync : log: ILogger * token: StreamToken * originState: 'state * events: 'event list -> Async<SyncResult<'state>>

/// Exposed by TransactEx / QueryEx, providing access to extended state information for cases where that's required
type ISyncContext<'state> =

    /// Represents a Checkpoint position on a Stream's timeline; Can be used to manage continuations via a Resolver's FromMemento method
    abstract member CreateMemento : unit -> StreamToken * 'state

    /// Exposes the underlying Store's internal Version for the underlying stream.
    /// An empty stream is Version 0; one with a single event is Version 1 etc.
    /// It's important to consider that this Version is more authoritative than inspecting the `Index` of the last event passed to
    /// your `fold` function - the codec may opt to ignore it
    abstract member Version : int64

    /// The present State of the stream within the context of this Flow
    abstract member State : 'state

/// Internal implementation of the Store agnostic load + run/render. See Decider.fs for App-facing APIs.
module internal Flow =

    /// Represents stream and folding state between the load and run/render phases
    type SyncContext<'event, 'state>
        (   originState : StreamToken * 'state,
            trySync : ILogger * StreamToken * 'state * 'event list -> Async<SyncResult<'state>>) =
        let mutable tokenAndState = originState

        let trySyncOr log events (handleFailureResync : Async<StreamToken * 'state> -> Async<bool>) : Async<bool> = async {
            let! res = let token, state = tokenAndState in trySync (log,token,state,events)
            match res with
            | SyncResult.Conflict resync ->
                return! handleFailureResync resync
            | SyncResult.Written (token', streamState') ->
                tokenAndState <- token', streamState'
                return true }

        interface ISyncContext<'state> with
            member _.CreateMemento() = tokenAndState
            member _.State = snd tokenAndState
            member _.Version = (fst tokenAndState).version

        member _.TryWithoutResync(log : ILogger, events) : Async<bool> =
            trySyncOr log events (fun _resync -> async { return false })
        member _.TryOrResync(runResync, attemptNumber: int, log : ILogger, events) : Async<bool> =
            let resyncInPreparationForRetry resync = async {
                let! streamState' = runResync log attemptNumber resync
                tokenAndState <- streamState'
                return false }
            trySyncOr log events resyncInPreparationForRetry

    /// Process a command, ensuring a consistent final state is established on the stream.
    /// 1.  make a decision predicated on the known state
    /// 2a. if no changes required, exit with known state
    /// 2b. if saved without conflict, exit with updated state
    /// 2b. if conflicting changes, retry by recommencing at step 1 with the updated state
    let run (log : ILogger) (maxSyncAttempts : int, resyncRetryPolicy, createMaxAttemptsExhaustedException)
        (context : SyncContext<'event, 'state>)
        (decide : ISyncContext<'state> -> Async<'result * 'event list>)
        (mapResult : 'result -> SyncContext<'event, 'state> -> 'view)
        : Async<'view> =

        if maxSyncAttempts < 1 then raise <| System.ArgumentOutOfRangeException("maxSyncAttempts", maxSyncAttempts, "should be >= 1")

        /// Run a decision cycle - decide what events should be appended given the presented state
        let rec loop attempt : Async<'view> = async {
            let log = if attempt = 1 then log else log.ForContext("syncAttempt", attempt)
            let! result, events = decide (context :> ISyncContext<'state>)
            if List.isEmpty events then
                log.Debug "No events generated"
                return mapResult result context
            elif attempt = maxSyncAttempts then
                // Special case: on final attempt, we won't be `resync`ing; we're giving up
                let! committed = context.TryWithoutResync(log, events)
                if not committed then
                    log.Debug "Max Sync Attempts exceeded"
                    return raise (createMaxAttemptsExhaustedException attempt)
                else
                    return mapResult result context
            else
                let! committed = context.TryOrResync(resyncRetryPolicy, attempt, log, events)
                if not committed then
                    log.Debug "Resyncing and retrying"
                    return! loop (attempt + 1)
                else
                    return mapResult result context }

        // Commence, processing based on the incoming state
        loop 1

    let transact (maxAttempts, resyncRetryPolicy, createMaxAttemptsExhaustedException) (stream : IStream<_, _>, log) decide mapResult : Async<'result> = async {
        let! streamState = stream.Load log
        let context = SyncContext(streamState, stream.TrySync)
        return! run log (maxAttempts, resyncRetryPolicy, createMaxAttemptsExhaustedException) context decide mapResult }

    let query (stream : IStream<'event, 'state>, log : ILogger, project: SyncContext<'event, 'state> -> 'result) : Async<'result> = async {
        let! streamState = stream.Load log
        let context = SyncContext(streamState, stream.TrySync)
        return project context }
