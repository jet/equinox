/// Internal data structures/impl. While these are intended to be legible, understanding the abstractions involved is only necessary if you are implementing a Store or a decorator thereof.
/// i.e., if you're seeking to understand the main usage flows of the Equinox library, that's in Decider.fs, not here
namespace Equinox.Core

/// Store-agnostic interface representing interactions a Flow can have with the state of a given event stream. Not intended for direct use by consumer code.
type IStream<'event, 'state> =

    /// Generate a stream token that represents a stream one believes to be empty to use as a Null Object when optimizing out the initial load roundtrip
    abstract LoadEmpty : unit -> StreamToken * 'state

    /// Obtain the state from the target stream
    abstract Load : log: Serilog.ILogger * allowStale : bool -> Async<StreamToken * 'state>

    /// Given the supplied `token` [and related `originState`], attempt to move to state `state'` by appending the supplied `events` to the underlying stream
    /// SyncResult.Written: implies the state is now the value represented by the Result's value
    /// SyncResult.Conflict: implies the `events` were not synced; if desired the consumer can use the included resync workflow in order to retry
    abstract TrySync : log: Serilog.ILogger * token: StreamToken * originState: 'state * events: 'event list -> Async<SyncResult<'state>>

/// Internal type used to represent the outcome of a TrySync operation
and [<NoEquality; NoComparison; RequireQualifiedAccess>] SyncResult<'state> =
    /// The write succeeded (the supplied token and state can be used to efficiently continue the processing if, and only if, desired)
    | Written of StreamToken * 'state
    /// The set of changes supplied to TrySync conflict with the present state of the underlying stream based on the configured policy for that store
    /// The inner is Async as some stores (and/or states) are such that determining the conflicting state (if, and only if, required) needs an extra trip to obtain
    | Conflict of Async<StreamToken * 'state>

/// Store-specific opaque token to be used for synchronization purposes
and [<NoComparison>] StreamToken = { value : obj; version: int64 }

/// Internal implementation of the Optimistic Concurrency Control loop within which a decider function runs. See Decider.fs for App-facing APIs.
module internal Flow =

    /// Process a command, ensuring a consistent final state is established on the stream.
    /// 1.  make a decision predicated on the known state
    /// 2a. if no changes required, exit with known state
    /// 2b. if conflicting changes, retry by recommencing at step 1 with the updated state
    /// 2c. if saved without conflict, exit with updated state
    let transact (originState : StreamToken * 'state)
        (decide : StreamToken * 'state -> Async<'result * 'event list>)
        (log : Serilog.ILogger)
        (trySync : Serilog.ILogger * StreamToken * 'state * 'event list -> Async<SyncResult<'state>>)
        (maxSyncAttempts, resyncRetryPolicy, createMaxAttemptsExhaustedException)
        (mapResult : 'result -> StreamToken * 'state -> 'view) : Async<'view> =

        // Runs one decision loop, potentially recursing with resynced state if there's a conflict on the write
        let rec loop (token, state) attempt : Async<'view> = async {
            let log = if attempt = 1 then log else log.ForContext("syncAttempt", attempt)
            match! decide (token, state) with
            | result, [] ->
                log.Debug "No events generated"
                return mapResult result (token, state)
            | result, events ->
                match! trySync (log, token, state, events) with
                | SyncResult.Conflict resync ->
                    if attempt <> maxSyncAttempts then
                        let! streamState' = resyncRetryPolicy log attempt resync
                        log.Debug "Resyncing and retrying"
                        return! loop streamState' (attempt + 1)
                    else
                        log.Debug "Max Sync Attempts exceeded"
                        return raise (createMaxAttemptsExhaustedException attempt)
                | SyncResult.Written (token', streamState') ->
                    return mapResult result (token', streamState') }
        // Commence, processing based on the incoming state
        loop originState 1
