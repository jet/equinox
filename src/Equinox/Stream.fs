// Defines base IStream interface that a Decider uses to load and append to a Stream. Stores have Category implementations fulfilling this contract.
// (IStream could be marked `internal`, but has been left public in order to facilitate experimenting with custom `Decider` re-implementations within an app)
namespace Equinox.Core

/// Store-agnostic interface implemented by Category, representing interactions a Transact/Query can have with the state of a given event stream.
type IStream<'event, 'state> =

    /// Generate a stream token representing a stream one believes to be empty for use as a Null Object when optimizing out the initial load roundtrip
    abstract LoadEmpty: unit -> struct (StreamToken * 'state)

    /// Obtain the state from the target stream
    abstract Load: maxAge: System.TimeSpan * requireLeader: bool * ct: CancellationToken -> Task<struct (StreamToken * 'state)>

    /// Given the supplied `token` [and related `originState`], attempt to move to state `state'` by appending the supplied `events` to the underlying stream
    /// SyncResult.Written: implies the state is now the value represented by the Result's value
    /// SyncResult.Conflict: implies the `events` were not synced; if desired the consumer can use the included resync workflow in order to retry
    abstract Sync: attempt: int * token: StreamToken * state: 'state * events: 'event[] * CancellationToken -> Task<SyncResult<'state>>

/// Internal type used to represent the outcome of a Sync
and [<NoEquality; NoComparison; RequireQualifiedAccess>] SyncResult<'state> =
    /// The write succeeded (the supplied token and state can be used to efficiently continue the processing if, and only if, desired)
    | Written of struct (StreamToken * 'state)
    /// The set of changes supplied Sync conflict with the present state of the underlying stream based on the configured policy for that store
    /// The inner is Async as some stores (and/or states) are such that determining the conflicting state (if, and only if, required) needs an extra trip to obtain
    | Conflict of (CancellationToken -> Task<struct (StreamToken * 'state)>)

/// Store-specific opaque token to be used for synchronization purposes
and [<Struct; NoEquality; NoComparison>] StreamToken = { value: obj; version: int64; streamBytes: int64 }

module internal Stream =

    let private run (decide: struct (StreamToken * 's) -> Task<struct ('r * 'e[])>) sync fromConflict mapResult =
        let rec loop attempt tokenAndState: Task<'v> = task { // don't try to inline or it won't compile as an efficient state machine
            match! decide tokenAndState with
            | result, [||] -> return mapResult result tokenAndState
            | result, events ->
                match! sync attempt tokenAndState events with
                | SyncResult.Written tokenAndState' -> return mapResult result tokenAndState'
                | SyncResult.Conflict loadConflictingTokenAndState ->
                    let! tokenAndState = fromConflict attempt loadConflictingTokenAndState
                    return! loop (attempt + 1) tokenAndState }
        loop 1

    let transact (stream: IStream<'e, 's>, fetch, decide, validateResync, mapResult: System.Func<'r, struct (StreamToken * 's), 'v>, ct): Task<'v> = task {
        let! originTokenAndState = fetch stream ct
        let decide tokenAndState = decide tokenAndState ct
        let sync attempt struct (token, state) events = stream.Sync(attempt, token, state, events, ct)
        let resyncFromConflict attempt resync = task { validateResync attempt; return! resync ct }
        let mapResult result tokenAndState = mapResult.Invoke(result, tokenAndState)
        return! run decide sync resyncFromConflict mapResult originTokenAndState }

    let query (stream, fetch, render: System.Func<struct (StreamToken * 's), 'v>, ct): Task<'v> = task {
        let! tokenAndState = fetch stream ct
        return render.Invoke tokenAndState }
