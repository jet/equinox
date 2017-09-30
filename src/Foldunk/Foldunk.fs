namespace Foldunk

open Serilog

/// Maintains a rolling folded State while Accumulating Events decided upon as part of a decision flow
type Context<'event, 'state>(fold, originState : 'state) =
    let accumulated = ResizeArray<'event>()

    /// The current folded State, based on the Stream's `originState` + any events that have been Accummulated during the the decision flow
    member __.State = __.Accumulated |> fold originState
    /// Invoke a decision function, gathering the events (if any) that it decides are necessary into the `Accummulated` sequence
    member __.Execute (decide : 'state -> 'event list) : unit =
        decide __.State |> accumulated.AddRange
    /// Invoke an Async decision function, gathering the events (if any) that it decides are necessary into the `Accummulated` sequence
    member __.ExecuteAsync (decide : 'state -> Async<'event list>) : Async<unit> = async {
        let! events = decide __.State
        accumulated.AddRange events }
    /// As per `Execute`, invoke a decision function, while also propagating an outcome yielded as the fst of an (outcome, events) pair
    member __.Decide (decide : 'state -> 'result * 'event list) =
        let result, newEvents = decide __.State
        accumulated.AddRange newEvents
        result
    /// As per `ExecuteAsync`, invoke a decision function, while also propagating an outcome yielded as the fst of an (outcome, events) pair
    member __.DecideAsync (decide : 'state -> Async<'result * 'event list>) : Async<'result> = async {
        let! result, newEvents = decide __.State
        accumulated.AddRange newEvents
        return result }
    /// The Events that have thus far been pended via the `decide` functions `Execute`/`Decide`d during the course of this flow
    member __.Accumulated = accumulated |> List.ofSeq
    /// Used to complete a Decision flow, yielding the supplied `outcome` (which may be unit) along with the Accumulated events to be synced to the stream
    member __.Complete outcome =
        outcome, __.Accumulated

/// Internal data stuctures. While these are intended to be legible, understand the abstractions involved is only necessary if you are implemening a Store or a decorator thereof.
[<RequireQualifiedAccess>]
module Storage =
    /// Store-specific opaque token to be used for synchronization purposes
    [<NoComparison>]
    type StreamToken = { value : obj }

    /// Representation of a current known state of the stream, including the concurrency token (e.g. Stream Version number)
    type StreamState<'event, 'state> = StreamToken * 'state option * 'event list

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
    type SyncResult<'event, 'state> =
        | Written of StreamState<'event, 'state>
        | Conflict of Async<StreamState<'event, 'state>>

/// Store-agnostic interface to the state of any given stream of events. Not intended for direct use by consumer code; this is an abstract interface to be implemented by a Store in order for the Application to be able to execute a standarized Flow via the Handler type
type IStream<'event, 'state> =
    /// Obtain the state from the stream named `streamName`
    abstract Load: log: ILogger
        -> Async<Storage.StreamState<'event, 'state>>
    /// Synchronize the state of the stream named `streamName`, appending the supplied `events`, in order to establish the supplied `state` value
    /// NB the central precondition is that the Stream has not diverged from the state represented by `token`;  where this is not met, the response signals the
    ///    need to reprocess by yielding an Error bearing the conflicting StreamState with which to retry (loaded in a specific manner optimal for the store)
    abstract TrySync: log: ILogger -> token: Storage.StreamToken * originState: 'state -> events: 'event list * state' : 'state
        // - Written: to signify Synchronization has succeeded, taking the yielded Stream State as the representation of what is understood to be the state
        // - Conflict: to signify the synch failed and the deicsion hence needs to be rerun based on the updated Stream State with which the changes conflicted
        -> Async<Storage.SyncResult<'event, 'state>>

// Exception yielded by Handler.Decide after `count` attempts have yielded conflicts at the point of syncing with the Store
exception FlowAttemptsExceededException of count: int

/// Internal implementation of the Store agnostic load + run/render. See Handler for App-facing APIs.
module private Flow =
    /// Represents stream and folding state between the load and run/render phases
    type SyncState<'event, 'state>
        (   fold, initial, originState : Storage.StreamState<'event, 'state>,
            trySync : ILogger -> Storage.StreamToken * 'state -> 'event list * 'state -> Async<Storage.SyncResult<'event, 'state>>) =
        let toTokenAndState fold initial ((token, stateOption, events) : Storage.StreamState<'event, 'state>) : Storage.StreamToken * 'state =
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
        member __.CreateContext(): Context<'event, 'state> =
            Context<'event, 'state>(fold, __.State)
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
    let load(fold : 'state -> 'event list -> 'state) (initial : 'state)
            (log : Serilog.ILogger) (stream : IStream<'event, 'state>) 
        : Async<SyncState<'event, 'state>> = async {
        let! streamState = stream.Load log
        return SyncState(fold, initial, streamState, stream.TrySync) }

    /// Render a projection from the folded State
    let render (query : 'state -> 'projection) (syncState : SyncState<'event, 'state>)
        : 'projection =
        syncState.State |> query

    /// Process a command, ensuring a consistent final state is established on the stream.
    /// 1.  make a decision given the known state
    /// 2a. if no changes required, exit with known state
    /// 2b. if saved without conflict, exit with updated state
    /// 2b. if conflicting changes, loop to retry against updated state
    let run (sync : SyncState<'event, 'state>) (maxAttempts : int) (log : ILogger) (decide : Context<'event, 'state> -> Async<'outcome * 'event list>)
        : Async<'outcome> =
        if maxAttempts < 1 then raise <| System.ArgumentOutOfRangeException("maxAttempts", maxAttempts, "should be >= 1")
        /// Run a decision cycle - decide what events should be appended given the presented state
        let rec loop attempt: Async<'outcome> = async {
            //let token, currentState = interpreter.Fold currentState
            let log = log.ForContext("Attempt", attempt)
            let ctx = sync.CreateContext()
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

/// Core Application-facing API. Wraps the handling of decision or query flow in a manner that is store agnostic
type Handler<'event, 'state>(fold, initial, ?maxAttempts) =
    /// Invoke the supplied `decide` function, syncing the accumulated events at the end. Tries up to `maxAttempts` times in the case of a conflict, throwing
    /// `FlowAttemptsExceededException` to signal failure to synchronize decision into the stream
    member __.Decide (stream : IStream<'event, 'state>) (log : Serilog.ILogger) (decide: Context<'event, 'state> -> Async<'outcome * 'event list>) = async {
        let! syncState = Flow.load fold initial log stream
        return! Flow.run syncState (defaultArg maxAttempts 3) log decide }
    /// Project from the folded `State` (without executing a decision flow or syncing new events to the stream as `Decide` does)
    member __.Query (stream : IStream<'event, 'state>) (log: Serilog.ILogger) (projection : 'state -> 'projected) : Async<'projected> = async {
        let! syncState = Flow.load fold initial log stream
        return syncState |> Flow.render projection }

/// Helper for defining backoffs within the definition of a retry policy for a store.
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