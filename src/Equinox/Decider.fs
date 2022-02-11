namespace Equinox

open System.Runtime.InteropServices

/// Exception yielded by Decider.Transact after `count` attempts have yielded conflicts at the point of syncing with the Store
type MaxResyncsExhaustedException(count) =
   inherit exn(sprintf "Concurrency violation; aborting after %i attempts." count)

/// Central Application-facing API. Wraps the handling of decision or query flows in a manner that is store agnostic
type Decider<'event, 'state>
    (   log, stream : Core.IStream<'event, 'state>, maxAttempts : int,
        [<Optional; DefaultParameterValue(null)>] ?createAttemptsExhaustedException : int -> exn,
        [<Optional; DefaultParameterValue(null)>] ?resyncPolicy) =

    do if maxAttempts < 1 then raise <| System.ArgumentOutOfRangeException("maxAttempts", maxAttempts, "should be >= 1")
    let fetch : LoadOption<'state> option -> (Serilog.ILogger -> Async<Core.StreamToken * 'state>) = function
        | None | Some RequireLoad ->                fun log -> stream.Load(log, allowStale = false)
        | Some AllowStale ->                        fun log -> stream.Load(log, allowStale = true)
        | Some AssumeEmpty ->                       fun _log -> async { return stream.LoadEmpty() }
        | Some (FromMemento (streamToken, state)) -> fun _log -> async { return (streamToken, state) }
    let query maybeOption project = async {
        let! tokenAndState = fetch maybeOption log
        return project tokenAndState }
    let run originTokenAndState decide mapResult =
        let resyncRetryPolicy = defaultArg resyncPolicy (fun _log _attemptNumber resyncF -> async { return! resyncF })
        let createDefaultAttemptsExhaustedException attempts : exn = MaxResyncsExhaustedException attempts :> exn
        let createMaxAttemptsExhaustedException = defaultArg createAttemptsExhaustedException createDefaultAttemptsExhaustedException
        let rec loop (token, state) attempt : Async<'view> = async {
            let log = if attempt = 1 then log else log.ForContext("syncAttempt", attempt)
            match! decide (token, state) with
            | result, [] ->
                log.Debug "No events generated"
                return mapResult result (token, state)
            | result, events ->
                match! stream.TrySync (log, token, state, events) with
                | Core.SyncResult.Conflict resync ->
                    if attempt <> maxAttempts then
                        let! streamState' = resyncRetryPolicy log attempt resync
                        log.Debug "Resyncing and retrying"
                        return! loop streamState' (attempt + 1)
                    else
                        log.Debug "Max Sync Attempts exceeded"
                        return raise (createMaxAttemptsExhaustedException attempt)
                | Core.SyncResult.Written (token', streamState') ->
                    return mapResult result (token', streamState') }
        loop originTokenAndState 1
    let transact maybeOption decide mapResult = async {
        let! originTokenAndState = fetch maybeOption log
        return! run originTokenAndState decide mapResult }
    let (|Context|) (token : Core.StreamToken, state) =
        { new ISyncContext<'state> with
            member _.State = state
            member _.Version = token.version
            member _.CreateMemento() = token, state }

    /// 0.  Invoke the supplied <c>interpret</c> function with the present state
    /// 1a. (if events yielded) Attempt to sync the yielded events events to the stream
    /// 1b. Tries up to <c>maxAttempts</c> times in the case of a conflict, throwing <c>MaxResyncsExhaustedException</c> to signal failure.
    member _.Transact(interpret : 'state -> 'event list, ?option) : Async<unit> =
        transact option (fun (_token, state) -> async { return (), interpret state }) (fun () _context -> ())

    /// 0.  Invoke the supplied <c>decide</c> function with the present state, holding the <c>'result</c>
    /// 1a. (if events yielded) Attempt to sync the yielded events events to the stream
    /// 1b. Tries up to <c>maxAttempts</c> times in the case of a conflict, throwing <c>MaxResyncsExhaustedException</c> to signal failure.
    /// 2.  Yield result
    member _.Transact(decide : 'state -> 'result * 'event list, ?option) : Async<'result> =
        transact option (fun (_token, state) -> async { return decide state }) (fun result _context -> result)

    /// 0.  Invoke the supplied <c>_Async_</c> <c>decide</c> function with the present state, holding the <c>'result</c>
    /// 1a. (if events yielded) Attempt to sync the yielded events events to the stream
    /// 1b. Tries up to <c>maxAttempts</c> times in the case of a conflict, throwing <c>MaxResyncsExhaustedException</c> to signal failure.
    /// 2.  Yield result
    member _.Transact(decide : 'state -> Async<'result * 'event list>, ?option) : Async<'result> =
        transact option (fun (_token, state) -> decide state) (fun result _context -> result)

    /// 0.  Invoke the supplied <c>_Async_</c> <c>decide</c> function with the present state (including extended context), holding the <c>'result</c>
    /// 1a. (if events yielded) Attempt to sync the yielded events events to the stream
    /// 1b. Tries up to <c>maxAttempts</c> times in the case of a conflict, throwing <c>MaxResyncsExhaustedException</c> to signal failure.
    /// 2.  Uses <c>mapResult</c> to render the final 'view from the <c>'result</c> and/or the final <c>ISyncContext</c>
    /// 3.  Yields the 'view
    member _.TransactEx(decide : ISyncContext<'state> -> Async<'result * 'event list>, mapResult : 'result -> ISyncContext<'state> -> 'view, ?option) : Async<'view> =
        transact option (fun (Context c) -> decide c) (fun r (Context c) -> mapResult r c)

    /// Project from the folded <c>'state</c>, without executing a decision flow as <c>Transact</c> does
    member _.Query(projection : 'state -> 'view, ?option) : Async<'view> =
        query option (fun (_token, state) -> projection state)

    /// Project from the stream's <c>'state<c> (including extended context), without executing a decision flow as <c>Transact<c> does
    member _.QueryEx(projection : ISyncContext<'state> -> 'view, ?option) : Async<'view> =
        query option (fun (Context c) -> projection c)

/// Store-agnostic Loading Options
and [<NoComparison; NoEquality>] LoadOption<'state> =
    /// Default policy; Obtain latest state from store based on consistency level configured
    | RequireLoad
    /// If the Cache holds any state, use that without checking the backing store for updates, implying:
    /// - maximizing how much we lean on Optimistic Concurrency Control when doing a `Transact` (you're still guaranteed a consistent outcome)
    /// - enabling stale reads [in the face of multiple writers (either in this process or in other processes)] when doing a `Query`
    | AllowStale
    /// Inhibit load from database based on the fact that the stream is likely not to have been initialized yet, and we will be generating events
    | AssumeEmpty
    /// <summary>Instead of loading from database, seed the loading process with the supplied memento, obtained via <c>ISyncContext.CreateMemento()</c></summary>
    | FromMemento of memento : (Core.StreamToken * 'state)

/// Exposed by TransactEx / QueryEx, providing access to extended state information for cases where that's required
and ISyncContext<'state> =

    /// Exposes the underlying Store's internal Version for the underlying stream.
    /// An empty stream is Version 0; one with a single event is Version 1 etc.
    /// It's important to consider that this Version is more authoritative than counting the events seen, or adding 1 to
    ///   the `Index` of the last event passed to your `fold` function - the codec may opt to ignore events
    abstract member Version : int64

    /// The present State of the stream within the context of this Flow
    abstract member State : 'state

    /// Represents a Checkpoint position on a Stream's timeline; Can be used to manage continuations via LoadOption.FromMemento
    abstract member CreateMemento : unit -> Core.StreamToken * 'state
