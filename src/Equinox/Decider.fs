namespace Equinox

open Equinox.Core
open System

module private Impl =

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

    let transact (stream: IStream<'e, 's>, fetch, decide, validateResync, mapResult: Func<'r, struct (StreamToken * 's), 'v>, ct): Task<'v> = task {
        let! originTokenAndState = fetch stream ct
        let decide tokenAndState = decide tokenAndState ct
        let sync attempt struct (token, state) events = stream.Sync(attempt, token, state, events, ct)
        let resyncFromConflict attempt resync = task { validateResync attempt; return! resync ct }
        let mapResult result tokenAndState = mapResult.Invoke(result, tokenAndState)
        return! run decide sync resyncFromConflict mapResult originTokenAndState }

    let query (stream, fetch, render: Func<struct (StreamToken * 's), 'v>, ct): Task<'v> = task {
        let! tokenAndState = fetch stream ct
        return render.Invoke tokenAndState }

/// Central Application-facing API for F#. Wraps the handling of decision or query flows in a manner that is store agnostic
/// NOTE: For C#, direct usage of DeciderCore is recommended
type Decider<'event, 'state>(inner: DeciderCore<'event, 'state>) =

    /// Provides access to lower-level APIs to enable building custom <c>Transact</c> / <c>Query</c> variations
    member val Core = inner

    /// 1.  Invoke the supplied <c>interpret</c> function with the present state to determine whether any write is to occur.
    /// 2. (if events yielded) Attempt to sync the yielded events to the stream.
    ///    (Restarts up to <c>maxAttempts</c> times with updated state per attempt, throwing <c>MaxResyncsExhaustedException</c> on failure of final attempt.)
    member _.Transact(interpret: 'state -> 'event[], ?load, ?attempts): Async<unit> = Async.call <| fun ct ->
        inner.Transact(interpret, ?load = load, ?attempts = attempts, ct = ct)

    /// 1. Invoke the supplied <c>interpret</c> function with the present state
    /// 2. (if events yielded) Attempt to sync the yielded events to the stream.
    ///    (Restarts up to <c>maxAttempts</c> times with updated state per attempt, throwing <c>MaxResyncsExhaustedException</c> on failure of final attempt.)
    /// 3. Uses <c>render</c> to generate a 'view from the persisted final state
    member _.Transact(interpret: 'state -> 'event[], render: 'state -> 'view, ?load, ?attempts): Async<'view> = Async.call <| fun ct ->
        inner.Transact(interpret, render, ?load = load, ?attempts = attempts, ct = ct)

    /// 1. Invoke the supplied <c>decide</c> function with the present state, holding the <c>'result</c>
    /// 2. (if events yielded) Attempt to sync the yielded events to the stream.
    ///    (Restarts up to <c>maxAttempts</c> times with updated state per attempt, throwing <c>MaxResyncsExhaustedException</c> on failure of final attempt.)
    /// 3. Yield result
    member _.Transact(decide: 'state -> 'result * 'event[], ?load, ?attempts): Async<'result> = Async.call <| fun ct ->
        inner.Transact(decide >> ValueTuple.Create, ?load = load, ?attempts = attempts, ct = ct)

    /// 1. Invoke the supplied <c>decide</c> function with the present state, holding the <c>'result</c>
    /// 2. (if events yielded) Attempt to sync the yielded events to the stream.
    ///    (Restarts up to <c>maxAttempts</c> times with updated state per attempt, throwing <c>MaxResyncsExhaustedException</c> on failure of final attempt.)
    /// 3. Yields a final 'view produced by <c>mapResult</c> from the <c>'result</c> and/or the final persisted <c>'state</c>
    member _.Transact(decide: 'state -> 'result * 'event[], mapResult: 'result -> 'state -> 'view, ?load, ?attempts): Async<'view> = Async.call <| fun ct ->
        inner.Transact(decide >> ValueTuple.Create, mapResult, ?load = load, ?attempts = attempts, ct = ct)

    /// 1. Invoke the supplied <c>decide</c> function with the current complete context, holding the <c>'result</c>
    /// 2. (if events yielded) Attempt to sync the yielded events to the stream.
    ///    (Restarts up to <c>maxAttempts</c> times with updated state per attempt, throwing <c>MaxResyncsExhaustedException</c> on failure of final attempt.)
    /// 3. Yields <c>result</c>
    member _.TransactEx(decide: ISyncContext<'state> -> 'result * 'event[], ?load, ?attempts): Async<'result> = Async.call <| fun ct ->
        inner.TransactEx(decide >> ValueTuple.Create, ?load = load, ?attempts = attempts, ct = ct)

    /// 1. Invoke the supplied <c>decide</c> function with the current complete context, holding the <c>'result</c>
    /// 2. (if events yielded) Attempt to sync the yielded events to the stream.
    ///    (Restarts up to <c>maxAttempts</c> times with updated state per attempt, throwing <c>MaxResyncsExhaustedException</c> on failure of final attempt.)
    /// 3. Yields a final 'view produced by <c>mapResult</c> from the <c>'result</c> and/or the final persisted <c>ISyncContext</c>
    member _.TransactEx(decide: ISyncContext<'state> -> 'result * 'event[], mapResult: 'result -> ISyncContext<'state> -> 'view,
                        ?load, ?attempts): Async<'view> = Async.call <| fun ct ->
        inner.TransactEx(decide = (decide >> ValueTuple.Create), mapResult = mapResult, ?load = load, ?attempts = attempts, ct = ct)

    /// Project from the folded <c>'state</c>, but without executing a decision flow as <c>Transact</c> does
    member _.Query(render: 'state -> 'view, ?load): Async<'view> = Async.call <| fun ct ->
        inner.Query(render, ?load = load, ct = ct)

    /// Project from the stream's complete context, but without executing a decision flow as <c>TransactEx<c> does
    member _.QueryEx(render: ISyncContext<'state> -> 'view, ?load): Async<'view> = Async.call <| fun ct ->
        inner.QueryEx(render, ?load = load, ct = ct)

    /// 1. Invoke the supplied <c>Async</c> <c>interpret</c> function with the present state
    /// 2. (if events yielded) Attempt to sync the yielded events to the stream.
    ///    (Restarts up to <c>maxAttempts</c> times with updated state per attempt, throwing <c>MaxResyncsExhaustedException</c> on failure of final attempt.)
    /// 3. Uses <c>render</c> to generate a 'view from the persisted final state
    member _.TransactAsync(interpret: 'state -> Async<'event[]>, render: 'state -> 'view, ?load, ?attempts): Async<'view> = Async.call <| fun ct ->
        inner.TransactAsync((fun s ct -> Async.StartImmediateAsTask(interpret s, ct)), render, ?load = load, ?attempts = attempts, ct = ct)

    /// 1. Invoke the supplied <c>Async</c> <c>decide</c> function with the present state, holding the <c>'result</c>
    /// 2. (if events yielded) Attempt to sync the yielded events to the stream.
    ///    (Restarts up to <c>maxAttempts</c> times with updated state per attempt, throwing <c>MaxResyncsExhaustedException</c> on failure of final attempt.)
    /// 3. Yield result
    member _.TransactAsync(decide: 'state -> Async<'result * 'event[]>, ?load, ?attempts): Async<'result> = Async.call <| fun ct ->
        let inline decide' s ct = task { let! r, es = Async.StartImmediateAsTask(decide s, ct) in return struct (r, es) }
        inner.TransactAsync(decide = decide', ?load = load, ?attempts = attempts, ct = ct)

    /// 1. Invoke the supplied <c>Async</c> <c>decide</c> function with the current complete context, holding the <c>'result</c>
    /// 2. (if events yielded) Attempt to sync the yielded events to the stream.
    ///    (Restarts up to <c>maxAttempts</c> times with updated state per attempt, throwing <c>MaxResyncsExhaustedException</c> on failure of final attempt.)
    /// 3. Yield result
    member _.TransactExAsync(decide: ISyncContext<'state> -> Async<'result * 'event[]>, ?load, ?attempts): Async<'result> = Async.call <| fun ct ->
        let decide' c ct = task { let! r, es = Async.StartImmediateAsTask(decide c, ct) in return struct (r, es) }
        inner.TransactExAsync(decide = decide', ?load = load, ?attempts = attempts, ct = ct)

    /// 1. Invoke the supplied <c>Async</c> <c>decide</c> function with the current complete context, holding the <c>'result</c>
    /// 2. (if events yielded) Attempt to sync the yielded events to the stream.
    ///    (Restarts up to <c>maxAttempts</c> times with updated state per attempt, throwing <c>MaxResyncsExhaustedException</c> on failure of final attempt.)
    /// 3. Yields a final 'view produced by <c>mapResult</c> from the <c>'result</c> and/or the final persisted <c>ISyncContext</c>
    member _.TransactExAsync(decide: ISyncContext<'state> -> Async<'result * 'event[]>, mapResult: 'result -> ISyncContext<'state> -> 'view,
                             ?load, ?attempts): Async<'view> = Async.call <| fun ct ->
        let inline decide' c ct = task { let! r, es = Async.StartImmediateAsTask(decide c, ct) in return struct (r, es) }
        inner.TransactExAsync(decide = decide', mapResult = mapResult, ?load = load, ?attempts = attempts, ct = ct)

/// Central Application-facing API. Wraps the handling of decision or query flows in a manner that is store agnostic
/// For F#, the async and FSharpFunc signatures in Decider tend to work better, but the API set is equivalent
and DeciderCore<'event, 'state>(stream: IStream<'event, 'state>) =

    let (|Context|) = SyncContext<'state>.Map

    /// 1.  Invoke the supplied <c>interpret</c> function with the present state to determine whether any write is to occur.
    /// 2. (if events yielded) Attempt to sync the yielded events to the stream.
    ///    (Restarts up to <c>maxAttempts</c> times with updated state per attempt, throwing <c>MaxResyncsExhaustedException</c> on failure of final attempt.)
    member _.Transact(interpret: Func<'state, 'event[]>,
                      [<O; D null>] ?load, [<O; D null>] ?attempts, [<O; D(CancellationToken())>]?ct): Task<unit> =
        let inline decide struct (_t: StreamToken, state) _ct = Task.FromResult struct ((), interpret.Invoke state)
        let inline mapRes () struct (_t: StreamToken, _s: 'state) = ()
        Impl.transact (stream, LoadPolicy.Fetch load, decide, AttemptsPolicy.Validate attempts, mapRes, defaultArg ct CancellationToken.None)

    /// 1. Invoke the supplied <c>interpret</c> function with the present state
    /// 2. (if events yielded) Attempt to sync the yielded events to the stream.
    ///    (Restarts up to <c>maxAttempts</c> times with updated state per attempt, throwing <c>MaxResyncsExhaustedException</c> on failure of final attempt.)
    /// 3. Uses <c>render</c> to generate a 'view from the persisted final state
    member _.Transact(interpret: Func<'state, 'event[]>, render: Func<'state, 'view>,
                      [<O; D null>] ?load, [<O; D null>] ?attempts, [<O; D(CancellationToken())>]?ct): Task<'view> =
        let inline decide struct (_token, state) _ct = Task.FromResult struct ((), interpret.Invoke state)
        let inline mapRes () struct (_token, state) = render.Invoke state
        Impl.transact (stream, LoadPolicy.Fetch load, decide, AttemptsPolicy.Validate attempts, mapRes, defaultArg ct CancellationToken.None)

    /// 1. Invoke the supplied <c>decide</c> function with the present state, holding the <c>'result</c>
    /// 2. (if events yielded) Attempt to sync the yielded events to the stream.
    ///    (Restarts up to <c>maxAttempts</c> times with updated state per attempt, throwing <c>MaxResyncsExhaustedException</c> on failure of final attempt.)
    /// 3. Yield result
    member _.Transact(decide: Func<'state, struct ('result * 'event[])>,
                      [<O; D null>] ?load, [<O; D null>] ?attempts, [<O; D null>] ?ct): Task<'result> =
        let inline decide struct (_token, state) _ct = decide.Invoke state |> Task.FromResult
        let inline mapRes r _ = r
        Impl.transact (stream, LoadPolicy.Fetch load, decide, AttemptsPolicy.Validate attempts, mapRes, defaultArg ct CancellationToken.None)

    /// 1. Invoke the supplied <c>decide</c> function with the present state, holding the <c>'result</c>
    /// 2. (if events yielded) Attempt to sync the yielded events to the stream.
    ///    (Restarts up to <c>maxAttempts</c> times with updated state per attempt, throwing <c>MaxResyncsExhaustedException</c> on failure of final attempt.)
    /// 3. Yields a final 'view produced by <c>mapResult</c> from the <c>'result</c> and/or the final persisted <c>'state</c>
    member _.Transact(decide: Func<'state, struct ('result * 'event[])>, mapResult: Func<'result, 'state, 'view>,
                      [<O; D null>] ?load, [<O; D null>] ?attempts, [<O; D null>] ?ct): Task<'view> =
        let inline decide struct (_token, state) _ct = decide.Invoke state |> Task.FromResult
        let inline mapRes r struct (_, s) = mapResult.Invoke(r, s)
        Impl.transact (stream, LoadPolicy.Fetch load, decide, AttemptsPolicy.Validate attempts, mapRes, defaultArg ct CancellationToken.None)

    /// 1. Invoke the supplied <c>decide</c> function with the current complete context, holding the <c>'result</c>
    /// 2. (if events yielded) Attempt to sync the yielded events to the stream.
    ///    (Restarts up to <c>maxAttempts</c> times with updated state per attempt, throwing <c>MaxResyncsExhaustedException</c> on failure of final attempt.)
    /// 3. Yields <c>result</c>
    member _.TransactEx(decide: Func<ISyncContext<'state>, struct ('result * 'event[])>,
                        [<O; D null>] ?load, [<O; D null>] ?attempts, [<O; D null>] ?ct): Task<'result> =
        let inline decide' (Context c) _ct = decide.Invoke(c) |> Task.FromResult
        let inline mapRes r _ = r
        Impl.transact (stream, LoadPolicy.Fetch load, decide', AttemptsPolicy.Validate attempts, mapRes, defaultArg ct CancellationToken.None)

    /// 1. Invoke the supplied <c>decide</c> function with the current complete context, holding the <c>'result</c>
    /// 2. (if events yielded) Attempt to sync the yielded events to the stream.
    ///    (Restarts up to <c>maxAttempts</c> times with updated state per attempt, throwing <c>MaxResyncsExhaustedException</c> on failure of final attempt.)
    /// 3. Yields a final 'view produced by <c>mapResult</c> from the <c>'result</c> and/or the final persisted <c>ISyncContext</c>
    member _.TransactEx(decide: Func<ISyncContext<'state>, struct ('result * 'event[])>, mapResult: Func<'result, ISyncContext<'state>, 'view>,
                        [<O; D null>] ?load, [<O; D null>] ?attempts, [<O; D null>] ?ct): Task<'view> =
        let inline decide (Context c) _ct = c |> decide.Invoke |> Task.FromResult
        let inline mapRes r (Context c) = mapResult.Invoke(r, c)
        Impl.transact (stream, LoadPolicy.Fetch load, decide, AttemptsPolicy.Validate attempts, mapRes, defaultArg ct CancellationToken.None)

    /// Project from the folded <c>'state</c>, but without executing a decision flow as <c>Transact</c> does
    member _.Query(render: Func<'state, 'view>, [<O; D null>] ?load, [<O; D null>] ?ct): Task<'view> =
        let render struct (_token, state) = render.Invoke(state)
        Impl.query (stream, LoadPolicy.Fetch load, render, defaultArg ct CancellationToken.None)

    /// Project from the stream's complete context, but without executing a decision flow as <c>TransactEx<c> does
    member _.QueryEx(render: Func<ISyncContext<'state>, 'view>, [<O; D null>] ?load, [<O; D null>] ?ct): Task<'view> =
        let render (Context c) = render.Invoke(c)
        Impl.query (stream, LoadPolicy.Fetch load, render, defaultArg ct CancellationToken.None)

    /// 1. Invoke the supplied <c>Async</c> <c>interpret</c> function with the present state
    /// 2. (if events yielded) Attempt to sync the yielded events to the stream.
    ///    (Restarts up to <c>maxAttempts</c> times with updated state per attempt, throwing <c>MaxResyncsExhaustedException</c> on failure of final attempt.)
    /// 3. Uses <c>render</c> to generate a 'view from the persisted final state
    member _.TransactAsync(interpret: Func<'state, CancellationToken, Task<'event[]>>, render: Func<'state, 'view>,
                           [<O; D null>] ?load, [<O; D null>] ?attempts, [<O; D null>] ?ct): Task<'view> =
        let inline decide struct (_token, state) ct = task { let! es = interpret.Invoke(state, ct) in return struct ((), es) }
        let inline mapRes () struct (_token, state) = render.Invoke state
        Impl.transact (stream, LoadPolicy.Fetch load, decide, AttemptsPolicy.Validate attempts, mapRes, defaultArg ct CancellationToken.None)

    /// 1. Invoke the supplied <c>Async</c> <c>decide</c> function with the present state, holding the <c>'result</c>
    /// 2. (if events yielded) Attempt to sync the yielded events to the stream.
    ///    (Restarts up to <c>maxAttempts</c> times with updated state per attempt, throwing <c>MaxResyncsExhaustedException</c> on failure of final attempt.)
    /// 3. Yield result
    member _.TransactAsync(decide: Func<'state, CancellationToken, Task<struct ('result * 'event[])>>,
                           [<O; D null>] ?load, [<O; D null>] ?attempts, [<O; D null>] ?ct): Task<'result> =
        let inline decide struct (_token, state) ct = decide.Invoke(state, ct)
        let inline mapRes r _ = r
        Impl.transact (stream, LoadPolicy.Fetch load, decide, AttemptsPolicy.Validate attempts, mapRes, defaultArg ct CancellationToken.None)

    /// 1. Invoke the supplied <c>Async</c> <c>decide</c> function with the current complete context, holding the <c>'result</c>
    /// 2. (if events yielded) Attempt to sync the yielded events to the stream.
    ///    (Restarts up to <c>maxAttempts</c> times with updated state per attempt, throwing <c>MaxResyncsExhaustedException</c> on failure of final attempt.)
    /// 3. Yield result
    member _.TransactExAsync(decide: Func<ISyncContext<'state>, CancellationToken, Task<struct ('result * 'event[])>>,
                             [<O; D null>] ?load, [<O; D null>] ?attempts, [<O; D null>] ?ct): Task<'result> =
        let inline decide (Context c) ct = decide.Invoke(c, ct)
        let inline mapRes r _ = r
        Impl.transact (stream, LoadPolicy.Fetch load, decide, AttemptsPolicy.Validate attempts, mapRes, defaultArg ct CancellationToken.None)

    /// 1. Invoke the supplied <c>Async</c> <c>decide</c> function with the current complete context, holding the <c>'result</c>
    /// 2. (if events yielded) Attempt to sync the yielded events to the stream.
    ///    (Restarts up to <c>maxAttempts</c> times with updated state per attempt, throwing <c>MaxResyncsExhaustedException</c> on failure of final attempt.)
    /// 3. Yields a final 'view produced by <c>mapResult</c> from the <c>'result</c> and/or the final persisted <c>ISyncContext</c>
    member _.TransactExAsync(decide: Func<ISyncContext<'state>, CancellationToken, Task<struct ('result * 'event[])>>, mapResult: Func<'result, ISyncContext<'state>, 'view>,
                             [<O; D null>] ?load, [<O; D null>] ?attempts, [<O; D null>] ?ct): Task<'view> =
        let inline decide (Context c) ct = decide.Invoke(c, ct)
        let inline mapRes r (Context c) = mapResult.Invoke(r, c)
        Impl.transact (stream, LoadPolicy.Fetch load, decide, AttemptsPolicy.Validate attempts, mapRes, defaultArg ct CancellationToken.None)

(* Options to tune loading policy - default is RequireLoad *)

/// Store-agnostic Loading Options
and [<NoComparison; NoEquality>] LoadOption<'state> =
    /// Default policy; Obtain latest state from store based on consistency level configured
    | RequireLoad
    /// Request that data be read with a quorum read / from a Leader connection
    | RequireLeader
    /// If the Cache holds any state, use that without checking the backing store for updates, implying:
    /// - maximizing how much we lean on Optimistic Concurrency Control when doing a `Transact` (though you're still guaranteed a consistent outcome)
    /// - enabling stale reads (without ever hitting the store, unless a writer sharing the same Cache does so) when doing a `Query`
    | AnyCachedValue
    /// If the Cache holds a state, and it's within the specified limit, use that without checking the backing store for updates, implying:
    /// - increasing how much we lean on Optimistic Concurrency Control when doing a `Transact` (though you're still guaranteed a consistent outcome)
    /// - limiting the frequency of reads to 1 request per stream per Cache per `age` when using `Query`
    | AllowStale of maxAge: TimeSpan
    /// Inhibit load from database based on the fact that the stream is likely not to have been initialized yet, and we will be generating events
    | AssumeEmpty
and internal LoadPolicy() =
    static member Fetch<'state, 'event>(x: LoadOption<'state> option)
        : IStream<'event, 'state> -> CancellationToken -> Task<struct (StreamToken * 'state)> =
        match x with
        | None | Some RequireLoad ->                 fun stream ct ->   stream.Load(maxAge = TimeSpan.Zero,     requireLeader = false, ct = ct)
        | Some RequireLeader ->                      fun stream ct ->   stream.Load(maxAge = TimeSpan.Zero,     requireLeader = true,  ct = ct)
        | Some AnyCachedValue ->                     fun stream ct ->   stream.Load(maxAge = TimeSpan.MaxValue, requireLeader = false, ct = ct)
        | Some (AllowStale maxAge) ->                fun stream ct ->   stream.Load(maxAge = maxAge,            requireLeader = false, ct = ct)
        | Some AssumeEmpty ->                        fun stream _ct ->  Task.FromResult(stream.LoadEmpty())

(* Retry / Attempts policy used to define policy for retrying based on the conflicting state when there's an Append conflict (default 3 retries) *)

and [<NoComparison; NoEquality; RequireQualifiedAccess>] Attempts =
    | Max of count: int
and internal AttemptsPolicy() =
    static member Validate(opt: Attempts option) =
        let maxAttempts = match opt with Some (Attempts.Max n) -> n | None -> 3
        if maxAttempts < 1 then raise (ArgumentOutOfRangeException(nameof opt, maxAttempts, "should be >= 1"))
        fun attempt -> if attempt = maxAttempts then raise (MaxResyncsExhaustedException attempt)

/// Exception yielded by Decider.Transact after `count` attempts have yielded conflicts at the point of syncing with the Store
and MaxResyncsExhaustedException(count) =
   inherit exn(sprintf "Concurrency violation; aborting after %i attempts." count)

/// Exposed by TransactEx / QueryEx, providing access to extended state information for cases where that's required
and [<NoComparison; NoEquality>]
    ISyncContext<'state> =

    /// Store-independent Version associated with the present <c>State</c>, based on the underlying Stream's write position.
    /// An empty stream is Version 0; one with a single event is Version 1 etc.
    /// NOTE Version is more authoritative than counting the events seen, or adding 1 to the `Index` of the last event
    ///      passed to your `fold` function; the codec may opt to filter out some events
    abstract member Version: int64

    /// The Storage occupied by the Events written to the underlying stream at the present time.
    /// Specific stores may vary whether this is available, or the basis and preciseness for how it is computed.
    abstract member StreamEventBytes: int64 voption

    /// The present State of the stream within the context of this Flow
    abstract member State: 'state

and internal SyncContext<'state> =

    static member Map(struct (token: StreamToken, state: 'state)) =
        { new ISyncContext<'state> with
            member _.State = state
            member _.Version = token.version
            member _.StreamEventBytes = match token.streamBytes with -1L -> ValueNone | b -> ValueSome b }
