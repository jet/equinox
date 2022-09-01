namespace Equinox

open System.Threading.Tasks

/// Central Application-facing API. Wraps the handling of decision or query flows in a manner that is store agnostic
type Decider<'event, 'state>(stream : Core.IStream<'event, 'state>) =

    let (|Context|) = SyncContext<'state>.Map

    /// 1.  Invoke the supplied <c>interpret</c> function with the present state to determine whether any write is to occur.
    /// 2. (if events yielded) Attempt to sync the yielded events to the stream.
    ///    (Restarts up to <c>maxAttempts</c> times with updated state per attempt, throwing <c>MaxResyncsExhaustedException</c> on failure of final attempt.)
    member _.Transact(interpret : 'state -> 'event list, ?load, ?attempts) : Async<unit> =
        let inline decide struct (_t : Core.StreamToken, state) _ct = Task.FromResult struct ((), interpret state)
        let inline mapRes () struct (_t : Core.StreamToken, _s : 'state) = ()
        Core.Impl.transact(stream, LoadPolicy.Fetch load, decide, AttemptsPolicy.Validate attempts, mapRes)

    /// 1. Invoke the supplied <c>interpret</c> function with the present state
    /// 2. (if events yielded) Attempt to sync the yielded events to the stream.
    ///    (Restarts up to <c>maxAttempts</c> times with updated state per attempt, throwing <c>MaxResyncsExhaustedException</c> on failure of final attempt.)
    /// 3. Uses <c>render</c> to generate a 'view from the persisted final state
    member _.Transact(interpret : 'state -> 'event list, render : 'state -> 'view, ?load, ?attempts) : Async<'view> =
        let inline decide struct (_token, state) _ct = Task.FromResult struct ((), interpret state)
        let inline mapRes () struct (_token, state) = render state
        Core.Impl.transact(stream, LoadPolicy.Fetch load, decide, AttemptsPolicy.Validate attempts, mapRes)

    /// 1. Invoke the supplied <c>decide</c> function with the present state, holding the <c>'result</c>
    /// 2. (if events yielded) Attempt to sync the yielded events to the stream.
    ///    (Restarts up to <c>maxAttempts</c> times with updated state per attempt, throwing <c>MaxResyncsExhaustedException</c> on failure of final attempt.)
    /// 3. Yield result
    member _.Transact(decide : 'state -> 'result * 'event list, ?load, ?attempts) : Async<'result> =
        let inline decide struct (_token, state) _ct = let r, e = decide state in Task.FromResult struct (r, e)
        let inline mapRes r _ = r
        Core.Impl.transact(stream, LoadPolicy.Fetch load, decide, AttemptsPolicy.Validate attempts, mapRes)

    /// 1. Invoke the supplied <c>decide</c> function with the present state, holding the <c>'result</c>
    /// 2. (if events yielded) Attempt to sync the yielded events to the stream.
    ///    (Restarts up to <c>maxAttempts</c> times with updated state per attempt, throwing <c>MaxResyncsExhaustedException</c> on failure of final attempt.)
    /// 3. Yields a final 'view produced by <c>mapResult</c> from the <c>'result</c> and/or the final persisted <c>'state</c>
    member _.Transact(decide : 'state -> 'result * 'event list, mapResult : 'result -> 'state -> 'view, ?load, ?attempts) : Async<'view> =
        let inline decide struct (_token, state) _ct = let r, e = decide state in Task.FromResult struct (r, e)
        let inline mapRes r struct (_, s) = mapResult r s
        Core.Impl.transact (stream, LoadPolicy.Fetch load, decide, AttemptsPolicy.Validate attempts, mapRes)

    /// 1. Invoke the supplied <c>decide</c> function with the current complete context, holding the <c>'result</c>
    /// 2. (if events yielded) Attempt to sync the yielded events to the stream.
    ///    (Restarts up to <c>maxAttempts</c> times with updated state per attempt, throwing <c>MaxResyncsExhaustedException</c> on failure of final attempt.)
    /// 3. Yields <c>result</c>
    member _.TransactEx(decide : ISyncContext<'state> -> 'result * 'event list, ?load, ?attempts) : Async<'result> =
        let inline decide (Context c) _ct = let r, e = decide c in Task.FromResult struct (r, e)
        let inline mapRes r _ = r
        Core.Impl.transact (stream, LoadPolicy.Fetch load, decide, AttemptsPolicy.Validate attempts, mapRes)

    /// 1. Invoke the supplied <c>decide</c> function with the current complete context, holding the <c>'result</c>
    /// 2. (if events yielded) Attempt to sync the yielded events to the stream.
    ///    (Restarts up to <c>maxAttempts</c> times with updated state per attempt, throwing <c>MaxResyncsExhaustedException</c> on failure of final attempt.)
    /// 3. Yields a final 'view produced by <c>mapResult</c> from the <c>'result</c> and/or the final persisted <c>ISyncContext</c>
    member _.TransactEx(decide : ISyncContext<'state> -> 'result * 'event list, mapResult : 'result -> ISyncContext<'state> -> 'view, ?load, ?attempts) : Async<'view> =
        let inline decide (Context c) _ct = let r, e = decide c in Task.FromResult struct (r, e)
        let rec inline mapRes r (Context c) = mapResult r c
        Core.Impl.transact (stream, LoadPolicy.Fetch load, decide, AttemptsPolicy.Validate attempts, mapRes)

    /// Project from the folded <c>'state</c>, but without executing a decision flow as <c>Transact</c> does
    member _.Query(render : 'state -> 'view, ?load) : Async<'view> =
        Core.Impl.query struct (stream, LoadPolicy.Fetch load, fun struct (_token, state) -> render state)

    /// Project from the stream's complete context, but without executing a decision flow as <c>TransactEx<c> does
    member _.QueryEx(render : ISyncContext<'state> -> 'view, ?load) : Async<'view> =
        Core.Impl.query struct (stream, LoadPolicy.Fetch load, fun (Context c) -> render c)

    /// 1. Invoke the supplied <c>Async</c> <c>interpret</c> function with the present state
    /// 2. (if events yielded) Attempt to sync the yielded events to the stream.
    ///    (Restarts up to <c>maxAttempts</c> times with updated state per attempt, throwing <c>MaxResyncsExhaustedException</c> on failure of final attempt.)
    /// 3. Uses <c>render</c> to generate a 'view from the persisted final state
    member _.TransactAsync(interpret : 'state -> Async<'event list>, render : 'state -> 'view, ?load, ?attempts) : Async<'view> =
        let inline decide struct (_token, state) _ct = task { let! es = interpret state in return struct ((), es) }
        let rec inline mapRes () struct (_token, state) = render state
        Core.Impl.transact (stream, LoadPolicy.Fetch load, decide, AttemptsPolicy.Validate attempts, mapRes)

    /// 1. Invoke the supplied <c>Async</c> <c>decide</c> function with the present state, holding the <c>'result</c>
    /// 2. (if events yielded) Attempt to sync the yielded events to the stream.
    ///    (Restarts up to <c>maxAttempts</c> times with updated state per attempt, throwing <c>MaxResyncsExhaustedException</c> on failure of final attempt.)
    /// 3. Yield result
    member _.TransactAsync(decide : 'state -> Async<'result * 'event list>, ?load, ?attempts) : Async<'result> =
        let inline decide struct (_token, state) _ct = task { let! r, e = decide state in return struct (r, e) }
        let rec inline mapRes r _ = r
        Core.Impl.transact (stream, LoadPolicy.Fetch load, decide, AttemptsPolicy.Validate attempts, mapRes)

    /// 1. Invoke the supplied <c>Async</c> <c>decide</c> function with the current complete context, holding the <c>'result</c>
    /// 2. (if events yielded) Attempt to sync the yielded events to the stream.
    ///    (Restarts up to <c>maxAttempts</c> times with updated state per attempt, throwing <c>MaxResyncsExhaustedException</c> on failure of final attempt.)
    /// 3. Yield result
    member _.TransactExAsync(decide : ISyncContext<'state> -> Async<'result * 'event list>, ?load, ?attempts) : Async<'result> =
        let inline decide (Context c) _ct = task { let! r, e = decide c in return struct (r, e) }
        let rec inline mapRes r _ = r
        Core.Impl.transact (stream, LoadPolicy.Fetch load, decide, AttemptsPolicy.Validate attempts, mapRes)

    /// 1. Invoke the supplied <c>Async</c> <c>decide</c> function with the current complete context, holding the <c>'result</c>
    /// 2. (if events yielded) Attempt to sync the yielded events to the stream.
    ///    (Restarts up to <c>maxAttempts</c> times with updated state per attempt, throwing <c>MaxResyncsExhaustedException</c> on failure of final attempt.)
    /// 3. Yields a final 'view produced by <c>mapResult</c> from the <c>'result</c> and/or the final persisted <c>ISyncContext</c>
    member _.TransactExAsync(decide : ISyncContext<'state> -> Async<'result * 'event list>, mapResult : 'result -> ISyncContext<'state> -> 'view, ?load, ?attempts) : Async<'view> =
        let inline decide (Context c) _ct = task { let! r, e = decide c in return struct (r, e) }
        let rec inline mapRes r (Context c) = mapResult r c
        Core.Impl.transact (stream, LoadPolicy.Fetch load, decide, AttemptsPolicy.Validate attempts, mapRes)

(* Options to tune loading policy - default is RequireLoad*)

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
    | FromMemento of memento : struct (Core.StreamToken * 'state)
and internal LoadPolicy() =
    static member Fetch<'state, 'event>(x : LoadOption<'state> option)
        : Core.IStream<'event, 'state> -> System.Threading.CancellationToken -> Task<struct (Core.StreamToken * 'state)> =
        match x with
        | None | Some RequireLoad ->                 fun stream ct ->   stream.Load(allowStale = false, ct = ct)
        | Some AllowStale ->                         fun stream ct ->   stream.Load(allowStale = true, ct = ct)
        | Some AssumeEmpty ->                        fun stream _ct ->  Task.FromResult(stream.LoadEmpty())
        | Some (FromMemento (streamToken, state)) -> fun _stream _ct -> Task.FromResult(streamToken, state)

(* Retry / Attempts policy used to define policy for resyncing state when there's an Append conflict (default 3 retries) *)

and [<NoComparison; NoEquality; RequireQualifiedAccess>] Attempts =
    | Max of count : int

and internal AttemptsPolicy() =

    static member Validate(opt : Attempts option) =
        let maxAttempts = match opt with Some (Attempts.Max n) -> n | None -> 3
        if maxAttempts < 1 then raise <| System.ArgumentOutOfRangeException(nameof opt, maxAttempts, "should be >= 1")
        fun attempt -> if attempt = maxAttempts then raise (MaxResyncsExhaustedException attempt)

/// Exception yielded by Decider.Transact after `count` attempts have yielded conflicts at the point of syncing with the Store
and MaxResyncsExhaustedException(count) =
   inherit exn(sprintf "Concurrency violation; aborting after %i attempts." count)

(* Extended context interface exposed by TransactEx / QueryEx *)

/// Exposed by TransactEx / QueryEx, providing access to extended state information for cases where that's required
and ISyncContext<'state> =

    /// Exposes the underlying Store's internal Version for the underlying stream.
    /// An empty stream is Version 0; one with a single event is Version 1 etc.
    /// It's important to consider that this Version is more authoritative than counting the events seen, or adding 1 to
    ///   the `Index` of the last event passed to your `fold` function - the codec may opt to ignore events
    abstract member Version : int64

    /// The Storage occupied by the Events written to the underlying stream at the present time.
    /// Specific stores may vary whether this is available, the basis and preciseness for how it is computed.
    abstract member StreamEventBytes : int64 voption

    /// The present State of the stream within the context of this Flow
    abstract member State : 'state

    /// Represents a Checkpoint position on a Stream's timeline; Can be used to manage continuations via LoadOption.FromMemento
    abstract member CreateMemento : unit -> struct (Core.StreamToken * 'state)

and internal SyncContext<'state> =

    static member Map(struct (token : Core.StreamToken, state : 'state)) =
        { new ISyncContext<'state> with
            member _.State = state
            member _.Version = token.version
            member _.StreamEventBytes = match token.streamBytes with -1L -> ValueNone | b -> ValueSome b
            member _.CreateMemento() = token, state }
