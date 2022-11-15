namespace Equinox

open Equinox.Core
open System
open System.Threading
open System.Threading.Tasks

/// Central Application-facing API. Wraps the handling of decision or query flows in a manner that is store agnostic
/// NOTE: For C#, direct usage of DeciderCore is recommended
type Decider<'event, 'state>(stream : IStream<'event, 'state>) =

    let inner = DeciderCore(stream)

    /// 1.  Invoke the supplied <c>interpret</c> function with the present state to determine whether any write is to occur.
    /// 2. (if events yielded) Attempt to sync the yielded events to the stream.
    ///    (Restarts up to <c>maxAttempts</c> times with updated state per attempt, throwing <c>MaxResyncsExhaustedException</c> on failure of final attempt.)
    member _.Transact(interpret : 'state -> 'event list, ?load, ?attempts) : Async<unit> = async {
        let! ct = Async.CancellationToken
        return! inner.Transact(interpret >> Seq.ofList, ?load = load, ?attempts = attempts, ct = ct) |> Async.AwaitTaskCorrect }

    /// 1. Invoke the supplied <c>interpret</c> function with the present state
    /// 2. (if events yielded) Attempt to sync the yielded events to the stream.
    ///    (Restarts up to <c>maxAttempts</c> times with updated state per attempt, throwing <c>MaxResyncsExhaustedException</c> on failure of final attempt.)
    /// 3. Uses <c>render</c> to generate a 'view from the persisted final state
    member _.Transact(interpret : 'state -> 'event list, render : 'state -> 'view, ?load, ?attempts) : Async<'view> = async {
        let! ct = Async.CancellationToken
        return! inner.Transact(interpret >> Seq.ofList, render, ?load = load, ?attempts = attempts, ct = ct) |> Async.AwaitTaskCorrect }

    /// 1. Invoke the supplied <c>decide</c> function with the present state, holding the <c>'result</c>
    /// 2. (if events yielded) Attempt to sync the yielded events to the stream.
    ///    (Restarts up to <c>maxAttempts</c> times with updated state per attempt, throwing <c>MaxResyncsExhaustedException</c> on failure of final attempt.)
    /// 3. Yield result
    member _.Transact(decide : 'state -> 'result * 'event list, ?load, ?attempts) : Async<'result> = async {
        let! ct = Async.CancellationToken
        let inline decide' s = let r, es = decide s in struct (r, Seq.ofList es)
        return! inner.Transact(decide', ?load = load, ?attempts = attempts, ct = ct) |> Async.AwaitTaskCorrect }

    /// 1. Invoke the supplied <c>decide</c> function with the present state, holding the <c>'result</c>
    /// 2. (if events yielded) Attempt to sync the yielded events to the stream.
    ///    (Restarts up to <c>maxAttempts</c> times with updated state per attempt, throwing <c>MaxResyncsExhaustedException</c> on failure of final attempt.)
    /// 3. Yields a final 'view produced by <c>mapResult</c> from the <c>'result</c> and/or the final persisted <c>'state</c>
    member _.Transact(decide : 'state -> 'result * 'event list, mapResult : 'result -> 'state -> 'view, ?load, ?attempts) : Async<'view> = async {
        let! ct = Async.CancellationToken
        let inline decide' s = let r, es = decide s in struct (r, Seq.ofList es)
        return! inner.Transact(decide', mapResult, ?load = load, ?attempts = attempts, ct = ct) |> Async.AwaitTaskCorrect }

    /// 1. Invoke the supplied <c>decide</c> function with the current complete context, holding the <c>'result</c>
    /// 2. (if events yielded) Attempt to sync the yielded events to the stream.
    ///    (Restarts up to <c>maxAttempts</c> times with updated state per attempt, throwing <c>MaxResyncsExhaustedException</c> on failure of final attempt.)
    /// 3. Yields <c>result</c>
    member _.TransactEx(decide : ISyncContext<'state> -> 'result * 'event list, ?load, ?attempts) : Async<'result> = async {
        let! ct = Async.CancellationToken
        let inline decide' c = let r, es = decide c in struct (r, Seq.ofList es)
        return! inner.TransactEx(decide = decide', ?load = load, ?attempts = attempts, ct = ct) |> Async.AwaitTaskCorrect }

    /// 1. Invoke the supplied <c>decide</c> function with the current complete context, holding the <c>'result</c>
    /// 2. (if events yielded) Attempt to sync the yielded events to the stream.
    ///    (Restarts up to <c>maxAttempts</c> times with updated state per attempt, throwing <c>MaxResyncsExhaustedException</c> on failure of final attempt.)
    /// 3. Yields a final 'view produced by <c>mapResult</c> from the <c>'result</c> and/or the final persisted <c>ISyncContext</c>
    member _.TransactEx(decide : ISyncContext<'state> -> 'result * 'event list, mapResult : 'result -> ISyncContext<'state> -> 'view,
                        ?load, ?attempts) : Async<'view> =  async {
        let! ct = Async.CancellationToken
        let inline decide' c = let r, es = decide c in struct (r, Seq.ofList es)
        return! inner.TransactEx(decide = decide', mapResult = mapResult, ?load = load, ?attempts = attempts, ct = ct) |> Async.AwaitTaskCorrect }

    /// Project from the folded <c>'state</c>, but without executing a decision flow as <c>Transact</c> does
    member _.Query(render : 'state -> 'view, ?load) : Async<'view> = async {
        let! ct = Async.CancellationToken
        return! inner.Query(render, ?load = load, ct = ct) |> Async.AwaitTaskCorrect }

    /// Project from the stream's complete context, but without executing a decision flow as <c>TransactEx<c> does
    member _.QueryEx(render : ISyncContext<'state> -> 'view, ?load) : Async<'view> =  async {
        let! ct = Async.CancellationToken
        return! inner.QueryEx(render, ?load = load, ct = ct) |> Async.AwaitTaskCorrect }

    /// 1. Invoke the supplied <c>Async</c> <c>interpret</c> function with the present state
    /// 2. (if events yielded) Attempt to sync the yielded events to the stream.
    ///    (Restarts up to <c>maxAttempts</c> times with updated state per attempt, throwing <c>MaxResyncsExhaustedException</c> on failure of final attempt.)
    /// 3. Uses <c>render</c> to generate a 'view from the persisted final state
    member _.TransactAsync(interpret : 'state -> Async<'event list>, render : 'state -> 'view, ?load, ?attempts) : Async<'view> =  async {
        let! ct = Async.CancellationToken
        let inline interpret' s ct = task { let! es = Async.StartImmediateAsTask(interpret s, ct) in return Seq.ofList es }
        return! inner.TransactAsync(interpret', render, ?load = load, ?attempts = attempts, ct = ct) |> Async.AwaitTaskCorrect }

    /// 1. Invoke the supplied <c>Async</c> <c>decide</c> function with the present state, holding the <c>'result</c>
    /// 2. (if events yielded) Attempt to sync the yielded events to the stream.
    ///    (Restarts up to <c>maxAttempts</c> times with updated state per attempt, throwing <c>MaxResyncsExhaustedException</c> on failure of final attempt.)
    /// 3. Yield result
    member _.TransactAsync(decide : 'state -> Async<'result * 'event list>, ?load, ?attempts) : Async<'result> =  async {
        let! ct = Async.CancellationToken
        let inline decide' s ct = task { let! r, es = Async.StartImmediateAsTask(decide s, ct) in return struct (r, Seq.ofList es) }
        return! inner.TransactAsync(decide = decide', ?load = load, ?attempts = attempts, ct = ct) |> Async.AwaitTaskCorrect }

    /// 1. Invoke the supplied <c>Async</c> <c>decide</c> function with the current complete context, holding the <c>'result</c>
    /// 2. (if events yielded) Attempt to sync the yielded events to the stream.
    ///    (Restarts up to <c>maxAttempts</c> times with updated state per attempt, throwing <c>MaxResyncsExhaustedException</c> on failure of final attempt.)
    /// 3. Yield result
    member _.TransactExAsync(decide : ISyncContext<'state> -> Async<'result * 'event list>, ?load, ?attempts) : Async<'result> =  async {
        let! ct = Async.CancellationToken
        let decide' c ct = task { let! r, es = Async.StartImmediateAsTask(decide c, ct) in return struct (r, Seq.ofList es) }
        return! inner.TransactExAsync(decide = decide', ?load = load, ?attempts = attempts, ct = ct) |> Async.AwaitTaskCorrect }

    /// 1. Invoke the supplied <c>Async</c> <c>decide</c> function with the current complete context, holding the <c>'result</c>
    /// 2. (if events yielded) Attempt to sync the yielded events to the stream.
    ///    (Restarts up to <c>maxAttempts</c> times with updated state per attempt, throwing <c>MaxResyncsExhaustedException</c> on failure of final attempt.)
    /// 3. Yields a final 'view produced by <c>mapResult</c> from the <c>'result</c> and/or the final persisted <c>ISyncContext</c>
    member _.TransactExAsync(decide : ISyncContext<'state> -> Async<'result * 'event list>, mapResult : 'result -> ISyncContext<'state> -> 'view,
                             ?load, ?attempts) : Async<'view> =  async {
        let! ct = Async.CancellationToken
        let inline decide' c ct = task { let! r, es = Async.StartImmediateAsTask(decide c, ct) in return struct (r, Seq.ofList es) }
        return! inner.TransactExAsync(decide = decide', mapResult = mapResult, ?load = load, ?attempts = attempts, ct = ct) |> Async.AwaitTaskCorrect }

/// Central Application-facing API. Wraps the handling of decision or query flows in a manner that is store agnostic
and DeciderCore<'event, 'state>(stream : IStream<'event, 'state>) =

    let (|Context|) = SyncContext<'state>.Map

    /// 1.  Invoke the supplied <c>interpret</c> function with the present state to determine whether any write is to occur.
    /// 2. (if events yielded) Attempt to sync the yielded events to the stream.
    ///    (Restarts up to <c>maxAttempts</c> times with updated state per attempt, throwing <c>MaxResyncsExhaustedException</c> on failure of final attempt.)
    member _.Transact(interpret : Func<'state, 'event seq>,
                      [<O; D null>] ?load, [<O; D null>] ?attempts, [<O; D(CancellationToken())>]?ct) : Task<unit> =
        let inline decide struct (_t : StreamToken, state) _ct = Task.FromResult struct ((), interpret.Invoke state)
        let inline mapRes () struct (_t : StreamToken, _s : 'state) = ()
        Impl.TransactAsync(stream, LoadPolicy.Fetch load, decide, AttemptsPolicy.Validate attempts, mapRes, defaultArg ct CancellationToken.None)

    /// 1. Invoke the supplied <c>interpret</c> function with the present state
    /// 2. (if events yielded) Attempt to sync the yielded events to the stream.
    ///    (Restarts up to <c>maxAttempts</c> times with updated state per attempt, throwing <c>MaxResyncsExhaustedException</c> on failure of final attempt.)
    /// 3. Uses <c>render</c> to generate a 'view from the persisted final state
    member _.Transact(interpret : Func<'state, 'event seq>, render : Func<'state, 'view>,
                      [<O; D null>] ?load, [<O; D null>] ?attempts, [<O; D(CancellationToken())>]?ct) : Task<'view> =
        let inline decide struct (_token, state) _ct = Task.FromResult struct ((), interpret.Invoke state)
        let inline mapRes () struct (_token, state) = render.Invoke state
        Impl.TransactAsync(stream, LoadPolicy.Fetch load, decide, AttemptsPolicy.Validate attempts, mapRes, defaultArg ct CancellationToken.None)

    /// 1. Invoke the supplied <c>decide</c> function with the present state, holding the <c>'result</c>
    /// 2. (if events yielded) Attempt to sync the yielded events to the stream.
    ///    (Restarts up to <c>maxAttempts</c> times with updated state per attempt, throwing <c>MaxResyncsExhaustedException</c> on failure of final attempt.)
    /// 3. Yield result
    member _.Transact(decide : Func<'state, struct ('result * 'event seq)>,
                      [<O; D null>] ?load, [<O; D null>] ?attempts, [<O; D null>] ?ct) : Task<'result> =
        let inline decide struct (_token, state) _ct = decide.Invoke state |> Task.FromResult
        let inline mapRes r _ = r
        Impl.TransactAsync(stream, LoadPolicy.Fetch load, decide, AttemptsPolicy.Validate attempts, mapRes, defaultArg ct CancellationToken.None)

    /// 1. Invoke the supplied <c>decide</c> function with the present state, holding the <c>'result</c>
    /// 2. (if events yielded) Attempt to sync the yielded events to the stream.
    ///    (Restarts up to <c>maxAttempts</c> times with updated state per attempt, throwing <c>MaxResyncsExhaustedException</c> on failure of final attempt.)
    /// 3. Yields a final 'view produced by <c>mapResult</c> from the <c>'result</c> and/or the final persisted <c>'state</c>
    member _.Transact(decide : Func<'state, struct ('result * 'event seq)>, mapResult : Func<'result, 'state, 'view>,
                      [<O; D null>] ?load, [<O; D null>] ?attempts, [<O; D null>] ?ct) : Task<'view> =
        let inline decide struct (_token, state) _ct = decide.Invoke state |> Task.FromResult
        let inline mapRes r struct (_, s) = mapResult.Invoke(r, s)
        Impl.TransactAsync(stream, LoadPolicy.Fetch load, decide, AttemptsPolicy.Validate attempts, mapRes, defaultArg ct CancellationToken.None)

    /// 1. Invoke the supplied <c>decide</c> function with the current complete context, holding the <c>'result</c>
    /// 2. (if events yielded) Attempt to sync the yielded events to the stream.
    ///    (Restarts up to <c>maxAttempts</c> times with updated state per attempt, throwing <c>MaxResyncsExhaustedException</c> on failure of final attempt.)
    /// 3. Yields <c>result</c>
    member _.TransactEx(decide : Func<ISyncContext<'state>, struct ('result * 'event seq)>,
                        [<O; D null>] ?load, [<O; D null>] ?attempts, [<O; D null>] ?ct) : Task<'result> =
        let inline decide' (Context c) _ct = let r = decide.Invoke(c) in Task.FromResult r
        let inline mapRes r _ = r
        Impl.TransactAsync(stream, LoadPolicy.Fetch load, decide', AttemptsPolicy.Validate attempts, mapRes, defaultArg ct CancellationToken.None)

    /// 1. Invoke the supplied <c>decide</c> function with the current complete context, holding the <c>'result</c>
    /// 2. (if events yielded) Attempt to sync the yielded events to the stream.
    ///    (Restarts up to <c>maxAttempts</c> times with updated state per attempt, throwing <c>MaxResyncsExhaustedException</c> on failure of final attempt.)
    /// 3. Yields a final 'view produced by <c>mapResult</c> from the <c>'result</c> and/or the final persisted <c>ISyncContext</c>
    member _.TransactEx(decide : Func<ISyncContext<'state>, struct ('result * 'event seq)>, mapResult : Func<'result, ISyncContext<'state>, 'view>,
                        [<O; D null>] ?load, [<O; D null>] ?attempts, [<O; D null>] ?ct) : Task<'view> =
        let inline decide (Context c) _ct = c |> decide.Invoke |> Task.FromResult
        let inline mapRes r (Context c) = mapResult.Invoke(r, c)
        Impl.TransactAsync(stream, LoadPolicy.Fetch load, decide, AttemptsPolicy.Validate attempts, mapRes, defaultArg ct CancellationToken.None)

    /// Project from the folded <c>'state</c>, but without executing a decision flow as <c>Transact</c> does
    member _.Query(render : Func<'state, 'view>, [<O; D null>] ?load, [<O; D null>] ?ct) : Task<'view> =
        Impl.QueryAsync(stream, LoadPolicy.Fetch load, (fun struct (_token, state) -> render.Invoke state), defaultArg ct CancellationToken.None)

    /// Project from the stream's complete context, but without executing a decision flow as <c>TransactEx<c> does
    member _.QueryEx(render : Func<ISyncContext<'state>, 'view>, [<O; D null>] ?load, [<O; D null>] ?ct) : Task<'view> =
        Impl.QueryAsync(stream, LoadPolicy.Fetch load, (fun (Context c) -> render.Invoke c), defaultArg ct CancellationToken.None)

    /// 1. Invoke the supplied <c>Async</c> <c>interpret</c> function with the present state
    /// 2. (if events yielded) Attempt to sync the yielded events to the stream.
    ///    (Restarts up to <c>maxAttempts</c> times with updated state per attempt, throwing <c>MaxResyncsExhaustedException</c> on failure of final attempt.)
    /// 3. Uses <c>render</c> to generate a 'view from the persisted final state
    member _.TransactAsync(interpret : Func<'state, CancellationToken, Task<'event seq>>, render : Func<'state, 'view>,
                           [<O; D null>] ?load, [<O; D null>] ?attempts, [<O; D null>] ?ct) : Task<'view> =
        let inline decide struct (_token, state) ct = task { let! es = interpret.Invoke(state, ct) in return struct ((), es) }
        let inline mapRes () struct (_token, state) = render.Invoke state
        Impl.TransactAsync(stream, LoadPolicy.Fetch load, decide, AttemptsPolicy.Validate attempts, mapRes, defaultArg ct CancellationToken.None)

    /// 1. Invoke the supplied <c>Async</c> <c>decide</c> function with the present state, holding the <c>'result</c>
    /// 2. (if events yielded) Attempt to sync the yielded events to the stream.
    ///    (Restarts up to <c>maxAttempts</c> times with updated state per attempt, throwing <c>MaxResyncsExhaustedException</c> on failure of final attempt.)
    /// 3. Yield result
    member _.TransactAsync(decide : Func<'state, CancellationToken, Task<struct ('result * 'event seq)>>,
                           [<O; D null>] ?load, [<O; D null>] ?attempts, [<O; D null>] ?ct) : Task<'result> =
        let inline decide struct (_token, state) ct = task { let! r, e = decide.Invoke(state, ct) in return struct (r, e) }
        let inline mapRes r _ = r
        Impl.TransactAsync(stream, LoadPolicy.Fetch load, decide, AttemptsPolicy.Validate attempts, mapRes, defaultArg ct CancellationToken.None)

    /// 1. Invoke the supplied <c>Async</c> <c>decide</c> function with the current complete context, holding the <c>'result</c>
    /// 2. (if events yielded) Attempt to sync the yielded events to the stream.
    ///    (Restarts up to <c>maxAttempts</c> times with updated state per attempt, throwing <c>MaxResyncsExhaustedException</c> on failure of final attempt.)
    /// 3. Yield result
    member _.TransactExAsync(decide : Func<ISyncContext<'state>, CancellationToken, Task<struct ('result * 'event seq)>>,
                             [<O; D null>] ?load, [<O; D null>] ?attempts, [<O; D null>] ?ct) : Task<'result> =
        let inline decide (Context c) ct = task { let! r, e = decide.Invoke (c, ct) in return struct (r, e) }
        let inline mapRes r _ = r
        Impl.TransactAsync(stream, LoadPolicy.Fetch load, decide, AttemptsPolicy.Validate attempts, mapRes, defaultArg ct CancellationToken.None)

    /// 1. Invoke the supplied <c>Async</c> <c>decide</c> function with the current complete context, holding the <c>'result</c>
    /// 2. (if events yielded) Attempt to sync the yielded events to the stream.
    ///    (Restarts up to <c>maxAttempts</c> times with updated state per attempt, throwing <c>MaxResyncsExhaustedException</c> on failure of final attempt.)
    /// 3. Yields a final 'view produced by <c>mapResult</c> from the <c>'result</c> and/or the final persisted <c>ISyncContext</c>
    member _.TransactExAsync(decide : Func<ISyncContext<'state>, CancellationToken, Task<struct ('result * 'event seq)>>, mapResult : Func<'result, ISyncContext<'state>, 'view>,
                             [<O; D null>] ?load, [<O; D null>] ?attempts, [<O; D null>] ?ct) : Task<'view> =
        let inline decide (Context c) ct = decide.Invoke(c, ct)
        let inline mapRes r (Context c) = mapResult.Invoke(r, c)
        Impl.TransactAsync(stream, LoadPolicy.Fetch load, decide, AttemptsPolicy.Validate attempts, mapRes, defaultArg ct CancellationToken.None)

(* Options to tune loading policy - default is RequireLoad *)

/// Store-agnostic Loading Options
and [<NoComparison; NoEquality>] LoadOption<'state> =
    /// Default policy; Obtain latest state from store based on consistency level configured
    | RequireLoad
    /// Request that data be read with a quorum read / from a Leader connection
    | RequireLeader
    /// If the Cache holds any state, use that without checking the backing store for updates, implying:
    /// - maximizing how much we lean on Optimistic Concurrency Control when doing a `Transact` (you're still guaranteed a consistent outcome)
    /// - enabling stale reads [in the face of multiple writers (either in this process or in other processes)] when doing a `Query`
    | AllowStale
    /// Inhibit load from database based on the fact that the stream is likely not to have been initialized yet, and we will be generating events
    | AssumeEmpty
    /// <summary>Instead of loading from database, seed the loading process with the supplied memento, obtained via <c>ISyncContext.CreateMemento()</c></summary>
    | FromMemento of memento : struct (StreamToken * 'state)
and internal LoadPolicy() =
    static member Fetch<'state, 'event>(x : LoadOption<'state> option)
        : IStream<'event, 'state> -> CancellationToken -> Task<struct (StreamToken * 'state)> =
        match x with
        | None | Some RequireLoad ->                 fun stream ct ->   stream.Load(allowStale = false, requireLeader = false, ct = ct)
        | Some RequireLeader ->                      fun stream ct ->   stream.Load(allowStale = false, requireLeader = true,  ct = ct)
        | Some AllowStale ->                         fun stream ct ->   stream.Load(allowStale = true,  requireLeader = false, ct = ct)
        | Some AssumeEmpty ->                        fun stream _ct ->  Task.FromResult(stream.LoadEmpty())
        | Some (FromMemento (streamToken, state)) -> fun _stream _ct -> Task.FromResult(streamToken, state)

(* Retry / Attempts policy used to define policy for resyncing state when there's an Append conflict (default 3 retries) *)

and [<NoComparison; NoEquality; RequireQualifiedAccess>] Attempts =
    | Max of count : int
and internal AttemptsPolicy() =
    static member Validate(opt : Attempts option) =
        let maxAttempts = match opt with Some (Attempts.Max n) -> n | None -> 3
        if maxAttempts < 1 then raise (ArgumentOutOfRangeException(nameof opt, maxAttempts, "should be >= 1"))
        fun attempt -> if attempt = maxAttempts then raise (MaxResyncsExhaustedException attempt)

/// Exception yielded by Decider.Transact after `count` attempts have yielded conflicts at the point of syncing with the Store
and MaxResyncsExhaustedException(count) =
   inherit exn(sprintf "Concurrency violation; aborting after %i attempts." count)


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
    abstract member CreateMemento : unit -> struct (StreamToken * 'state)

and internal SyncContext<'state> =

    static member Map(struct (token : StreamToken, state : 'state)) =
        { new ISyncContext<'state> with
            member _.State = state
            member _.Version = token.version
            member _.StreamEventBytes = match token.streamBytes with -1L -> ValueNone | b -> ValueSome b
            member _.CreateMemento() = token, state }

[<Struct>]
type StreamId =
    { categoryName : string; streamId : string }
    override x.ToString() = String.Concat(x.categoryName, '-', x.streamId)

module StreamId =

    module Internal =
        /// Throws if a candidate category includes a '-', is null, or is empty
        let inline validateCategory (rawCategory : string) =
            if rawCategory |> String.IsNullOrEmpty then invalidArg "rawCategory" "may not be null or empty"
            if rawCategory.IndexOf '-' <> -1 then invalidArg "rawCategory" "may not contain embedded '-' symbols"

        /// Throws if a candidate id element includes a '_', is null, or is empty
        let inline validateElement (rawElement : string) =
            if rawElement |> String.IsNullOrEmpty then invalidArg "rawElement" "may not contain null or empty components"
            if rawElement.IndexOf '_' <> -1 then invalidArg "rawElement" "may not contain embedded '_' symbols"

        /// Low level helper used to gate ingestion from a canonical form
        /// Does NOT validate the StreamId portion wrt numbers of embedded `_` or `-` chars
        let create category sid =
            validateCategory category
            { categoryName = category; streamId = sid }
        let toString (x : StreamId) = string x

    let private mapElement f id = let t = f id in Internal.validateElement t; t
    let map category f id = Internal.create category (mapElement f id)
    let private mapElements (elements : string seq) : string =
        for x in elements do Internal.validateElement x
        String.Join("_", elements)
    let map2 category f f2 (id1, id2) = Internal.create category (mapElements (seq { yield f id1; yield f2 id2 }))
    let map3 category f f2 f3 (id1, id2, id3) = Internal.create category (mapElements (seq { yield f id1; yield f2 id2; yield f3 id3 }))
