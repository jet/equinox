namespace Equinox

open Equinox.Core
open Equinox.Core.Tracing
open System

type private CacheEntry<'state>(initialToken: StreamToken, initialState: 'state, initialTimestamp: int64) =
    let mutable currentToken = initialToken
    let mutable currentState = initialState
    let mutable verifiedTimestamp = initialTimestamp
    let tryGet () =
        if verifiedTimestamp = 0 then ValueNone
        else ValueSome (struct (currentToken, currentState))
    let mutable cell = AsyncLazy<struct(int64 * (struct (StreamToken * 'state)))>.Empty
    static member CreateEmpty() =
        new CacheEntry<'state>(Unchecked.defaultof<StreamToken>, Unchecked.defaultof<'state>, 0)
    member x.TryGetValue(): (struct (StreamToken * 'state)) voption =
        lock x tryGet
    member x.MergeUpdates(isStale: Func<StreamToken, StreamToken, bool>, timestamp, token, state) =
        lock x <| fun () ->
            if not (isStale.Invoke(currentToken, token)) then
                currentToken <- token
                currentState <- state
                if verifiedTimestamp < timestamp then // Don't count attempts to overwrite with stale state as verification
                    verifiedTimestamp <- timestamp
    // Follows high level flow of AsyncCacheCell.Await - read the comments there, and the AsyncCacheCell tests first!
    member x.ReadThrough(maxAge: TimeSpan, isStale, load) = task {
        let cacheEntryValidityCheckTimestamp = System.Diagnostics.Stopwatch.GetTimestamp()
        let isWithinMaxAge cachedValueTimestamp = Stopwatch.TicksToSeconds(cacheEntryValidityCheckTimestamp - cachedValueTimestamp) <= maxAge.TotalSeconds
        let fetchStateConsistently () = struct (cell, tryGet (), isWithinMaxAge verifiedTimestamp)
        match lock x fetchStateConsistently with
        | _, ValueSome cachedValue, true ->
            return cachedValue
        | ourInitialCellState, maybeBaseState, _ -> // If it's not good enough for us, trigger a request (though someone may have beaten us to that)

        // Inspect/await any concurrent attempt to see if it is sufficient for our needs
        match! ourInitialCellState.TryAwaitValid() with
        | ValueSome (fetchCommencedTimestamp, res) when isWithinMaxAge fetchCommencedTimestamp -> return res
        | _ ->

        // .. it wasn't; join the race to dispatch a request (others following us will share our fate via the TryAwaitValid)
        let newInstance = AsyncLazy(load maybeBaseState)
        let _ = System.Threading.Interlocked.CompareExchange(&cell, newInstance, ourInitialCellState)
        let! timestamp, (token, state as res) = cell.Await()
        x.MergeUpdates(isStale, timestamp, token, state) // merge observed result into the cache
        return res }

type Cache private (inner: System.Runtime.Caching.MemoryCache) =
    let tryLoad key =
        match inner.Get key with
        | null -> ValueNone
        | :? CacheEntry<'state> as existingEntry -> existingEntry.TryGetValue()
        | x -> failwith $"tryLoad Incompatible cache entry %A{x}"
    let addOrGet key policy entry =
        match inner.AddOrGetExisting(key, entry, policy = policy) with
        | null -> Ok entry
        | :? CacheEntry<'state> as existingEntry -> Error existingEntry
        | x -> failwith $"addOrGet Incompatible cache entry %A{x}"
    let getElseAddEmptyEntry key policy =
        match addOrGet key policy (CacheEntry<'state>.CreateEmpty()) with
        | Ok fresh -> fresh
        | Error existingEntry -> existingEntry
    let addOrMergeCacheEntry isStale key options timestamp struct (token, state) =
        let entry = CacheEntry(token, state, timestamp)
        match addOrGet key options entry with
        | Ok _ -> () // Our fresh one got added
        | Error existingEntry -> existingEntry.MergeUpdates(isStale, timestamp, token, state)
    new (name, sizeMb: int) =
        let config = System.Collections.Specialized.NameValueCollection(1)
        config.Add("cacheMemoryLimitMegabytes", string sizeMb);
        Cache(new System.Runtime.Caching.MemoryCache(name, config))
    // if there's a non-zero maxAge, concurrent read attempts share the roundtrip (and its fate, if it throws)
    member _.Load(key, maxAge, isStale, policy, loadOrReload) = task {
        let loadOrReload maybeBaseState () = task {
            let act = System.Diagnostics.Activity.Current
            if act <> null then act.AddCacheHit(ValueOption.isSome maybeBaseState) |> ignore
            let ts = System.Diagnostics.Stopwatch.GetTimestamp()
            let! res = loadOrReload maybeBaseState
            return struct (ts, res) }
        if maxAge = TimeSpan.Zero then // Boring algorithm that has each caller independently load/reload the data and then cache it
            let maybeBaseState = tryLoad key
            let! timestamp, res = loadOrReload maybeBaseState ()
            addOrMergeCacheEntry isStale key policy timestamp res
            return res
        else // ensure we have an entry in the cache for this key; coordinate retrieval through that
            let cacheSlot = getElseAddEmptyEntry key policy
            return! cacheSlot.ReadThrough(maxAge, isStale, loadOrReload) }
    // Newer values get saved; equal values update the last retrieval timestamp
    member _.Save(key, isStale, policy, timestamp, token, state) =
        addOrMergeCacheEntry isStale key policy timestamp (token, state)

type [<NoComparison; NoEquality; RequireQualifiedAccess>] CachingStrategy =
    /// Retain a single 'state per streamName.
    /// Each cache hit for a stream renews the retention period for the defined <c>window</c>.
    /// Upon expiration of the defined <c>window</c> from the point at which the cache was entry was last used, a full reload is triggered.
    /// Unless a <c>LoadOption</c> is used, cache hits still incur a roundtrip to load any subsequently-added events.
    | SlidingWindow of Cache * window: TimeSpan
    /// Retain a single 'state per streamName.
    /// Upon expiration of the defined <c>period</c>, a full reload is triggered.
    /// Unless a <c>LoadOption</c> is used, cache hits still incur a roundtrip to load any subsequently-added events.
    | FixedTimeSpan of Cache * period: TimeSpan
    /// Prefix is used to segregate multiple folded states per stream when they are stored in the cache.
    /// Semantics are otherwise identical to <c>SlidingWindow</c>.
    | SlidingWindowPrefixed of Cache * window: TimeSpan * prefix: string
