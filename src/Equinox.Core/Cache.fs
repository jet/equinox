namespace Equinox.Core

open System
open System.Runtime.Caching
open System.Threading.Tasks

type [<NoEquality; NoComparison; Struct>] CacheItemOptions =
    | AbsoluteExpiration of ae: DateTimeOffset
    | RelativeExpiration of re: TimeSpan
module internal CacheItemOptions =
    let toPolicy = function
        | AbsoluteExpiration absolute -> CacheItemPolicy(AbsoluteExpiration = absolute)
        | RelativeExpiration relative -> CacheItemPolicy(SlidingExpiration = relative)

type ICache =
    abstract member Load: key: string
                          * maxAge: TimeSpan
                          * isStale: Func<StreamToken, StreamToken, bool>
                          * options: CacheItemOptions
                          * loadOrReload: (struct (StreamToken * 'state) voption -> Task<struct (StreamToken * 'state)>)
                          -> Task<struct (StreamToken * 'state)>
    abstract member Save: key: string
                          * isStale: Func<StreamToken, StreamToken, bool>
                          * options: CacheItemOptions
                          * token: StreamToken * state: 'state
                          -> unit

namespace Equinox

open Equinox.Core
open Equinox.Core.Tracing
open System
open System.Threading.Tasks

type internal CacheEntry<'state>(initialToken: StreamToken, initialState: 'state, initialVerified: int64) =
    let mutable currentToken = initialToken
    let mutable currentState = initialState
    let mutable lastVerified = initialVerified
    let tryGet () =
        if lastVerified = 0 then ValueNone
        else ValueSome (struct (currentToken, currentState))
    let mutable cell = AsyncLazy.Empty
    static member CreateEmpty() =
        new CacheEntry<'state>(Unchecked.defaultof<StreamToken>, Unchecked.defaultof<'state>, 0)
    member x.TryGetValue(): (struct (StreamToken * 'state)) voption =
        lock x tryGet
    member x.MergeUpdates(isStale: Func<StreamToken, StreamToken, bool>, token, state, timestamp) =
        lock x <| fun () ->
            if not (isStale.Invoke(currentToken, token)) then
                currentToken <- token
                currentState <- state
                if lastVerified < timestamp then // Don't count attempts to overwrite with stale state as verification
                    lastVerified <- timestamp
    // Follows high level flow of AsyncCacheCell.Await - read the comments there, and the AsyncCacheCell tests first!
    member x.ReadThrough(maxAge: TimeSpan, isStale, load) : Task<struct (StreamToken * 'state)> = task {
        let timestamp = System.Diagnostics.Stopwatch.GetTimestamp()
        let struct (current, cached, lastVerified) = // note we need the lastVerified to be consistent so needs to be under the lock
            lock x <| fun () -> struct (cell, tryGet(), lastVerified)
        let freshOrFetch =
            match cached with
            | ValueSome cachedValue as cachedTokenAndState ->
                if Stopwatch.TicksToSeconds(timestamp - lastVerified) <= maxAge.TotalSeconds then Ok cachedValue
                else Error (load cachedTokenAndState)
            | ValueNone as noTokenAndState -> Error (load noTokenAndState)
        match freshOrFetch with
        | Ok validatedCachedState -> return validatedCachedState
        | Error loadOrReload ->
            let newInstance = AsyncLazy(loadOrReload)
            let _ = System.Threading.Interlocked.CompareExchange(&cell, newInstance, current)
            let! struct (token, state) as res = cell.Await()
            if obj.ReferenceEquals(cell, current) then x.MergeUpdates(isStale, token, state, timestamp)
            return res }

type Cache private (inner: System.Runtime.Caching.MemoryCache) =
    let tryLoad key =
        match inner.Get key with
        | null -> ValueNone
        | :? CacheEntry<'state> as existingEntry -> existingEntry.TryGetValue()
        | x -> failwith $"TryGet Incompatible cache entry %A{x}"
    let getElseAddEmptyEntry key options =
        let fresh = CacheEntry<'state>.CreateEmpty()
        match inner.AddOrGetExisting(key, fresh, CacheItemOptions.toPolicy options) with
        | null -> fresh
        | :? CacheEntry<'state> as existingEntry -> existingEntry
        | x -> failwith $"getElseAddEmptyEntry Incompatible cache entry %A{x}"
    let addOrMergeCacheEntry isStale key options struct (token, state) =
        let timestamp = System.Diagnostics.Stopwatch.GetTimestamp()
        let entry = CacheEntry(token, state, timestamp)
        match inner.AddOrGetExisting(key, entry, CacheItemOptions.toPolicy options) with
        | null -> () // Our fresh one got added
        | :? CacheEntry<'state> as existingEntry -> existingEntry.MergeUpdates(isStale, token, state, timestamp)
        | x -> failwith $"UpdateIfNewer Incompatible cache entry %A{x}"
    new (name, sizeMb: int) =
        let config = System.Collections.Specialized.NameValueCollection(1)
        config.Add("cacheMemoryLimitMegabytes", string sizeMb);
        Cache(new System.Runtime.Caching.MemoryCache(name, config))
    interface ICache with
        // if there's a non-zero maxAge, concurrent read attempts share the roundtrip (and its fate, if it throws)
        member x.Load(key, maxAge, isStale, options, loadOrReload) = task {
            let loadOrReload maybeBaseState () =
                let act = System.Diagnostics.Activity.Current
                if act <> null then act.AddCacheHit(ValueOption.isSome maybeBaseState) |> ignore
                loadOrReload maybeBaseState
            if maxAge = TimeSpan.Zero then // Boring algorithm that has each caller independently load/reload the data and then cache it
                let maybeBaseState = tryLoad key
                let! res = loadOrReload maybeBaseState ()
                addOrMergeCacheEntry isStale key options res
                return res
            else // ensure we have an entry in the cache for this key; coordinate retrieval via a SingleReaderGate within that
                let cacheSlot = getElseAddEmptyEntry key options
                let! struct (token, state) as res = cacheSlot.ReadThrough(maxAge, isStale, loadOrReload)
                cacheSlot.MergeUpdates(isStale, token, state, System.Diagnostics.Stopwatch.GetTimestamp())
                return res }
        // Newer values get saved; equal values update the last retrieval timestamp
        member _.Save(key, isStale, options, token, state) =
            addOrMergeCacheEntry isStale key options (token, state)

type [<NoComparison; NoEquality; RequireQualifiedAccess>] CachingStrategy =
    /// Retain a single 'state per streamName.
    /// Each cache hit for a stream renews the retention period for the defined <c>window</c>.
    /// Upon expiration of the defined <c>window</c> from the point at which the cache was entry was last used, a full reload is triggered.
    /// Unless <c>LoadOption.AllowStale</c> is used, each cache hit still incurs a roundtrip to load any subsequently-added events.
    | SlidingWindow of ICache * window: TimeSpan
    /// Retain a single 'state per streamName.
    /// Upon expiration of the defined <c>period</c>, a full reload is triggered.
    /// Unless <c>LoadOption.AllowStale</c> is used, each cache hit still incurs a roundtrip to load any subsequently-added events.
    | FixedTimeSpan of ICache * period: TimeSpan
    /// Prefix is used to segregate multiple folded states per stream when they are stored in the cache.
    /// Semantics are otherwise identical to <c>SlidingWindow</c>.
    | SlidingWindowPrefixed of ICache * window: TimeSpan * prefix: string
