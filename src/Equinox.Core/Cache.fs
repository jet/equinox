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
    let mutable cell = AsyncLazy.Empty
    let tryGet () =
        if lastVerified = 0 then ValueNone
        else ValueSome (struct (currentToken, currentState))
    let tryGetWithLastVerified () =
        if lastVerified = 0 then ValueNone
        else ValueSome (struct (currentToken, currentState, lastVerified))
    static member CreateEmpty() =
        new CacheEntry<'state>(Unchecked.defaultof<StreamToken>, Unchecked.defaultof<'state>, 0)
    member x.TryGetValue(): (struct (StreamToken * 'state)) voption =
        lock x tryGet
    member x.UpdateIfNewer(isStale: Func<StreamToken, StreamToken, bool>, token, state, timestamp) =
        lock x <| fun () ->
            if not (isStale.Invoke(currentToken, token)) then
                currentToken <- token
                currentState <- state
                lastVerified <- timestamp
    /// Gets or asynchronously recomputes a cached value depending on expiry and availability
    member x.ReadThrough(maxAge: TimeSpan, isStale, load) : Task<struct (StreamToken * 'state)> = task {
        // Each concurrent execution takes a copy of the cell, and attempts to reuse the value; later used to ensure only one triggers the workflow
        let now = System.Diagnostics.Stopwatch.GetTimestamp()
        let struct (current, entry) =
            lock x <| fun () -> struct (cell, tryGetWithLastVerified())
        let next =
            match entry with
            | ValueSome (token, state, lastVerified) ->
                let cached = struct (token, state)
                if Stopwatch.ElapsedSeconds(now, lastVerified) <= maxAge.TotalSeconds then Ok cached
                else Error (load (ValueSome cached))
            | ValueNone -> Error (load ValueNone)
        match next with
        | Ok res -> return res
        | Error loadOrReload ->

        // Prepare a new instance, with cancellation under our control (it won't start until the first Await on the LazyTask triggers it though)
        let newInstance = AsyncLazy(loadOrReload)
        // If there are concurrent executions, the first through the gate wins; everybody else awaits the instance the winner wrote
        let _ = System.Threading.Interlocked.CompareExchange(&cell, newInstance, current)
        let! struct (token, state) as res = cell.Await()
        if obj.ReferenceEquals(cell, current) then x.UpdateIfNewer(isStale, token, state, now)
        return res }

type Cache private (inner: System.Runtime.Caching.MemoryCache) =
    let addCachedFlag wasCached =
        let act = System.Diagnostics.Activity.Current
        if act <> null then act.AddCacheHit(wasCached) |> ignore
    new (name, sizeMb: int) =
        let config = System.Collections.Specialized.NameValueCollection(1)
        config.Add("cacheMemoryLimitMegabytes", string sizeMb);
        Cache(new System.Runtime.Caching.MemoryCache(name, config))
    interface ICache with
        member x.Load(key, maxAge, isStale, options, loadOrReload) = task {
            let fetch cached () = addCachedFlag (ValueOption.isSome cached); loadOrReload cached
            if maxAge = TimeSpan.Zero then
                let cached =
                    match inner.Get key with
                    | null -> addCachedFlag false; ValueNone
                    | :? CacheEntry<'state> as existingEntry -> existingEntry.TryGetValue()
                    | x -> failwith $"TryGet Incompatible cache entry %A{x}"
                addCachedFlag (ValueOption.isSome cached)
                let! struct (token, state) as res = fetch cached ()
                (x : ICache).Save(key, isStale, options, token, state)
                return res
            else
                let entry =
                    let fresh = CacheEntry<'state>.CreateEmpty()
                    match inner.AddOrGetExisting(key, fresh, CacheItemOptions.toPolicy options) with
                    | null -> fresh
                    | :? CacheEntry<'state> as existingEntry -> existingEntry
                    | x -> failwith $"CreateSlot Incompatible cache entry %A{x}"
                let! struct (token, state) as res = entry.ReadThrough(maxAge, isStale, fetch)
                entry.UpdateIfNewer(isStale, token, state, System.Diagnostics.Stopwatch.GetTimestamp())
                return res }
        member _.Save(key, isStale, options, token, state) =
            let timestamp = System.Diagnostics.Stopwatch.GetTimestamp()
            let entry = CacheEntry(token, state, timestamp)
            match inner.AddOrGetExisting(key, entry, CacheItemOptions.toPolicy options) with
            | null -> () // Our fresh one got added
            | :? CacheEntry<'state> as existingEntry -> existingEntry.UpdateIfNewer(isStale, token, state, timestamp)
            | x -> failwith $"Save Incompatible cache entry %A{x}"

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
