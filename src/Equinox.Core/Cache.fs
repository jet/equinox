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
    abstract member TryGet:
                          key: string
                           -> Task<struct (StreamToken * 'state) voption>
    abstract member UpdateIfNewer:
                          key: string
                          * isStale: System.Func<StreamToken, StreamToken, bool>
                          * options: CacheItemOptions
                          * token: StreamToken
                          * state: 'state
                           -> Task<unit>

namespace Equinox

open Equinox.Core
open System
open System.Threading.Tasks

type internal CacheEntry<'state>(initialToken: StreamToken, initialState: 'state) =
    let mutable currentToken = initialToken
    let mutable currentState = initialState
    member x.Value: struct (StreamToken * 'state) =
        lock x <| fun () ->
            currentToken, currentState
    member x.UpdateIfNewer(isStale: System.Func<StreamToken, StreamToken, bool>, other: CacheEntry<'state>) =
        lock x <| fun () ->
            let struct (candidateToken, state) = other.Value
            if not (isStale.Invoke(currentToken, candidateToken)) then
                currentToken <- candidateToken
                currentState <- state

type Cache private (inner: System.Runtime.Caching.MemoryCache) =
    new (name, sizeMb: int) =
        let config = System.Collections.Specialized.NameValueCollection(1)
        config.Add("cacheMemoryLimitMegabytes", string sizeMb);
        Cache(new System.Runtime.Caching.MemoryCache(name, config))
    interface ICache with
        member _.TryGet key =
            match inner.Get key with
            | null -> ValueNone |> Task.FromResult
            | :? CacheEntry<'state> as existingEntry -> ValueSome existingEntry.Value |> Task.FromResult
            | x -> failwithf "TryGet Incompatible cache entry %A" x
        member _.UpdateIfNewer(key, isStale, options, token, state) =
            let freshEntry = CacheEntry(token, state)
            match inner.AddOrGetExisting(key, freshEntry, CacheItemOptions.toPolicy options) with
            | null -> Task.FromResult()
            | :? CacheEntry<'state> as existingEntry -> existingEntry.UpdateIfNewer(isStale, freshEntry); Task.FromResult()
            | x -> failwithf "UpdateIfNewer Incompatible cache entry %A" x

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
