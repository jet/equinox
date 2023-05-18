namespace Equinox.Core

open System.Threading.Tasks

type [<NoEquality; NoComparison; Struct>] CacheItemOptions =
    | AbsoluteExpiration of ae: System.DateTimeOffset
    | RelativeExpiration of re: System.TimeSpan

[<AllowNullLiteral>]
type CacheEntry<'state>(initialToken: StreamToken, initialState: 'state) =
    let mutable currentToken = initialToken
    let mutable currentState = initialState
    member x.UpdateIfNewer(compare: struct (StreamToken * StreamToken) -> int64, other: CacheEntry<'state>) =
        lock x <| fun () ->
            let struct (token, state) = other.Value
            let res = compare (currentToken, token)
            if res < 0 then // Accept fresher values, ignore equal or older
                currentToken <- token
                currentState <- state

    member x.Value: struct (StreamToken * 'state) =
        lock x <| fun () ->
            currentToken, currentState

type ICache =
    abstract member UpdateIfNewer: key: string * options: CacheItemOptions * (struct (StreamToken * StreamToken) -> int64) * entry: CacheEntry<'state> -> Task<unit>
    abstract member TryGet: key: string -> Task<struct (StreamToken * 'state) voption>

namespace Equinox

open Equinox.Core
open System.Runtime.Caching
open System.Threading.Tasks

[<NoComparison; NoEquality; RequireQualifiedAccess>]
type CachingStrategy =
    /// Retain a single 'state per streamName.
    /// Each cache hit for a stream renews the retention period for the defined <c>window</c>.
    /// Upon expiration of the defined <c>window</c> from the point at which the cache was entry was last used, a full reload is triggered.
    /// Unless <c>LoadOption.AllowStale</c> is used, each cache hit still incurs a roundtrip to load any subsequently-added events.
    | SlidingWindow of ICache * window: System.TimeSpan
    /// Retain a single 'state per streamName.
    /// Upon expiration of the defined <c>period</c>, a full reload is triggered.
    /// Unless <c>LoadOption.AllowStale</c> is used, each cache hit still incurs a roundtrip to load any subsequently-added events.
    | FixedTimeSpan of ICache * period: System.TimeSpan
    /// Prefix is used to segregate multiple folded states per stream when they are stored in the cache.
    /// Semantics are otherwise identical to <c>SlidingWindow</c>.
    | SlidingWindowPrefixed of ICache * window: System.TimeSpan * prefix: string
module private CachingStrategy =
    let toPolicy = function
        | AbsoluteExpiration absolute -> CacheItemPolicy(AbsoluteExpiration = absolute)
        | RelativeExpiration relative -> CacheItemPolicy(SlidingExpiration = relative)

type Cache private (inner: MemoryCache) =
    new (name, sizeMb: int) =
        let config = System.Collections.Specialized.NameValueCollection(1)
        config.Add("cacheMemoryLimitMegabytes", string sizeMb);
        Cache(new MemoryCache(name, config))
    interface ICache with
        member _.UpdateIfNewer(key, options, compare, entry) =
            match inner.AddOrGetExisting(key, box entry, CachingStrategy.toPolicy options) with
            | null -> Task.FromResult()
            | :? CacheEntry<'state> as existingEntry -> existingEntry.UpdateIfNewer(compare, entry); Task.FromResult()
            | x -> failwithf "UpdateIfNewer Incompatible cache entry %A" x

        member _.TryGet key =
            match inner.Get key with
            | null -> ValueNone |> Task.FromResult
            | :? CacheEntry<'state> as existingEntry -> ValueSome existingEntry.Value |> Task.FromResult
            | x -> failwithf "TryGet Incompatible cache entry %A" x

