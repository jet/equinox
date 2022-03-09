namespace Equinox.Core

open System

type CacheItemOptions =
    | AbsoluteExpiration of DateTimeOffset
    | RelativeExpiration of TimeSpan

[<AllowNullLiteral>]
type CacheEntry<'state>(initialToken: StreamToken, initialState: 'state, supersedes: StreamToken -> StreamToken -> bool) =
    let mutable currentToken, currentState = initialToken, initialState
    member x.UpdateIfNewer(other : CacheEntry<'state>) =
        lock x <| fun () ->
            let otherToken, otherState = other.Value
            if otherToken |> supersedes currentToken then
                currentToken <- otherToken
                currentState <- otherState

    member x.Value : StreamToken * 'state =
        lock x <| fun () ->
            currentToken, currentState

type ICache =
    abstract member UpdateIfNewer : key: string * options: CacheItemOptions * entry: CacheEntry<'state> -> unit
    abstract member TryGet : key: string -> (StreamToken * 'state) option

module Caching =

    type internal CachingDecorator<'event, 'state, 'context>(inner : ICategory<'event, 'state, string, 'context>, updateCache : string -> StreamToken * 'state -> unit) =

        let cache streamName inner = async {
            let! tokenAndState = inner
            updateCache streamName tokenAndState
            return tokenAndState }

        interface ICategory<'event, 'state, string, 'context> with
            member _.Load(log, streamName : string, opt) : Async<StreamToken * 'state> =
                inner.Load(log, streamName, opt) |> cache streamName

            member _.TrySync(log : Serilog.ILogger, streamName, streamToken, state, events : 'event list, context) : Async<SyncResult<'state>> = async {
                match! inner.TrySync(log, streamName, streamToken, state, events, context) with
                | SyncResult.Conflict resync -> return SyncResult.Conflict (resync |> cache streamName)
                | SyncResult.Written (token', state') ->
                    updateCache streamName (token', state')
                    return SyncResult.Written (token', state') }

    let applyCacheUpdatesWithSlidingExpiration
            (cache : ICache)
            (prefix : string)
            (slidingExpiration : TimeSpan)
            (category : ICategory<'event, 'state, string, 'context>)
            supersedes
            : ICategory<'event, 'state, string, 'context> =
        let mkCacheEntry (initialToken : StreamToken, initialState : 'state) = new CacheEntry<'state>(initialToken, initialState, supersedes)
        let options = CacheItemOptions.RelativeExpiration slidingExpiration
        let addOrUpdateSlidingExpirationCacheEntry streamName value = cache.UpdateIfNewer(prefix + streamName, options, mkCacheEntry value)
        CachingDecorator<'event, 'state, 'context>(category, addOrUpdateSlidingExpirationCacheEntry) :> _

    let applyCacheUpdatesWithFixedTimeSpan
            (cache : ICache)
            (prefix : string)
            (lifetime : TimeSpan)
            (category : ICategory<'event, 'state, string, 'context>)
            supersedes
            : ICategory<'event, 'state, string, 'context> =
        let mkCacheEntry (initialToken : StreamToken, initialState : 'state) = CacheEntry<'state>(initialToken, initialState, supersedes)
        let addOrUpdateFixedLifetimeCacheEntry streamName value =
            let expirationPoint = let creationDate = DateTimeOffset.UtcNow in creationDate.Add lifetime
            let options = CacheItemOptions.AbsoluteExpiration expirationPoint
            cache.UpdateIfNewer(prefix + streamName, options, mkCacheEntry value)
        CachingDecorator<'event, 'state, 'context>(category, addOrUpdateFixedLifetimeCacheEntry) :> _

namespace Equinox

open System.Runtime.Caching
open Equinox.Core

type Cache(name, sizeMb : int) =
    let cache =
        let config = System.Collections.Specialized.NameValueCollection(1)
        config.Add("cacheMemoryLimitMegabytes", string sizeMb);
        new MemoryCache(name, config)

    let toPolicy (cacheItemOption: CacheItemOptions) =
        match cacheItemOption with
        | AbsoluteExpiration absolute -> CacheItemPolicy(AbsoluteExpiration = absolute)
        | RelativeExpiration relative -> CacheItemPolicy(SlidingExpiration = relative)

    interface ICache with
        member _.UpdateIfNewer(key, options, entry) =
            let policy = toPolicy options
            match cache.AddOrGetExisting(key, box entry, policy) with
            | null -> ()
            | :? CacheEntry<'state> as existingEntry -> existingEntry.UpdateIfNewer entry
            | x -> failwithf "UpdateIfNewer Incompatible cache entry %A" x

        member _.TryGet key =
            match cache.Get key with
            | null -> None
            | :? CacheEntry<'state> as existingEntry -> Some existingEntry.Value
            | x -> failwithf "TryGet Incompatible cache entry %A" x
