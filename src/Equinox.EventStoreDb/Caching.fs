module Equinox.Core.Caching

open System.Threading.Tasks

type internal Decorator<'event, 'state, 'context>(
        inner : ICategory<'event, 'state, 'context>, updateCache : string -> struct (StreamToken * 'state) -> Task<unit>) =

    let cache streamName (inner : Task<_>) = task {
        let! tokenAndState = inner
        do! updateCache streamName tokenAndState
        return tokenAndState }

    interface ICategory<'event, 'state, 'context> with
        member _.Load(log, categoryName, streamId, streamName, allowStale, ct) =
            inner.Load(log, categoryName, streamId, streamName, allowStale, ct) |> cache streamName

        member _.TrySync(log, categoryName, streamId, streamName, context, maybeInit, streamToken, state, events, ct) = task {
            match! inner.TrySync((log, categoryName, streamId, streamName, context, maybeInit, streamToken, state, events, ct)) with
            | SyncResult.Conflict resync -> return SyncResult.Conflict (fun ct -> resync ct |> cache streamName)
            | SyncResult.Written (token', state') ->
                do! updateCache streamName (token', state')
                return SyncResult.Written (token', state') }

let applyCacheUpdatesWithSlidingExpiration
        (cache : ICache)
        (prefix : string)
        (slidingExpiration : System.TimeSpan)
        (category : ICategory<'event, 'state, 'context>)
        supersedes
        : ICategory<'event, 'state, 'context> =
    let mkCacheEntry struct (initialToken : StreamToken, initialState : 'state) = new CacheEntry<'state>(initialToken, initialState, supersedes)
    let options = CacheItemOptions.RelativeExpiration slidingExpiration
    let addOrUpdateSlidingExpirationCacheEntry streamName value = cache.UpdateIfNewer(prefix + streamName, options, mkCacheEntry value)
    Decorator<'event, 'state, 'context>(category, addOrUpdateSlidingExpirationCacheEntry) :> _

let applyCacheUpdatesWithFixedTimeSpan
        (cache : ICache)
        (prefix : string)
        (lifetime : System.TimeSpan)
        (category : ICategory<'event, 'state, 'context>)
        supersedes
        : ICategory<'event, 'state, 'context> =
    let mkCacheEntry struct (initialToken : StreamToken, initialState : 'state) = CacheEntry<'state>(initialToken, initialState, supersedes)
    let addOrUpdateFixedLifetimeCacheEntry streamName value =
        let expirationPoint = let creationDate = System.DateTimeOffset.UtcNow in creationDate.Add lifetime
        let options = CacheItemOptions.AbsoluteExpiration expirationPoint
        cache.UpdateIfNewer(prefix + streamName, options, mkCacheEntry value)
    Decorator<'event, 'state, 'context>(category, addOrUpdateFixedLifetimeCacheEntry) :> _
