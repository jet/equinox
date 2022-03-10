module Equinox.Core.Caching

type internal Decorator<'event, 'state, 'context>(inner : ICategory<'event, 'state, string, 'context>, updateCache : string -> StreamToken * 'state -> Async<unit>) =

    let cache streamName inner = async {
        let! tokenAndState = inner
        do! updateCache streamName tokenAndState
        return tokenAndState }

    interface ICategory<'event, 'state, string, 'context> with
        member _.Load(log, streamName : string, opt) : Async<StreamToken * 'state> =
            inner.Load(log, streamName, opt) |> cache streamName

        member _.TrySync(log : Serilog.ILogger, streamName, streamToken, state, events : 'event list, context) : Async<SyncResult<'state>> = async {
            match! inner.TrySync(log, streamName, streamToken, state, events, context) with
            | SyncResult.Conflict resync -> return SyncResult.Conflict (resync |> cache streamName)
            | SyncResult.Written (token', state') ->
                do! updateCache streamName (token', state')
                return SyncResult.Written (token', state') }

let applyCacheUpdatesWithSlidingExpiration
        (cache : ICache)
        (prefix : string)
        (slidingExpiration : System.TimeSpan)
        (category : ICategory<'event, 'state, string, 'context>)
        supersedes
        : ICategory<'event, 'state, string, 'context> =
    let mkCacheEntry (initialToken : StreamToken, initialState : 'state) = new CacheEntry<'state>(initialToken, initialState, supersedes)
    let options = CacheItemOptions.RelativeExpiration slidingExpiration
    let addOrUpdateSlidingExpirationCacheEntry streamName value = cache.UpdateIfNewer(prefix + streamName, options, mkCacheEntry value)
    Decorator<'event, 'state, 'context>(category, addOrUpdateSlidingExpirationCacheEntry) :> _

let applyCacheUpdatesWithFixedTimeSpan
        (cache : ICache)
        (prefix : string)
        (lifetime : System.TimeSpan)
        (category : ICategory<'event, 'state, string, 'context>)
        supersedes
        : ICategory<'event, 'state, string, 'context> =
    let mkCacheEntry (initialToken : StreamToken, initialState : 'state) = CacheEntry<'state>(initialToken, initialState, supersedes)
    let addOrUpdateFixedLifetimeCacheEntry streamName value =
        let expirationPoint = let creationDate = System.DateTimeOffset.UtcNow in creationDate.Add lifetime
        let options = CacheItemOptions.AbsoluteExpiration expirationPoint
        cache.UpdateIfNewer(prefix + streamName, options, mkCacheEntry value)
    Decorator<'event, 'state, 'context>(category, addOrUpdateFixedLifetimeCacheEntry) :> _
