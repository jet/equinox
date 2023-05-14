module Equinox.Core.Caching

open Equinox.Core.Tracing
open Serilog
open System
open System.Threading
open System.Threading.Tasks

type IReloadableCategory<'event, 'state, 'context> =
    inherit ICategory<'event, 'state, 'context>

    abstract Reload: log: ILogger * streamName: string * requireLeader: bool * streamToken: StreamToken * state: 'state * ct: CancellationToken
                     -> Task<struct (StreamToken * 'state)>

type private Decorator<'event, 'state, 'context>
    (   category : IReloadableCategory<'event, 'state, 'context>,
        tryReadCache : string -> Task<voption<struct (StreamToken * 'state)>>,
        updateCache : string -> struct (StreamToken * 'state) -> Task<unit>) =

    let cache streamName (inner : CancellationToken -> Task<struct (StreamToken * 'state)>) ct = task {
        let! tokenAndState = inner ct
        do! updateCache streamName tokenAndState
        return tokenAndState }

    interface ICategory<'event, 'state, 'context> with
        member _.Load(log, categoryName, streamId, streamName, allowStale, requireLeader, ct) = task {
            match! tryReadCache streamName with
            | ValueNone -> return! cache streamName (fun ct -> category.Load(log, categoryName, streamId, streamName, allowStale, requireLeader, ct)) ct
            | ValueSome tokenAndState when allowStale -> return tokenAndState // read already updated TTL, no need to write
            | ValueSome (token, state) -> return! cache streamName (fun ct -> category.Reload(log, streamName, requireLeader, token, state, ct)) ct }
        member _.TrySync(log : ILogger, categoryName, streamId, streamName, context, maybeInit, streamToken, state, events, ct) = task {
            match! category.TrySync(log, categoryName, streamId, streamName, context, maybeInit, streamToken, state, events, ct) with
            | SyncResult.Written tokenAndState' ->
                do! updateCache streamName tokenAndState'
                return SyncResult.Written tokenAndState'
            | SyncResult.Conflict resync ->
                return SyncResult.Conflict (cache streamName resync) }

let private updateWithSlidingExpiration (cache : ICache, prefix : string) (slidingExpiration : TimeSpan) supersedes =
    let mkCacheEntry struct (initialToken : StreamToken, initialState : 'state) = CacheEntry<'state>(initialToken, initialState)
    let options = CacheItemOptions.RelativeExpiration slidingExpiration
    fun streamName value ->
        cache.UpdateIfNewer(prefix + streamName, options, supersedes, mkCacheEntry value)

let private updateWithFixedTimeSpan (cache : ICache, prefix : string) (period : TimeSpan) supersedes =
    let mkCacheEntry struct (initialToken : StreamToken, initialState : 'state) = CacheEntry<'state>(initialToken, initialState)
    fun streamName value ->
        let expirationPoint = let creationDate = DateTimeOffset.UtcNow in creationDate.Add period
        let options = CacheItemOptions.AbsoluteExpiration expirationPoint
        cache.UpdateIfNewer(prefix + streamName, options, supersedes, mkCacheEntry value)

let private decorate x supersedes (cat : IReloadableCategory<_, _, _>): ICategory<_, _, _> =
    let tryGet (cache : ICache) key = task {
        let! cacheItem = cache.TryGet key
        let act = System.Diagnostics.Activity.Current
        if act <> null then act.AddCacheHit(ValueOption.isSome cacheItem) |> ignore
        return cacheItem }
    let dec (cache : ICache) upd = Decorator(cat, tryGet cache, upd)
    let sliding cache window  prefix = dec cache (updateWithSlidingExpiration (cache, prefix) window supersedes)
    let fixed_ cache period = dec cache (updateWithFixedTimeSpan (cache, null) period supersedes)
    match x with
    | Equinox.CachingStrategy.SlidingWindow (cache, window) -> sliding cache window null
    | Equinox.CachingStrategy.FixedTimeSpan (cache, period) -> fixed_ cache period
    | Equinox.CachingStrategy.SlidingWindowPrefixed (cache, window, prefix) -> sliding cache window prefix

let apply supersedes x (cat : IReloadableCategory<_, _, _>): ICategory<_, _, _> =
    match x with
    | None -> cat
    | Some x -> decorate x supersedes cat
