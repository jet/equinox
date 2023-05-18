module Equinox.Core.Caching

open Equinox.Core.Tracing
open Serilog
open System
open System.Threading
open System.Threading.Tasks

type IReloadable<'state> =
    abstract Reload: log: ILogger * streamName: string * requireLeader: bool * streamToken: StreamToken * state: 'state * ct: CancellationToken
                     -> Task<struct (StreamToken * 'state)>

type private Decorator<'event, 'state, 'context, 'cat when 'cat :> ICategory<'event, 'state, 'context> and 'cat :> IReloadable<'state> >
    (category: 'cat, cache : ICache, compare, updateCache: string -> struct (StreamToken * 'state) -> Task<unit>) =
    let tryRead key = task {
        let! cacheItem = cache.TryGet key
        let act = System.Diagnostics.Activity.Current
        if act <> null then act.AddCacheHit(ValueOption.isSome cacheItem) |> ignore
        return cacheItem }
    let save streamName (inner: CancellationToken -> Task<struct (StreamToken * 'state)>) ct = task {
        let! tokenAndState = inner ct
        do! updateCache streamName tokenAndState
        return tokenAndState }
    interface ICategory<'event, 'state, 'context> with
        member _.Load(log, categoryName, streamId, streamName, maxStaleness, requireLeader, ct) = task {
            match! tryRead streamName with
            | ValueNone -> return! save streamName (fun ct -> category.Load(log, categoryName, streamId, streamName, maxStaleness, requireLeader, ct)) ct
            | ValueSome tokenAndState when maxStaleness = TimeSpan.MaxValue -> return tokenAndState // read already updated TTL, no need to write
            | ValueSome (token, state) -> return! save streamName (fun ct -> category.Reload(log, streamName, requireLeader, token, state, ct)) ct }
        member _.TrySync(log, categoryName, streamId, streamName, context, maybeInit, streamToken, state, events, ct) = task {
            match! category.TrySync(log, categoryName, streamId, streamName, context, maybeInit, streamToken, state, events, ct) with
            | SyncResult.Written tokenAndState' ->
                do! updateCache streamName tokenAndState'
                return SyncResult.Written tokenAndState'
            | SyncResult.Conflict resync ->
                return SyncResult.Conflict (save streamName resync) }

let private mkCacheEntry struct (initialToken: StreamToken, initialState: 'state) = CacheEntry<'state>(initialToken, initialState)
let private updateWithSlidingExpiration (cache: ICache) (prefix: string) compare (slidingExpiration: TimeSpan) streamName value =
    let options = CacheItemOptions.RelativeExpiration slidingExpiration
    cache.UpdateIfNewer(prefix + streamName, options, compare, mkCacheEntry value)
let private updateWithFixedTimeSpan (cache: ICache) (prefix: string) compare (period: TimeSpan) streamName value =
    let expirationPoint = let creationDate = DateTimeOffset.UtcNow in creationDate.Add period
    let options = CacheItemOptions.AbsoluteExpiration expirationPoint
    cache.UpdateIfNewer(prefix + streamName, options, compare, mkCacheEntry value)

let private fixed_ cache compare period = struct (cache, updateWithFixedTimeSpan cache null compare period)
let private sliding cache prefix compare window = struct (cache, updateWithSlidingExpiration cache prefix compare window)
let private mapStrategy compare = function
    | Equinox.CachingStrategy.FixedTimeSpan (cache, period) -> fixed_ cache compare period
    | Equinox.CachingStrategy.SlidingWindow (cache, window) -> sliding cache null compare window
    | Equinox.CachingStrategy.SlidingWindowPrefixed (cache, window, prefix) -> sliding cache prefix compare window

let apply compare x (cat: 'cat when 'cat :> ICategory<'event, 'state, 'context> and 'cat :> IReloadable<'state>): ICategory<_, _, _> =
    match x with
    | None -> cat
    | Some x -> let struct (cache, upd) = mapStrategy compare x in Decorator(cat, cache, compare, upd)
