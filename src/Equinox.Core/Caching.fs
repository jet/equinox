module Equinox.Core.Caching

open Equinox.Core.Tracing
open Serilog
open System
open System.Diagnostics
open System.Threading
open System.Threading.Tasks

type IReloadable<'state> =
    abstract Reload: log: ILogger * streamName: string * requireLeader: bool * streamToken: StreamToken * state: 'state * ct: CancellationToken
                     -> Task<struct (StreamToken * 'state)>

type private Decorator<'event, 'state, 'context, 'cat when 'cat :> ICategory<'event, 'state, 'context> and 'cat :> IReloadable<'state> >
    (category: 'cat, tryGet, updateCache: string -> struct (StreamToken * 'state) -> Task<unit>) =
    let tryRead key = task {
        let! cacheItem = tryGet key
        let act = Activity.Current
        if act <> null then act.AddCacheHit(ValueOption.isSome cacheItem) |> ignore
        return cacheItem }
    let save streamName (inner: CancellationToken -> Task<struct (StreamToken * 'state)>) ct = task {
        let! tokenAndState = inner ct
        do! updateCache streamName tokenAndState
        return tokenAndState }
    interface ICategory<'event, 'state, 'context> with
        member _.Load(log, categoryName, streamId, streamName, allowStale, requireLeader, ct) = task {
            match! tryRead streamName with
            | ValueNone -> return! save streamName (fun ct -> category.Load(log, categoryName, streamId, streamName, allowStale, requireLeader, ct)) ct
            | ValueSome tokenAndState when allowStale -> return tokenAndState // read already updated TTL, no need to write
            | ValueSome (token, state) -> return! save streamName (fun ct -> category.Reload(log, streamName, requireLeader, token, state, ct)) ct }
        member _.TrySync(log, categoryName, streamId, streamName, context, maybeInit, streamToken, state, events, ct) = task {
            match! category.TrySync(log, categoryName, streamId, streamName, context, maybeInit, streamToken, state, events, ct) with
            | SyncResult.Written tokenAndState' ->
                do! updateCache streamName tokenAndState'
                return SyncResult.Written tokenAndState'
            | SyncResult.Conflict resync ->
                return SyncResult.Conflict (save streamName resync) }

let private mkCacheEntry struct (initialToken: StreamToken, initialState: 'state) = CacheEntry<'state>(initialToken, initialState)
let private updateWithSlidingExpiration (cache: ICache) (prefix: string) supersedes (slidingExpiration: TimeSpan) streamName value =
    let options = CacheItemOptions.RelativeExpiration slidingExpiration
    cache.UpdateIfNewer(prefix + streamName, options, supersedes, mkCacheEntry value)
let private updateWithFixedTimeSpan (cache: ICache) (prefix: string) supersedes (period: TimeSpan) streamName value =
    let expirationPoint = let creationDate = DateTimeOffset.UtcNow in creationDate.Add period
    let options = CacheItemOptions.AbsoluteExpiration expirationPoint
    cache.UpdateIfNewer(prefix + streamName, options, supersedes, mkCacheEntry value)

let private fixed_ cache supersedes period = struct (cache, updateWithFixedTimeSpan cache null supersedes period)
let private sliding cache prefix supersedes window = struct (cache, updateWithSlidingExpiration cache prefix supersedes window)
let private mapStrategy supersedes = function
    | Equinox.CachingStrategy.FixedTimeSpan (cache, period) -> fixed_ cache supersedes period
    | Equinox.CachingStrategy.SlidingWindow (cache, window) -> sliding cache null supersedes window
    | Equinox.CachingStrategy.SlidingWindowPrefixed (cache, window, prefix) -> sliding cache prefix supersedes window

let apply supersedes x (cat: 'cat when 'cat :> ICategory<'event, 'state, 'context> and 'cat :> IReloadable<'state>): ICategory<_, _, _> =
    match x with
    | None -> cat
    | Some x -> let struct (cache, upd) = mapStrategy supersedes x in Decorator(cat, cache.TryGet, upd)
