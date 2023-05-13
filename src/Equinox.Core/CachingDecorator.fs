namespace Equinox.Core

open Serilog
open System
open System.Threading
open System.Threading.Tasks

type IReloadableCategory<'event, 'state, 'context> =
    inherit ICategory<'event, 'state, 'context>

    abstract Reload: log: ILogger * streamName: string * requireLeader: bool * streamToken: StreamToken * state: 'state * ct: CancellationToken
                     -> Task<struct (StreamToken * 'state)>

type CachingDecorator<'event, 'state, 'context>
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

module internal Cache =

    let updateWithSlidingExpiration (cache : ICache, prefix : string) (slidingExpiration : TimeSpan) supersedes =
        let mkCacheEntry struct (initialToken : StreamToken, initialState : 'state) = CacheEntry<'state>(initialToken, initialState)
        let options = CacheItemOptions.RelativeExpiration slidingExpiration
        fun streamName value ->
            cache.UpdateIfNewer(prefix + streamName, options, supersedes, mkCacheEntry value)

    let updateWithFixedTimeSpan (cache : ICache, prefix : string) (period : TimeSpan) supersedes =
        let mkCacheEntry struct (initialToken : StreamToken, initialState : 'state) = CacheEntry<'state>(initialToken, initialState)
        fun streamName value ->
            let expirationPoint = let creationDate = DateTimeOffset.UtcNow in creationDate.Add period
            let options = CacheItemOptions.AbsoluteExpiration expirationPoint
            cache.UpdateIfNewer(prefix + streamName, options, supersedes, mkCacheEntry value)
