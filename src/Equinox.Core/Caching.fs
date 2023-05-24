module Equinox.Core.Caching

open Serilog
open System
open System.Threading
open System.Threading.Tasks

type IReloadable<'state> =
    abstract Reload: log: ILogger * streamName: string * requireLeader: bool * streamToken: StreamToken * state: 'state * ct: CancellationToken
                     -> Task<struct (StreamToken * 'state)>

let private tee f (inner: CancellationToken -> Task<struct (StreamToken * 'state)>) ct = task {
    let! tokenAndState = inner ct
    f tokenAndState
    return tokenAndState }

type private Decorator<'event, 'state, 'context, 'cat when 'cat :> ICategory<'event, 'state, 'context> and 'cat :> IReloadable<'state> >
    (category: 'cat, cache: ICache, isStale, createKey, createOptions) =
    interface ICategory<'event, 'state, 'context> with
        member _.Load(log, categoryName, streamId, streamName, maxAge, requireLeader, ct) = task {
            let loadOrReload = function
                | ValueNone -> category.Load(log, categoryName, streamId, streamName, maxAge, requireLeader, ct)
                | ValueSome (struct (token, state)) -> category.Reload(log, streamName, requireLeader, token, state, ct)
            return! cache.Load(createKey streamName, maxAge, isStale, createOptions (), loadOrReload) }
        member _.TrySync(log, categoryName, streamId, streamName, context, maybeInit, streamToken, state, events, ct) = task {
            let timestamp = System.Diagnostics.Stopwatch.GetTimestamp()
            let save struct (token, state) = cache.Save(createKey streamName, isStale, createOptions (), timestamp, token, state)
            match! category.TrySync(log, categoryName, streamId, streamName, context, maybeInit, streamToken, state, events, ct) with
            | SyncResult.Written tokenAndState' ->
                save tokenAndState'
                return SyncResult.Written tokenAndState'
            | SyncResult.Conflict resync ->
                return SyncResult.Conflict (tee save resync) }

let private mkKey prefix streamName =
    prefix + streamName

let private optionsSlidingExpiration (slidingExpiration: TimeSpan) () =
    CacheItemOptions.RelativeExpiration slidingExpiration
let private optionsFixedTimeSpan (period: TimeSpan) () =
    let expirationPoint = let creationDate = DateTimeOffset.UtcNow in creationDate.Add period
    CacheItemOptions.AbsoluteExpiration expirationPoint

let private mapStrategy = function
    | Equinox.CachingStrategy.FixedTimeSpan (cache, period) -> struct (        cache, mkKey null,   optionsFixedTimeSpan period)
    | Equinox.CachingStrategy.SlidingWindow (cache, window) ->                 cache, mkKey null,   optionsSlidingExpiration window
    | Equinox.CachingStrategy.SlidingWindowPrefixed (cache, window, prefix) -> cache, mkKey prefix, optionsSlidingExpiration window

let apply isStale x (cat: 'cat when 'cat :> ICategory<'event, 'state, 'context> and 'cat :> IReloadable<'state>): ICategory<_, _, _> =
    match x with
    | None -> cat
    | Some x -> mapStrategy x |> fun struct (cache, createKey, createOptions) -> Decorator(cat, cache, isStale, createKey, createOptions)
