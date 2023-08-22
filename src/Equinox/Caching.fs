module Equinox.Core.Caching

type IReloadable<'state> =
    abstract Reload: log: Serilog.ILogger * streamName: string * requireLeader: bool * streamToken: StreamToken * state: 'state * ct: CancellationToken
                     -> Task<struct (StreamToken * 'state)>

let private tee f (inner: CancellationToken -> Task<struct (StreamToken * 'state)>) ct = task {
    let! tokenAndState = inner ct
    f tokenAndState
    return tokenAndState }

type private Decorator<'event, 'state, 'req, 'cat when 'cat :> ICategory<'event, 'state, 'req> and 'cat :> IReloadable<'state> >
    (category: 'cat, cache: Equinox.Cache, isStale, createKey, createOptions) =
    interface ICategory<'event, 'state, 'req> with
        member _.Empty = category.Empty
        member _.Load(log, categoryName, streamId, streamName, maxAge, requireLeader, ct) = task {
            let loadOrReload ct = function
                | ValueNone -> category.Load(log, categoryName, streamId, streamName, maxAge, requireLeader, ct)
                | ValueSome (struct (token, state)) -> category.Reload(log, streamName, requireLeader, token, state, ct)
            return! cache.Load(createKey streamName, maxAge, isStale, createOptions (), loadOrReload, ct) }
        member _.Sync(log, categoryName, streamId, streamName, context, streamToken, state, events, ct) = task {
            let timestamp = System.Diagnostics.Stopwatch.GetTimestamp() // NB take the timestamp before any potential write takes place
            let save struct (token, state) = cache.Save(createKey streamName, isStale, createOptions (), timestamp, token, state)
            match! category.Sync(log, categoryName, streamId, streamName, context, streamToken, state, events, ct) with
            | SyncResult.Written tokenAndState' ->
                save tokenAndState'
                return SyncResult.Written tokenAndState'
            | SyncResult.Conflict resync ->
                return SyncResult.Conflict (tee save resync) }

let private mkKey prefix streamName =
    prefix + streamName

let private policySlidingExpiration (slidingExpiration: System.TimeSpan) () =
    System.Runtime.Caching.CacheItemPolicy(SlidingExpiration = slidingExpiration)
let private policyFixedTimeSpan (period: System.TimeSpan) () =
    let expirationPoint = let creationDate = System.DateTimeOffset.UtcNow in creationDate.Add period
    System.Runtime.Caching.CacheItemPolicy(AbsoluteExpiration = expirationPoint)

let apply isStale x (cat: 'cat when 'cat :> ICategory<'event, 'state, 'req> and 'cat :> IReloadable<'state>) =
    match x with
    | Equinox.CachingStrategy.NoCaching ->                                     (cat : ICategory<_, _, _>)
    | Equinox.CachingStrategy.FixedTimeSpan (cache, period) ->                 Decorator(cat, cache, isStale, mkKey null,   policyFixedTimeSpan period)
    | Equinox.CachingStrategy.SlidingWindow (cache, window) ->                 Decorator(cat, cache, isStale, mkKey null,   policySlidingExpiration window)
    | Equinox.CachingStrategy.SlidingWindowPrefixed (cache, window, prefix) -> Decorator(cat, cache, isStale, mkKey prefix, policySlidingExpiration window)
