module Equinox.Core.Tests.CachingTests

open Swensen.Unquote
open System
open System.Threading
open System.Threading.Tasks
open Xunit

type State = int
type Event = unit

// We don't worry about the tokens in this test scenario - the timestamps are the deciding factor
let mkToken () : Equinox.Core.StreamToken = { value = null; version = 0L; streamBytes = -1L }
let isStale (cur: Equinox.Core.StreamToken) (candidate: Equinox.Core.StreamToken) =
    ArgumentNullException.ThrowIfNull(cur.value, nameof cur)
    ArgumentNullException.ThrowIfNull(candidate.value, nameof candidate)
    false

type SpyCategory() =

    let mutable state = 0
    let mutable loads = 0
    let mutable reloads = 0

    member _.Loads = loads
    member _.Reloads = reloads
    member _.State = state
    member val Delay : TimeSpan = TimeSpan.Zero with get, set

    interface Equinox.Core.ICategory<Event, State, unit> with
        member x.Load(_log, _cat, _sid, _sn, _maxAge, _requireLeader, ct) = task {
            Interlocked.Increment &loads |> ignore
            do! Task.Delay(x.Delay, ct)
            return struct (mkToken(), Interlocked.Increment &state)
        }
        member _.TrySync(_log, _cat, _sid, _sn, _ctx, _maybeInit, _originToken, originState, events, _ct) = task {
            return Equinox.Core.SyncResult.Written (mkToken(), originState + events.Length)
        }

    interface Equinox.Core.Caching.IReloadable<State> with
        member x.Reload(_log, _sn, _requireLeader, _streamToken, _baseState, ct) = task {
            Interlocked.Increment &reloads |> ignore
            do! Task.Delay(x.Delay, ct)
            return struct (mkToken(), Interlocked.Increment &state)
        }

let load sn maxAge (sut: Equinox.Core.ICategory<_, _, _>) =
    sut.Load(Serilog.Log.Logger, null, null, sn, maxAge, false, CancellationToken.None)

let writeOriginState = 99
let expectedWriteState = 99 + 2 // events written

let write sn (sut: Equinox.Core.ICategory<_, _, _>) = task {
    let! wr = sut.TrySync(Serilog.Log.Logger, null, null, sn, (), ValueNone, Unchecked.defaultof<_>, writeOriginState, Array.replicate 2 (), CancellationToken.None)
    let wState' = trap <@ match wr with Equinox.Core.SyncResult.Written (_token, state') -> state' | _ -> failwith "unexpected" @>
    test <@ expectedWriteState = wState' @>
}

// Pinning the fact that the algorithm is not sensitive to the reuse of the initial value of a cache entry
let [<Fact>] ``AsyncLazy.Empty is a true singleton, does not allocate`` () =
    let i1 = Equinox.Core.AsyncLazy<int>.Empty
    let i2 = Equinox.Core.AsyncLazy<int>.Empty
    test <@ obj.ReferenceEquals(i1, i2) @>

let [<Fact>] ``No strategy, no wrapping`` () =
    let cat = SpyCategory()
    let sut = Equinox.Core.Caching.apply isStale None cat
    test <@ obj.ReferenceEquals(cat, sut) @>

type Tests() =

    let cache = Equinox.Cache("tests", 1)
    let strategy = Equinox.CachingStrategy.SlidingWindow (cache, TimeSpan.FromMinutes 10)
    let cat = SpyCategory()
    let sut = Equinox.Core.Caching.apply isStale (Some strategy) cat
    let sn = Guid.NewGuid |> string

    let write () = write sn sut

    let anyCachedValue () = load sn TimeSpan.MaxValue sut
    let requireLoad () = load sn TimeSpan.Zero sut

    let [<Fact>] ``anyCachedValue basics`` () = task {
        let! struct (_token, state) = anyCachedValue ()
        test <@ (1, 1, 0) = (state, cat.Loads, cat.Reloads) @>
        do! write ()
        let! struct (_token, state) = anyCachedValue ()
        test <@ (expectedWriteState, 1, 0) = (state, cat.Loads, cat.Reloads) @>
        let! struct (_token, state) = requireLoad ()
        test <@ (2, 1, 1) = (state, cat.Loads, cat.Reloads) @> }

    let [<Fact>] ``requireLoad vs anyCachedValue`` () = task {
        let! struct (_token, state) = requireLoad ()
        test <@ (1, 1, 0) = (state, cat.Loads, cat.Reloads) @>
        let! struct (_token, state) = requireLoad () // This time, the cache entry should be used as the base state for the load
        test <@ (2, 1, 1) = (state, cat.Loads, cat.Reloads) @>
        let! struct (_token, state) = requireLoad () // no matter how close to the preceding load, a fresh roundtrip occurs
        test <@ (3, 1, 2) = (state, cat.Loads, cat.Reloads) @>

        let t4 = requireLoad ()

        // Trigger a concurrent write (it should lose the cache update race)
        do! write ()
        let! struct (_token, state) = t4
        test <@ (4, 1, 3) = (state, cat.Loads, cat.Reloads) @>
        // a raw read updates the cache, even though it's very fresh
        let! struct (_token, state) = requireLoad ()
        test <@ (5, 1, 4) = (state, cat.Loads, cat.Reloads) @>

        // a write overwrites it (with an older value because our predicate is broken)
        do! write ()

        // a stale read sees the written value
        let! struct (_token, state) = anyCachedValue ()
        test <@ (expectedWriteState, 1, 4) = (state, cat.Loads, cat.Reloads) @>
        // a raw read updates the cache
        let! struct (_token, state) = requireLoad ()
        test <@ (6, 1, 5) = (state, cat.Loads, cat.Reloads) @>
        // and the stale read sees same directly
        let! struct (_token, state) = anyCachedValue ()
        test <@ (6, 1, 5) = (state, cat.Loads, cat.Reloads) @> }

    let [<Fact>] ``requireLoad does not unify loads``  () = task {
        cat.Delay <- TimeSpan.FromMilliseconds 20
        let t1 = requireLoad ()
        do! Task.Delay 10
        test <@ (1, 0) = (cat.Loads, cat.Reloads) @>
        do! Task.Delay 60 // wait for the loaded value to get cached (35 should do, but MacOS CI cold start disagrees...)
        let! struct (_token, state) = requireLoad ()
        test <@ 2 = state && (1, 1) = (cat.Loads, cat.Reloads) @>
        let! struct (_token, state) = t1
        test <@ 1 = state && (1, 1) = (cat.Loads, cat.Reloads) @> }

    let allowStale toleranceMs = load sn (TimeSpan.FromMilliseconds toleranceMs) sut

    let [<Fact>] ``allowStale unifies compatible concurrent loads`` () = task {
        cat.Delay <- TimeSpan.FromMilliseconds 50
        let t1 = allowStale 1
        do! Task.Delay 10
        test <@ (1, 0) = (cat.Loads, cat.Reloads) @>
        let! struct (_token, state) = allowStale 200
        test <@ (1, 1, 0) = (state, cat.Loads, cat.Reloads) @>
        let! struct (_token, state) = t1
        test <@ (1, 1, 0) = (state, cat.Loads, cat.Reloads) @> }

    let [<Fact>] ``allowStale handles concurrent incompatible loads correctly`` () = task {
        cat.Delay <- TimeSpan.FromMilliseconds 50
        let t1 = allowStale 1
        do! Task.Delay 20 // Give the load a chance to start
        let t2 = allowStale 1 // any cached value should be at least 50 old (and the overlapping call should not have started 45 late)
        let! struct (_token, state) = t2
        test <@ (2, 2) = (state, cat.Loads + cat.Reloads) @>
        let! struct (_token, state) = t1
        test <@ (1, 2) = (state, cat.Loads + cat.Reloads) @> }

    let [<Fact>] ``allowStale handles overlapped incompatible loads correctly`` () = task {
        cat.Delay <- TimeSpan.FromMilliseconds 50
        let t1 = allowStale 1
        do! Task.Delay 20
        test <@ (1, 0) = (cat.Loads, cat.Reloads) @>
        do! Task.Delay 50
        let! struct (_token, state) = allowStale 79
        test <@ (2, 1, 1) = (state, cat.Loads, cat.Reloads) @>
        let! struct (_token, state) = t1
        test <@ (1, 1, 1) = (state, cat.Loads, cat.Reloads) @> }

    let [<Fact>] ``readThrough scenarios`` () = task {
        let! struct (_token, state) = requireLoad ()
        test <@ (1, 1, 0) = (state, cat.Loads, cat.Reloads) @>

        let! struct (_token, state) = allowStale 1000 // Existing cached entry is used, as fresh enough
        test <@ (1, 1, 0) = (state, cat.Loads, cat.Reloads) @>
        do! Task.Delay 50
        let! struct (_token, state) = allowStale 1000 // Does not load, or extend lifetime
        test <@ (1, 1, 0) = (state, cat.Loads, cat.Reloads) @>
        let! struct (_token, state) = allowStale 50 // Triggers reload as delay of 50 above has rendered entry stale
        test <@ (2, 1, 1) = (state, cat.Loads, cat.Reloads) @>

        cat.Delay <- TimeSpan.FromMilliseconds 50
        let load1 = requireLoad ()
        do! Task.Delay 10 // Make the load1 read enter a delay state (of 50)
        cat.Delay <- TimeSpan.FromMilliseconds 90 // Next read picks up the longer delay
        // These reads start after the first read so replace the older value in the cache
        let load2 = allowStale 1 // NB this read overlaps with load1 task, ReadThrough should coalesce with next ...
        let load3 = allowStale 10 // ... should wind up internally sharing with load2 (despite taking 80, it's valid if it starts within 10)
        let! struct (_t1, r1) = load2 // should be reused by load3 (and should be the winning cache entry)
        let! struct (_t2, r2) = load3 // requested last - should be same result as r1
        let! struct (_t3, r3) = load1 // We awaited it last, but expect it to have completed first
        test <@ (4, 4, 3) = (r1, r2, r3) && (1, 3) = (cat.Loads, cat.Reloads) @>
        // NOTE While 90 should be fine in next statement, it's not fine on a cold CI rig, don't adjust!
        let! struct (_token, state) = allowStale 200 // Delay of 90 overlapped with delay of 50+10 should not have expired the entry
        test <@ (4, 1, 3) = (state, cat.Loads, cat.Reloads) @> // The newer cache entry won
        cat.Delay <- TimeSpan.FromMilliseconds 10 // Reduce the delay, but we do want to overlap a write
        let t4 = allowStale 200 // Delay of 75 in load2/load3 should not have aged the read result beyond 200
        do! Task.Delay 10 // ensure delay has been picked up, before...
        cat.Delay <- TimeSpan.Zero // no further delays required for the rest of the tests

        // Trigger a concurrent write (it should lose the cache update race)
        do! write ()
        let! struct (_token, state) = t4
        test <@ (4, 1, 3) = (state, cat.Loads, cat.Reloads) @> // read outcome is cached (does not see overlapping write)
        // a raw read updates the cache, even though it's very fresh
        let! struct (_token, state) = requireLoad ()
        test <@ (5, 1, 4) = (state, cat.Loads, cat.Reloads) @>
        // a readThrough re-uses that
        let! struct (_token, state) = allowStale 10
        test <@ (5, 1, 4) = (state, cat.Loads, cat.Reloads) @>

        // anyCachedValue is just a special case of read through in this implementation
        // ... so it works the same
        let! struct (_token, state) = anyCachedValue ()
        test <@ (5, 1, 4) = (state, cat.Loads, cat.Reloads) @>

        // a write overwrites it (with an older value because our predicate is broken)
        do! write ()
        // an allowStale sees the written value
        let! struct (_token, state) = allowStale 30
        test <@ (expectedWriteState, 1, 4) = (state, cat.Loads, cat.Reloads) @>
        // a raw read updates the cache
        let! struct (_token, state) = requireLoad ()
        test <@ (6, 1, 5) = (state, cat.Loads, cat.Reloads) @>
        // and the allowStale / anyCachedValue sees same
        let! struct (_token, state) = allowStale 10
        test <@ (6, 1, 5) = (state, cat.Loads, cat.Reloads) @>
        let! struct (_token, state) = anyCachedValue ()
        test <@ (6, 1, 5) = (state, cat.Loads, cat.Reloads) @> }
