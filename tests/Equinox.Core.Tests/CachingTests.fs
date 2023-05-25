module Equinox.Core.Tests.CachingTests

open Equinox.Core
open Swensen.Unquote
open System
open System.Threading
open System.Threading.Tasks
open Xunit

type State = int
type Event = unit

// We don't worry about the tokens in this test scenario - the timestamps are the deciding factor
let mkToken () : Equinox.Core.StreamToken = { value = null; version = 0L; streamBytes = -1L }
let isStale _cur _candidate = false

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
            Interlocked.Increment &state |> ignore
            return struct (mkToken(), state)
        }
        member _.TrySync(_log, _cat, _sid, _sn, _ctx, _maybeInit, _originToken, originState, events, _ct) = task {
            return Equinox.Core.SyncResult.Written (mkToken(), originState + events.Length)
        }

    interface Equinox.Core.Caching.IReloadable<State> with
        member x.Reload(_log, _sn, _requireLeader, _streamToken, _baseState, ct) = task {
            Interlocked.Increment &reloads |> ignore
            do! Task.Delay(x.Delay, ct)
            Interlocked.Increment &state |> ignore
            return struct (mkToken(), state)
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
    let i1 = AsyncLazy<int>.Empty
    let i2 = AsyncLazy<int>.Empty
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

    let requireLoad () = load sn TimeSpan.Zero sut
    let allowStale () = load sn TimeSpan.MaxValue sut
    let write () = write sn sut

    let [<Fact>] ``requireLoad vs allowStale`` () = task {
        let! struct (_token, state) = requireLoad ()
        test <@ 1 = state && (1, 0) = (cat.Loads, cat.Reloads) @>
        let! struct (_token, state) = requireLoad () // This time, the cache entry should be used as the base state for the load
        test <@ 2 = state && (1, 1) = (cat.Loads, cat.Reloads) @>
        let! struct (_token, state) = requireLoad () // no matter how close to the preceding load, a fresh roundtrip occurs
        test <@ 3 = state && (1, 2) = (cat.Loads, cat.Reloads) @>

        let t4 = requireLoad ()

        // Trigger a concurrent write (it should lose the cache update race)
        do! write ()
        let! struct (_token, state) = t4
        test <@ 4 = state && (1, 3) = (cat.Loads, cat.Reloads) @>
        // a raw read updates the cache, even though it's very fresh
        let! struct (_token, state) = requireLoad ()
        test <@ 5 = state && (1, 4) = (cat.Loads, cat.Reloads) @>

        // a write overwrites it (with an older value because our predicate is broken)
        do! write ()

        // a stale read sees the written value
        let! struct (_token, state) = allowStale ()
        test <@ expectedWriteState = state && (1, 4) = (cat.Loads, cat.Reloads) @>
        // a raw read updates the cache
        let! struct (_token, state) = requireLoad ()
        test <@ 6 = state && (1, 5) = (cat.Loads, cat.Reloads) @>
        // and the stale read sees same directly
        let! struct (_token, state) = allowStale ()
        test <@ 6 = state && (1, 5) = (cat.Loads, cat.Reloads) @>
    }

    let [<Fact>] ``requireLoad does not unify loads``  () = task {
        cat.Delay <- TimeSpan.FromMilliseconds 50
        let t1 = requireLoad ()
        do! Task.Delay 10
        test <@ (1, 0) = (cat.Loads, cat.Reloads) @>
        do! Task.Delay 50 // wait for the loaded value to get cached
        let! struct (_token, state) = requireLoad ()
        test <@ 2 = state && (1, 1) = (cat.Loads, cat.Reloads) @>
        let! struct (_token, state) = t1
        test <@ 1 = state && (1, 1) = (cat.Loads, cat.Reloads) @>
    }

    let loadReadThrough toleranceMs = load sn (TimeSpan.FromMilliseconds toleranceMs) sut

    let [<Fact>] ``readThrough unifies compatible concurrent loads``  () = task {
        cat.Delay <- TimeSpan.FromMilliseconds 50
        let t1 = loadReadThrough 1
        do! Task.Delay 10
        test <@ (1, 0) = (cat.Loads, cat.Reloads) @>
        let! struct (_token, state) = loadReadThrough 15
        test <@ 1 = state && (1, 0) = (cat.Loads, cat.Reloads) @>
        let! struct (_token, state) = t1
        test <@ 1 = state && (1, 0) = (cat.Loads, cat.Reloads) @>
    }

    let [<Fact>] ``readThrough handles concurrent incompatible loads correctly``  () = task {
        cat.Delay <- TimeSpan.FromMilliseconds 50
        let t1 = loadReadThrough 1
        do! Task.Delay 10
        test <@ (1, 0) = (cat.Loads, cat.Reloads) @>
        let! struct (_token, state) = loadReadThrough 9
        test <@ 2 = state && (2, 0) = (cat.Loads, cat.Reloads) @>
        let! struct (_token, state) = t1
        test <@ 1 = state && (2, 0) = (cat.Loads, cat.Reloads) @>
    }

    let [<Fact>] ``readThrough handles overlapped incompatible loads correctly``  () = task {
        cat.Delay <- TimeSpan.FromMilliseconds 50
        let t1 = loadReadThrough 1
        do! Task.Delay 10
        test <@ (1, 0) = (cat.Loads, cat.Reloads) @>
        do! Task.Delay 50
        let! struct (_token, state) = loadReadThrough 59
        test <@ 2 = state && (1, 1) = (cat.Loads, cat.Reloads) @>
        let! struct (_token, state) = t1
        test <@ 1 = state && (1, 1) = (cat.Loads, cat.Reloads) @>
    }

    let [<Fact>] ``readThrough scenarios`` () = task {
        let! struct (_token, state) = requireLoad ()
        test <@ 1 = state && (1, 0) = (cat.Loads, cat.Reloads) @>

        let! struct (_token, state) = loadReadThrough 1000 // Existing cached entry is used, as fresh enough
        test <@ 1 = state && (1, 0) = (cat.Loads, cat.Reloads) @>
        do! Task.Delay 100
        let! struct (_token, state) = loadReadThrough 1000 // Does not load, or extend lifetime
        test <@ 1 = state && (1, 0) = (cat.Loads, cat.Reloads) @>
        let! struct (_token, state) = loadReadThrough 50 // Triggers reload as delay of 100 above has rendered entry stale
        test <@ 2 = state && (1, 1) = (cat.Loads, cat.Reloads) @>
        cat.Delay <- TimeSpan.FromMilliseconds 50
        let t3 = requireLoad ()
        do! Task.Delay 2 // Make the main read enter a delay state (of 500); ensure readThrough values are expired
        cat.Delay <- TimeSpan.FromMilliseconds 100 // Next read picks up the longer delay
        // These reads start after the first read so replace the older value in the cache
        let t1 = loadReadThrough 1
        let t2 = loadReadThrough 1 // NB this read overlaps with t1 task, ReadThrough should coalesce
        let! struct (_t2, r2) = t2 // started last, awaited first - should be same result as r1 (and should be the winning cache entry)
        let! struct (_t1, r1) = t1 // started first, but should be same as r2
        let! struct (_t3, r3) = t3 // We awaited it last, but it returned a result first
        test <@ 3 = r3 && 4 = r1 && (1, 3) = (cat.Loads, cat.Reloads) && r1 = r2 @>
        let! struct (_token, state) = loadReadThrough 150 // Delay of 100 overlapped with delay of 50 should not have expired the entry
        test <@ 4 = state && (1, 3) = (cat.Loads, cat.Reloads) @> // The newer cache entry won
        cat.Delay <- TimeSpan.FromMilliseconds 10 // Reduce the delay, but we do want to overlap a write
        let t4 = loadReadThrough 1000 // Delay of 1000 in t1/t2 should have aged the read result, so should trigger a read
        do! Task.Delay 2
        cat.Delay <- TimeSpan.Zero // no further delays required for the rest of the tests

        // Trigger a concurrent write (it should lose the cache update race)
        do! write ()
        let! struct (_token, state) = t4
        test <@ 4 = state && (1, 3) = (cat.Loads, cat.Reloads) @>
        // a raw read updates the cache, even though it's very fresh
        let! struct (_token, state) = requireLoad ()
        test <@ 5 = state && (1, 4) = (cat.Loads, cat.Reloads) @>
        // a readThrough re-uses that
        let! struct (_token, state) = loadReadThrough 10
        test <@ 5 = state && (1, 4) = (cat.Loads, cat.Reloads) @>

        // allowStale is just a special case of read through in this implementation
        // ... so it works the same
        let! struct (_token, state) = allowStale ()
        test <@ 5 = state && (1, 4) = (cat.Loads, cat.Reloads) @>

        // a write overwrites it (with an older value because our predicate is broken)
        do! write ()
        // a readThrough sees the written value
        let! struct (_token, state) = loadReadThrough 10
        test <@ expectedWriteState = state && (1, 4) = (cat.Loads, cat.Reloads) @>
        // a raw read updates the cache
        let! struct (_token, state) = requireLoad ()
        test <@ 6 = state && (1, 5) = (cat.Loads, cat.Reloads) @>
        // and the readThrough / allowStale sees same
        let! struct (_token, state) = loadReadThrough 10
        test <@ 6 = state && (1, 5) = (cat.Loads, cat.Reloads) @>
        let! struct (_token, state) = allowStale ()
        test <@ 6 = state && (1, 5) = (cat.Loads, cat.Reloads) @>
    }
