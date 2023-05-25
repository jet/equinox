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

let cache = Equinox.Cache("tests", 1)
let strategy = Equinox.CachingStrategy.SlidingWindow (cache, TimeSpan.FromMinutes 10)

let [<Fact>] ``No strategy, no wrapping`` () =
    let cat = SpyCategory()
    let sut = Equinox.Core.Caching.apply isStale None cat
    test <@ obj.ReferenceEquals(cat, sut) @>

let load sn maxAge (sut : Equinox.Core.ICategory<_, _, _>) =
    sut.Load(Serilog.Log.Logger, null, null, sn, maxAge, false, CancellationToken.None)

let [<Fact>] ``happy path`` () = task {
    let cat = SpyCategory()
    let sut = Equinox.Core.Caching.apply isStale (Some strategy) cat
    let sn = Guid.NewGuid |> string

    let requireLoad () = load sn TimeSpan.Zero sut
    let! struct (_token, state) = requireLoad ()
    test <@ 1 = state && (1, 0) = (cat.Loads, cat.Reloads) @>
    let! struct (_token, state) = requireLoad () // This time, the cache entry should be used as the base state for the load
    test <@ 2 = state && (1, 1) = (cat.Loads, cat.Reloads) @>
    let! struct (_token, state) = requireLoad () // no matter how close to the preceding load, a fresh roundtrip occurs
    test <@ 3 = state && (1, 2) = (cat.Loads, cat.Reloads) @>

    let writeOriginState = 99
    let expectedWriteState = 99 + 2 // events written

    let write () = task {
        let! wr = sut.TrySync(Serilog.Log.Logger, null, null, sn, (), ValueNone, Unchecked.defaultof<_>, writeOriginState, Array.replicate 2 (), CancellationToken.None)
        let wState' = trap <@ match wr with Equinox.Core.SyncResult.Written (_token, state') -> state' | _ -> failwith "unexpected" @>
        test <@ expectedWriteState = wState' @>
    }

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

    let allowStale () = load sn TimeSpan.MaxValue sut

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
