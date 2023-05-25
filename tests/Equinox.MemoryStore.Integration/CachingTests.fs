// Ironically, MemoryStore does not implement caching
// (but we don't yet have an Equinox.Tests - when we do the AsyncCacheCell and AsyncBatchingGate tests should move too)
module Equinox.MemoryStore.Integration.CachingTests

open System.Threading
open Xunit
open Swensen.Unquote
open System
open System.Threading.Tasks

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
    member _.State with get () = state and set value = state <- value
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

let [<Fact>] happy () = task {
    let cat = SpyCategory(State = 0)
    let sut = Equinox.Core.Caching.apply isStale (Some strategy) cat
    let sn = Guid.NewGuid |> string

    let loadRaw () = load sn TimeSpan.Zero sut
    let! struct (_token, state) = loadRaw ()
    test <@ 1 = state && (1, 0) = (cat.Loads, cat.Reloads) @>
    let! struct (_token, state) = loadRaw () // This time, the cache entry should be used as the base state for the load
    test <@ 2 = state && (1, 1) = (cat.Loads, cat.Reloads) @>
    let! struct (_token, state) = loadRaw () // no matter how close to the preceding load, a fresh roundtrip occurs
    test <@ 3 = state && (1, 2) = (cat.Loads, cat.Reloads) @>

    let writeOriginState = 99
    let expectedWriteState = 99 + 2 // events written

    let write () = task {
        let! wr = sut.TrySync(Serilog.Log.Logger, null, null, sn, (), ValueNone, Unchecked.defaultof<_>, writeOriginState, Array.replicate 2 (), CancellationToken.None)
        let wState' = trap <@ match wr with Equinox.Core.SyncResult.Written (_token, wstate') -> wstate' | _ -> failwith "unexpected" @>
        test <@ expectedWriteState = wState' @>
    }

    let t4 = loadRaw ()

    // Trigger a concurrent write (it should lose the cache update race)
    do! write ()
    let! struct (_token, state) = t4
    test <@ 4 = state && (1, 3) = (cat.Loads, cat.Reloads) @>
    // a raw read updates the cache, even though it's very fresh
    let! struct (_token, state) = loadRaw ()
    test <@ 5 = state && (1, 4) = (cat.Loads, cat.Reloads) @>

    // a write overwrites it (with an older value because our predicate is broken)
    do! write ()
    // a read through sees the written value
    let! struct (_token, state) = load sn TimeSpan.MaxValue sut
    test <@ expectedWriteState = state && (1, 4) = (cat.Loads, cat.Reloads) @>
    // a raw read updates the cache
    let! struct (_token, state) = loadRaw ()
    test <@ 6 = state && (1, 5) = (cat.Loads, cat.Reloads) @>
    // and the read through sees same
    let! struct (_token, state) = load sn TimeSpan.MaxValue sut
    test <@ 6 = state && (1, 5) = (cat.Loads, cat.Reloads) @>
}
