module Equinox.MessageDb.Integration.MessageDbIntegration

open System.Threading
open Domain
open Equinox.Core.Tracing
open FSharp.UMX
open Swensen.Unquote
open System.Diagnostics
open System

let defaultBatchSize = 500

open Equinox.MessageDb

let connectToLocalStore () = async {
  let connectionString = "Host=localhost; Username=message_store; Password=; Database=message_store; Port=5432; Maximum Pool Size=10"
  return MessageDbClient(connectionString)
}
type Context = MessageDbContext
type Category<'event, 'state, 'context> = MessageDbCategory<'event, 'state, 'context>

let createContext connection batchSize = Context(connection, batchSize = batchSize)

module SimplestThing =
    type Event =
        | StuffHappened
        interface TypeShape.UnionContract.IUnionContract
    let codec = EventCodec.gen<Event>

    let evolve (_state: Event) (event: Event) = event
    let fold = Seq.fold evolve
    let initial = StuffHappened
    let resolve log context =
        Category(context, codec, fold, initial)
        |> Equinox.Decider.resolve log
    let [<Literal>] Category = "SimplestThing"

module Cart =
    let fold, initial = Cart.Fold.fold, Cart.Fold.initial
    let codec = Cart.Events.codec
    let createServiceWithoutOptimization log context =
        Category(context, codec, fold, initial)
        |> Equinox.Decider.resolve log
        |> Cart.create

    let snapshot = Cart.Fold.snapshotEventCaseName, Cart.Fold.snapshot
    let createServiceWithAdjacentSnapshotting log context =
        Category(context, codec, fold, initial, access = AccessStrategy.AdjacentSnapshots snapshot)
        |> Equinox.Decider.resolve log
        |> Cart.create

    let createServiceWithCaching log context cache =
        let sliding20m = Equinox.CachingStrategy.SlidingWindow (cache, TimeSpan.FromMinutes 20.)
        Category(context, codec, fold, initial, sliding20m)
        |> Equinox.Decider.resolve log
        |> Cart.create

    let createServiceWithSnapshottingAndCaching log context cache =
        let sliding20m = Equinox.CachingStrategy.SlidingWindow (cache, TimeSpan.FromMinutes 20.)
        Category(context, codec, fold, initial, sliding20m, AccessStrategy.AdjacentSnapshots snapshot)
        |> Equinox.Decider.resolve log
        |> Cart.create

module ContactPreferences =
    let fold, initial = ContactPreferences.Fold.fold, ContactPreferences.Fold.initial
    let codec = ContactPreferences.Events.codec
    let createServiceWithoutOptimization log connection =
        let context = createContext connection defaultBatchSize
        Category(context, codec, fold, initial)
        |> Equinox.Decider.resolve log
        |> ContactPreferences.create

    let createService log connection =
        Category(createContext connection 1, codec, fold, initial, access = AccessStrategy.LatestKnownEvent)
        |> Equinox.Decider.resolve log
        |> ContactPreferences.create

let addAndThenRemoveItems optimistic exceptTheLastOne context cartId skuId (service: Cart.Service) count =
        service.ExecuteManyAsync(cartId, optimistic, seq {
            for i in 1..count do
                yield Cart.SyncItem (context, skuId, Some i, None)
                if not exceptTheLastOne || i <> count then
                    yield Cart.SyncItem (context, skuId, Some 0, None) })
let addAndThenRemoveItemsManyTimes context cartId skuId service count =
    addAndThenRemoveItems false false context cartId skuId service count
let addAndThenRemoveItemsManyTimesExceptTheLastOne context cartId skuId service count =
    addAndThenRemoveItems false true context cartId skuId service count
let addAndThenRemoveItemsOptimisticManyTimesExceptTheLastOne context cartId skuId service count =
    addAndThenRemoveItems true true context cartId skuId service count

type Listener() =
    let scope = Guid.NewGuid()
    let v = AsyncLocal()
    do v.Value <- scope
    let spans = ResizeArray()
    let listener = new ActivityListener(
        ActivityStarted = (fun s -> if v.Value = scope then spans.Add(s)),
        ShouldListenTo = (fun s -> s.Name = Equinox.Core.Tracing.SourceName),
        Sample = (fun _ -> ActivitySamplingResult.AllDataAndRecorded))
    do ActivitySource.AddActivityListener(listener)

    member _.Spans() = spans
    member _.Clear() = spans.Clear()
    member _.TestSpans(tests: _[]) =
        let len = max tests.Length spans.Count
        // this is funky because we want to ensure the same number of tests and spans
        for i in 0..len - 1 do
            tests[i] spans[i]
        spans.Clear()

    interface IDisposable with
        member _.Dispose() = listener.Dispose()

let span (m: (string * obj) list) (span: Activity) =
    let spanDict = Map.ofList [
        for key, _ in m do
          key, span.GetTagItem(key)
        "name", span.DisplayName :> obj ]
    test <@ spanDict = Map.ofList m @>

type GeneralTests() =

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_EVENTSTORE")>]
    let ``Can roundtrip against Store, correctly batching the reads [without any optimizations]`` (ctx, skuId) = async {
        let log = Serilog.Log.Logger
        use listener = new Listener()
        let! connection = connectToLocalStore ()

        let batchSize = 3
        let context = createContext connection batchSize
        let service = Cart.createServiceWithoutOptimization log context

        // The command processing should trigger only a single read and a single write call
        let addRemoveCount = 6
        let cartId = % Guid.NewGuid()

        do! addAndThenRemoveItemsManyTimesExceptTheLastOne ctx cartId skuId service addRemoveCount
        listener.TestSpans([|
            span([
                "name", "Transact"
                Tags.batches, 1
                Tags.loaded_count, 0
                Tags.append_count, 11
            ])
        |])

        // Validate basic operation; Key side effect: Log entries will be emitted to `capture`
        let! state = service.Read cartId
        let expectedEventCount = 2 * addRemoveCount - 1
        test <@ addRemoveCount = match state with { items = [{ quantity = quantity }] } -> quantity | _ -> failwith "nope" @>

        // Need to read 4 batches to read 11 events in batches of 3
        let expectedBatches = ceil(float expectedEventCount/float batchSize) |> int
        listener.TestSpans([|
            span(["name", "Query"; Tags.batches, expectedBatches; Tags.loaded_count, expectedEventCount])
        |])
    }

    [<AutoData(MaxTest = 2, SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_EVENTSTORE")>]
    let ``Can roundtrip against Store, managing sync conflicts by retrying [without any optimizations]`` (ctx, initialState) = async {
        let log = Serilog.Log.Logger
        use listener = new Listener()

        let! connection = connectToLocalStore()
        // Ensure batching is included at some point in the proceedings
        let batchSize = 3

        let ctx, (sku11, sku12, sku21, sku22) = ctx
        let cartId = % Guid.NewGuid()

        // establish base stream state
        let context = createContext connection batchSize
        let service1 = Cart.createServiceWithoutOptimization log context
        let! maybeInitialSku =
            let streamEmpty, skuId = initialState
            async {
                if streamEmpty then return None
                else
                    let addRemoveCount = 2
                    do! addAndThenRemoveItemsManyTimesExceptTheLastOne ctx cartId skuId service1 addRemoveCount
                    return Some (skuId, addRemoveCount) }

        let act prepare (service: Cart.Service) skuId count =
            service.ExecuteManyAsync(cartId, false, prepare = prepare, commands = [Cart.SyncItem (ctx, skuId, Some count, None)])

        let eventWaitSet () = let e = new ManualResetEvent(false) in (Async.AwaitWaitHandle e |> Async.Ignore), async { e.Set() |> ignore }
        let w0, s0 = eventWaitSet ()
        let w1, s1 = eventWaitSet ()
        let w2, s2 = eventWaitSet ()
        let w3, s3 = eventWaitSet ()
        let w4, s4 = eventWaitSet ()
        let t1 = async {
            // Wait for other to have state, signal we have it, await conflict and handle
            let prepare = async {
                do! w0
                do! s1
                do! w2 }
            do! act prepare service1 sku11 11
            // Wait for other side to load; generate conflict
            let prepare = async { do! w3 }
            do! act prepare service1 sku12 12
            // Signal conflict generated
            do! s4 }
        let context = createContext connection batchSize
        let service2 = Cart.createServiceWithoutOptimization log context
        let t2 = async {
            // Signal we have state, wait for other to do same, engineer conflict
            let prepare = async {
                do! s0
                do! w1 }
            do! act prepare service2 sku21 21
            // Signal conflict is in place
            do! s2
            // Await our conflict
            let prepare = async {
                do! s3
                do! w4 }
            do! act prepare service2 sku22 22 }
        // Act: Engineer the conflicts and applications
        do! Async.Parallel [t1; t2] |> Async.Ignore

        let syncs = listener.Spans() |> List.ofSeq
        // Load state
        let! result = service1.Read cartId

        // Ensure correct values got persisted
        let has sku qty = result.items |> List.exists (fun { skuId = s; quantity = q } -> (sku, qty) = (s, q))
        test <@ maybeInitialSku |> Option.forall (fun (skuId, quantity) -> has skuId quantity)
                && has sku11 11 && has sku12 12
                && has sku21 21 && has sku22 22 @>
        // Intended conflicts pertained
        let conflicts = syncs |> List.filter(fun s -> s.DisplayName = "Transact" && s.GetTagItem(Tags.conflict) = true)
        test <@ List.length conflicts = 2 @> }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_EVENTSTORE")>]
    let ``Can correctly read and update against Store, with LatestKnownEvent Access Strategy`` id value = async {
        use listener = new Listener()
        let log = Serilog.Log.Logger
        let! client = connectToLocalStore()
        let service = ContactPreferences.createService log client

        // Feed some junk into the stream
        for i in 0..11 do
            let quickSurveysValue = i % 2 = 0
            do! service.Update(id, { value with quickSurveys = quickSurveysValue })
        // Ensure there will be something to be changed by the Update below
        do! service.Update(id, { value with quickSurveys = not value.quickSurveys })

        listener.Clear()
        do! service.Update(id, value)

        let! result = service.Read id
        listener.TestSpans([|
            span([ "name", "Transact"; Tags.loaded_count, 1; Tags.append_count, 1 ])
            span([ "name", "Query"; Tags.loaded_count, 1 ])
        |])
        test <@ value = result @>
    }

    let loadCached hit batches count (span: Activity) =
        test <@ span.DisplayName = "Load"
                && span.GetTagItem(Tags.cache_hit) = hit
                && span.GetTagItem(Tags.batches) = batches
                && span.GetTagItem(Tags.loaded_count) = count @>

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_EVENTSTORE")>]
    let ``Can roundtrip against Store, correctly caching to avoid redundant reads`` (ctx, skuId) = async {
        use listener = new Listener()
        let log = Serilog.Log.Logger
        let! client = connectToLocalStore ()
        let batchSize = 10
        let cache = Equinox.Cache("cart", sizeMb = 50)
        let context = createContext client batchSize
        let createServiceCached () = Cart.createServiceWithCaching log context cache
        let service1, service2, service3 = createServiceCached (), createServiceCached (), Cart.createServiceWithoutOptimization log context
        let cartId = % Guid.NewGuid()

        // Trigger 9 events, then reload
        do! addAndThenRemoveItemsManyTimesExceptTheLastOne ctx cartId skuId service1 5
        listener.TestSpans([|
            span ["name", "Transact"; Tags.cache_hit, false; Tags.batches, 1; Tags.loaded_count, 0; Tags.append_count, 9]
        |])

        let! resStale = service2.ReadStale cartId
        listener.TestSpans([|
            span ["name", "Query"; Tags.cache_hit, true; Tags.batches, null; Tags.loaded_count, null ]
        |])
        let! resFresh = service2.Read cartId
        // // Because we're caching writes, stale vs fresh reads are equivalent
        test <@ resStale = resFresh @>
        // ... should see a write plus a batched forward read as position is cached
        listener.TestSpans([|
            span ["name", "Query"; Tags.cache_hit, true; Tags.batches, 1; Tags.loaded_count, 0]
        |])

        let skuId2 = SkuId <| Guid.NewGuid()
        do! addAndThenRemoveItemsManyTimesExceptTheLastOne ctx cartId skuId2 service1 1
        listener.TestSpans([|
            span ["name", "Transact"; Tags.cache_hit, true; Tags.batches, 1; Tags.loaded_count, 0; Tags.append_count, 1]
        |])

        // While we now have 12 events, we should be able to read them with a single call
        // Do a stale read - we will see outs
        let! res = service2.ReadStale cartId
        // result after 10 should be different to result after 12
        test <@ res <> resFresh @>
        // but we don't do a roundtrip to get it
        listener.TestSpans([|
            span ["name", "Query"; Tags.cache_hit, true; Tags.batches, null; Tags.loaded_count, null ]
        |])
        let! _ = service2.Read cartId
        listener.TestSpans([|
            span ["name", "Query"; Tags.cache_hit, true; Tags.batches, 1; Tags.loaded_count, 0 ]
        |])
        // As the cache is up to date, we can transact against the cached value and do a null transaction without a roundtrip
        do! addAndThenRemoveItemsOptimisticManyTimesExceptTheLastOne ctx cartId skuId2 service1 1
        listener.TestSpans([|
            span ["name", "Transact"; Tags.cache_hit, true; Tags.batches, null; Tags.loaded_count, null ]
        |])
        // As the cache is up to date, we can do an optimistic append, saving a Read roundtrip
        let skuId3 = SkuId <| Guid.NewGuid()
        do! addAndThenRemoveItemsOptimisticManyTimesExceptTheLastOne ctx cartId skuId3 service1 1
        listener.TestSpans([|
            span ["name", "Transact"; Tags.cache_hit, true; Tags.batches, null; Tags.loaded_count, null; Tags.append_count, 1]
        |])
        // If we don't have a cache attached, we don't benefit from / pay the price for any optimism
        let skuId4 = SkuId <| Guid.NewGuid()
        do! addAndThenRemoveItemsOptimisticManyTimesExceptTheLastOne ctx cartId skuId4 service3 1
        // Need 2 batches to do the reading
        listener.TestSpans([|
            // this time, we did something, so we see the append call
            span ["name", "Transact"; Tags.cache_hit, null; Tags.batches, 2; Tags.loaded_count, 11; Tags.append_count, 1]
        |])
        // we've engineered a clash with the cache state (service3 doest participate in caching)
        // Conflict with cached state leads to a read forward to resync; Then we'll idempotently decide not to do any append
        do! addAndThenRemoveItemsOptimisticManyTimesExceptTheLastOne ctx cartId skuId4 service2 1
        listener.TestSpans([|
            span ["name", "Transact"; Tags.cache_hit, true; Tags.conflict, true ]
        |])
    }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_EVENTSTORE")>]
    let ``Version is 0-based`` () = async {
        let! connection = connectToLocalStore ()

        let batchSize = 3
        let context = createContext connection batchSize
        let id = Guid.NewGuid()
        let toStreamId (x: Guid) = x.ToString "N"
        let decider = SimplestThing.resolve Serilog.Log.Logger context SimplestThing.Category (Equinox.StreamId.gen toStreamId id)

        let! before, after = decider.TransactEx(
            (fun state -> state.Version, [SimplestThing.StuffHappened]),
            mapResult = (fun result ctx-> result, ctx.Version))
        test <@ [before; after] = [0L; 1L] @>
    }

type AdjacentSnapshotTests() =

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_EVENTSTORE")>]
    let ``Can roundtrip against Store, correctly snapshotting to avoid redundant reads`` (ctx, skuId) = async {
        use listener = new Listener()
        let log = Serilog.Log.Logger
        let! client = connectToLocalStore ()
        let batchSize = 10
        let context = createContext client batchSize
        let service = Cart.createServiceWithAdjacentSnapshotting  log context

        // Trigger 8 events, then reload
        let cartId = % Guid.NewGuid()
        do! addAndThenRemoveItemsManyTimes ctx cartId skuId service 4
        let! _ = service.Read cartId
        listener.TestSpans([|
            span ["name", "Transact"; Tags.batches, 1; Tags.loaded_count, 0; Tags.append_count, 8; Tags.snapshot_version, -1L]
            span ["name", "Query"; Tags.batches, 1; Tags.loaded_count, 8; Tags.snapshot_version, -1L]
        |])

        // Add two more, which should push it over the threshold and hence trigger an append of a snapshot event
        do! addAndThenRemoveItemsManyTimes ctx cartId skuId service 1
        listener.TestSpans([|
            span ["name", "Transact"; Tags.batches, 1; Tags.loaded_count, 8; Tags.append_count, 2; Tags.snapshot_version, -1L
                  Tags.snapshot_written, true]
        |])
        // We now have 10 events and should be able to read them with a single call
        let! _ = service.Read cartId
        listener.TestSpans([|
            span ["name", "Query"; Tags.batches, 1; Tags.loaded_count, 0; Tags.snapshot_version, 10L]
        |])

        // Add 8 more; total of 18 should not trigger snapshotting as we snapshotted at Event Number 10
        do! addAndThenRemoveItemsManyTimes ctx cartId skuId service 4
        listener.TestSpans([|
            span ["name", "Transact"; Tags.batches, 1; Tags.loaded_count, 0
                  Tags.snapshot_version, 10L; Tags.append_count, 8]
        |])

        // While we now have 18 events, we should be able to read them with a single call
        let! _ = service.Read cartId
        listener.TestSpans([|
            span ["name", "Query"; Tags.batches, 1; Tags.loaded_count, 8
                  Tags.snapshot_version, 10L]
        |])

        // add two more events, triggering a snapshot, then read it in a single snapshotted read
        do! addAndThenRemoveItemsManyTimes ctx cartId skuId service 1
        // and reload the 20 events with a single read
        let! _ = service.Read cartId
        listener.TestSpans([|
            span ["name", "Transact"; Tags.batches, 1; Tags.loaded_count, 8; Tags.snapshot_version, 10L
                  Tags.append_count, 2; Tags.snapshot_written, true]
            span ["name", "Query"; Tags.batches, 1; Tags.loaded_count, 0
                  Tags.snapshot_version, 20L]
        |])
    }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_EVENTSTORE")>]
    let ``Can combine snapshotting with caching against Store`` (ctx, skuId) = async {
        let log = Serilog.Log.Logger
        use listener = new Listener()
        let! client = connectToLocalStore()
        let batchSize = 10
        let context = createContext client batchSize
        let service1 = Cart.createServiceWithAdjacentSnapshotting log context
        let cache = Equinox.Cache("cart", sizeMb = 50)
        let context = createContext client batchSize
        let service2 = Cart.createServiceWithSnapshottingAndCaching log context cache

        // Trigger 8 events, then reload
        let cartId = % Guid.NewGuid()
        do! addAndThenRemoveItemsManyTimes ctx cartId skuId service1 4
        let! _ = service2.Read cartId

        // ... should not see a snapshot write as we are inside the batch threshold
        listener.TestSpans([|
            span ["name", "Transact"; Tags.batches, 1; Tags.loaded_count, 0; Tags.snapshot_version, -1L
                  Tags.cache_hit, null; Tags.append_count, 8; Tags.snapshot_written, null]
            span ["name", "Query"; Tags.batches, 1; Tags.loaded_count, 8; Tags.snapshot_version, -1L
                  Tags.cache_hit, false]

        |])

        // Add two more, which should push it over the threshold and hence trigger generation of a snapshot event
        do! addAndThenRemoveItemsManyTimes ctx cartId skuId service1 1
        listener.TestSpans([|
            span ["name", "Transact"; Tags.batches, 1; Tags.loaded_count, 8; Tags.snapshot_version, -1L
                  Tags.cache_hit, null; Tags.append_count, 2; Tags.snapshot_written, true]
        |])
        // We now have 10 events, we should be able to read them with a single snapshotted read
        let! _ = service1.Read cartId
        listener.TestSpans([|
            span ["name", "Query"; Tags.batches, 1; Tags.loaded_count, 0; Tags.snapshot_version, 10L
                  Tags.cache_hit, null]
        |])

        // Add 8 more; total of 18 should not trigger snapshotting as the snapshot is at version 10
        do! addAndThenRemoveItemsManyTimes ctx cartId skuId service1 4
        listener.TestSpans([|
            span ["name", "Transact"; Tags.batches, 1; Tags.loaded_count, 0; Tags.snapshot_version, 10L
                  Tags.cache_hit, null; Tags.append_count, 8; Tags.snapshot_written, null]
        |])

        // While we now have 18 events, we should be able to read them with a single snapshotted read
        let! _ = service1.Read cartId
        listener.TestSpans([|
            span ["name", "Query"; Tags.batches, 1; Tags.loaded_count, 8; Tags.snapshot_version, 10L
                  Tags.cache_hit, null]
        |])

        // ... trigger a second snapshotting
        do! addAndThenRemoveItemsManyTimes ctx cartId skuId service1 1
        // and we _could_ reload the 20 events with a single read. However we are using the cache, which last saw it with 10 events, which necessitates two reads
        let! _ = service2.Read cartId
        listener.TestSpans([|
            span ["name", "Transact"; Tags.batches, 1; Tags.loaded_count, 8; Tags.snapshot_version, 10L
                  Tags.cache_hit, null; Tags.append_count, 2; Tags.snapshot_written, true]
            span ["name", "Query"; Tags.batches, 2; Tags.loaded_count, 12; Tags.snapshot_version, null
                  Tags.cache_hit, true]
        |])
    }
