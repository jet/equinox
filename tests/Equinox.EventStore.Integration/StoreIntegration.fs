module Equinox.Store.Integration.StoreIntegration

open Domain
open Equinox.Integration.Infrastructure
open FSharp.UMX
open Serilog
open Swensen.Unquote
open System.Threading
open System

#if STORE_POSTGRES
open Equinox.SqlStreamStore
open Equinox.SqlStreamStore.Postgres

let connectToLocalStore (_ : ILogger) =
    Connector("Host=localhost;User Id=postgres;database=EQUINOX_TEST_DB",autoCreate=true).Establish()
#else
#if STORE_MSSQL
open Equinox.SqlStreamStore
open Equinox.SqlStreamStore.MsSql

let connectToLocalStore (_ : ILogger) =
    Connector(sprintf "Server=localhost,1433;User=sa;Password=!Passw0rd;Database=test",autoCreate=true).Establish()
#else
#if STORE_MYSQL
open Equinox.SqlStreamStore
open Equinox.SqlStreamStore.MySql

let connectToLocalStore (_ : ILogger) =
    Connector(sprintf "Server=localhost;User=root;Database=EQUINOX_TEST_DB",autoCreate=true).Establish()
#else // STORE_EVENTSTORE
open Equinox.EventStore

/// Connect directly to a locally running EventStore Node without using Gossip-driven discovery
/// To establish a local node to run the tests against:
///   1. cinst eventstore-oss -y # where cinst is an invocation of the Chocolatey Package Installer on Windows
///   2. & $env:ProgramData\chocolatey\bin\EventStore.ClusterNode.exe --gossip-on-single-node --discover-via-dns 0 --ext-http-port=30778
/// (For this specific suite only, omitting the args will also work as the Gossip-related ports are irrelevant, but other tests would fail)
let connectToLocalStore log =
    Connector("admin", "changeit", reqTimeout=TimeSpan.FromSeconds 3., reqRetries=3, log=Logger.SerilogVerbose log, tags=["I",Guid.NewGuid() |> string])
        .Establish("Equinox-integration", Discovery.Uri(Uri "tcp://localhost:1113"),ConnectionStrategy.ClusterSingle NodePreference.Master)
#endif
#endif
#endif

let defaultBatchSize = 500
let createGesGateway connection batchSize = Context(connection, BatchingPolicy(maxBatchSize = batchSize))

module Cart =
    let fold, initial = Domain.Cart.Fold.fold, Domain.Cart.Fold.initial
    let codec = Domain.Cart.Events.codec
    let snapshot = Domain.Cart.Fold.isOrigin, Domain.Cart.Fold.snapshot
    let createServiceWithoutOptimization log gateway =
        let resolve (id,opt) = Resolver(gateway, Domain.Cart.Events.codec, fold, initial).Resolve(id,?option=opt)
        Backend.Cart.create log resolve
    let createServiceWithCompaction log gateway =
        let resolve (id,opt) = Resolver(gateway, codec, fold, initial, access = AccessStrategy.RollingSnapshots snapshot).Resolve(id,?option=opt)
        Backend.Cart.create log resolve
    let createServiceWithCaching log gateway cache =
        let sliding20m = CachingStrategy.SlidingWindow (cache, TimeSpan.FromMinutes 20.)
        Backend.Cart.create log (fun (id,opt) -> Resolver(gateway, codec, fold, initial, sliding20m).Resolve(id,?option=opt))
    let createServiceWithCompactionAndCaching log gateway cache =
        let sliding20m = CachingStrategy.SlidingWindow (cache, TimeSpan.FromMinutes 20.)
        Backend.Cart.create log (fun (id,opt) -> Resolver(gateway, codec, fold, initial, sliding20m, AccessStrategy.RollingSnapshots snapshot).Resolve(id,?option=opt))

module ContactPreferences =
    let fold, initial = Domain.ContactPreferences.Fold.fold, Domain.ContactPreferences.Fold.initial
    let codec = Domain.ContactPreferences.Events.codec
    let createServiceWithoutOptimization log connection =
        let gateway = createGesGateway connection defaultBatchSize
        Backend.ContactPreferences.create log (Resolver(gateway, codec, fold, initial).Resolve)
    let createService log connection =
        let resolver = Resolver(createGesGateway connection 1, codec, fold, initial, access = AccessStrategy.LatestKnownEvent)
        Backend.ContactPreferences.create log resolver.Resolve

#nowarn "1182" // From hereon in, we may have some 'unused' privates (the tests)

type Tests(testOutputHelper) =
    let testOutput = TestOutputAdapter testOutputHelper

    let addAndThenRemoveItems optimistic exceptTheLastOne context cartId skuId (service: Backend.Cart.Service) count =
        service.ExecuteManyAsync(cartId, optimistic, seq {
            for i in 1..count do
                yield Domain.Cart.SyncItem (context, skuId, Some i, None)
                if not exceptTheLastOne || i <> count then
                    yield Domain.Cart.SyncItem (context, skuId, Some 0, None) })
    let addAndThenRemoveItemsManyTimes context cartId skuId service count =
        addAndThenRemoveItems false false context cartId skuId service count
    let addAndThenRemoveItemsManyTimesExceptTheLastOne context cartId skuId service count =
        addAndThenRemoveItems false true context cartId skuId service count
    let addAndThenRemoveItemsOptimisticManyTimesExceptTheLastOne context cartId skuId service count =
        addAndThenRemoveItems true true context cartId skuId service count

    let createLoggerWithCapture () =
        let capture = LogCaptureBuffer()
        let logger =
            Serilog.LoggerConfiguration()
                .WriteTo.Sink(testOutput)
                .WriteTo.Sink(capture)
                .WriteTo.Seq("http://localhost:5341")
                .CreateLogger()
        logger, capture

    let singleSliceForward = EsAct.SliceForward
    let singleBatchForward = [EsAct.SliceForward; EsAct.BatchForward]
    let batchForwardAndAppend = singleBatchForward @ [EsAct.Append]

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_EVENTSTORE")>]
    let ``Can roundtrip against EventStore, correctly batching the reads [without any optimizations]`` context skuId = Async.RunSynchronously <| async {
        let log, capture = createLoggerWithCapture ()
        let! conn = connectToLocalStore log

        let batchSize = 3
        let gateway = createGesGateway conn batchSize
        let service = Cart.createServiceWithoutOptimization log gateway

        // The command processing should trigger only a single read and a single write call
        let addRemoveCount = 6
        let cartId = % System.Guid.NewGuid()

        do! addAndThenRemoveItemsManyTimesExceptTheLastOne context cartId skuId service addRemoveCount
        test <@ batchForwardAndAppend = capture.ExternalCalls @>

        // Restart the counting
        capture.Clear()

        // Validate basic operation; Key side effect: Log entries will be emitted to `capture`
        let! state = service.Read cartId
        let expectedEventCount = 2 * addRemoveCount - 1
        test <@ addRemoveCount = match state with { items = [{ quantity = quantity }] } -> quantity | _ -> failwith "nope" @>

        // Need to read 4 batches to read 11 events in batches of 3
        let expectedBatches = ceil(float expectedEventCount/float batchSize) |> int
        test <@ List.replicate (expectedBatches-1) singleSliceForward @ singleBatchForward = capture.ExternalCalls @>
    }

    [<AutoData(MaxTest = 2, SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_EVENTSTORE")>]
    let ``Can roundtrip against EventStore, managing sync conflicts by retrying [without any optimizations]`` ctx initialState = Async.RunSynchronously <| async {
        let log1, capture1 = createLoggerWithCapture ()
        let! conn = connectToLocalStore log1
        // Ensure batching is included at some point in the proceedings
        let batchSize = 3

        let context, (sku11, sku12, sku21, sku22) = ctx
        let cartId = % System.Guid.NewGuid()


        // establish base stream state
        let gateway = createGesGateway conn batchSize
        let service1 = Cart.createServiceWithoutOptimization log1 gateway
        let! maybeInitialSku =
            let (streamEmpty, skuId) = initialState
            async {
                if streamEmpty then return None
                else
                    let addRemoveCount = 2
                    do! addAndThenRemoveItemsManyTimesExceptTheLastOne context cartId skuId service1 addRemoveCount
                    return Some (skuId, addRemoveCount) }

        let act prepare (service : Backend.Cart.Service) log skuId count =
            service.ExecuteManyAsync(cartId, false, prepare = prepare, commands = [Domain.Cart.SyncItem (context, skuId, Some count, None)])

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
            do! act prepare service1 log1 sku11 11
            // Wait for other side to load; generate conflict
            let prepare = async { do! w3 }
            do! act prepare service1 log1 sku12 12
            // Signal conflict generated
            do! s4 }
        let log2, capture2 = createLoggerWithCapture ()
        let gateway = createGesGateway conn batchSize
        let service2 = Cart.createServiceWithoutOptimization log2 gateway
        let t2 = async {
            // Signal we have state, wait for other to do same, engineer conflict
            let prepare = async {
                do! s0
                do! w1 }
            do! act prepare service2 log2 sku21 21
            // Signal conflict is in place
            do! s2
            // Await our conflict
            let prepare = async {
                do! s3
                do! w4 }
            do! act prepare service2 log2 sku22 22 }
        // Act: Engineer the conflicts and applications, with logging into capture1 and capture2
        do! Async.Parallel [t1; t2] |> Async.Ignore

        // Load state
        let! result = service1.Read cartId

        // Ensure correct values got persisted
        let has sku qty = result.items |> List.exists (fun { skuId = s; quantity = q } -> (sku, qty) = (s, q))
        test <@ maybeInitialSku |> Option.forall (fun (skuId, quantity) -> has skuId quantity)
                && has sku11 11 && has sku12 12
                && has sku21 21 && has sku22 22 @>
       // Intended conflicts pertained
        let hadConflict= function EsEvent (EsAction EsAct.AppendConflict) -> Some () | _ -> None
        test <@ [1; 1] = [for c in [capture1; capture2] -> c.ChooseCalls hadConflict |> List.length] @>
    }

    let singleBatchBackwards = [EsAct.SliceBackward; EsAct.BatchBackward]
    let batchBackwardsAndAppend = singleBatchBackwards @ [EsAct.Append]

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_EVENTSTORE")>]
    let ``Can roundtrip against EventStore, correctly compacting to avoid redundant reads`` context skuId = Async.RunSynchronously <| async {
        let log, capture = createLoggerWithCapture ()
        let! conn = connectToLocalStore log
        let batchSize = 10
        let gateway = createGesGateway conn batchSize
        let service = Cart.createServiceWithCompaction log gateway

        // Trigger 10 events, then reload
        let cartId = % System.Guid.NewGuid()
        do! addAndThenRemoveItemsManyTimes context cartId skuId service 5
        let! _ = service.Read cartId

        // ... should see a single read as we are inside the batch threshold
        test <@ batchBackwardsAndAppend @ singleBatchBackwards = capture.ExternalCalls @>

        // Add two more, which should push it over the threshold and hence trigger inclusion of a snapshot event (but not incurr extra roundtrips)
        capture.Clear()
        do! addAndThenRemoveItemsManyTimes context cartId skuId service 1
        test <@ batchBackwardsAndAppend = capture.ExternalCalls @>

        // While we now have 13 events, we should be able to read them with a single call
        capture.Clear()
        let! _ = service.Read cartId
        test <@ singleBatchBackwards = capture.ExternalCalls @>

        // Add 8 more; total of 21 should not trigger snapshotting as Event Number 12 (the 13th one) is a shapshot
        capture.Clear()
        do! addAndThenRemoveItemsManyTimes context cartId skuId service 4
        test <@ batchBackwardsAndAppend = capture.ExternalCalls @>

        // While we now have 21 events, we should be able to read them with a single call
        capture.Clear()
        let! _ = service.Read cartId
        // ... and trigger a second snapshotting (inducing a single additional read + write)
        do! addAndThenRemoveItemsManyTimes context cartId skuId service 1
        // and reload the 24 events with a single read
        let! _ = service.Read cartId
        test <@ singleBatchBackwards @ batchBackwardsAndAppend @ singleBatchBackwards = capture.ExternalCalls @>
    }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_EVENTSTORE")>]
    let ``Can correctly read and update against EventStore, with LatestKnownEvent Access Strategy`` id value = Async.RunSynchronously <| async {
        let log, capture = createLoggerWithCapture ()
        let! conn = connectToLocalStore log
        let service = ContactPreferences.createService log conn

        // Feed some junk into the stream
        for i in 0..11 do
            let quickSurveysValue = i % 2 = 0
            do! service.Update(id, { value with quickSurveys = quickSurveysValue })
        // Ensure there will be something to be changed by the Update below
        do! service.Update(id, { value with quickSurveys = not value.quickSurveys })

        capture.Clear()
        do! service.Update(id, value)

        let! result = service.Read id
        test <@ value = result @>

        test <@ batchBackwardsAndAppend @ singleBatchBackwards = capture.ExternalCalls @>
    }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_EVENTSTORE")>]
    let ``Can roundtrip against EventStore, correctly caching to avoid redundant reads`` context skuId = Async.RunSynchronously <| async {
        let log, capture = createLoggerWithCapture ()
        let! conn = connectToLocalStore log
        let batchSize = 10
        let cache = Equinox.Cache("cart", sizeMb = 50)
        let gateway = createGesGateway conn batchSize
        let createServiceCached () = Cart.createServiceWithCaching log gateway cache
        let service1, service2, service3 = createServiceCached (), createServiceCached (), Cart.createServiceWithoutOptimization log gateway
        let cartId = % Guid.NewGuid()

        // Trigger 10 events, then reload
        do! addAndThenRemoveItemsManyTimesExceptTheLastOne context cartId skuId service1 5
        test <@ batchForwardAndAppend = capture.ExternalCalls @>
        let! resStale = service2.ReadStale cartId
        test <@ batchForwardAndAppend = capture.ExternalCalls @>
        let! resFresh = service2.Read cartId
        // Because we're caching writes, stale vs fresh reads are equivalent
        test <@ resStale = resFresh @>
        // ... should see a write plus a batched forward read as position is cached
        test <@ batchForwardAndAppend @ singleBatchForward = capture.ExternalCalls @>

        // Add two more - the roundtrip should only incur a single read
        capture.Clear()
        let skuId2 = SkuId <| Guid.NewGuid()
        do! addAndThenRemoveItemsManyTimesExceptTheLastOne context cartId skuId2 service1 1
        test <@ batchForwardAndAppend = capture.ExternalCalls @>

        // While we now have 12 events, we should be able to read them with a single call
        capture.Clear()
        // Do a stale read - we will see outs
        let! res = service2.ReadStale cartId
        // result after 10 should be different to result after 12
        test <@ res <> resFresh @>
        // but we don't do a roundtrip to get it
        test <@ [] = capture.ExternalCalls @>
        let! resDefault = service2.Read cartId
        test <@ singleBatchForward = capture.ExternalCalls @>

        // Optimistic transactions
        capture.Clear()
        // As the cache is up to date, we can transact against the cached value and do a null transaction without a roundtrip
        do! addAndThenRemoveItemsOptimisticManyTimesExceptTheLastOne context cartId skuId2 service1 1
        test <@ [] = capture.ExternalCalls @>
        // As the cache is up to date, we can do an optimistic append, saving a Read roundtrip
        let skuId3 = SkuId <| Guid.NewGuid()
        do! addAndThenRemoveItemsOptimisticManyTimesExceptTheLastOne context cartId skuId3 service1 1
        // this time, we did something, so we see the append call
        test <@ [EsAct.Append] = capture.ExternalCalls @>

        // If we don't have a cache attached, we don't benefit from / pay the price for any optimism
        capture.Clear()
        let skuId4 = SkuId <| Guid.NewGuid()
        do! addAndThenRemoveItemsOptimisticManyTimesExceptTheLastOne context cartId skuId4 service3 1
        // Need 2 batches to do the reading
        test <@ [EsAct.SliceForward] @ singleBatchForward @ [EsAct.Append] = capture.ExternalCalls @>
        // we've engineered a clash with the cache state (service3 doest participate in caching)
        // Conflict with cached state leads to a read forward to resync; Then we'll idempotently decide not to do any append
        capture.Clear()
        do! addAndThenRemoveItemsOptimisticManyTimesExceptTheLastOne context cartId skuId4 service2 1
        test <@ [EsAct.AppendConflict; EsAct.SliceForward; EsAct.BatchForward] = capture.ExternalCalls @>
    }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_EVENTSTORE")>]
    let ``Can combine compaction with caching against EventStore`` context skuId = Async.RunSynchronously <| async {
        let log, capture = createLoggerWithCapture ()
        let! conn = connectToLocalStore log
        let batchSize = 10
        let gateway = createGesGateway conn batchSize
        let service1 = Cart.createServiceWithCompaction log gateway
        let cache = Equinox.Cache("cart", sizeMb = 50)
        let gateway = createGesGateway conn batchSize
        let service2 = Cart.createServiceWithCompactionAndCaching log gateway cache

        // Trigger 10 events, then reload
        let cartId = % System.Guid.NewGuid()
        do! addAndThenRemoveItemsManyTimes context cartId skuId service1 5
        let! _ = service2.Read cartId

        // ... should see a single read as we are inside the batch threshold
        test <@ batchBackwardsAndAppend @ singleBatchBackwards = capture.ExternalCalls @>

        // Add two more, which should push it over the threshold and hence trigger inclusion of a snapshot event (but not incur extra roundtrips)
        capture.Clear()
        do! addAndThenRemoveItemsManyTimes context cartId skuId service1 1
        test <@ batchBackwardsAndAppend = capture.ExternalCalls @>

        // While we now have 13 events, we should be able to read them backwards with a single call
        capture.Clear()
        let! _ = service1.Read cartId
        test <@ singleBatchBackwards = capture.ExternalCalls @>

        // Add 8 more; total of 21 should not trigger snapshotting as Event Number 12 (the 13th one) is a snapshot
        capture.Clear()
        do! addAndThenRemoveItemsManyTimes context cartId skuId service1 4
        test <@ batchBackwardsAndAppend = capture.ExternalCalls @>

        // While we now have 21 events, we should be able to read them with a single call
        capture.Clear()
        let! _ = service1.Read cartId
        // ... and trigger a second snapshotting (inducing a single additional read + write)
        do! addAndThenRemoveItemsManyTimes context cartId skuId service1 1
        // and we _could_ reload the 24 events with a single read if reading backwards. However we are using the cache, which last saw it with 10 events, which necessitates two reads
        let! _ = service2.Read cartId
        let suboptimalExtraSlice = [singleSliceForward]
        test <@ singleBatchBackwards @ batchBackwardsAndAppend @ suboptimalExtraSlice @ singleBatchForward = capture.ExternalCalls @>
    }
