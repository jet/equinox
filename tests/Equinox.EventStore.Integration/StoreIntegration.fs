module Equinox.Store.Integration.StoreIntegration

open Domain
open FSharp.UMX
open Serilog
open Swensen.Unquote
open System.Diagnostics
open System.Threading
open System

let defaultBatchSize = 500

#if STORE_POSTGRES
open Equinox.SqlStreamStore
open Equinox.SqlStreamStore.Postgres

let connectToLocalStore (_ : ILogger) =
    Connector("Host=localhost;User Id=postgres;password=password;database=EQUINOX_TEST_DB",autoCreate=true).Establish()

type Context = SqlStreamStoreContext
type Category<'event, 'state, 'context> = SqlStreamStoreCategory<'event, 'state, 'context>
#endif
#if STORE_MSSQL
open Equinox.SqlStreamStore
open Equinox.SqlStreamStore.MsSql

let connectToLocalStore (_ : ILogger) =
    Connector(sprintf "Server=localhost,1433;User=sa;Password=mssql1Ipw;Database=EQUINOX_TEST_DB",autoCreate=true).Establish()

(* WORKAROUND FOR https://github.com/microsoft/mssql-docker/issues/2#issuecomment-1059819719
AFTER `docker compose up`, run:

docker exec -it equinox-mssql /opt/mssql-tools/bin/sqlcmd \
    -S localhost -U sa -P mssql1Ipw \
    -Q "CREATE database EQUINOX_TEST_DB"
*)

type Context = SqlStreamStoreContext
type Category<'event, 'state, 'context> = SqlStreamStoreCategory<'event, 'state, 'context>
#endif
#if STORE_MYSQL
open Equinox.SqlStreamStore
open Equinox.SqlStreamStore.MySql

let connectToLocalStore (_ : ILogger) =
    Connector(sprintf "Server=localhost;User=root;Database=EQUINOX_TEST_DB",autoCreate=true).Establish()

type Context = SqlStreamStoreContext
type Category<'event, 'state, 'context> = SqlStreamStoreCategory<'event, 'state, 'context>
#endif
#if STORE_MESSAGEDB
open Equinox.MessageDb
open OpenTelemetry
open OpenTelemetry.Trace
open OpenTelemetry.Resources

let connectToLocalStore _ = async {
  let connectionString = "Host=localhost; Username=message_store; Password=; Database=message_store; Port=5433; Maximum Pool Size=10"
  let connector = MessageDbConnector(connectionString)
  return connector.Establish()
}
type Context = MessageDbContext
type Category<'event, 'state, 'context> = MessageDbCategory<'event, 'state, 'context>
#endif
#if STORE_EVENTSTOREDB
open Equinox.EventStoreDb

/// Connect directly to a locally running EventStoreDB Node using gRPC, without using Gossip-driven discovery
let connectToLocalStore (_log : ILogger) = async {
    let c = EventStoreConnector(reqTimeout=TimeSpan.FromSeconds 3., reqRetries=3, (*, log=Logger.SerilogVerbose log,*) tags=["I",Guid.NewGuid() |> string])
    let conn = c.Establish("Equinox-integration", Discovery.ConnectionString "esdb://localhost:2111,localhost:2112,localhost:2113?tls=true&tlsVerifyCert=false", ConnectionStrategy.ClusterSingle EventStore.Client.NodePreference.Leader)
    return conn }
#endif
#if STORE_EVENTSTORE_LEGACY
open Equinox.EventStore

// NOTE: use `docker compose up` to establish the standard 3 node config at ports 1113/2113
let connectToLocalStore log =
    // NOTE: disable cert validation for this test suite. ABSOLUTELY DO NOT DO THIS FOR ANY CODE THAT WILL EVER HIT A STAGING OR PROD SERVER
    EventStoreConnector("admin", "changeit", custom = (fun c -> c.DisableServerCertificateValidation()),
    reqTimeout=TimeSpan.FromSeconds 3., reqRetries=3, log=Logger.SerilogVerbose log, tags=["I",Guid.NewGuid() |> string]
#if EVENTSTORE_NO_CLUSTER
    // Connect directly to the locally running EventStore Node without using Gossip-driven discovery
    ).Establish("Equinox-integration", Discovery.Uri(Uri "tcp://localhost:1113"), ConnectionStrategy.ClusterSingle NodePreference.Master)
#else
    // Connect directly to the locally running EventStore Node using Gossip-driven discovery
    ).Establish("Equinox-integration", Discovery.GossipDns "localhost", ConnectionStrategy.ClusterTwinPreferSlaveReads)
#endif
#endif
#if STORE_EVENTSTORE_LEGACY || STORE_EVENTSTOREDB
type Context = EventStoreContext
type Category<'event, 'state, 'context> = EventStoreCategory<'event, 'state, 'context>
#endif

let createContext connection batchSize = Context(connection, batchSize = batchSize)

#if NET
let source = new ActivitySource("StoreIntegration")
#endif

module SimplestThing =
    type Event =
        | StuffHappened
        interface TypeShape.UnionContract.IUnionContract
    let codec = EventCodec.gen<Event>

    let evolve (state: Event) (event: Event) = event
    let fold = Seq.fold evolve
    let initial = StuffHappened
    let resolve log context =
        Category(context, codec, fold, initial)
        |> Equinox.Decider.resolve log
    let [<Literal>] Category = "SimplestThing"

module Cart =
    let fold, initial = Cart.Fold.fold, Cart.Fold.initial
    let codec = Cart.Events.codec
    let snapshot = Cart.Fold.isOrigin, Cart.Fold.snapshot
    let createServiceWithoutOptimization log context =
        Category(context, codec, fold, initial)
        |> Equinox.Decider.resolve log
        |> Cart.create

    #if !STORE_MESSAGEDB
    let createServiceWithCompaction log context =
        Category(context, codec, fold, initial, access = AccessStrategy.RollingSnapshots snapshot)
        |> Equinox.Decider.resolve log
        |> Cart.create
    #endif
    let createServiceWithCaching log context cache =
        let sliding20m = CachingStrategy.SlidingWindow (cache, TimeSpan.FromMinutes 20.)
        Category(context, codec, fold, initial, sliding20m)
        |> Equinox.Decider.resolve log
        |> Cart.create

    #if !STORE_MESSAGEDB
    let createServiceWithCompactionAndCaching log context cache =
        let sliding20m = CachingStrategy.SlidingWindow (cache, TimeSpan.FromMinutes 20.)
        Category(context, codec, fold, initial, sliding20m, AccessStrategy.RollingSnapshots snapshot)
        |> Equinox.Decider.resolve log
        |> Cart.create
    #endif

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

type Tests(testOutputHelper) =
    #if STORE_MESSAGEDB
    let sdk =
        Sdk.CreateTracerProviderBuilder()
           .SetResourceBuilder(ResourceBuilder.CreateDefault().AddService(serviceName = "mdbi"))
           .AddSource("Equinox.MessageDb")
           .AddSource("StoreIntegration")
           .AddSource("Npqsl")
           .AddOtlpExporter(fun opts -> opts.Endpoint <- Uri("http://localhost:4317"))
           .Build()
    #endif

    let output = TestContext(testOutputHelper)

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

#if STORE_EVENTSTOREDB // gRPC does not expose slice metrics
    let sliceForward = []
#else
    let sliceForward = [EsAct.SliceForward]
#endif
    let singleBatchForward = sliceForward @ [EsAct.BatchForward]
    let batchForwardAndAppend = singleBatchForward @ [EsAct.Append]

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_EVENTSTORE")>]
    let ``Can roundtrip against Store, correctly batching the reads [without any optimizations]`` (ctx, skuId) = Async.RunSynchronously <| async {
        #if NET
        use _ = source.StartActivity("Can roundtrip against Store, correctly batching the reads [without any optimizations]")
        #endif
        let log, capture = output.CreateLoggerWithCapture()
        let! connection = connectToLocalStore log

        let batchSize = 3
        let context = createContext connection batchSize
        let service = Cart.createServiceWithoutOptimization log context

        // The command processing should trigger only a single read and a single write call
        let addRemoveCount = 6
        let cartId = % Guid.NewGuid()

        do! addAndThenRemoveItemsManyTimesExceptTheLastOne ctx cartId skuId service addRemoveCount
        test <@ batchForwardAndAppend = capture.ExternalCalls @>

        // Restart the counting
        capture.Clear()

        // Validate basic operation; Key side effect: Log entries will be emitted to `capture`
        let! state = service.Read cartId
        let expectedEventCount = 2 * addRemoveCount - 1
        test <@ addRemoveCount = match state with { items = [{ quantity = quantity }] } -> quantity | _ -> failwith "nope" @>

        // Need to read 4 batches to read 11 events in batches of 3
        let expectedBatches = ceil(float expectedEventCount/float batchSize) |> int
        test <@ (List.replicate (expectedBatches-1) sliceForward |> List.concat) @ singleBatchForward = capture.ExternalCalls @>
    }

    [<AutoData(MaxTest = 2, SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_EVENTSTORE")>]
    let ``Can roundtrip against Store, managing sync conflicts by retrying [without any optimizations]`` (ctx, initialState) = Async.RunSynchronously <| async {
        #if NET
        use _ = source.StartActivity("Can roundtrip against Store, managing sync conflicts by retrying [without any optimizations]")
        #endif
        let log1, capture1 = output.CreateLoggerWithCapture()

        let! connection = connectToLocalStore log1
        // Ensure batching is included at some point in the proceedings
        let batchSize = 3

        let ctx, (sku11, sku12, sku21, sku22) = ctx
        let cartId = % Guid.NewGuid()

        // establish base stream state
        let context = createContext connection batchSize
        let service1 = Cart.createServiceWithoutOptimization log1 context
        let! maybeInitialSku =
            let streamEmpty, skuId = initialState
            async {
                if streamEmpty then return None
                else
                    let addRemoveCount = 2
                    do! addAndThenRemoveItemsManyTimesExceptTheLastOne ctx cartId skuId service1 addRemoveCount
                    return Some (skuId, addRemoveCount) }

        let act prepare (service : Cart.Service) log skuId count =
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
            do! act prepare service1 log1 sku11 11
            // Wait for other side to load; generate conflict
            let prepare = async { do! w3 }
            do! act prepare service1 log1 sku12 12
            // Signal conflict generated
            do! s4 }
        let log2, capture2 = output.CreateLoggerWithCapture()
        let context = createContext connection batchSize
        let service2 = Cart.createServiceWithoutOptimization log2 context
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

#if STORE_MESSAGEDB // MessageDB doesn't report Batches for "Read Last Event" scenarios
    let singleBatchBackwards = [EsAct.ReadLast]
    let batchBackwardsAndAppend = [EsAct.ReadLast; EsAct.Append]
#else
#if STORE_EVENTSTOREDB // gRPC does not expose slice metrics
    let sliceBackward = []
#else
    let sliceBackward = [EsAct.SliceBackward]
#endif
    let singleBatchBackwards = sliceBackward @ [EsAct.BatchBackward]
    let batchBackwardsAndAppend = singleBatchBackwards @ [EsAct.Append]
#endif

#if !STORE_MESSAGEDB
    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_EVENTSTORE")>]
    let ``Can roundtrip against Store, correctly compacting to avoid redundant reads`` (ctx, skuId) = Async.RunSynchronously <| async {
        #if NET
        use _ = source.StartActivity("Can roundtrip against Store, correctly compacting to avoid redundant reads")
        #endif
        let log, capture = output.CreateLoggerWithCapture()
        let! client = connectToLocalStore log
        let batchSize = 10
        let context = createContext client batchSize
        let service = Cart.createServiceWithCompaction log context

        // Trigger 10 events, then reload
        let cartId = % Guid.NewGuid()
        do! addAndThenRemoveItemsManyTimes ctx cartId skuId service 5
        let! _ = service.Read cartId

        // ... should see a single read as we are inside the batch threshold
        test <@ batchBackwardsAndAppend @ singleBatchBackwards = capture.ExternalCalls @>

        // Add two more, which should push it over the threshold and hence trigger inclusion of a snapshot event (but not incurr extra roundtrips)
        capture.Clear()
        do! addAndThenRemoveItemsManyTimes ctx cartId skuId service 1
        test <@ batchBackwardsAndAppend = capture.ExternalCalls @>

        // While we now have 13 events, we should be able to read them with a single call
        capture.Clear()
        let! _ = service.Read cartId
        test <@ singleBatchBackwards = capture.ExternalCalls @>

        // Add 8 more; total of 21 should not trigger snapshotting as Event Number 12 (the 13th one) is a shapshot
        capture.Clear()
        do! addAndThenRemoveItemsManyTimes ctx cartId skuId service 4
        test <@ batchBackwardsAndAppend = capture.ExternalCalls @>

        // While we now have 21 events, we should be able to read them with a single call
        capture.Clear()
        let! _ = service.Read cartId
        // ... and trigger a second snapshotting (inducing a single additional read + write)
        do! addAndThenRemoveItemsManyTimes ctx cartId skuId service 1
        // and reload the 24 events with a single read
        let! _ = service.Read cartId
        test <@ singleBatchBackwards @ batchBackwardsAndAppend @ singleBatchBackwards = capture.ExternalCalls @>
    }
#endif

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_EVENTSTORE")>]
    let ``Can correctly read and update against Store, with LatestKnownEvent Access Strategy`` id value = Async.RunSynchronously <| async {
        #if NET
        use _ = source.StartActivity("Can correctly read and update against Store, with LatestKnownEvent Access Strategy")
        #endif
        let log, capture = output.CreateLoggerWithCapture()
        let! client = connectToLocalStore log
        let service = ContactPreferences.createService log client

        // Feed some junk into the stream
        for i in 0..11 do
            let quickSurveysValue = i % 2 = 0
            do! service.Update(id, { value with quickSurveys = quickSurveysValue })
        // Ensure there will be something to be changed by the Update below
        do! service.Update(id, { value with quickSurveys = not value.quickSurveys })

        capture.Clear()
        do! service.Update(id, value)

        let! result = service.Read id
        test <@ batchBackwardsAndAppend @ singleBatchBackwards = capture.ExternalCalls @>
        test <@ value = result @>
    }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_EVENTSTORE")>]
    let ``Can roundtrip against Store, correctly caching to avoid redundant reads`` (ctx, skuId) = Async.RunSynchronously <| async {
        #if NET
        use _ = source.StartActivity("Can roundtrip against Store, correctly caching to avoid redundant reads")
        #endif
        let log, capture = output.CreateLoggerWithCapture()
        let! client = connectToLocalStore log
        let batchSize = 10
        let cache = Equinox.Cache("cart", sizeMb = 50)
        let context = createContext client batchSize
        let createServiceCached () = Cart.createServiceWithCaching log context cache
        let service1, service2, service3 = createServiceCached (), createServiceCached (), Cart.createServiceWithoutOptimization log context
        let cartId = % Guid.NewGuid()

        // Trigger 10 events, then reload
        do! addAndThenRemoveItemsManyTimesExceptTheLastOne ctx cartId skuId service1 5
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
        do! addAndThenRemoveItemsManyTimesExceptTheLastOne ctx cartId skuId2 service1 1
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
        do! addAndThenRemoveItemsOptimisticManyTimesExceptTheLastOne ctx cartId skuId2 service1 1
        test <@ [] = capture.ExternalCalls @>
        // As the cache is up to date, we can do an optimistic append, saving a Read roundtrip
        let skuId3 = SkuId <| Guid.NewGuid()
        do! addAndThenRemoveItemsOptimisticManyTimesExceptTheLastOne ctx cartId skuId3 service1 1
        // this time, we did something, so we see the append call
        test <@ [EsAct.Append] = capture.ExternalCalls @>

        // If we don't have a cache attached, we don't benefit from / pay the price for any optimism
        capture.Clear()
        let skuId4 = SkuId <| Guid.NewGuid()
        do! addAndThenRemoveItemsOptimisticManyTimesExceptTheLastOne ctx cartId skuId4 service3 1
        // Need 2 batches to do the reading
        test <@ sliceForward @ singleBatchForward @ [EsAct.Append] = capture.ExternalCalls @>
        // we've engineered a clash with the cache state (service3 doest participate in caching)
        // Conflict with cached state leads to a read forward to resync; Then we'll idempotently decide not to do any append
        capture.Clear()
        do! addAndThenRemoveItemsOptimisticManyTimesExceptTheLastOne ctx cartId skuId4 service2 1
        test <@ [EsAct.AppendConflict; yield! sliceForward; EsAct.BatchForward] = capture.ExternalCalls @>
    }

#if !STORE_MESSAGEDB
    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_EVENTSTORE")>]
    let ``Can combine compaction with caching against Store`` (ctx, skuId) = Async.RunSynchronously <| async {
        let log, capture = output.CreateLoggerWithCapture()
        let! client = connectToLocalStore log
        let batchSize = 10
        let context = createContext client batchSize
        let service1 = Cart.createServiceWithCompaction log context
        let cache = Equinox.Cache("cart", sizeMb = 50)
        let context = createContext client batchSize
        let service2 = Cart.createServiceWithCompactionAndCaching log context cache

        // Trigger 10 events, then reload
        let cartId = % Guid.NewGuid()
        do! addAndThenRemoveItemsManyTimes ctx cartId skuId service1 5
        let! _ = service2.Read cartId

        // ... should see a single read as we are inside the batch threshold
        test <@ batchBackwardsAndAppend @ singleBatchBackwards = capture.ExternalCalls @>

        // Add two more, which should push it over the threshold and hence trigger inclusion of a snapshot event (but not incur extra roundtrips)
        capture.Clear()
        do! addAndThenRemoveItemsManyTimes ctx cartId skuId service1 1
        test <@ batchBackwardsAndAppend = capture.ExternalCalls @>

        // While we now have 13 events, we should be able to read them backwards with a single call
        capture.Clear()
        let! _ = service1.Read cartId
        test <@ singleBatchBackwards = capture.ExternalCalls @>

        // Add 8 more; total of 21 should not trigger snapshotting as Event Number 12 (the 13th one) is a snapshot
        capture.Clear()
        do! addAndThenRemoveItemsManyTimes ctx cartId skuId service1 4
        test <@ batchBackwardsAndAppend = capture.ExternalCalls @>

        // While we now have 21 events, we should be able to read them with a single call
        capture.Clear()
        let! _ = service1.Read cartId
        // ... and trigger a second snapshotting (inducing a single additional read + write)
        do! addAndThenRemoveItemsManyTimes ctx cartId skuId service1 1
        // and we _could_ reload the 24 events with a single read if reading backwards. However we are using the cache, which last saw it with 10 events, which necessitates two reads
        let! _ = service2.Read cartId
        let suboptimalExtraSlice : EsAct list = sliceForward
        test <@ singleBatchBackwards @ batchBackwardsAndAppend @ suboptimalExtraSlice @ singleBatchForward = capture.ExternalCalls @>
    }
#endif
    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_EVENTSTORE")>]
    let ``Version is 0-based`` () = Async.RunSynchronously <| async {
        #if NET
        use _ = source.StartActivity("Version is 0-based")
        #endif
        let log, _ = output.CreateLoggerWithCapture()
        let! connection = connectToLocalStore log

        let batchSize = 3
        let context = createContext connection batchSize
        let id = Guid.NewGuid()
        let toStreamId (x : Guid) = x.ToString "N"
        let decider = SimplestThing.resolve log context (Equinox.Target.gen SimplestThing.Category toStreamId id)

        let! before, after = decider.TransactEx(
            (fun state -> state.Version, [SimplestThing.StuffHappened]),
            mapResult = (fun result ctx-> result, ctx.Version))
        test <@ [before; after] = [0L; 1L] @>
    }

#if STORE_MESSAGEDB
    interface IDisposable with
      member _.Dispose() = sdk.Shutdown() |> ignore
#endif
