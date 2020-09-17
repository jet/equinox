module Equinox.CosmosStore.Integration.CosmosIntegration

open Domain
open Equinox.CosmosStore
open Equinox.CosmosStore.Integration.Infrastructure
open FSharp.UMX
open Swensen.Unquote
open System
open System.Threading

module Cart =
    let fold, initial = Domain.Cart.Fold.fold, Domain.Cart.Fold.initial
    let snapshot = Domain.Cart.Fold.isOrigin, Domain.Cart.Fold.snapshot
    let codec = Domain.Cart.Events.codecStj IntegrationJsonSerializer.options
    let createServiceWithoutOptimization store log =
        let resolve (id,opt) = CosmosStoreCategory(store, codec, fold, initial, CachingStrategy.NoCaching, AccessStrategy.Unoptimized).Resolve(id,?option=opt)
        Backend.Cart.create log resolve
    let projection = "Compacted",snd snapshot
    /// Trigger looking in Tip (we want those calls to occur, but without leaning on snapshots, which would reduce the paths covered)
    let createServiceWithEmptyUnfolds store log =
        let unfArgs = Domain.Cart.Fold.isOrigin, fun _ -> Seq.empty
        let resolve (id,opt) = CosmosStoreCategory(store, codec, fold, initial, CachingStrategy.NoCaching, AccessStrategy.MultiSnapshot unfArgs).Resolve(id,?option=opt)
        Backend.Cart.create log resolve
    let createServiceWithSnapshotStrategy store log =
        let resolve (id,opt) = CosmosStoreCategory(store, codec, fold, initial, CachingStrategy.NoCaching, AccessStrategy.Snapshot snapshot).Resolve(id,?option=opt)
        Backend.Cart.create log resolve
    let createServiceWithSnapshotStrategyAndCaching store log cache =
        let sliding20m = CachingStrategy.SlidingWindow (cache, TimeSpan.FromMinutes 20.)
        let resolve (id,opt) = CosmosStoreCategory(store, codec, fold, initial, sliding20m, AccessStrategy.Snapshot snapshot).Resolve(id,?option=opt)
        Backend.Cart.create log resolve
    let createServiceWithRollingState store log =
        let access = AccessStrategy.RollingState Domain.Cart.Fold.snapshot
        let resolve (id,opt) = CosmosStoreCategory(store, codec, fold, initial, CachingStrategy.NoCaching, access).Resolve(id,?option=opt)
        Backend.Cart.create log resolve

module ContactPreferences =
    let fold, initial = Domain.ContactPreferences.Fold.fold, Domain.ContactPreferences.Fold.initial
    let codec = Domain.ContactPreferences.Events.codecStj IntegrationJsonSerializer.options
    let createServiceWithoutOptimization createContext defaultBatchSize log _ignoreWindowSize _ignoreCompactionPredicate =
        let context = createContext defaultBatchSize
        let resolve = CosmosStoreCategory(context, codec, fold, initial, CachingStrategy.NoCaching, AccessStrategy.Unoptimized).Resolve
        Backend.ContactPreferences.create log resolve
    let createService log store =
        let resolve = CosmosStoreCategory(store, codec, fold, initial, CachingStrategy.NoCaching, AccessStrategy.LatestKnownEvent).Resolve
        Backend.ContactPreferences.create log resolve
    let createServiceWithLatestKnownEvent store log cachingStrategy =
        let resolve = CosmosStoreCategory(store, codec, fold, initial, cachingStrategy, AccessStrategy.LatestKnownEvent).Resolve
        Backend.ContactPreferences.create log resolve

#nowarn "1182" // From hereon in, we may have some 'unused' privates (the tests)

type Tests(testOutputHelper) =
    inherit TestsWithLogCapture(testOutputHelper)
    let log,capture = base.Log, base.Capture

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

    let verifyRequestChargesMax rus =
        let tripRequestCharges = [ for e, c in capture.RequestCharges -> sprintf "%A" e, c ]
        test <@ float rus >= Seq.sum (Seq.map snd tripRequestCharges) @>

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``Can roundtrip against Cosmos, correctly batching the reads [without reading the Tip]`` context skuId = Async.RunSynchronously <| async {
        let maxItemsPerRequest = 2
        let store = createPrimaryContext log maxItemsPerRequest

        let service = Cart.createServiceWithoutOptimization store log
        capture.Clear() // for re-runs of the test

        let cartId = % Guid.NewGuid()
        // The command processing should trigger only a single read and a single write call
        let addRemoveCount = 40
        let eventsPerAction = addRemoveCount * 2 - 1
        let transactions = 6
        for i in [1..transactions] do
            do! addAndThenRemoveItemsManyTimesExceptTheLastOne context cartId skuId service addRemoveCount
            // Extra roundtrip required after maxItemsPerRequest is exceeded
            let expectedBatchesOfItems = max 1 ((i-1) / maxItemsPerRequest)
            test <@ i = i && List.replicate expectedBatchesOfItems EqxAct.ResponseBackward @ [EqxAct.QueryBackward; EqxAct.Append] = capture.ExternalCalls @>
            verifyRequestChargesMax 61 // 57.09 [5.24 + 54.78] // 5.5 observed for read
            capture.Clear()

        // Validate basic operation; Key side effect: Log entries will be emitted to `capture`
        let! state = service.Read cartId
        let expectedEventCount = transactions * eventsPerAction
        test <@ addRemoveCount = match state with { items = [{ quantity = quantity }] } -> quantity | _ -> failwith "nope" @>

        let expectedResponses = transactions/maxItemsPerRequest + 1
        test <@ List.replicate expectedResponses EqxAct.ResponseBackward @ [EqxAct.QueryBackward] = capture.ExternalCalls @>
        verifyRequestChargesMax 11 // 10.01
    }

    [<AutoData(MaxTest = 2, SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``Can roundtrip against Cosmos, managing sync conflicts by retrying`` ctx initialState = Async.RunSynchronously <| async {
        let log1, capture1 = log, capture
        capture1.Clear()
        let batchSize = 3
        let store = createPrimaryContext log1 batchSize
        // Ensure batching is included at some point in the proceedings

        let context, (sku11, sku12, sku21, sku22) = ctx
        let cartId = % Guid.NewGuid()

        // establish base stream state
        let service1 = Cart.createServiceWithEmptyUnfolds store log1
        let! maybeInitialSku =
            let (streamEmpty, skuId) = initialState
            async {
                if streamEmpty then return None
                else
                    let addRemoveCount = 2
                    do! addAndThenRemoveItemsManyTimesExceptTheLastOne context cartId skuId service1 addRemoveCount
                    return Some (skuId, addRemoveCount) }

        let act prepare (service : Backend.Cart.Service) skuId count =
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
            do! act prepare service1 sku11 11
            // Wait for other side to load; generate conflict
            let prepare = async { do! w3 }
            do! act prepare service1 sku12 12
            // Signal conflict generated
            do! s4 }
        let log2, capture2 = TestsWithLogCapture.CreateLoggerWithCapture testOutputHelper
        use _flush = log2
        let service2 = Cart.createServiceWithEmptyUnfolds store log2
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
        // Act: Engineer the conflicts and applications, with logging into capture1 and capture2
        do! Async.Parallel [t1; t2] |> Async.Ignore

        // Load state
        let! result = service1.Read cartId

        // Ensure correct values got persisted
        let has sku qty = result.items |> List.exists (fun { skuId = s; quantity = q } -> (sku, qty) = (s, q))
        test <@ maybeInitialSku |> Option.forall (fun (skuId, quantity) -> has skuId quantity)
                && has sku11 11 && has sku12 12
                && has sku21 21 && has sku22 22 @>
       // Intended conflicts arose
        let conflict = function EqxAct.Conflict | EqxAct.Resync as x -> Some x | _ -> None
#if EVENTS_IN_TIP
        test <@ let c2 = List.choose conflict capture2.ExternalCalls
                [EqxAct.Resync] = List.choose conflict capture1.ExternalCalls
                && [EqxAct.Resync] = c2 @>
#else
        test <@ let c2 = List.choose conflict capture2.ExternalCalls
                [EqxAct.Conflict] = List.choose conflict capture1.ExternalCalls
                && [EqxAct.Conflict] = c2 @>
#endif
    }

    let singleBatchBackwards = [EqxAct.ResponseBackward; EqxAct.QueryBackward]
    let batchBackwardsAndAppend = singleBatchBackwards @ [EqxAct.Append]

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``Can correctly read and update against Cosmos with LatestKnownEvent Access Strategy`` value = Async.RunSynchronously <| async {
        let store = createPrimaryContext log 1
        let service = ContactPreferences.createService log store

        let id = ContactPreferences.Id (let g = System.Guid.NewGuid() in g.ToString "N")
        //let (Domain.ContactPreferences.Id email) = id ()
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

        test <@ [EqxAct.Tip; EqxAct.Append; EqxAct.Tip] = capture.ExternalCalls @>

        (* Verify pruning does not affect the copies of the events maintained as Unfolds *)

        //let ctx = createPrimaryEventsContext log None
        //do! Async.Sleep 1000
        // Needs to share the same client for the session key to be threaded through
        // If we run on an independent context, we won't see (and hence prune) the full set of events
        let ctx = Core.EventsContext(store, log)
        let streamName = ContactPreferences.streamName id |> FsCodec.StreamName.toString

        // Prune all the events
        let! deleted, deferred, trimmedPos = Core.Events.prune ctx streamName 14L
        test <@ deleted = 14 && deferred = 0 && trimmedPos = 14L @>

        // Prove they're gone
        capture.Clear()
        let! res = Core.Events.get ctx streamName 0L Int32.MaxValue
        test <@ [EqxAct.ResponseForward; EqxAct.QueryForward] = capture.ExternalCalls @>
        test <@ [||] = res @>
        verifyRequestChargesMax 3 // 2.99

        // But we can still read (there's no cache so we'll definitely be reading)
        capture.Clear()
        let! _ = service.Read id
        test <@ value = result @>
        test <@ [EqxAct.Tip] = capture.ExternalCalls @>
        verifyRequestChargesMax 1
    }

     [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``Can correctly read and update Contacts against Cosmos with RollingUnfolds Access Strategy`` value = Async.RunSynchronously <| async {
        let store = createPrimaryContext log 1
        let service = ContactPreferences.createServiceWithLatestKnownEvent store log CachingStrategy.NoCaching

        let id = ContactPreferences.Id (let g = System.Guid.NewGuid() in g.ToString "N")
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

        test <@ [EqxAct.Tip; EqxAct.Append; EqxAct.Tip] = capture.ExternalCalls @>
    }

    [<AutoData(MaxTest = 2, SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``Can roundtrip Cart against Cosmos with RollingUnfolds, detecting conflicts based on _etag`` ctx initialState = Async.RunSynchronously <| async {
        let log1, capture1 = log, capture
        capture1.Clear()
        let store = createPrimaryContext log1 1

        let context, (sku11, sku12, sku21, sku22) = ctx
        let cartId = % Guid.NewGuid()

        // establish base stream state
        let service1 = Cart.createServiceWithRollingState store log1
        let! maybeInitialSku =
            let (streamEmpty, skuId) = initialState
            async {
                if streamEmpty then return None
                else
                    let addRemoveCount = 2
                    do! addAndThenRemoveItemsManyTimesExceptTheLastOne context cartId skuId service1 addRemoveCount
                    return Some (skuId, addRemoveCount) }

        let act prepare (service : Backend.Cart.Service) skuId count =
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
            do! act prepare service1 sku11 11
            // Wait for other side to load; generate conflict
            let prepare = async { do! w3 }
            do! act prepare service1 sku12 12
            // Signal conflict generated
            do! s4 }
        let log2, capture2 = TestsWithLogCapture.CreateLoggerWithCapture testOutputHelper
        use _flush = log2
        let service2 = Cart.createServiceWithRollingState store log2
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
        // Act: Engineer the conflicts and applications, with logging into capture1 and capture2
        do! Async.Parallel [t1; t2] |> Async.Ignore

        // Load state
        let! result = service1.Read cartId

        // Ensure correct values got persisted
        let has sku qty = result.items |> List.exists (fun { skuId = s; quantity = q } -> (sku, qty) = (s, q))
        test <@ maybeInitialSku |> Option.forall (fun (skuId, quantity) -> has skuId quantity)
                && has sku11 11 && has sku12 12
                && has sku21 21 && has sku22 22 @>
        // Intended conflicts arose
        let conflict = function EqxAct.Conflict | EqxAct.Resync as x -> Some x | _ -> None
        test <@ let c2 = List.choose conflict capture2.ExternalCalls
                [EqxAct.Resync] = List.choose conflict capture1.ExternalCalls
                && [EqxAct.Resync] = c2 @>
    }

     [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``Can roundtrip against Cosmos, using Snapshotting to avoid queries`` context skuId = Async.RunSynchronously <| async {
        let batchSize = 10
        let store = createPrimaryContext log batchSize
        let createServiceIndexed () = Cart.createServiceWithSnapshotStrategy store log
        let service1, service2 = createServiceIndexed (), createServiceIndexed ()
        capture.Clear()

        // Trigger 10 events, then reload
        let cartId = % Guid.NewGuid()
        do! addAndThenRemoveItemsManyTimes context cartId skuId service1 5
        let! _ = service2.Read cartId

        // ... should see a single read as we are writes are cached
        test <@ [EqxAct.TipNotFound; EqxAct.Append; EqxAct.Tip] = capture.ExternalCalls @>

        // Add two more - the roundtrip should only incur a single read
        capture.Clear()
        do! addAndThenRemoveItemsManyTimes context cartId skuId service1 1
        test <@ [EqxAct.Tip; EqxAct.Append] = capture.ExternalCalls @>

        // While we now have 12 events, we should be able to read them with a single call
        capture.Clear()
        let! _ = service2.Read cartId
        test <@ [EqxAct.Tip] = capture.ExternalCalls @>

        (* Verify pruning does not affect snapshots, though Tip is re-read in this scenario due to lack of caching *)

        let ctx = Core.EventsContext(store, log)
        let streamName = Cart.streamName cartId |> FsCodec.StreamName.toString
        // Prune all the events
        let! deleted, deferred, trimmedPos = Core.Events.prune ctx streamName 12L
        test <@ deleted = 12 && deferred = 0 && trimmedPos = 12L @>

        // Prove they're gone
        capture.Clear()
        let! res = Core.Events.get ctx streamName 0L Int32.MaxValue
        test <@ [EqxAct.ResponseForward; EqxAct.QueryForward] = capture.ExternalCalls @>
        test <@ [||] = res @>
        verifyRequestChargesMax 3 // 2.99

        // But we can still read (there's no cache so we'll definitely be reading)
        capture.Clear()
        let! _ = service2.Read cartId
        test <@ [EqxAct.Tip] = capture.ExternalCalls @>
        verifyRequestChargesMax 1
    }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``Can roundtrip against Cosmos, correctly using Snapshotting and Cache to avoid redundant reads`` context skuId = Async.RunSynchronously <| async {
        let batchSize = 10
        let store = createPrimaryContext log batchSize
        let cache = Equinox.Cache("cart", sizeMb = 50)
        let createServiceCached () = Cart.createServiceWithSnapshotStrategyAndCaching store log cache
        let service1, service2 = createServiceCached (), createServiceCached ()
        capture.Clear()

        // Trigger 10 events, then reload
        let cartId = % Guid.NewGuid()
        do! addAndThenRemoveItemsManyTimes context cartId skuId service1 5
        let! _ = service2.Read cartId

        // ... should see a single Cached Indexed read given writes are cached and writer emits etag
        test <@ [EqxAct.TipNotFound; EqxAct.Append; EqxAct.TipNotModified] = capture.ExternalCalls @>

        // Add two more - the roundtrip should only incur a single read, which should be cached by virtue of being a second one in succession
        capture.Clear()
        do! addAndThenRemoveItemsManyTimes context cartId skuId service1 1
        test <@ [EqxAct.TipNotModified; EqxAct.Append] = capture.ExternalCalls @>

        // While we now have 12 events, we should be able to read them with a single call
        capture.Clear()
        let! _ = service2.ReadStale cartId
        // A Stale read doesn't roundtrip
        test <@ [] = capture.ExternalCalls @>
        let! _ = service2.Read cartId
        let! _ = service2.Read cartId
        // First is cached because writer emits etag, second remains cached
        test <@ [EqxAct.TipNotModified; EqxAct.TipNotModified] = capture.ExternalCalls @>

        // Optimistic write mode saves the TipNotModified
        capture.Clear()
        do! addAndThenRemoveItemsOptimisticManyTimesExceptTheLastOne context cartId skuId service1 1
        test <@ [EqxAct.Append] = capture.ExternalCalls @>

        (* Verify pruning does not affect snapshots, and does not touch the Tip *)

        let ctx = Core.EventsContext(store, log)
        let streamName = Cart.streamName cartId |> FsCodec.StreamName.toString
        // Prune all the events
        let! deleted, deferred, trimmedPos = Core.Events.prune ctx streamName 13L
        test <@ deleted = 13 && deferred = 0 && trimmedPos = 13L @>

        // Prove they're gone
        capture.Clear()
        let! res = Core.Events.get ctx streamName 0L Int32.MaxValue
        test <@ [EqxAct.ResponseForward; EqxAct.QueryForward] = capture.ExternalCalls @>
        test <@ [||] = res @>
        verifyRequestChargesMax 3 // 2.99

        // But we can still read (service2 shares the cache so is aware of the last writes, and pruning does not invalidate the Tip)
        capture.Clear()
        let! _ = service2.Read cartId
        test <@ [EqxAct.TipNotModified] = capture.ExternalCalls @>
        verifyRequestChargesMax 1
    }
