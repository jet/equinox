module Equinox.Cosmos.Integration.CosmosIntegration

open Domain
open Equinox.Cosmos
open Equinox.Cosmos.Integration.Infrastructure
open FSharp.UMX
open Swensen.Unquote
open System
open System.Threading

module Cart =
    let fold, initial = Domain.Cart.Fold.fold, Domain.Cart.Fold.initial
    let snapshot = Domain.Cart.Fold.isOrigin, Domain.Cart.Fold.snapshot
    let codec = Domain.Cart.Events.codec
    let createServiceWithoutOptimization connection batchSize log =
        let store = createCosmosContext connection batchSize
        let resolve (id,opt) = Resolver(store, codec, fold, initial, CachingStrategy.NoCaching, AccessStrategy.Unoptimized).Resolve(id,?option=opt)
        Backend.Cart.Service(log, resolve)
    let projection = "Compacted",snd snapshot
    /// Trigger looking in Tip (we want those calls to occur, but without leaning on snapshots, which would reduce the paths covered)
    let createServiceWithEmptyUnfolds connection batchSize log =
        let store = createCosmosContext connection batchSize
        let unfArgs = Domain.Cart.Fold.isOrigin, fun _ -> Seq.empty
        let resolve (id,opt) = Resolver(store, codec, fold, initial, CachingStrategy.NoCaching, AccessStrategy.MultiSnapshot unfArgs).Resolve(id,?option=opt)
        Backend.Cart.Service(log, resolve)
    let createServiceWithSnapshotStrategy connection batchSize log =
        let store = createCosmosContext connection batchSize
        let resolve (id,opt) = Resolver(store, codec, fold, initial, CachingStrategy.NoCaching, AccessStrategy.Snapshot snapshot).Resolve(id,?option=opt)
        Backend.Cart.Service(log, resolve)
    let createServiceWithSnapshotStrategyAndCaching connection batchSize log cache =
        let store = createCosmosContext connection batchSize
        let sliding20m = CachingStrategy.SlidingWindow (cache, TimeSpan.FromMinutes 20.)
        let resolve (id,opt) = Resolver(store, codec, fold, initial, sliding20m, AccessStrategy.Snapshot snapshot).Resolve(id,?option=opt)
        Backend.Cart.Service(log, resolve)
    let createServiceWithRollingState connection log =
        let store = createCosmosContext connection 1
        let access = AccessStrategy.RollingState Domain.Cart.Fold.snapshot
        let resolve (id,opt) = Resolver(store, codec, fold, initial, CachingStrategy.NoCaching, access).Resolve(id,?option=opt)
        Backend.Cart.Service(log, resolve)

module ContactPreferences =
    let fold, initial = Domain.ContactPreferences.Fold.fold, Domain.ContactPreferences.Fold.initial
    let codec = Domain.ContactPreferences.Events.codec
    let createServiceWithoutOptimization createGateway defaultBatchSize log _ignoreWindowSize _ignoreCompactionPredicate =
        let gateway = createGateway defaultBatchSize
        let resolve = Resolver(gateway, codec, fold, initial, CachingStrategy.NoCaching, AccessStrategy.Unoptimized).Resolve
        Backend.ContactPreferences.Service(log, resolve)
    let createService log createGateway =
        let resolve = Resolver(createGateway 1, codec, fold, initial, CachingStrategy.NoCaching, AccessStrategy.LatestKnownEvent).Resolve
        Backend.ContactPreferences.Service(log, resolve)
    let createServiceWithLatestKnownEvent createGateway log cachingStrategy =
        let resolve = Resolver(createGateway 1, codec, fold, initial, cachingStrategy, AccessStrategy.LatestKnownEvent).Resolve
        Backend.ContactPreferences.Service(log, resolve)

#nowarn "1182" // From hereon in, we may have some 'unused' privates (the tests)

type Tests(testOutputHelper) =
    inherit TestsWithLogCapture(testOutputHelper)
    let log,capture = base.Log, base.Capture

    let addAndThenRemoveItems optimistic exceptTheLastOne context cartId skuId (service: Backend.Cart.Service) count =
        service.FlowAsync(cartId, optimistic, fun _ctx execute ->
            for i in 1..count do
                execute <| Domain.Cart.AddItem (context, skuId, i)
                if not exceptTheLastOne || i <> count then
                    execute <| Domain.Cart.RemoveItem (context, skuId) )
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
        let! conn = connectToSpecifiedCosmosOrSimulator log

        let maxItemsPerRequest = 2
        let service = Cart.createServiceWithoutOptimization conn maxItemsPerRequest log
        capture.Clear() // for re-runs of the test

        let cartId = % Guid.NewGuid()
        // The command processing should trigger only a single read and a single write call
        let addRemoveCount = 2
        let eventsPerAction = addRemoveCount * 2 - 1
        let transactions = 6
        for i in [1..transactions] do
            do! addAndThenRemoveItemsManyTimesExceptTheLastOne context cartId skuId service addRemoveCount
            // Extra roundtrip required after maxItemsPerRequest is exceeded
            let expectedBatchesOf2Items = (i-1) / maxItemsPerRequest + 1
            test <@ i = i && List.replicate expectedBatchesOf2Items EqxAct.ResponseBackward @ [EqxAct.QueryBackward; EqxAct.Append] = capture.ExternalCalls @>
            verifyRequestChargesMax 48 // 47.29
            capture.Clear()

        // Validate basic operation; Key side effect: Log entries will be emitted to `capture`
        let! state = service.Read cartId
        let expectedEventCount = transactions * eventsPerAction
        test <@ addRemoveCount = match state with { items = [{ quantity = quantity }] } -> quantity | _ -> failwith "nope" @>

        let expectedResponses = transactions/maxItemsPerRequest + 1
        test <@ List.replicate expectedResponses EqxAct.ResponseBackward @ [EqxAct.QueryBackward] = capture.ExternalCalls @>
        verifyRequestChargesMax 12 // 11.8
    }

    [<AutoData(MaxTest = 2, SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``Can roundtrip against Cosmos, managing sync conflicts by retrying`` ctx initialState = Async.RunSynchronously <| async {
        let log1, capture1 = log, capture
        capture1.Clear()
        let! conn = connectToSpecifiedCosmosOrSimulator log1
        // Ensure batching is included at some point in the proceedings
        let batchSize = 3

        let context, (sku11, sku12, sku21, sku22) = ctx
        let cartId = % Guid.NewGuid()

        // establish base stream state
        let service1 = Cart.createServiceWithEmptyUnfolds conn batchSize log1
        let! maybeInitialSku =
            let (streamEmpty, skuId) = initialState
            async {
                if streamEmpty then return None
                else
                    let addRemoveCount = 2
                    do! addAndThenRemoveItemsManyTimesExceptTheLastOne context cartId skuId service1 addRemoveCount
                    return Some (skuId, addRemoveCount) }

        let act prepare (service : Backend.Cart.Service) skuId count =
            service.FlowAsync(cartId, false, prepare = prepare, flow = fun _ctx execute ->
                execute <| Domain.Cart.AddItem (context, skuId, count))

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
        let service2 = Cart.createServiceWithEmptyUnfolds conn batchSize log2
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
        let! conn = connectToSpecifiedCosmosOrSimulator log
        let service = ContactPreferences.createService log (createCosmosContext conn)

        let email = let g = System.Guid.NewGuid() in g.ToString "N"
        //let (Domain.ContactPreferences.Id email) = id ()
        // Feed some junk into the stream
        for i in 0..11 do
            let quickSurveysValue = i % 2 = 0
            do! service.Update email { value with quickSurveys = quickSurveysValue }
        // Ensure there will be something to be changed by the Update below
        do! service.Update email { value with quickSurveys = not value.quickSurveys }

        capture.Clear()
        do! service.Update email value

        let! result = service.Read email
        test <@ value = result @>

        test <@ [EqxAct.Tip; EqxAct.Append; EqxAct.Tip] = capture.ExternalCalls @>
    }

     [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``Can correctly read and update Contacts against Cosmos with RollingUnfolds Access Strategy`` value = Async.RunSynchronously <| async {
        let! conn = connectToSpecifiedCosmosOrSimulator log
        let service = ContactPreferences.createServiceWithLatestKnownEvent (createCosmosContext conn) log CachingStrategy.NoCaching

        let email = let g = System.Guid.NewGuid() in g.ToString "N"
        // Feed some junk into the stream
        for i in 0..11 do
            let quickSurveysValue = i % 2 = 0
            do! service.Update email { value with quickSurveys = quickSurveysValue }
        // Ensure there will be something to be changed by the Update below
        do! service.Update email { value with quickSurveys = not value.quickSurveys }

        capture.Clear()
        do! service.Update email value

        let! result = service.Read email
        test <@ value = result @>

        test <@ [EqxAct.Tip; EqxAct.Append; EqxAct.Tip] = capture.ExternalCalls @>
    }

     [<AutoData(MaxTest = 2, SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``Can roundtrip Cart against Cosmos with RollingUnfolds, detecting conflicts based on _etag`` ctx initialState = Async.RunSynchronously <| async {
        let log1, capture1 = log, capture
        capture1.Clear()
        let! conn = connectToSpecifiedCosmosOrSimulator log1

        let context, (sku11, sku12, sku21, sku22) = ctx
        let cartId = % Guid.NewGuid()

        // establish base stream state
        let service1 = Cart.createServiceWithRollingState conn log1
        let! maybeInitialSku =
            let (streamEmpty, skuId) = initialState
            async {
                if streamEmpty then return None
                else
                    let addRemoveCount = 2
                    do! addAndThenRemoveItemsManyTimesExceptTheLastOne context cartId skuId service1 addRemoveCount
                    return Some (skuId, addRemoveCount) }

        let act prepare (service : Backend.Cart.Service) skuId count =
            service.FlowAsync(cartId, false, prepare = prepare, flow = fun _ctx execute ->
                execute <| Domain.Cart.AddItem (context, skuId, count))

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
        let service2 = Cart.createServiceWithRollingState conn log2
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
        let! conn = connectToSpecifiedCosmosOrSimulator log
        let batchSize = 10
        let createServiceIndexed () = Cart.createServiceWithSnapshotStrategy conn batchSize log
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
    }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``Can roundtrip against Cosmos, correctly using Snapshotting and Cache to avoid redundant reads`` context skuId = Async.RunSynchronously <| async {
        let! conn = connectToSpecifiedCosmosOrSimulator log
        let batchSize = 10
        let cache = Equinox.Cache("cart", sizeMb = 50)
        let createServiceCached () = Cart.createServiceWithSnapshotStrategyAndCaching conn batchSize log cache
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
    }