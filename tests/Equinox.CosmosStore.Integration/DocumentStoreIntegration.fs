module Equinox.Store.Integration.DocumentStoreIntegration

open Domain
open Equinox.Core
open FSharp.UMX
open Swensen.Unquote
open System
open System.Threading
#if STORE_DYNAMO
open Equinox.DynamoStore
open Equinox.DynamoStore.Integration.CosmosFixtures
#else
open Equinox.CosmosStore
open Equinox.CosmosStore.Integration.CosmosFixtures
#endif

module Cart =
    let fold, initial = Cart.Fold.fold, Cart.Fold.initial
    let snapshot = Cart.Fold.isOrigin, Cart.Fold.snapshot
#if STORE_DYNAMO
    let codec = Cart.Events.codec |> FsCodec.Deflate.EncodeTryDeflate
#else
    let codec = Cart.Events.codecJe
#endif
    let createServiceWithoutOptimization log context =
        StoreCategory(context, codec, fold, initial, CachingStrategy.NoCaching, AccessStrategy.Unoptimized)
        |> Equinox.Decider.resolve log
        |> Cart.create
    /// Trigger looking in Tip (we want those calls to occur, but without leaning on snapshots, which would reduce the paths covered)
    let createServiceWithEmptyUnfolds log context =
        let unfArgs = Cart.Fold.isOrigin, fun _ -> Array.empty
        StoreCategory(context, codec, fold, initial, CachingStrategy.NoCaching, AccessStrategy.MultiSnapshot unfArgs)
        |> Equinox.Decider.resolve log
        |> Cart.create
    let createServiceWithSnapshotStrategy log context =
        StoreCategory(context, codec, fold, initial, CachingStrategy.NoCaching, AccessStrategy.Snapshot snapshot)
        |> Equinox.Decider.resolve log
        |> Cart.create
    let createServiceWithSnapshotStrategyAndCaching log context cache =
        let sliding20m = CachingStrategy.SlidingWindow (cache, TimeSpan.FromMinutes 20.)
        StoreCategory(context, codec, fold, initial, sliding20m, AccessStrategy.Snapshot snapshot)
        |> Equinox.Decider.resolve log
        |> Cart.create
    let createServiceWithRollingState log context =
        let access = AccessStrategy.RollingState Cart.Fold.snapshot
        StoreCategory(context, codec, fold, initial, CachingStrategy.NoCaching, access)
        |> Equinox.Decider.resolve log
        |> Cart.create
    let streamName = Cart.streamId >> StreamName.render Cart.Category

module ContactPreferences =
    let fold, initial = ContactPreferences.Fold.fold, ContactPreferences.Fold.initial
#if STORE_DYNAMO
    let codec = ContactPreferences.Events.codec |> FsCodec.Deflate.EncodeTryDeflate
#else
    let codec = ContactPreferences.Events.codecJe
#endif
    let private createServiceWithLatestKnownEvent context log cachingStrategy =
        StoreCategory(context, codec, fold, initial, cachingStrategy, AccessStrategy.LatestKnownEvent)
        |> Equinox.Decider.resolve log
        |> ContactPreferences.create
    let createServiceWithoutCaching log context =
        createServiceWithLatestKnownEvent context log CachingStrategy.NoCaching
    let createServiceWithCaching log context cache =
        let sliding20m = CachingStrategy.SlidingWindow (cache, TimeSpan.FromMinutes 20.)
        createServiceWithLatestKnownEvent context log sliding20m
    let streamName = ContactPreferences.streamId >> StreamName.render ContactPreferences.Category

[<Xunit.Collection "DocStore">]
type Tests(testOutputHelper) =
    let testContext = TestContext(testOutputHelper)
    let log, capture = testContext.CreateLoggerWithCapture()

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

    let verifyRequestChargesMax rus =
        let tripRequestCharges = [ for e, c in capture.RequestCharges -> sprintf "%A" e, c ]
        test <@ float rus >= Seq.sum (Seq.map snd tripRequestCharges) @>

    // There's currently a discrepancy between real DynamoDb and the similar wrt whether a continuation token is returned
    // when you hit the max count as you read the final item in a stream. Leaving it ugly in the hope we get to delete it.
    let expectFinalExtraPage () =
#if STORE_DYNAMO
        discoverConnection () |> snd |> isSimulatorServiceUrl
#else
        false
#endif

    [<AutoData(MaxTest = 2, SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``Can roundtrip against DocStore, correctly batching the reads`` (eventsInTip, cartContext, skuId) = async {
        capture.Clear() // for re-runs of the test
        let addRemoveCount = 40
        let eventsPerAction = addRemoveCount * 2 - 1
        let queryMaxItems = 3
        let context = createPrimaryContextEx log queryMaxItems (if eventsInTip then eventsPerAction else 0)

        let service = Cart.createServiceWithoutOptimization log context
        let expectedResponses n =
            let finalEmptyPage = if expectFinalExtraPage () then 1 else 0
            let tipItem = 1
            let expectedItems = tipItem + (if eventsInTip then n / 2 else n) + finalEmptyPage
            max 1 (int (ceil (float expectedItems / float queryMaxItems)))

        let cartId = % Guid.NewGuid()
        // The command processing will trigger QueryB operations as no snapshots etc are being used
        let transactions = 6
        for i in [1..transactions] do
            do! addAndThenRemoveItemsManyTimesExceptTheLastOne cartContext cartId skuId service addRemoveCount
            test <@ i = i && List.replicate (expectedResponses (i-1)) EqxAct.ResponseBackward @ [EqxAct.QueryBackward; EqxAct.Append] = capture.ExternalCalls @>
#if STORE_DYNAMO
            if eventsInTip then verifyRequestChargesMax 190 // 189.5 [9.5; 180.0]
#else
            if eventsInTip then verifyRequestChargesMax 76 // 76.0 [3.72; 72.28]
#endif
            else verifyRequestChargesMax 79 // 78.37 [3.15; 75.22]
            capture.Clear()

        // Validate basic operation; Key side effect: Log entries will be emitted to `capture`
        let! state = service.Read cartId
        test <@ addRemoveCount = match state with { items = [{ quantity = quantity }] } -> quantity | _ -> failwith "nope" @>

        test <@ List.replicate (expectedResponses transactions) EqxAct.ResponseBackward @ [EqxAct.QueryBackward] = capture.ExternalCalls @>
#if STORE_DYNAMO
        if eventsInTip then verifyRequestChargesMax 12 // 11.5
#else
        if eventsInTip then verifyRequestChargesMax 9 // 8.05
#endif
        else verifyRequestChargesMax 15 // 14.01
    }

#if STORE_DYNAMO
    [<AutoData(MaxTest = 2, SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``Can read stream without traversing tip`` (cartContext, skuId, countToTry) = async {
        capture.Clear() // for re-runs of the test
        let addRemoveCount = 40
        let _expectedEvents = addRemoveCount * 2 - 1
        // Read only one batch, that does not contain the tip
        let queryMaxBatches = 1
        let eventsInTip = 0
        let context = createPrimaryContextEx log queryMaxBatches eventsInTip
        let service = Cart.createServiceWithSnapshotStrategy log context

        let cartId: CartId = % Guid.NewGuid()

        do! addAndThenRemoveItemsManyTimesExceptTheLastOne cartContext cartId skuId service addRemoveCount

        let! ct = Async.CancellationToken
        let eventsContext = Equinox.DynamoStore.Core.EventsContext(context, log)
        let streamName = Cart.streamName cartId
        let countToTry = max addRemoveCount countToTry
        let! events = eventsContext.Read(FsCodec.StreamName.parse streamName, ct, 1L, maxCount = countToTry) |> Async.AwaitTask
        [| 1..1+countToTry-1 |] =! [| for e in events -> int e.Index |]
    }
#endif

    [<AutoData(MaxTest = 2, SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``Can roundtrip against DocStore, managing sync conflicts by retrying`` (eventsInTip, ctx, initialState) = async {
        capture.Clear()
        let log1, capture1 = log, capture
        let queryMaxItems = 3
        let context = createPrimaryContextEx log1 queryMaxItems (if eventsInTip then 10 else 0)
        // Ensure batching is included at some point in the proceedings

        let cartContext, (sku11, sku12, sku21, sku22) = ctx
        let cartId = % Guid.NewGuid()

        // establish base stream state
        let service1 = Cart.createServiceWithEmptyUnfolds log1 context
        let! maybeInitialSku =
            let streamEmpty, skuId = initialState
            async {
                if streamEmpty then return None
                else
                    let addRemoveCount = 2
                    do! addAndThenRemoveItemsManyTimesExceptTheLastOne cartContext cartId skuId service1 addRemoveCount
                    return Some (skuId, addRemoveCount) }

        let act prepare (service: Cart.Service) skuId count =
            service.ExecuteManyAsync(cartId, false, prepare = prepare, commands = [Cart.SyncItem (cartContext, skuId, Some count, None)])

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
        let log2, capture2 = testContext.CreateLoggerWithCapture()
        use _flush = log2
        let service2 = Cart.createServiceWithEmptyUnfolds log2 context
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
#if !STORE_DYNAMO
        if eventsInTip then
            test <@ let c2 = List.choose conflict capture2.ExternalCalls
                    [EqxAct.Resync] = List.choose conflict capture1.ExternalCalls
                    && [EqxAct.Resync] = c2 @>
#else
        if false then ()
#endif
        else
            test <@ let c2 = List.choose conflict capture2.ExternalCalls
                    [EqxAct.Conflict] = List.choose conflict capture1.ExternalCalls
                    && [EqxAct.Conflict] = c2 @>
    }

    [<AutoData(MaxTest = 2, SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``Can correctly read and update Contacts against DocStore with LatestKnownEvent without Caching`` (eventsInTip, value: ContactPreferences.Events.Preferences) = async {
        let context = createPrimaryContextEx log 1 (if eventsInTip then 1 else 0)
        let service = ContactPreferences.createServiceWithoutCaching log context
        // We need to be sure every Update changes something as we rely on an expected number of events in the end
        let value = if value <> ContactPreferences.Fold.initial then value else { value with manyPromotions = true }

        let id = ContactPreferences.ClientId (let g = Guid.NewGuid() in g.ToString "N")
        // Ensure there will be something to be changed by the Update below
        for i in 0..13 do
            do! service.Update(id, if i % 2 = 0 then value else { value with quickSurveys = not value.quickSurveys })
        capture.Clear()

        do! service.Update(id, value)

        let! result = service.Read id
        test <@ value = result @>

        test <@ [EqxAct.Tip; EqxAct.Append; EqxAct.Tip] = capture.ExternalCalls @>

        (* Verify pruning does not affect the copies of the events maintained as Unfolds *)

        // Needs to share the same context (with inner CosmosClient) for the session token to be threaded through
        // If we run on an independent context, we won't see (and hence prune) the full set of events
        let ctx = Core.EventsContext(context, log)
        let streamName = ContactPreferences.streamName id

        // Prune all the events
        let! deleted, deferred, trimmedPos = Core.Events.pruneUntil ctx streamName 14L
        test <@ deleted = 15 && deferred = 0 && trimmedPos = 15L @>

        // Prove we notice they're gone
        capture.Clear()
        let! res = Core.Events.get ctx streamName 0L Int32.MaxValue |> Async.Catch
        test <@ match res with
                | Choice2Of2 e -> e.Message.StartsWith "Origin event not found; no Archive Container supplied"
                                  || e.Message.StartsWith "Origin event not found; no Archive Table supplied"
                | x -> failwithf "Unexpected %A" x @>
        let finalEmptyPage = if expectFinalExtraPage () then 1 else 0
        test <@ [yield! Seq.replicate (1 + finalEmptyPage) EqxAct.ResponseForward; EqxAct.QueryForward] = capture.ExternalCalls @>
        verifyRequestChargesMax 3 // 2.99

        // But not forgotten
        capture.Clear()
        let! pos = Core.Events.getNextIndex ctx streamName
        test <@ [EqxAct.Tip] = capture.ExternalCalls @> // Note in the current impl, this read is not cached
        test <@ 15L = pos @>
        verifyRequestChargesMax 1

        // And we can still read the Snapshot from the Tip's unfolds (there's no caching so we'll definitely be reading)
        capture.Clear()
        let! _ = service.Read id
        test <@ value = result @>
        test <@ [EqxAct.Tip] = capture.ExternalCalls @>
        verifyRequestChargesMax 1
    }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``Can correctly read and update Contacts against DocStore with LatestKnownEvent`` value = async {
        let context = createPrimaryContextEx log 1 10
        let cache = Equinox.Cache("contacts", sizeMb = 50)
        let service = ContactPreferences.createServiceWithCaching log context cache

        let id = ContactPreferences.ClientId (let g = Guid.NewGuid() in g.ToString "N")
        // Ensure there will be something to be changed by the Update below
        for i in 1..13 do
            do! service.Update(id, if i % 2 = 0 then value else { value with quickSurveys = not value.quickSurveys })
        capture.Clear()

        do! service.Update(id, value)

        let! result = service.Read id
        test <@ value = result @>

        let! result = service.ReadStale id // should not trigger roundtrip
        test <@ value = result @>

        test <@ [EqxAct.TipNotModified; EqxAct.Append; EqxAct.TipNotModified] = capture.ExternalCalls @>
    }

    [<AutoData(MaxTest = 2, SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``Can roundtrip against DocStore with RollingState, detecting conflicts based on etag`` (ctx, initialState) = async {
        let log1, capture1 = log, capture
        capture1.Clear()
        let context = createPrimaryContextEx log1 1 10

        let cartContext, (sku11, sku12, sku21, sku22) = ctx
        let cartId = % Guid.NewGuid()

        // establish base stream state
        let service1 = Cart.createServiceWithRollingState log1 context
        let! maybeInitialSku =
            let streamEmpty, skuId = initialState
            async {
                if streamEmpty then return None
                else
                    let addRemoveCount = 2
                    do! addAndThenRemoveItemsManyTimesExceptTheLastOne cartContext cartId skuId service1 addRemoveCount
                    return Some (skuId, addRemoveCount) }

        let act prepare (service: Cart.Service) skuId count =
            service.ExecuteManyAsync(cartId, false, prepare = prepare, commands = [Cart.SyncItem (cartContext, skuId, Some count, None)])

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
        let log2, capture2 = testContext.CreateLoggerWithCapture()
        use _flush = log2
        let service2 = Cart.createServiceWithRollingState log2 context
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
#if STORE_DYNAMO // Failed conditions do not yield the conflicting state, so it needs to be a separate load
        test <@ let c2 = List.choose conflict capture2.ExternalCalls
                [EqxAct.Conflict] = List.choose conflict capture1.ExternalCalls
                && [EqxAct.Conflict] = c2 @>
#else
        test <@ let c2 = List.choose conflict capture2.ExternalCalls
                [EqxAct.Resync] = List.choose conflict capture1.ExternalCalls
                && [EqxAct.Resync] = c2 @>
#endif
    }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``Can roundtrip against DocStore, using Snapshotting to avoid queries`` (cartContext, skuId) = async {
        let queryMaxItems = 10
        let context = createPrimaryContextEx log queryMaxItems 10
        let createServiceIndexed () = Cart.createServiceWithSnapshotStrategy log context
        let service1, service2 = createServiceIndexed (), createServiceIndexed ()
        capture.Clear()

        // Trigger 10 events, then reload
        let cartId = % Guid.NewGuid()
        do! addAndThenRemoveItemsManyTimes cartContext cartId skuId service1 5
        let! _ = service2.Read cartId

        // ... should see a single read as we are writes are cached
        test <@ [EqxAct.TipNotFound; EqxAct.Append; EqxAct.Tip] = capture.ExternalCalls @>

        // Add two more - the roundtrip should only incur a single read
        capture.Clear()
        do! addAndThenRemoveItemsManyTimes cartContext cartId skuId service1 1
        test <@ [EqxAct.Tip; EqxAct.Append] = capture.ExternalCalls @>

        // While we now have 12 events, we should be able to read them with a single call
        capture.Clear()
        let! _ = service2.Read cartId
        test <@ [EqxAct.Tip] = capture.ExternalCalls @>

        (* Verify pruning does not affect snapshots, though Tip is re-read in this scenario due to lack of caching *)

        let ctx = Core.EventsContext(context, log)
        // Prune all the events
        let streamName = Cart.streamName cartId
        let! deleted, deferred, trimmedPos = Core.Events.pruneUntil ctx streamName 11L
        test <@ deleted = 12 && deferred = 0 && trimmedPos = 12L @>

        // Show alarms are raised when they're gone
        capture.Clear()
        let! res = Core.Events.get ctx streamName 0L Int32.MaxValue |> Async.Catch
        test <@ match res with
                | Choice2Of2 e -> e.Message.StartsWith "Origin event not found; no Archive Container supplied"
                                  || e.Message.StartsWith "Origin event not found; no Archive Table supplied"
                | x -> failwithf "Unexpected %A" x @>
        test <@ [EqxAct.ResponseForward; EqxAct.QueryForward] = capture.ExternalCalls @>
        verifyRequestChargesMax 3 // 2.99

        // But we can still read (there's no cache so we'll definitely be reading)
        capture.Clear()
        let! _ = service2.Read cartId
        test <@ [EqxAct.Tip] = capture.ExternalCalls @>
        verifyRequestChargesMax 1
    }

    [<AutoData(MaxTest = 2, SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``Can roundtrip against DocStore, correctly using Snapshotting and Cache to avoid redundant reads`` (eventsInTip, cartContext, skuId) = async {
        let context = createPrimaryContextEx log 10 (if eventsInTip then 10 else 0)
        let cache = Equinox.Cache("cart", sizeMb = 50)
        let createServiceCached () = Cart.createServiceWithSnapshotStrategyAndCaching log context cache
        let service1, service2 = createServiceCached (), createServiceCached ()
        capture.Clear()

        // Trigger 10 events, then reload
        let cartId = % Guid.NewGuid()
        do! addAndThenRemoveItemsManyTimes cartContext cartId skuId service1 5
        let! _ = service2.Read cartId

        // ... should see a single Cached Indexed read given writes are cached and writer emits etag
        test <@ [EqxAct.TipNotFound; EqxAct.Append; EqxAct.TipNotModified] = capture.ExternalCalls @>

        // Add two more - the roundtrip should only incur a single read, which should be cached by virtue of being a second one in succession
        capture.Clear()
        do! addAndThenRemoveItemsManyTimes cartContext cartId skuId service1 1
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
        do! addAndThenRemoveItemsOptimisticManyTimesExceptTheLastOne cartContext cartId skuId service1 1
        test <@ [EqxAct.Append] = capture.ExternalCalls @>

        (* Verify pruning does not affect snapshots, and does not touch the Tip *)

        let ctx = Core.EventsContext(context, log)
        let streamName = Cart.streamName cartId
        // Prune all the events
        let! deleted, deferred, trimmedPos = Core.Events.pruneUntil ctx streamName 12L
        test <@ deleted = 13 && deferred = 0 && trimmedPos = 13L @>

        // Show that we hear about it if we try to load the events
        capture.Clear()
        let! res = Core.Events.get ctx streamName 0L Int32.MaxValue |> Async.Catch
        test <@ match res with
                | Choice2Of2 e -> e.Message.StartsWith "Origin event not found; no Archive Container supplied"
                                  || e.Message.StartsWith "Origin event not found; no Archive Table supplied"
                | x -> failwithf "Unexpected %A" x @>
        test <@ [EqxAct.ResponseForward; EqxAct.QueryForward] = capture.ExternalCalls @>
        verifyRequestChargesMax 3 // 2.99

        // But we can still read (service2 shares the cache so is aware of the last writes)
        // When events are in the Tip, the Unfolds are invalidated and reloaded as a side-effect of the pruning triggering an etag change
        capture.Clear()
        let! _ = service2.Read cartId
        test <@ [if eventsInTip then EqxAct.Tip else EqxAct.TipNotModified] = capture.ExternalCalls @>
        // Charges are 1 RU regardless of whether a reload occurs, as the snapshot is tiny
        verifyRequestChargesMax 1
    }

    interface IDisposable with member _.Dispose() = log.Dispose()
