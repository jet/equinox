module Equinox.Cosmos.Integration.CosmosIntegration

open Domain
open Equinox.Cosmos.Integration.Infrastructure
open Equinox.Cosmos.Builder
open Swensen.Unquote
open System.Threading
open System

let serializationSettings = Newtonsoft.Json.Converters.FSharp.Settings.CreateCorrect()
let genCodec<'Union when 'Union :> TypeShape.UnionContract.IUnionContract>() =
    Equinox.UnionCodec.JsonUtf8.Create<'Union>(serializationSettings)

module Cart =
    let fold, initial, project = Domain.Cart.Folds.fold, Domain.Cart.Folds.initial, Domain.Cart.Folds.compact
    let codec = genCodec<Domain.Cart.Events.Event>()
    let createServiceWithoutOptimization connection batchSize log =
        let store = createEqxStore connection batchSize
        let resolveStream = EqxStreamBuilder(store, codec, fold, initial).Create
        Backend.Cart.Service(log, resolveStream)
    let createServiceWithProjection connection batchSize log =
        let store = createEqxStore connection batchSize
        let resolveStream = EqxStreamBuilder(store, codec, fold, initial, AccessStrategy.Projection project).Create
        Backend.Cart.Service(log, resolveStream)
    let createServiceWithProjectionAndCaching connection batchSize log cache =
        let store = createEqxStore connection batchSize
        let sliding20m = CachingStrategy.SlidingWindow (cache, TimeSpan.FromMinutes 20.)
        let resolveStream = EqxStreamBuilder(store, codec, fold, initial, AccessStrategy.Projection project, sliding20m).Create
        Backend.Cart.Service(log, resolveStream)

module ContactPreferences =
    let fold, initial, eventTypes = Domain.ContactPreferences.Folds.fold, Domain.ContactPreferences.Folds.initial, Domain.ContactPreferences.Events.eventTypeNames
    let codec = genCodec<Domain.ContactPreferences.Events.Event>()
    let createServiceWithoutOptimization createGateway defaultBatchSize log _ignoreWindowSize _ignoreCompactionPredicate =
        let gateway = createGateway defaultBatchSize
        let resolveStream = EqxStreamBuilder(gateway, codec, fold, initial).Create
        Backend.ContactPreferences.Service(log, resolveStream)
    let createService createGateway log =
        let resolveStream = EqxStreamBuilder(createGateway 1, codec, fold, initial, AccessStrategy.AnyKnownEventType eventTypes).Create
        Backend.ContactPreferences.Service(log, resolveStream)

#nowarn "1182" // From hereon in, we may have some 'unused' privates (the tests)

type Tests(testOutputHelper) =
    inherit TestsWithLogCapture(testOutputHelper)
    let log,capture = base.Log, base.Capture

    let addAndThenRemoveItems exceptTheLastOne context cartId skuId (service: Backend.Cart.Service) count =
        service.FlowAsync(cartId, fun _ctx execute ->
            for i in 1..count do
                execute <| Domain.Cart.AddItem (context, skuId, i)
                if not exceptTheLastOne || i <> count then
                    execute <| Domain.Cart.RemoveItem (context, skuId) )
    let addAndThenRemoveItemsManyTimes context cartId skuId service count =
        addAndThenRemoveItems false context cartId skuId service count
    let addAndThenRemoveItemsManyTimesExceptTheLastOne context cartId skuId service count =
        addAndThenRemoveItems true context cartId skuId service count

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``Can roundtrip against Cosmos, correctly batching the reads [without using the Index for reads]`` context skuId = Async.RunSynchronously <| async {
        let! conn = connectToSpecifiedCosmosOrSimulator log

        let batchSize = 3
        let service = Cart.createServiceWithoutOptimization conn batchSize log
        capture.Clear() // for re-runs of the test

        let cartId = Guid.NewGuid() |> CartId
        // The command processing should trigger only a single read and a single write call
        let addRemoveCount = 6
        do! addAndThenRemoveItemsManyTimesExceptTheLastOne context cartId skuId service addRemoveCount
        test <@ [EqxAct.SliceWaste; EqxAct.BatchBackward; EqxAct.Append] = capture.ExternalCalls @>
        // Restart the counting
        capture.Clear()

        // Validate basic operation; Key side effect: Log entries will be emitted to `capture`
        let! state = service.Read cartId
        let expectedEventCount = 2 * addRemoveCount - 1
        test <@ addRemoveCount = match state with { items = [{ quantity = quantity }] } -> quantity | _ -> failwith "nope" @>

        // Need to read 4 batches to read 11 events in batches of 3
        let expectedBatches = ceil(float expectedEventCount/float batchSize) |> int
        test <@ List.replicate (expectedBatches-1) EqxAct.SliceBackward @ [EqxAct.SliceBackward; EqxAct.BatchBackward] = capture.ExternalCalls @>
    }

    [<AutoData(MaxTest = 2, SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``Can roundtrip against Cosmos, managing sync conflicts by retrying`` withOptimizations ctx initialState = Async.RunSynchronously <| async {
        let log1, capture1 = log, capture
        capture1.Clear()
        let! conn = connectToSpecifiedCosmosOrSimulator log1
        // Ensure batching is included at some point in the proceedings
        let batchSize = 3

        let context, (sku11, sku12, sku21, sku22) = ctx
        let cartId = Guid.NewGuid() |> CartId

        // establish base stream state
        let service1 =
            if withOptimizations then Cart.createServiceWithProjection conn batchSize log1
            else Cart.createServiceWithProjection conn batchSize log1
        let! maybeInitialSku =
            let (streamEmpty, skuId) = initialState
            async {
                if streamEmpty then return None
                else
                    let addRemoveCount = 2
                    do! addAndThenRemoveItemsManyTimesExceptTheLastOne context cartId skuId service1 addRemoveCount
                    return Some (skuId, addRemoveCount) }

        let act prepare (service : Backend.Cart.Service) skuId count =
            service.FlowAsync(cartId, prepare = prepare, flow = fun _ctx execute ->
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
        let service2 = Cart.createServiceWithProjection conn batchSize log2
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
       // Intended conflicts pertained
        let conflict = function EqxAct.Conflict | EqxAct.Resync as x -> Some x | _ -> None
        test <@ let c2 = List.choose conflict capture2.ExternalCalls
                [EqxAct.Resync] = List.choose conflict capture1.ExternalCalls
                && [EqxAct.Resync] = c2 @>
    }

    let singleBatchBackwards = [EqxAct.SliceBackward; EqxAct.BatchBackward]
    let batchBackwardsAndAppend = singleBatchBackwards @ [EqxAct.Append]

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``Can correctly read and update against Cosmos with EventsAreState Access Strategy`` value = Async.RunSynchronously <| async {
        let! conn = connectToSpecifiedCosmosOrSimulator log
        let service = ContactPreferences.createService (createEqxStore conn) log

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

        test <@ [EqxAct.Index; EqxAct.Append; EqxAct.Index] = capture.ExternalCalls @>
    }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``Can roundtrip against Cosmos, using Projection to avoid queries`` context skuId = Async.RunSynchronously <| async {
        let! conn = connectToSpecifiedCosmosOrSimulator log
        let batchSize = 10
        let createServiceIndexed () = Cart.createServiceWithProjection conn batchSize log
        let service1, service2 = createServiceIndexed (), createServiceIndexed ()
        capture.Clear()

        // Trigger 10 events, then reload
        let cartId = Guid.NewGuid() |> CartId
        do! addAndThenRemoveItemsManyTimes context cartId skuId service1 5
        let! _ = service2.Read cartId

        // ... should see a single read as we are writes are cached
        test <@ [EqxAct.IndexNotFound; EqxAct.Append; EqxAct.Index] = capture.ExternalCalls @>

        // Add two more - the roundtrip should only incur a single read
        capture.Clear()
        do! addAndThenRemoveItemsManyTimes context cartId skuId service1 1
        test <@ [EqxAct.Index; EqxAct.Append] = capture.ExternalCalls @>

        // While we now have 12 events, we should be able to read them with a single call
        capture.Clear()
        let! _ = service2.Read cartId
        test <@ [EqxAct.Index] = capture.ExternalCalls @>
    }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``Can roundtrip against Cosmos, correctly using Projection and Cache to avoid redundant reads`` context skuId = Async.RunSynchronously <| async {
        let! conn = connectToSpecifiedCosmosOrSimulator log
        let batchSize = 10
        let cache = Caching.Cache("cart", sizeMb = 50)
        let createServiceCached () = Cart.createServiceWithProjectionAndCaching conn batchSize log cache
        let service1, service2 = createServiceCached (), createServiceCached ()
        capture.Clear()

        // Trigger 10 events, then reload
        let cartId = Guid.NewGuid() |> CartId
        do! addAndThenRemoveItemsManyTimes context cartId skuId service1 5
        let! _ = service2.Read cartId

        // ... should see a single Cached Indexed read given writes are cached and writer emits etag
        test <@ [EqxAct.IndexNotFound; EqxAct.Append; EqxAct.IndexNotModified] = capture.ExternalCalls @>

        // Add two more - the roundtrip should only incur a single read, which should be cached by virtue of being a second one in successono
        capture.Clear()
        do! addAndThenRemoveItemsManyTimes context cartId skuId service1 1
        test <@ [EqxAct.IndexNotModified; EqxAct.Append] = capture.ExternalCalls @>

        // While we now have 12 events, we should be able to read them with a single call
        capture.Clear()
        let! _ = service2.Read cartId
        let! _ = service2.Read cartId
        // First is cached because writer emits etag, second remains cached
        test <@ [EqxAct.IndexNotModified; EqxAct.IndexNotModified] = capture.ExternalCalls @>
    }