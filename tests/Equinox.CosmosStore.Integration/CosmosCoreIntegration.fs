module Equinox.CosmosStore.Integration.CosmosCoreIntegration

open Equinox.CosmosStore.Core
open Equinox.CosmosStore.Integration.Infrastructure
open FsCodec
open FSharp.Control
open Newtonsoft.Json.Linq
open Swensen.Unquote
open Serilog
open System
open System.Text

#nowarn "1182" // From hereon in, we may have some 'unused' privates (the tests)

type TestEvents() =
    static member private Create(i, ?eventType, ?json) =
        EventData.FromUtf8Bytes
            (   sprintf "%s:%d" (defaultArg eventType "test_event") i,
                Encoding.UTF8.GetBytes(defaultArg json "{\"d\":\"d\"}"),
                Encoding.UTF8.GetBytes "{\"m\":\"m\"}")
    static member Create(i, c) = Array.init c (fun x -> TestEvents.Create(x+i))

type Tests(testOutputHelper) =
    inherit TestsWithLogCapture(testOutputHelper)
    let log, capture = base.Log, base.Capture

    /// As we generate side-effects per run, we want each  FSCheck-triggered invocation of the test run to work in its own stream
    let testIterations = ref 0
    let (|TestStream|) (name: Guid) =
        incr testIterations
        sprintf "events-%O-%i" name !testIterations

    let verifyRequestChargesMax rus =
        let tripRequestCharges = [ for e, c in capture.RequestCharges -> sprintf "%A" e, c ]
        test <@ float rus >= Seq.sum (Seq.map snd tripRequestCharges) @>

    [<AutoData(MaxTest = 2, SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let append (eventsInTip, TestStream streamName) = Async.RunSynchronously <| async {
        capture.Clear()
        let ctx = createPrimaryEventsContext log defaultQueryMaxItems (if eventsInTip then 1 else 0)

        let index = 0L
        let! res = Events.append ctx streamName index <| TestEvents.Create(0,1)
        test <@ AppendResult.Ok 1L = res @>
        test <@ [EqxAct.Append] = capture.ExternalCalls @>
        if eventsInTip then verifyRequestChargesMax 24 // 23.72
        else verifyRequestChargesMax 36 // 35.97 // 43.53 observed

        // Clear the counters
        capture.Clear()

        let! res = Events.append ctx streamName 1L <| TestEvents.Create(1,5)
        test <@ AppendResult.Ok 6L = res @>
        test <@ [EqxAct.Append] = capture.ExternalCalls @>
        // We didnt request small batches or splitting so it's not dramatically more expensive to write N events
        if eventsInTip then verifyRequestChargesMax 42 // 41.36
        else verifyRequestChargesMax 43 // 42.82
    }

    // It's conceivable that in the future we might allow zero-length batches as long as a sync mechanism leveraging the etags and unfolds update mechanisms
    // As it stands with the NoTipEvents stored proc, permitting empty batches a) yields an invalid state b) provides no conceivable benefit
    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``append Throws when passed an empty batch`` (TestStream streamName) = Async.RunSynchronously <| async {
        let ctx = createPrimaryEventsContext log defaultQueryMaxItems 10

        let index = 0L
        let! res = Events.append ctx streamName index (TestEvents.Create(0,0)) |> Async.Catch
        test <@ match res with Choice2Of2 ((:? InvalidOperationException) as ex) -> ex.Message.StartsWith "Must write either events or unfolds." | x -> failwithf "%A" x @>
    }

    let blobEquals (x: byte[]) (y: byte[]) = System.Linq.Enumerable.SequenceEqual(x,y)
    let stringOfUtf8 (x: byte[]) = Encoding.UTF8.GetString(x)
    let xmlDiff (x: string) (y: string) =
        match JsonDiffPatchDotNet.JsonDiffPatch().Diff(JToken.Parse x,JToken.Parse y) with
        | null -> ""
        | d -> string d
    let verifyUtf8JsonEquals i x y =
        let sx,sy = stringOfUtf8 x, stringOfUtf8 y
        test <@ ignore i; blobEquals x y || "" = xmlDiff sx sy @>

    let add6EventsIn2BatchesEx ctx streamName splitAt = async {
        let index = 0L
        let! res = Events.append ctx streamName index <| TestEvents.Create(0, splitAt)

        test <@ AppendResult.Ok (int64 splitAt) = res @>
        let! res = Events.append ctx streamName (int64 splitAt) <| TestEvents.Create(splitAt, 6 - splitAt)
        test <@ AppendResult.Ok 6L = res @>
        // Only start counting RUs from here
        capture.Clear()
        return TestEvents.Create(0,6)
    }

    let add6EventsIn2Batches ctx streamName = add6EventsIn2BatchesEx ctx streamName 1

    let verifyCorrectEventsEx direction baseIndex (expected: IEventData<_>[]) (xs: ITimelineEvent<byte[]>[]) =
        let xs, baseIndex =
            if direction = Equinox.CosmosStore.Core.Direction.Forward then xs, baseIndex
            else Array.rev xs, baseIndex - int64 (Array.length expected) + 1L
        test <@ [for i in 0..expected.Length - 1 -> baseIndex + int64 i] = [for r in xs -> r.Index] @>
        test <@ [for e in expected -> e.EventType] = [ for r in xs -> r.EventType ] @>
        for i,x,y in Seq.mapi2 (fun i x y -> i,x,y) [for e in expected -> e.Data] [for r in xs -> r.Data] do
            verifyUtf8JsonEquals i x y
    let verifyCorrectEventsBackward = verifyCorrectEventsEx Equinox.CosmosStore.Core.Direction.Backward
    let verifyCorrectEvents = verifyCorrectEventsEx Equinox.CosmosStore.Core.Direction.Forward

    [<AutoData(MaxTest = 2, SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``appendAtEnd and getNextIndex`` (eventsInTip, extras, TestStream streamName) = Async.RunSynchronously <| async {
        // If a fail triggers a rerun, we need to dump the previous log entries captured
        capture.Clear()

        let ctx = createPrimaryEventsContext log 1 (if eventsInTip then 1 else 0)

        let! pos = Events.getNextIndex ctx streamName
        test <@ [EqxAct.TipNotFound] = capture.ExternalCalls @>
        0L =! pos
        verifyRequestChargesMax 1 // for a 404 by definition
        capture.Clear()

        let mutable pos = 0L
        for appendBatchSize in [4; 5; 9] do
            let! res = Events.appendAtEnd ctx streamName <| TestEvents.Create (int pos, appendBatchSize)
            test <@ [EqxAct.Append] = capture.ExternalCalls @>
            pos <- pos + int64 appendBatchSize
            pos =! res
            if eventsInTip then verifyRequestChargesMax 50 // 49.58
            else verifyRequestChargesMax 46 // 45.16
            capture.Clear()

            let! res = Events.getNextIndex ctx streamName
            test <@ [EqxAct.Tip] = capture.ExternalCalls @>
            verifyRequestChargesMax 2
            pos =! res
            capture.Clear()

        let! res = Events.appendAtEnd ctx streamName <| TestEvents.Create (int pos,42)
        pos <- pos + 42L
        pos =! res
        test <@ [EqxAct.Append] = capture.ExternalCalls @>
        if eventsInTip then verifyRequestChargesMax 48 // 47.58
        else verifyRequestChargesMax 50 // 49.74
        capture.Clear()

        let! res = Events.getNextIndex ctx streamName
        test <@ [EqxAct.Tip] = capture.ExternalCalls @>
        verifyRequestChargesMax 2
        capture.Clear()
        pos =! res

        // Demonstrate benefit/mechanism for using the Position-based API to avail of the etag tracking
        let stream  = ctx.StreamId streamName

        let extrasCount = match extras with x when x > 50 -> 5000 | x when x < 1 -> 1 | x -> x*100
        let! _pos = ctx.NonIdempotentAppend(stream, TestEvents.Create (int pos,extrasCount))
        test <@ [EqxAct.Append] = capture.ExternalCalls @>
        if eventsInTip then verifyRequestChargesMax 451 // 450.03
        else verifyRequestChargesMax 448 // 447.5 // 463.01 observed
        capture.Clear()

        let! pos = ctx.Sync(stream,?position=None)
        test <@ [EqxAct.Tip] = capture.ExternalCalls @>
        verifyRequestChargesMax 5 // 41 observed // for a 200, you'll pay a lot (we omitted to include the position that NonIdempotentAppend yielded)
        capture.Clear()

        let! _pos = ctx.Sync(stream,pos)
        test <@ [EqxAct.TipNotModified] = capture.ExternalCalls @>
        verifyRequestChargesMax 1 // for a 302 by definition - when an etag IfNotMatch is honored, you only pay one RU
    }

    [<AutoData(MaxTest = 2, SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``append - fails on non-matching`` (eventsInTip, TestStream streamName) = Async.RunSynchronously <| async {
        capture.Clear()
        let ctx = createPrimaryEventsContext log 10 (if eventsInTip then 1 else 0)

        // Attempt to write, skipping Index 0
        let! res = Events.append ctx streamName 1L <| TestEvents.Create(0,1)
        // Resync is returned if no events should need to be re-queried
        test <@ [EqxAct.Resync] = capture.ExternalCalls @>
        // The response aligns with a normal conflict in that it passes the entire set of conflicting events ()
        test <@ AppendResult.Conflict (0L,[||]) = res @>
        if eventsInTip then verifyRequestChargesMax 12 // 11.49
        else verifyRequestChargesMax 12 // 11.18
        capture.Clear()

        // Now write at the correct position
        let expected = TestEvents.Create(0,1)
        let! res = Events.append ctx streamName 0L expected
        test <@ AppendResult.Ok 1L = res @>
        test <@ [EqxAct.Append] = capture.ExternalCalls @>
        if eventsInTip then verifyRequestChargesMax 27 // 26.4
        else verifyRequestChargesMax 40 // 39.39
        capture.Clear()

        // Try overwriting it (a competing consumer would see the same)
        let! res = Events.append ctx streamName 0L <| TestEvents.Create(-42,2)
        // This time we get passed the conflicting events - we pay a little for that, but that's unavoidable
        match eventsInTip, res, capture.ExternalCalls with
        | true, AppendResult.Conflict (1L, e), [EqxAct.Resync] ->
            verifyCorrectEvents 0L expected e
            verifyRequestChargesMax 10 // 9.93
        | false, AppendResult.ConflictUnknown 1L, [EqxAct.Conflict] ->
            verifyRequestChargesMax 12 // 11.9
        | x -> x |> failwithf "Unexpected %A"
    }

    (* Forward *)

    [<AutoData(MaxTest = 2, SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let get (eventsInTip, TestStream streamName) = Async.RunSynchronously <| async {
        let ctx = createPrimaryEventsContext log 3 (if eventsInTip then 10 else 0)

        // We're going to ignore the first, to prove we can
        let! expected = add6EventsIn2Batches ctx streamName
        let expected = Array.tail expected

        let! res = Events.get ctx streamName 1L 10

        verifyCorrectEvents 1L expected res

        test <@ [EqxAct.ResponseForward; EqxAct.QueryForward] = capture.ExternalCalls @>
        if eventsInTip then verifyRequestChargesMax 3
        else verifyRequestChargesMax 5 // (4.15) // WAS 13 with SDK bugs// 12.81
    }

    [<AutoData(MaxTest = 2, SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``get in 2 batches`` (eventsInTip, TestStream streamName) = Async.RunSynchronously <| async {
        let ctx = createPrimaryEventsContext log 1 (if eventsInTip then 1 else 0)

        let! expected = add6EventsIn2BatchesEx ctx streamName 2
        let expected = expected |> Array.take 3

        let! res = Events.get ctx streamName 0L 3

        verifyCorrectEvents 0L expected res

        // 2 items atm
        test <@ [EqxAct.ResponseForward; EqxAct.ResponseForward; EqxAct.QueryForward] = capture.ExternalCalls @>
        verifyRequestChargesMax 7 // 6.01
    }

    [<AutoData(MaxTest = 2, SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``get Lazy`` (eventsInTip, TestStream streamName) = Async.RunSynchronously <| async {
        let ctx = createPrimaryEventsContext log 1 (if eventsInTip then 3 else 0)

        let! expected = add6EventsIn2BatchesEx ctx streamName 4

        let! res = Events.getAll ctx streamName 0L 1 |> AsyncSeq.concatSeq |> AsyncSeq.takeWhileInclusive (fun _ -> false) |> AsyncSeq.toArrayAsync
        let expected = expected |> Array.take 1

        verifyCorrectEvents 0L expected res
        test <@ [EqxAct.ResponseForward; EqxAct.QueryForward] = capture.ExternalCalls @>
        let queryRoundTripsAndItemCounts = function
            | EqxEvent (Equinox.CosmosStore.Core.Log.Event.Query (Equinox.CosmosStore.Core.Direction.Forward, responses, { count = c })) -> Some (responses,c)
            | _ -> None
        // validate that, because we stopped after 1 item, we only needed one trip (which contained 4 events)
        [1,4] =! capture.ChooseCalls queryRoundTripsAndItemCounts
        if eventsInTip then verifyRequestChargesMax 4 // 3.06
        else verifyRequestChargesMax 4 // 3.08
    }

    (* Backward *)

    [<AutoData(MaxTest = 2, SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let getBackwards (eventsInTip, TestStream streamName) = Async.RunSynchronously <| async {
        let ctx = createPrimaryEventsContext log 1 (if eventsInTip then 1 else 0)

        let! expected = add6EventsIn2Batches ctx streamName

        // We want to skip reading the last
        let expected = Array.take 5 expected |> Array.tail

        let! res = Events.getBackwards ctx streamName 4L 4

        verifyCorrectEventsBackward 4L expected res

        test <@ [EqxAct.ResponseBackward; EqxAct.QueryBackward] = capture.ExternalCalls @>
        verifyRequestChargesMax 3
    }

    [<AutoData(MaxTest = 2, SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``getBackwards in 2 batches`` (eventsInTip, TestStream streamName) = Async.RunSynchronously <| async {
        let ctx = createPrimaryEventsContext log 1 (if eventsInTip then 1 else 0)

        let! expected = add6EventsIn2BatchesEx ctx streamName 2

        // We want to skip reading the last two, which means getting both, but disregarding some of the second batch
        let expected = Array.take 4 expected

        let! res = Events.getBackwards ctx streamName 3L 4

        verifyCorrectEventsBackward 3L expected res

        test <@ List.replicate 2 EqxAct.ResponseBackward @ [EqxAct.QueryBackward] = capture.ExternalCalls @>
        verifyRequestChargesMax 7 // 6.01
    }

    [<AutoData(MaxTest = 2, SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``getBackwards Lazy`` (eventsInTip, TestStream streamName) = Async.RunSynchronously <| async {
        let ctx = createPrimaryEventsContext log 1 (if eventsInTip then 3 else 0)

        let! expected = add6EventsIn2BatchesEx ctx streamName 4

        let! res =
            Events.getAllBackwards ctx streamName 10L 1
            |> AsyncSeq.concatSeq
            |> AsyncSeq.takeWhileInclusive (fun x -> x.Index <> 4L)
            |> AsyncSeq.toArrayAsync
        let expected = expected |> Array.skip 4 // omit index 0, 1 as we vote to finish at 2L

        verifyCorrectEventsBackward 5L expected res
        // only 1 request of 1 item triggered
        let pages = if eventsInTip then 1 else 2 // in V2, the query excluded the Tip; in V3 with tipMaxEvents, we return the tip but it has no data
        test <@ [yield! Seq.replicate pages EqxAct.ResponseBackward; EqxAct.QueryBackward] = capture.ExternalCalls @>
        // validate that, despite only requesting max 1 item, we only needed one trip, bearing 5 items (from which one item was omitted)
        let queryRoundTripsAndItemCounts = function
            | EqxEvent (Equinox.CosmosStore.Core.Log.Event.Query (Equinox.CosmosStore.Core.Direction.Backward, responses, { count = c })) -> Some (responses,c)
            | _ -> None
        let expectedPagesAndEvents = [pages, 2]
        expectedPagesAndEvents =! capture.ChooseCalls queryRoundTripsAndItemCounts
        if eventsInTip then verifyRequestChargesMax 3 // 2.98
        else verifyRequestChargesMax 6 // 5.66
    }

    (* Prune *)

    [<AutoData(MaxTest = 2, SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let prune (eventsInTip, TestStream streamName) = Async.RunSynchronously <| async {
        let ctx, ctxUnsafe = createPrimaryEventsContextWithUnsafe log 10 (if eventsInTip then 3 else 0)
        let! expected = add6EventsIn2BatchesEx ctx streamName 4

        // We should still the correct high-water mark even if we don't delete anything
        capture.Clear()
        let! deleted, deferred, trimmedPos = Events.pruneUntil ctx streamName -1L
        test <@ deleted = 0 && deferred = 0 && trimmedPos = 0L @>
        test <@ [EqxAct.PruneResponse; EqxAct.Prune] = capture.ExternalCalls @>
        verifyRequestChargesMax 3 // 2.86

        // Trigger deletion of first batch, but as we're in the middle of the next Batch...
        capture.Clear()
        let! deleted, deferred, trimmedPos = Events.pruneUntil ctx streamName 4L
        if eventsInTip then // the Tip has a Trim operation applied to remove one event
            test <@ deleted = 5 && deferred = 0 && trimmedPos = 5L @>
            test <@ [EqxAct.PruneResponse; EqxAct.Delete; EqxAct.Trim; EqxAct.Prune] = capture.ExternalCalls @>
            verifyRequestChargesMax 39 // 13.33 + 22.33 + 2.86
        else // the one event from the final batch needs to be deferred
            test <@ deleted = 4 && deferred = 1 && trimmedPos = 4L @>
            test <@ [EqxAct.PruneResponse; EqxAct.Delete; EqxAct.Prune] = capture.ExternalCalls @>
            verifyRequestChargesMax 17 // [13.33; 2.9]

        let pos = if eventsInTip then 5L else 4L
        let! res = Events.get ctxUnsafe streamName 0L Int32.MaxValue
        verifyCorrectEvents pos (Array.skip (int pos) expected) res

        // Repeat the process, but this time there should be no actual deletes
        capture.Clear()
        let! deleted, deferred, trimmedPos = Events.pruneUntil ctx streamName 4L
        test <@ deleted = 0 && deferred = (if eventsInTip then 0 else 1) && trimmedPos = pos @>
        test <@ [EqxAct.PruneResponse; EqxAct.Prune] = capture.ExternalCalls @>
        verifyRequestChargesMax 3 // 2.86

        // We should still get the high-water mark even if we asked for less
        capture.Clear()
        let! deleted, deferred, trimmedPos = Events.pruneUntil ctx streamName 3L
        test <@ deleted = 0 && deferred = 0 && trimmedPos = pos @>
        test <@ [EqxAct.PruneResponse; EqxAct.Prune] = capture.ExternalCalls @>
        verifyRequestChargesMax 3 // 2.86

        let! res = Events.get ctxUnsafe streamName 0L Int32.MaxValue
        verifyCorrectEvents pos (Array.skip (int pos) expected) res

        // Delete second batch
        capture.Clear()
        let! deleted, deferred, trimmedPos = Events.pruneUntil ctx streamName 5L
        test <@ deleted = (if eventsInTip then 1 else 2) && deferred = 0 && trimmedPos = 6L @>
        test <@ [EqxAct.PruneResponse; (if eventsInTip then EqxAct.Trim else EqxAct.Delete); EqxAct.Prune] = capture.ExternalCalls @>
        if eventsInTip then verifyRequestChargesMax 26 // [22.33; 2.83]
        else verifyRequestChargesMax 17 // 13.33 + 2.86

        let! res = Events.get ctxUnsafe streamName 0L Int32.MaxValue
        test <@ [||] = res @>

        // Attempt to repeat
        capture.Clear()
        let! deleted, deferred, trimmedPos = Events.pruneUntil ctx streamName 6L
        test <@ deleted = 0 && deferred = 0 && trimmedPos = 6L @>
        test <@ [EqxAct.PruneResponse; EqxAct.Prune] = capture.ExternalCalls @>
        verifyRequestChargesMax 3 // 2.83
    }

    (* Fallback *)

    [<AutoData(MaxTest = 2, SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let fallback (eventsInTip, TestStream streamName) = Async.RunSynchronously <| async {
        let ctx1, ctx1Unsafe = createPrimaryEventsContextWithUnsafe log 10 (if eventsInTip then 3 else 0)
        let ctx2 = createSecondaryEventsContext log defaultQueryMaxItems
        let ctx12 = createFallbackEventsContext log defaultQueryMaxItems

        let! expected = add6EventsIn2BatchesEx ctx1 streamName 4
        // Add the same events to the secondary container
        let! _ = add6EventsIn2Batches ctx2 streamName

        // Trigger deletion of first batch from primary
        let! deleted, deferred, trimmedPos = Events.pruneUntil ctx1 streamName 4L
        if eventsInTip then test <@ deleted = 5 && deferred = 0 && trimmedPos = 5L @>
        else test <@ deleted = 4 && deferred = 1 && trimmedPos = 4L @>

        // Prove it's gone
        capture.Clear()
        let! res = Events.get ctx1Unsafe streamName 0L Int32.MaxValue
        test <@ [EqxAct.ResponseForward; EqxAct.QueryForward] = capture.ExternalCalls @>
        let pos = if eventsInTip then 5L else 4L
        verifyCorrectEvents pos (Array.skip (int pos) expected) res
        if eventsInTip then verifyRequestChargesMax 3 // 2.83
        else verifyRequestChargesMax 4 // 3.04

        // Prove the full set exists in the secondary
        capture.Clear()
        let! res = Events.get ctx2 streamName 0L Int32.MaxValue
        test <@ [EqxAct.ResponseForward; EqxAct.QueryForward] = capture.ExternalCalls @>
        verifyCorrectEvents 0L expected res
        verifyRequestChargesMax 4 // 3.09

        // Prove we can fallback with full set in secondary
        capture.Clear()
        let! res = Events.get ctx12 streamName 0L Int32.MaxValue
        test <@ [EqxAct.ResponseForward; EqxAct.QueryForward; EqxAct.ResponseForward; EqxAct.QueryForward] = capture.ExternalCalls @>
        verifyCorrectEvents 0L expected res
        verifyRequestChargesMax 7 // 3.04 + 3.06

        // Delete second batch in primary
        capture.Clear()
        let! deleted, deferred, trimmedPos = Events.pruneUntil ctx1 streamName 5L
        test <@ deleted = (if eventsInTip then 1 else 2) && deferred = 0 && trimmedPos = 6L @>

        // Nothing left in primary
        capture.Clear()
        let! res = Events.get ctx1Unsafe streamName 0L Int32.MaxValue
        test <@ [EqxAct.ResponseForward; EqxAct.QueryForward] = capture.ExternalCalls @>
        test <@ [||] = res @>
        verifyRequestChargesMax 3 // 2.99

        // But primary retains high-water mark
        capture.Clear()
        let! res = Events.getNextIndex ctx1 streamName
        test <@ [EqxAct.Tip] = capture.ExternalCalls @>
        test <@ 6L = res @>
        verifyRequestChargesMax 1

        // Fallback queries secondary (unless we actually delete the Tip too)
        capture.Clear()
        let! res = Events.get ctx12 streamName 0L Int32.MaxValue
        test <@ [EqxAct.ResponseForward; EqxAct.QueryForward; EqxAct.ResponseForward; EqxAct.QueryForward] = capture.ExternalCalls @>
        verifyCorrectEvents 0L expected res
        verifyRequestChargesMax 7 // 2.99+3.09

        // NOTE lazy variants don't presently apply fallback logic
    }
