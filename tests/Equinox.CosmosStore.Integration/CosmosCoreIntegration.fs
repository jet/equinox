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
    let mkContextWithItemLimit log maxItems =
        createPrimaryEventsContextEx log (Some maxItems) 10
    let mkContext log = mkContextWithItemLimit log 10

    let verifyRequestChargesMax rus =
        let tripRequestCharges = [ for e, c in capture.RequestCharges -> sprintf "%A" e, c ]
        test <@ float rus >= Seq.sum (Seq.map snd tripRequestCharges) @>

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let append (TestStream streamName) = Async.RunSynchronously <| async {
        let ctx = mkContext log
        capture.Clear()

        let index = 0L
        let! res = Events.append ctx streamName index <| TestEvents.Create(0,1)
        test <@ AppendResult.Ok 1L = res @>
        test <@ [EqxAct.Append] = capture.ExternalCalls @>
#if !NO_EVENTS_IN_TIP
        verifyRequestChargesMax 19 // 18.5
#else
        verifyRequestChargesMax 34 // 33.07
#endif
        // Clear the counters
        capture.Clear()

        let! res = Events.append ctx streamName 1L <| TestEvents.Create(1,5)
        test <@ AppendResult.Ok 6L = res @>
        test <@ [EqxAct.Append] = capture.ExternalCalls @>
        // We didnt request small batches or splitting so it's not dramatically more expensive to write N events
#if !NO_EVENTS_IN_TIP
        verifyRequestChargesMax 26 // 25.69
#else
        verifyRequestChargesMax 41 // 40.68
#endif
    }

    // It's conceivable that in the future we might allow zero-length batches as long as a sync mechanism leveraging the etags and unfolds update mechanisms
    // As it stands with the NoTipEvents stored proc, permitting empty batches a) yields an invalid state b) provides no conceivable benefit
    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``append Throws when passed an empty batch`` (TestStream streamName) = Async.RunSynchronously <| async {
        let ctx = mkContext log

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

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``appendAtEnd and getNextIndex`` (extras, TestStream streamName) = Async.RunSynchronously <| async {
        // If a fail triggers a rerun, we need to dump the previous log entries captured
        capture.Clear()

        let ctx = mkContextWithItemLimit log 1

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
            verifyRequestChargesMax 46 // 45.16
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
#if !NO_EVENTS_IN_TIP
        verifyRequestChargesMax 43 // was 42.19
#else
        verifyRequestChargesMax 50 // 49.74
#endif
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
        verifyRequestChargesMax 448 // 447.5 // 463.01 observed
        capture.Clear()

        let! pos = ctx.Sync(stream,?position=None)
        test <@ [EqxAct.Tip] = capture.ExternalCalls @>
        verifyRequestChargesMax 5 // 41 observed // for a 200, you'll pay a lot (we omitted to include the position that NonIdempotentAppend yielded)
        capture.Clear()

        let! _pos = ctx.Sync(stream,pos)
        test <@ [EqxAct.TipNotModified] = capture.ExternalCalls @>
        verifyRequestChargesMax 1 // for a 302 by definition - when an etag IfNotMatch is honored, you only pay one RU
    }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``append - fails on non-matching`` (TestStream streamName) = Async.RunSynchronously <| async {
        let ctx = mkContext log

        // Attempt to write, skipping Index 0
        let! res = Events.append ctx streamName 1L <| TestEvents.Create(0,1)
        test <@ [EqxAct.Resync] = capture.ExternalCalls @>
        // The response aligns with a normal conflict in that it passes the entire set of conflicting events ()
        test <@ AppendResult.Conflict (0L,[||]) = res @>
#if !NO_EVENTS_IN_TIP
        verifyRequestChargesMax 5 // 4.96
#else
        verifyRequestChargesMax 6 // 5.5
#endif
        capture.Clear()

        // Now write at the correct position
        let expected = TestEvents.Create(0,1)
        let! res = Events.append ctx streamName 0L expected
        test <@ AppendResult.Ok 1L = res @>
        test <@ [EqxAct.Append] = capture.ExternalCalls @>
#if !NO_EVENTS_IN_TIP
        verifyRequestChargesMax 18 // 17.62
#else
        verifyRequestChargesMax 36 // 35.78
#endif
        capture.Clear()

        // Try overwriting it (a competing consumer would see the same)
        let! res = Events.append ctx streamName 0L <| TestEvents.Create(-42,2)
        // This time we get passed the conflicting events - we pay a little for that, but that's unavoidable
        match res with
#if !NO_EVENTS_IN_TIP
        | AppendResult.Conflict (1L, e) -> verifyCorrectEvents 0L expected e
#else
        | AppendResult.ConflictUnknown 1L -> ()
#endif
        | x -> x |> failwithf "Unexpected %A"
#if !NO_EVENTS_IN_TIP
        test <@ [EqxAct.Resync] = capture.ExternalCalls @>
        verifyRequestChargesMax 4 // 3.94
#else
        test <@ [EqxAct.Conflict] = capture.ExternalCalls @>
        verifyRequestChargesMax 6 // 5.63 // 6.64
#endif
    }

    (* Forward *)

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let get (TestStream streamName) = Async.RunSynchronously <| async {
        capture.Clear()
        let ctx = mkContextWithItemLimit log 3

        // We're going to ignore the first, to prove we can
        let! expected = add6EventsIn2Batches ctx streamName
        let expected = Array.tail expected

        let! res = Events.get ctx streamName 1L 10

        verifyCorrectEvents 1L expected res

        test <@ [EqxAct.ResponseForward; EqxAct.QueryForward] = capture.ExternalCalls @>
#if !NO_EVENTS_IN_TIP
        verifyRequestChargesMax 3
#else
        verifyRequestChargesMax 5 // (4.15) // WAS 13 with SDK bugs// 12.81 // was 3 before introduction of multi-event batches
#endif
    }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``get in 2 batches`` (TestStream streamName) = Async.RunSynchronously <| async {
        let ctx = createPrimaryEventsContextEx log (Some 1) 0

        let! expected = add6EventsIn2Batches ctx streamName
        let expected = expected |> Array.take 3

        let! res = Events.get ctx streamName 0L 3

        verifyCorrectEvents 0L expected res

        // 2 items atm
        test <@ [EqxAct.ResponseForward; EqxAct.ResponseForward; EqxAct.QueryForward] = capture.ExternalCalls @>
        verifyRequestChargesMax 7 // 6.01
    }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``get Lazy`` (TestStream streamName) = Async.RunSynchronously <| async {
        let ctx = createPrimaryEventsContextEx log (Some 1) 0

        let! expected = add6EventsIn2Batches ctx streamName

        let! res = Events.getAll ctx streamName 0L 1 |> AsyncSeq.concatSeq |> AsyncSeq.takeWhileInclusive (fun _ -> false) |> AsyncSeq.toArrayAsync
        let expected = expected |> Array.take 1

        verifyCorrectEvents 0L expected res
        test <@ [EqxAct.ResponseForward; EqxAct.QueryForward] = capture.ExternalCalls @>
        let queryRoundTripsAndItemCounts = function
            | EqxEvent (Equinox.CosmosStore.Core.Log.Event.Query (Equinox.CosmosStore.Core.Direction.Forward, responses, { count = c })) -> Some (responses,c)
            | _ -> None
        // validate that, despite only requesting max 1 item, we only needed one trip (which contained only one item)
        [1,1] =! capture.ChooseCalls queryRoundTripsAndItemCounts
        verifyRequestChargesMax 3 // 2.97 (2.88 in Tip)
    }

    (* Backward *)

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let getBackwards (TestStream streamName) = Async.RunSynchronously <| async {
        let ctx = mkContextWithItemLimit log 1

        let! expected = add6EventsIn2Batches ctx streamName

        // We want to skip reading the last
        let expected = Array.take 5 expected |> Array.tail

        let! res = Events.getBackwards ctx streamName 4L 4

        verifyCorrectEventsBackward 4L expected res

        test <@ [EqxAct.ResponseBackward; EqxAct.QueryBackward] = capture.ExternalCalls @>
        verifyRequestChargesMax 3
    }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``getBackwards in 2 batches`` (TestStream streamName) = Async.RunSynchronously <| async {
        let ctx = createPrimaryEventsContextEx log (Some 1) 0

        let! expected = add6EventsIn2Batches ctx streamName

        // We want to skip reading the last two, which means getting both, but disregarding some of the second batch
        let expected = Array.take 4 expected

        let! res = Events.getBackwards ctx streamName 3L 4

        verifyCorrectEventsBackward 3L expected res

        test <@ List.replicate 2 EqxAct.ResponseBackward @ [EqxAct.QueryBackward] = capture.ExternalCalls @>
        verifyRequestChargesMax 7 // 6.01
    }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``getBackwards Lazy`` (TestStream streamName) = Async.RunSynchronously <| async {
        let ctx = createPrimaryEventsContextEx log (Some 1) 3

        let! expected = add6EventsIn2BatchesEx ctx streamName 4

        let! res =
            Events.getAllBackwards ctx streamName 10L 1
            |> AsyncSeq.concatSeq
            |> AsyncSeq.takeWhileInclusive (fun x -> x.Index <> 4L)
            |> AsyncSeq.toArrayAsync
        let expected = expected |> Array.skip 4 // omit index 0, 1 as we vote to finish at 2L

        verifyCorrectEventsBackward 5L expected res
        // only 1 request of 1 item triggered
        test <@ [EqxAct.ResponseBackward; EqxAct.QueryBackward] = capture.ExternalCalls @>
        // validate that, despite only requesting max 1 item, we only needed one trip, bearing 5 items (from which one item was omitted)
        let queryRoundTripsAndItemCounts = function
            | EqxEvent (Equinox.CosmosStore.Core.Log.Event.Query (Equinox.CosmosStore.Core.Direction.Backward, responses, { count = c })) -> Some (responses,c)
            | _ -> None
        [1, 2] =! capture.ChooseCalls queryRoundTripsAndItemCounts
        verifyRequestChargesMax 3 // 2.98
    }

    (* Prune *)

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let prune (TestStream streamName) = Async.RunSynchronously <| async {
        let ctx = mkContextWithItemLimit log 10

        let! expected = add6EventsIn2Batches ctx streamName

        // Trigger deletion of first batch
        let! deleted, deferred, trimmedPos = Events.prune ctx streamName 5L
        test <@ deleted = 1 && deferred = 4 && trimmedPos = 1L @>
        test <@ [EqxAct.PruneResponse; EqxAct.Delete; EqxAct.Prune] = capture.ExternalCalls @>
        verifyRequestChargesMax 17 // 13.33 + 2.9

        let! res = Events.get ctx streamName 0L Int32.MaxValue
        verifyCorrectEvents 1L (Array.skip 1 expected) res

        // Repeat the process, but this time there should be no actual deletes
        capture.Clear()
        let! deleted, deferred, trimmedPos = Events.prune ctx streamName 4L
        test <@ deleted = 0 && deferred = 3 && trimmedPos = 1L @>
        test <@ [EqxAct.PruneResponse; EqxAct.Prune] = capture.ExternalCalls @>
        verifyRequestChargesMax 3 // 2.86

        let! res = Events.get ctx streamName 0L Int32.MaxValue
        verifyCorrectEvents 1L (Array.skip 1 expected) res

        // Delete second batch
        capture.Clear()
        let! deleted, deferred, trimmedPos = Events.prune ctx streamName 6L
        test <@ deleted = 5 && deferred = 0 && trimmedPos = 6L @>
        test <@ [EqxAct.PruneResponse; EqxAct.Delete; EqxAct.Prune] = capture.ExternalCalls @>
        verifyRequestChargesMax 17 // 13.33 + 2.86

        let! res = Events.get ctx streamName 0L Int32.MaxValue
        test <@ [||] = res @>

        // Attempt to repeat
        capture.Clear()
        let! deleted, deferred, trimmedPos = Events.prune ctx streamName 6L
        test <@ deleted = 0 && deferred = 0 && trimmedPos = 6L @>
        test <@ [EqxAct.PruneResponse; EqxAct.Prune] = capture.ExternalCalls @>
        verifyRequestChargesMax 3 // 2.83
    }

    (* Fallback *)

#if NO_EVENTS_IN_TIP
    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let fallback (TestStream streamName) = Async.RunSynchronously <| async {
        let ctx1 = createPrimaryEventsContext log None
        let ctx2 = createSecondaryEventsContext log None
        let ctx12 = createFallbackEventsContext log None

        let! expected = add6EventsIn2Batches ctx1 streamName
        // Add the same events to the secondary container
        let! _ = add6EventsIn2Batches ctx2 streamName

        // Trigger deletion of first batch from primary
        let! deleted, deferred, trimmedPos = Events.prune ctx1 streamName 5L
        test <@ deleted = 1 && deferred = 4 && trimmedPos = 1L @>

        // Prove it's gone
        capture.Clear()
        let! res = Events.get ctx1 streamName 0L Int32.MaxValue
        test <@ [EqxAct.ResponseForward; EqxAct.QueryForward] = capture.ExternalCalls @>
        verifyCorrectEvents 1L (Array.skip 1 expected) res
        verifyRequestChargesMax 4 // 3.04

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
        let! deleted, deferred, trimmedPos = Events.prune ctx1 streamName 6L
        test <@ deleted = 5 && deferred = 0 && trimmedPos = 6L @>

        // Nothing left in primary
        capture.Clear()
        let! res = Events.get ctx1 streamName 0L Int32.MaxValue
        test <@ [EqxAct.ResponseForward; EqxAct.QueryForward] = capture.ExternalCalls @>
        test <@ [||] = res @>
        verifyRequestChargesMax 3 // 2.99

        // Fallback still does two queries (the first one is empty) // TODO demonstrate Primary read is only of Tip when using snapshots
        capture.Clear()
        let! res = Events.get ctx12 streamName 0L Int32.MaxValue
//        test <@ [EqxAct.ResponseForward; EqxAct.QueryForward; EqxAct.ResponseForward; EqxAct.QueryForward] = capture.ExternalCalls @>
        verifyCorrectEvents 0L expected res
        verifyRequestChargesMax 7 // 2.99+3.09

        // NOTE lazy variants don't presently apply fallback logic
    }
#endif
