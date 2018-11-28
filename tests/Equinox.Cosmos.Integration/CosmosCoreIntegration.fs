﻿module Equinox.Cosmos.Integration.CoreIntegration

open Equinox.Cosmos.Integration.Infrastructure
open Equinox.Cosmos
open Equinox.Cosmos.Core
open FSharp.Control
open Newtonsoft.Json.Linq
open Swensen.Unquote
open Serilog
open System
open System.Text

#nowarn "1182" // From hereon in, we may have some 'unused' privates (the tests)

type EventData = { eventType:string; data: byte[] } with
    interface Events.IEvent with
        member __.EventType = __.eventType
        member __.Data = __.data
        member __.Meta = Encoding.UTF8.GetBytes("{\"m\":\"m\"}")
    static member private Create(i, ?eventType, ?json) : Events.IEvent =
        {   eventType = sprintf "%s:%d" (defaultArg eventType "test_event") i
            data = System.Text.Encoding.UTF8.GetBytes(defaultArg json "{\"d\":\"d\"}") } :> _
    static member Create(i, c) = Array.init c (fun x -> EventData.Create(x+i))

type Tests(testOutputHelper) =
    inherit TestsWithLogCapture(testOutputHelper)
    let log, capture = base.Log, base.Capture

    /// As we generate side-effects per run, we want each  FSCheck-triggered invocation of the test run to work in its own stream
    let testIterations = ref 0
    let (|TestStream|) (name: Guid) =
        incr testIterations
        sprintf "events-%O-%i" name !testIterations
    let mkContextWithItemLimit conn defaultBatchSize =
        EqxContext(conn,collections,log,?defaultMaxItems=defaultBatchSize,maxEventsPerSlice=10)
    let mkContext conn = mkContextWithItemLimit conn None

    let verifyRequestChargesMax rus =
        let tripRequestCharges = [ for e, c in capture.RequestCharges -> sprintf "%A" e, c ]
        test <@ float rus >= Seq.sum (Seq.map snd tripRequestCharges) @>

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let append (TestStream streamName) = Async.RunSynchronously <| async {
        let! conn = connectToSpecifiedCosmosOrSimulator log
        let ctx = mkContext conn

        let index = 0L
        let! res = Events.append ctx streamName index <| EventData.Create(0,1)
        test <@ AppendResult.Ok 1L = res @>
        test <@ [EqxAct.Append] = capture.ExternalCalls @>
        verifyRequestChargesMax 10
        // Clear the counters
        capture.Clear()

        let! res = Events.append ctx streamName 1L <| EventData.Create(1,5)
        test <@ AppendResult.Ok 6L = res @>
        test <@ [EqxAct.Append] = capture.ExternalCalls @>
        // We didnt request small batches or splitting so it's not dramatically more expensive to write N events
        verifyRequestChargesMax 29 // observed 28.61 // was 11
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

    let add6EventsIn2Batches ctx streamName = async {
        let index = 0L
        let! res = Events.append ctx streamName index <| EventData.Create(0,1)

        test <@ AppendResult.Ok 1L = res @>
        let! res = Events.append ctx streamName 1L <| EventData.Create(1,5)
        test <@ AppendResult.Ok 6L = res @>
        // Only start counting RUs from here
        capture.Clear()
        return EventData.Create(0,6)
    }

    let verifyCorrectEventsEx direction baseIndex (expected: Events.IEvent []) (xs: Events.IIndexedEvent[]) =
        let xs, baseIndex =
            if direction = Direction.Forward then xs, baseIndex
            else Array.rev xs, baseIndex - int64 (Array.length expected) + 1L
        test <@ [for i in 0..expected.Length - 1 -> baseIndex + int64 i] = [for r in xs -> r.Index] @>
        test <@ [for e in expected -> e.EventType] = [ for r in xs -> r.EventType ] @>
        for i,x,y in Seq.mapi2 (fun i x y -> i,x,y) [for e in expected -> e.Data] [for r in xs -> r.Data] do
            verifyUtf8JsonEquals i x y
    let verifyCorrectEventsBackward = verifyCorrectEventsEx Direction.Backward
    let verifyCorrectEvents = verifyCorrectEventsEx Direction.Forward

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``appendAtEnd and getNextIndex`` (extras, TestStream streamName) = Async.RunSynchronously <| async {
        let! conn = connectToSpecifiedCosmosOrSimulator log
        let ctx = mkContextWithItemLimit conn (Some 1)

        // If a fail triggers a rerun, we need to dump the previous log entries captured
        capture.Clear()
        let! pos = Events.getNextIndex ctx streamName
        test <@ [EqxAct.TipNotFound] = capture.ExternalCalls @>
        0L =! pos
        verifyRequestChargesMax 1 // for a 404 by definition
        capture.Clear()

        let mutable pos = 0L
        for appendBatchSize in [4; 5; 9] do
            let! res = Events.appendAtEnd ctx streamName <| EventData.Create (int pos,appendBatchSize)
            test <@ [EqxAct.Append] = capture.ExternalCalls @>
            pos <- pos + int64 appendBatchSize
            pos =! res
            verifyRequestChargesMax 20 // 15.59 observed
            capture.Clear()

            let! res = Events.getNextIndex ctx streamName
            test <@ [EqxAct.Tip] = capture.ExternalCalls @>
            verifyRequestChargesMax 2
            pos =! res
            capture.Clear()

        let! res = Events.appendAtEnd ctx streamName <| EventData.Create (int pos,42)
        pos <- pos + 42L
        pos =! res
        test <@ [EqxAct.Append] = capture.ExternalCalls @>
        verifyRequestChargesMax 20
        capture.Clear()

        let! res = Events.getNextIndex ctx streamName
        test <@ [EqxAct.Tip] = capture.ExternalCalls @>
        verifyRequestChargesMax 2
        capture.Clear()
        pos =! res

        // Demonstrate benefit/mechanism for using the Position-based API to avail of the etag tracking
        let stream  = ctx.CreateStream streamName

        let extrasCount = match extras with x when x > 50 -> 5000 | x when x < 1 -> 1 | x -> x*100
        let! _pos = ctx.NonIdempotentAppend(stream, EventData.Create (int pos,extrasCount))
        test <@ [EqxAct.Append] = capture.ExternalCalls @>
        verifyRequestChargesMax 300 // 278 observed
        capture.Clear()

        let! pos = ctx.Sync(stream,?position=None)
        test <@ [EqxAct.Tip] = capture.ExternalCalls @>
        verifyRequestChargesMax 50 // 41 observed // for a 200, you'll pay a lot (we omitted to include the position that NonIdempotentAppend yielded)
        capture.Clear()

        let! _pos = ctx.Sync(stream,pos)
        test <@ [EqxAct.TipNotModified] = capture.ExternalCalls @>
        verifyRequestChargesMax 1 // for a 302 by definition - when an etag IfNotMatch is honored, you only pay one RU
        capture.Clear()
    }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``append - fails on non-matching`` (TestStream streamName) = Async.RunSynchronously <| async {
        let! conn = connectToSpecifiedCosmosOrSimulator log
        let ctx = mkContext conn

        // Attempt to write, skipping Index 0
        let! res = Events.append ctx streamName 1L <| EventData.Create(0,1)
        test <@ [EqxAct.Resync] = capture.ExternalCalls @>
        // The response aligns with a normal conflict in that it passes the entire set of conflicting events ()
        test <@ AppendResult.Conflict (0L,[||]) = res @>
        verifyRequestChargesMax 5
        capture.Clear()

        // Now write at the correct position
        let expected = EventData.Create(1,1)
        let! res = Events.append ctx streamName 0L expected
        test <@ AppendResult.Ok 1L = res @>
        test <@ [EqxAct.Append] = capture.ExternalCalls @>
        verifyRequestChargesMax 10
        capture.Clear()

        // Try overwriting it (a competing consumer would see the same)
        let! res = Events.append ctx streamName 0L <| EventData.Create(-42,2)
        // This time we get passed the conflicting events - we pay a little for that, but that's unavoidable
        match res with
        | AppendResult.Conflict (1L, e) -> verifyCorrectEvents 0L expected e
        | x -> x |> failwithf "Unexpected %A"
        test <@ [EqxAct.Resync] = capture.ExternalCalls @>
        verifyRequestChargesMax 5 // 4.02
        capture.Clear()
    }

    (* Forward *)

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let get (TestStream streamName) = Async.RunSynchronously <| async {
        let! conn = connectToSpecifiedCosmosOrSimulator log
        let ctx = mkContextWithItemLimit conn (Some 3)

        // We're going to ignore the first, to prove we can
        let! expected = add6EventsIn2Batches ctx streamName
        let expected = Array.tail expected

        let! res = Events.get ctx streamName 1L 10

        verifyCorrectEvents 1L expected res

        test <@ [EqxAct.ResponseForward; EqxAct.QueryForward] = capture.ExternalCalls @>
        verifyRequestChargesMax 4 // 3.14 // was 3 before introduction of multi-event batches
    }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``get (in 2 batches)`` (TestStream streamName) = Async.RunSynchronously <| async {
        let! conn = connectToSpecifiedCosmosOrSimulator log
        let ctx = mkContextWithItemLimit conn (Some 1)

        let! expected = add6EventsIn2Batches ctx streamName
        let expected = expected |> Array.take 3

        let! res = Events.get ctx streamName 0L 3

        verifyCorrectEvents 0L expected res

        // 2 items atm
        test <@ [EqxAct.ResponseForward; EqxAct.ResponseForward; EqxAct.QueryForward] = capture.ExternalCalls @>
        verifyRequestChargesMax 6 } // 5.77

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``get Lazy`` (TestStream streamName) = Async.RunSynchronously <| async {
        let! conn = connectToSpecifiedCosmosOrSimulator log
        let ctx = mkContextWithItemLimit conn (Some 1)

        let! expected = add6EventsIn2Batches ctx streamName
        capture.Clear()

        let! res = Events.getAll ctx streamName 0L 1 |> AsyncSeq.concatSeq |> AsyncSeq.takeWhileInclusive (fun _ -> false) |> AsyncSeq.toArrayAsync
        let expected = expected |> Array.take 1

        verifyCorrectEvents 0L expected res
        test <@ [EqxAct.ResponseForward; EqxAct.QueryForward] = capture.ExternalCalls @>
        let queryRoundTripsAndItemCounts = function EqxEvent (Log.Query (Direction.Forward, responses, { count = c })) -> Some (responses,c) | _ -> None
        // validate that, despite only requesting max 1 item, we only needed one trip (which contained only one item)
        [1,1] =! capture.ChooseCalls queryRoundTripsAndItemCounts
        verifyRequestChargesMax 3 // 2.97
    }

    (* Backward *)

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let getBackwards (TestStream streamName) = Async.RunSynchronously <| async {
        let! conn = connectToSpecifiedCosmosOrSimulator log
        let ctx = mkContextWithItemLimit conn (Some 1)

        let! expected = add6EventsIn2Batches ctx streamName

        // We want to skip reading the last
        let expected = Array.take 5 expected |> Array.tail

        let! res = Events.getBackwards ctx streamName 4L 4

        verifyCorrectEventsBackward 4L expected res

        test <@ [EqxAct.ResponseBackward; EqxAct.QueryBackward] = capture.ExternalCalls @>
        verifyRequestChargesMax 3
    }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``getBackwards (2 batches)`` (TestStream streamName) = Async.RunSynchronously <| async {
        let! conn = connectToSpecifiedCosmosOrSimulator log
        let ctx = mkContextWithItemLimit conn (Some 1)

        let! expected = add6EventsIn2Batches ctx streamName

        // We want to skip reading the last two, which means getting both, but disregarding some of the second batch
        let expected = Array.take 4 expected

        let! res = Events.getBackwards ctx streamName 3L 4

        verifyCorrectEventsBackward 3L expected res

        test <@ List.replicate 2 EqxAct.ResponseBackward @ [EqxAct.QueryBackward] = capture.ExternalCalls @>
        verifyRequestChargesMax 6 // 5.77
    }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``getBackwards Lazy`` (TestStream streamName) = Async.RunSynchronously <| async {
        let! conn = connectToSpecifiedCosmosOrSimulator log
        let ctx = mkContextWithItemLimit conn (Some 1)

        let! expected = add6EventsIn2Batches ctx streamName
        capture.Clear()

        let! res = Events.getAllBackwards ctx streamName 10L 1 |> AsyncSeq.concatSeq |> AsyncSeq.takeWhileInclusive (fun x -> x.Index <> 2L) |> AsyncSeq.toArrayAsync
        let expected = expected |> Array.skip 2 // omit index 0, 1 as we vote to finish at 2L

        verifyCorrectEventsBackward 5L expected res
        // only 1 request of 1 item triggered
        test <@ [EqxAct.ResponseBackward; EqxAct.QueryBackward] = capture.ExternalCalls @>
        // validate that, despite only requesting max 1 item, we only needed one trip, bearing 5 items (from which one item was omitted)
        let queryRoundTripsAndItemCounts = function EqxEvent (Log.Query (Direction.Backward, responses, { count = c })) -> Some (responses,c) | _ -> None
        [1,5] =! capture.ChooseCalls queryRoundTripsAndItemCounts
        verifyRequestChargesMax 3 // 2.98
    }