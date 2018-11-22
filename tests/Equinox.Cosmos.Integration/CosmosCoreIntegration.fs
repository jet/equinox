module Equinox.Cosmos.Integration.CoreIntegration

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

module Null =
    let defaultValue d x = if x = null then d else x

type EventData = { eventType:string; data: byte[] } with
    interface Events.IEvent with
        member __.EventType = __.eventType
        member __.Data = __.data
        member __.Meta = Encoding.UTF8.GetBytes("{\"m\":\"m\"}")
    static member Create(eventType,?json) : Events.IEvent =
        {   eventType = eventType
            data = System.Text.Encoding.UTF8.GetBytes(defaultArg json "{\"d\":\"d\"}") } :> _

type Tests(testOutputHelper) =
    inherit TestsWithLogCapture(testOutputHelper)
    let log, capture = base.Log, base.Capture

    /// As we generate side-effects per run, we want each  FSCheck-triggered invocation of the test run to work in its own stream
    let testIterations = ref 0
    let (|TestStream|) (name:Guid) =
        incr testIterations
        sprintf "events-%O-%i" name !testIterations
    let (|TestDbCollStream|) (TestStream streamName) =
        streamName
    let mkContextWithItemLimit conn defaultBatchSize =
        EqxContext(conn,collections,log,?defaultMaxItems=defaultBatchSize)
    let mkContext conn = mkContextWithItemLimit conn None

    let verifyRequestChargesMax rus =
        let tripRequestCharges = [ for e, c in capture.RequestCharges -> sprintf "%A" e, c ]
        test <@ float rus >= Seq.sum (Seq.map snd tripRequestCharges) @>

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let append (TestDbCollStream (streamName)) = Async.RunSynchronously <| async {
        let! conn = connectToSpecifiedCosmosOrSimulator log
        let ctx = mkContext conn

        let event = EventData.Create("test_event")
        let index = 0L
        let! res = Events.append ctx streamName index [|event|]
        test <@ AppendResult.Ok 1L = res @>
        test <@ [EqxAct.Append] = capture.ExternalCalls @>
        verifyRequestChargesMax 12 // was 10, observed 11.03
        // Clear the counters
        capture.Clear()

        let! res = Events.append ctx streamName 1L (Array.replicate 5 event)
        test <@ AppendResult.Ok 6L = res @>
        test <@ [EqxAct.Append] = capture.ExternalCalls @>
        // We didnt request small batches or splitting so it's not dramatically more expensive to write N events
        verifyRequestChargesMax 30 // observed 26.62 was 11
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
        let event = EventData.Create("test_event")
        let! res = Events.append ctx streamName index [|event|]
        test <@ AppendResult.Ok 1L = res @>
        let! res = Events.append ctx streamName 1L (Array.replicate 5 event)
        test <@ AppendResult.Ok 6L = res @>
        // Only start counting RUs from here
        capture.Clear()
        return Array.replicate 6 event
    }

    let verifyCorrectEventsEx direction baseIndex (expected: Events.IEvent []) (res: Events.IOrderedEvent[]) =
        test <@ expected.Length = res.Length @>
        match direction with
        | Direction.Forward -> test <@ [for i in 0..expected.Length - 1 -> baseIndex + int64 i] = [ for r in res -> r.Index ] @>
        | Direction.Backward -> test <@ [for i in 0..expected.Length-1 -> baseIndex - int64 i] = [ for r in res -> r.Index ] @>

        test <@ [for e in expected -> e.EventType] = [ for r in res -> r.EventType ] @>
        for i,x,y in Seq.mapi2 (fun i x y -> i,x,y) [for e in expected -> e.Data] [ for r in res -> r.Data ] do
            verifyUtf8JsonEquals i x y
    let verifyCorrectEventsBackward = verifyCorrectEventsEx Direction.Backward
    let verifyCorrectEvents = verifyCorrectEventsEx Direction.Forward

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``appendAtEnd and getNextIndex`` (extras, TestDbCollStream streamName) = Async.RunSynchronously <| async {
        let! conn = connectToSpecifiedCosmosOrSimulator log
        let ctx = mkContextWithItemLimit conn (Some 1)

        // If a fail triggers a rerun, we need to dump the previous log entries captured
        capture.Clear()
        let! pos = Events.getNextIndex ctx streamName
        test <@ [EqxAct.IndexedNotFound] = capture.ExternalCalls @>
        0L =! pos
        verifyRequestChargesMax 1 // for a 404 by definition
        capture.Clear()

        let event = EventData.Create("test_event")
        let mutable pos = 0L
        let ae = false // TODO fix bug
        for appendBatchSize in [4; 5; 9] do
            if ae then
                let! res = Events.appendAtEnd ctx streamName (Array.replicate appendBatchSize event)
                pos <- pos + int64 appendBatchSize
                //let! res = Events.append ctx streamName pos (Array.replicate appendBatchSize event)
                test <@ [EqxAct.Append] = capture.ExternalCalls @>
                pos =! res
            else
                let! res = Events.append ctx streamName pos (Array.replicate appendBatchSize event)
                pos <- pos + int64 appendBatchSize
                //let! res = Events.append ctx streamName pos (Array.replicate appendBatchSize event)
                test <@ [EqxAct.Append] = capture.ExternalCalls @>
                AppendResult.Ok pos =! res
            verifyRequestChargesMax 50 // was 20, observed 41.64 // 15.59 observed
            capture.Clear()

        let! res = Events.appendAtEnd ctx streamName (Array.replicate 42 event)
        pos <- pos + 42L
        pos =! res
        test <@ [EqxAct.Append] = capture.ExternalCalls @>
        verifyRequestChargesMax 180 // observed 167.32 // was 20
        capture.Clear()

        let! res = Events.getNextIndex ctx streamName
        test <@ [EqxAct.Indexed] = capture.ExternalCalls @>
        verifyRequestChargesMax 2
        capture.Clear()
        pos =! res

        // Demonstrate benefit/mechanism for using the Position-based API to avail of the etag tracking
        let stream  = ctx.CreateStream streamName

        let max = 2000 // observed to time out server side // WAS 5000
        let extrasCount = match extras with x when x * 100 > max -> max | x when x < 1 -> 1 | x -> x*100
        let! _pos = ctx.NonIdempotentAppend(stream, Array.replicate extrasCount event)
        test <@ [EqxAct.Append] = capture.ExternalCalls @>
        verifyRequestChargesMax 1050 // 1038.15 observed // was 300 // 278 observed
        capture.Clear()

        let! pos = ctx.Sync(stream,?position=None)
        test <@ [EqxAct.Indexed] = capture.ExternalCalls @>
        verifyRequestChargesMax 50 // 41 observed // for a 200, you'll pay a lot (we omitted to include the position that NonIdempotentAppend yielded)
        capture.Clear()

        let! _pos = ctx.Sync(stream,pos)
        test <@ [EqxAct.IndexedCached] = capture.ExternalCalls @>
        verifyRequestChargesMax 1 // for a 302 by definition - when an etag IfNotMatch is honored, you only pay one RU
        capture.Clear()
    }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``append - fails on non-matching`` (TestDbCollStream streamName) = Async.RunSynchronously <| async {
        let! conn = connectToSpecifiedCosmosOrSimulator log
        let ctx = mkContext conn

        // Attempt to write, skipping Index 0
        let event = EventData.Create("test_event")
        let! res = Events.append ctx streamName 1L [|event|]
        test <@ [EqxAct.Resync] = capture.ExternalCalls @>
        // The response aligns with a normal conflict in that it passes the entire set of conflicting events ()
        test <@ AppendResult.Conflict (0L,[||]) = res @>
        verifyRequestChargesMax 5
        capture.Clear()

        // Now write at the correct position
        let expected = [|event|]
        let! res = Events.append ctx streamName 0L expected
        test <@ AppendResult.Ok 1L = res @>
        test <@ [EqxAct.Append] = capture.ExternalCalls @>
        verifyRequestChargesMax 12 // was 10, observed 10.57
        capture.Clear()

        // Try overwriting it (a competing consumer would see the same)
        let! res = Events.append ctx streamName 0L [|event; event|]
        // This time we get passed the conflicting events - we pay a little for that, but that's unavoidable
        match res with
        | AppendResult.Conflict (1L, e) -> verifyCorrectEvents 0L expected e
        | x -> x |> failwithf "Unexpected %A"
        test <@ [EqxAct.Resync] = capture.ExternalCalls @>
        verifyRequestChargesMax 5 // observed 4.21 // was 4
        capture.Clear()
    }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let get (TestDbCollStream streamName) = Async.RunSynchronously <| async {
        let! conn = connectToSpecifiedCosmosOrSimulator log
        let ctx = mkContextWithItemLimit conn (Some 3)

        // We're going to ignore the first, to prove we can
        let! expected = add6EventsIn2Batches ctx streamName
        let expected = Array.tail expected

        let! res = Events.get ctx streamName 1L 10

        verifyCorrectEvents 1L expected res

        test <@ List.replicate 2 EqxAct.SliceForward @ [EqxAct.BatchForward] = capture.ExternalCalls @>
        verifyRequestChargesMax 8 // observed 6.14 // was 3
    }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let getBackwards (TestDbCollStream streamName) = Async.RunSynchronously <| async {
        let! conn = connectToSpecifiedCosmosOrSimulator log
        let ctx = mkContextWithItemLimit conn (Some 2)

        let! expected = add6EventsIn2Batches ctx streamName

        // We want to skip reading the last
        let expected = Array.take 5 expected

        let! res = Events.getBackwards ctx streamName 4L 5

        verifyCorrectEventsBackward 4L expected res

        test <@ List.replicate 3 EqxAct.SliceBackward @ [EqxAct.BatchBackward] = capture.ExternalCalls @>
        verifyRequestChargesMax 10 // observed 8.98 // was 3
    }

    // TODO AsyncSeq version

    // TODO 2 batches test

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``get (in 2 batches)`` (TestDbCollStream streamName) = Async.RunSynchronously <| async {
        let! conn = connectToSpecifiedCosmosOrSimulator log
        let ctx = mkContextWithItemLimit conn (Some 2)

        let! expected = add6EventsIn2Batches ctx streamName
        let expected = Array.tail expected |> Array.take 3

        let! res = Events.get ctx streamName 1L 3

        verifyCorrectEvents 1L expected res

        // 2 items atm
        test <@ [EqxAct.SliceForward; EqxAct.SliceForward; EqxAct.BatchForward] = capture.ExternalCalls @>
        verifyRequestChargesMax 7 // observed 6.14 // was 6
    }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let getAll (TestDbCollStream streamName) = Async.RunSynchronously <| async {
        let! conn = connectToSpecifiedCosmosOrSimulator log
        let ctx = mkContextWithItemLimit conn (Some 2)

        let! expected = add6EventsIn2Batches ctx streamName

        let! res = Events.get ctx streamName 1L 4 // Events.getAll >> AsyncSeq.concatSeq |> AsyncSeq.toArrayAsync
        let expected = expected |> Array.tail |> Array.take 4

        verifyCorrectEvents 1L expected res

        // TODO [implement and] prove laziness
        test <@ List.replicate 2 EqxAct.SliceForward @ [EqxAct.BatchForward] = capture.ExternalCalls @>
        verifyRequestChargesMax 10 // observed 8.99 // was 3
    }

    // TODO mine other integration tests