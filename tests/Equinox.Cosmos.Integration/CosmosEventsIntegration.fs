module Equinox.Cosmos.Integration.CosmosEventsIntegration

open Equinox.Cosmos.Integration.Infrastructure
open Equinox.Cosmos
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
    interface Store.IEvent with
        member __.EventType = __.eventType
        member __.Data = __.data
        member __.Meta = Encoding.UTF8.GetBytes("{\"m\":\"m\"}")
    static member Create(eventType,?json) : Store.IEvent =
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
    let (|TestDbCollStream|) (TestStream sid) = let (StoreCollection (dbId,collId,sid)) = sid in dbId,collId,sid
    let mkContextWithSliceLimit conn dbId collId maxEventsPerSlice = Events.Context(conn,dbId,collId,defaultBatchSize,log,?maxEventsPerSlice=maxEventsPerSlice)
    let mkContext conn dbId collId = mkContextWithSliceLimit conn dbId collId None

    let verifyRequestChargesBelow rus =
        let tripRequestCharges = [ for e, c in capture.RequestCharges -> sprintf "%A" e, c ]
        test <@ float rus > Seq.sum (Seq.map snd tripRequestCharges) @>

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let append (TestDbCollStream (dbId,collId,sid)) = Async.RunSynchronously <| async {
        let! conn = connectToSpecifiedCosmosOrSimulator log
        let ctx = mkContext conn dbId collId

        let event = EventData.Create("test_event")
        let sn = 0L
        let! res = Events.append ctx sid sn [|event|]
        test <@ Events.AppendResult.Ok 1L = res @>

        verifyRequestChargesBelow 10
        // Clear the counters
        capture.Clear()

        let! res = Events.append ctx sid 1L (Array.replicate 5 event)
        test <@ Events.AppendResult.Ok 6L = res @>
        // We didnt request small batches or splitting so it's not dramatically more expensive to write N events
        verifyRequestChargesBelow 10
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

    let add6EventsIn2Batches ctx sid = async {
        let sn = 0L
        let event = EventData.Create("test_event")
        let! res = Events.append ctx sid sn [|event|]
        test <@ Events.AppendResult.Ok 1L = res @>
        let! res = Events.append ctx sid 1L (Array.replicate 5 event)
        test <@ Events.AppendResult.Ok 6L = res @>
        // Only start counting RUs from here
        capture.Clear()
        return Array.replicate 6 event
    }

    let verifyCorrectEventsEx direction baseIndex (expected: Store.IEvent []) (res: Store.IOrderedEvent[]) =
        test <@ expected.Length = res.Length @>
        match direction with
        | Store.Direction.Forward -> test <@ [for i in 0..expected.Length - 1 -> baseIndex + int64 i] = [ for r in res -> r.Index ] @>
        | Store.Direction.Backward -> test <@ [for i in 0..expected.Length-1 -> baseIndex - int64 i] = [ for r in res -> r.Index ] @>

        test <@ [for e in expected -> e.EventType] = [ for r in res -> r.EventType ] @>
        for i,x,y in Seq.mapi2 (fun i x y -> i,x,y) [for e in expected -> e.Data] [ for r in res -> r.Data ] do
            verifyUtf8JsonEquals i x y
    let verifyCorrectEventsBackward = verifyCorrectEventsEx Store.Direction.Backward
    let verifyCorrectEvents = verifyCorrectEventsEx Store.Direction.Forward

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let get (TestDbCollStream (dbId,collId,sid)) = Async.RunSynchronously <| async {
        let! conn = connectToSpecifiedCosmosOrSimulator log
        let ctx = mkContext conn dbId collId

        // We're going to ignore the first, to prove we can
        let! expected = add6EventsIn2Batches ctx sid
        let expected = Array.skip 1 expected

        let! res = Events.get ctx sid 1L 1

        verifyCorrectEvents 1L expected res

        test <@ [EqxAct.SliceForward; EqxAct.BatchForward] = capture.ExternalCalls @>
        verifyRequestChargesBelow 3
    }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let getBackwards (TestDbCollStream (dbId,collId,sid)) = Async.RunSynchronously <| async {
        let! conn = connectToSpecifiedCosmosOrSimulator log
        let ctx = mkContext conn dbId collId

        let! expected = add6EventsIn2Batches ctx sid

        // We want to skip reading the last
        let expected = Array.take 5 expected

        let! res = Events.getBackwards ctx sid 4L 2

        verifyCorrectEventsBackward 4L expected res

        test <@ [EqxAct.SliceBackward; EqxAct.BatchBackward] = capture.ExternalCalls @>
        verifyRequestChargesBelow 3
    }

    // TODO AsyncSeq version

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``get (in 2 batches)`` (TestDbCollStream (dbId,collId,sid)) = Async.RunSynchronously <| async {
        let! conn = connectToSpecifiedCosmosOrSimulator log
        let ctx = mkContextWithSliceLimit conn dbId collId (Some 1)

        let! expected = add6EventsIn2Batches ctx sid
        let expected = Array.skip 1 expected

        let! res = Events.get ctx sid 1L 1

        verifyCorrectEvents 1L expected res

        // 2 Slices this time
        test <@ [EqxAct.SliceForward; EqxAct.SliceForward; EqxAct.BatchForward] = capture.ExternalCalls @>
        verifyRequestChargesBelow 6
    }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let getAll (TestDbCollStream (dbId,collId,sid)) = Async.RunSynchronously <| async {
        let! conn = connectToSpecifiedCosmosOrSimulator log
        let ctx = mkContext conn dbId collId

        let! expected = add6EventsIn2Batches ctx sid

        let! res = Events.get ctx sid 1L 2 // Events.getAll >> AsyncSeq.concatSeq |> AsyncSeq.toArrayAsync
        let expected = Array.skip 1 expected

        verifyCorrectEvents 1L expected res

        // TODO [implement and] prove laziness
        test <@ [EqxAct.SliceForward; EqxAct.BatchForward] = capture.ExternalCalls @>
        verifyRequestChargesBelow 3
    }