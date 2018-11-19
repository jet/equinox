module Equinox.Cosmos.Integration.CosmosEventsIntegration

open Equinox.Cosmos.Integration.Infrastructure
open Equinox.Cosmos
open Swensen.Unquote
open Serilog
open System
open System.Text

#nowarn "1182" // From hereon in, we may have some 'unused' privates (the tests)

open Newtonsoft.Json.Linq

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
    let log = base.Log

    let (|TestStream|) (name:Guid) = sprintf "events-%O" name
    let (|TestDbCollStream|) (TestStream sid) = let (StoreCollection (dbId,collId,sid)) = sid in dbId,collId,sid
    let mkContext conn dbId collId = Events.Context(conn,dbId,collId,defaultBatchSize)

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let append (TestDbCollStream (dbId,collId,sid)) = Async.RunSynchronously <| async {
        let! conn = connectToSpecifiedCosmosOrSimulator log
        let ctx = mkContext conn dbId collId

        let events = [|EventData.Create("test_event")|]
        let sn = 0L
        let! res = Events.append ctx sid sn events
        test <@ Events.AppendResult.Ok 1L = res @>
    }

    let blobEquals (x: byte[]) (y: byte[]) = System.Linq.Enumerable.SequenceEqual(x,y)
    let stringOfUtf8 (x: byte[]) = Encoding.UTF8.GetString(x)
    let xmlDiff (x: string) (y: string) =
        match JsonDiffPatchDotNet.JsonDiffPatch().Diff(JToken.Parse x,JToken.Parse y) with
        | null -> ""
        | d -> string d
    let testUtf8JsonEquals i x y =
        let sx,sy = stringOfUtf8 x, stringOfUtf8 y
        test <@ ignore i; blobEquals x y && "" = xmlDiff sx sy @>

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let get (TestDbCollStream (dbId,collId,sid)) = Async.RunSynchronously <| async {
        let! conn = connectToSpecifiedCosmosOrSimulator log
        let ctx = mkContext conn dbId collId

        let sn = 0L
        let events = [|EventData.Create("test_event")|]
        let! res = Events.append ctx sid sn events
        test <@ Events.AppendResult.Ok 1L = res @>

        let! res = Events.get ctx sid sn events.Length
        test <@ res.Length = events.Length @>

        test <@ List.init 1 int64 = [for r in res -> r.Index ] @>

        test <@ [for e in events -> e.EventType] = [ for r in res -> r.EventType ] @>

        for i,x,y in Seq.mapi2 (fun i x y -> i,x,y) [for e in events -> e.Data] [ for r in res -> r.Data ] do
            testUtf8JsonEquals i x y
    }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let getAll (TestDbCollStream (dbId,collId,sid)) = Async.RunSynchronously <| async {
        let! conn = connectToSpecifiedCosmosOrSimulator log
        let ctx = mkContext conn dbId collId

        let sn = 0L
        let events = [|EventData.Create("test_event")|]
        let! res = Events.append ctx sid sn events
        test <@ Events.AppendResult.Ok 1L = res @>

        let! res = Events.getAll ctx sid sn events.Length
        test <@ res.Length = events.Length @>

        test <@ List.init 1 int64 = [for r in res -> r.Index ] @>

        test <@ [for e in events -> e.EventType] = [ for r in res -> r.EventType ] @>

        for i,x,y in Seq.mapi2 (fun i x y -> i,x,y) [for e in events -> e.Data] [ for r in res -> r.Data ] do
            testUtf8JsonEquals i x y
    }