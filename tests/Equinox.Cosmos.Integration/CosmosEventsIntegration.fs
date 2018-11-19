module Equinox.Cosmos.Integration.CosmosEventsIntegration

open Equinox.Cosmos.Integration.Infrastructure
open Equinox.Cosmos
open Swensen.Unquote
open Serilog

#nowarn "1182" // From hereon in, we may have some 'unused' privates (the tests)

open System

type EventData = { eventType:string; data: byte[] } with
    interface Store.IEvent with
        member __.EventType = __.eventType
        member __.Data = __.data
        member __.Meta = System.Text.Encoding.UTF8.GetBytes("{\"m\":\"m\"}")
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

    let blobEquals (x:byte[]) (y:byte[]) = System.Linq.Enumerable.SequenceEqual(x,y)

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

        test <@ [for e in events -> e.EventType] = [ for r in res -> r.EventType ] @>

        test <@ List.forall2 blobEquals [for e in events -> e.Data] [ for r in res -> r.Data ] @>
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

        test <@ [for e in events -> e.EventType] = [ for r in res -> r.EventType ] @>

        test <@ List.forall2 blobEquals [for e in events -> e.Data] [ for r in res -> r.Data ] @>
    }