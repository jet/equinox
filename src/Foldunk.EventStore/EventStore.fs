module Foldunk.EventStore

open EventStore.ClientAPI
open Foldunk
open FSharp.Control
open Serilog // NB must shadow EventStore.ClientAPI.ILogger
open System

module private Append =
    let writeEventsAsync (conn : IEventStoreConnection) (streamName : string) (version : int) (events : EventData[]) =
        conn.AppendToStreamAsync(streamName, version, events) |> Async.AwaitTaskCorrect
    let loggedWriteEvents (conn : IEventStoreConnection) (log : Serilog.ILogger) (streamName : string) (version : int) (events : EventData[]) = async {
        let! t, result = writeEventsAsync conn streamName version events |> Stopwatch.Time
        log.Information("{ExternalCall} {Action} {Stream} {Version} {Count} {Latency}", true, "AppendToStreamAsync", streamName, version, events.Length, t.Elapsed)
        return result }

[<RequireQualifiedAccess>]
type Direction = Forward | Backward

module private Read =
    let readSliceAsync (conn : IEventStoreConnection) (streamName : string) (direction : Direction) (batchSize : int) (startPos : int) = async {
        let call = 
            match direction with
            | Direction.Forward ->  conn.ReadStreamEventsForwardAsync(streamName, startPos, batchSize, resolveLinkTos = false)  
            | Direction.Backward -> conn.ReadStreamEventsBackwardAsync(streamName, startPos, batchSize, resolveLinkTos = false)
        return! call |> Async.AwaitTaskCorrect }
    let loggedReadSlice (log : Serilog.ILogger) conn streamName direction batchSize startPos = async {
        let! t, slice = readSliceAsync conn streamName direction batchSize startPos |> Stopwatch.Time
        let payloadSize = slice.Events |> Array.sumBy (fun e -> e.Event.Data.Length)
        let action = match direction with Direction.Forward -> "ReadStreamEventsForwardAsync" | Direction.Backward -> "ReadStreamEventsBackwardAsync"
        log.Information("{ExternalCall} {Action} {Stream} {Version} {SliceLength} {TotalPayloadSize} {Latency}", true, action, streamName, startPos, batchSize, payloadSize, t.Elapsed)
        return slice }
    let readBatches (log : Serilog.ILogger) (readSlice : Serilog.ILogger -> int -> Async<StreamEventsSlice>) (maxPermittedBatchReads : int option) (startPosition : int) =
        let rec loop batchCount pos = asyncSeq {
            match maxPermittedBatchReads with
            | Some mpbr when batchCount >= mpbr -> invalidOp "batch Limit exceeded"
            | _ -> ()

            let batchLogger = log.ForContext( "BatchIndex", batchCount)
            let! slice = readSlice batchLogger pos
            match slice.Status with
            | SliceReadStatus.StreamDeleted -> raise <| EventStore.ClientAPI.Exceptions.StreamDeletedException(slice.Stream)
            | SliceReadStatus.StreamNotFound -> yield Some ExpectedVersion.NoStream, Array.empty
            | SliceReadStatus.Success ->
                let version = if batchCount = 0 then Some slice.LastEventNumber else None
                yield version, slice.Events
                if not slice.IsEndOfStream then
                    yield! loop (batchCount + 1) slice.NextEventNumber
            | x -> raise <| System.ArgumentOutOfRangeException("SliceReadStatus", x, "Unknown result value") }
        loop 0 startPosition

module private GesEventSumAdapters =
    open Foldunk.EventSum
    let encodedEventOfResolvedEvent (x : ResolvedEvent) = { EventType = x.Event.EventType; Payload = x.Event.Data }
    //let private encodedEventsOfResolvedEventBatches: AsyncSeq<ResolvedEvent[]> -> AsyncSeq<EncodedEvent<byte[]>> =
    //    AsyncSeq.map (Array.toSeq >> Seq.map encodedEventOfResolvedEvent)
    //    >> AsyncSeq.concatSeq
    let private encodeBatchToEventDataBatch (events : EncodedEvent<byte[]> seq) : EventData[] =
        let eventDataOfEncodedEvent x = EventData(Guid.NewGuid(), x.EventType, (*isJson*) true, x.Payload, [||])
        [| for e in events -> eventDataOfEncodedEvent e |]
    let encodeEvents (codec : EventSum.IEventSumEncoder<'event, byte[]>) (events : 'event seq) : EventData[] = 
        events |> Seq.map codec.Encode |> encodeBatchToEventDataBatch
    let decodeKnownEvents (codec : EventSum.IEventSumEncoder<'event, byte[]>) (events : ResolvedEvent[]) : 'event seq =
        events |> Seq.map encodedEventOfResolvedEvent |> Seq.choose codec.TryDecode

type GesStreamStore(conn, batchSize, ?maxPermittedBatchReads) =
    let loadForwardsFrom streamName log startPosition : Async<int * ResolvedEvent[]> = async {
        let mergeBatches (batches: AsyncSeq<int option * ResolvedEvent[]>) = async {
            let versionFromStream = ref None
            let! (events : ResolvedEvent[]) =
                batches
                |> AsyncSeq.map (function None, events -> events | (Some _) as reportedVersion, events -> versionFromStream := reportedVersion; events)
                |> AsyncSeq.concatSeq
                |> AsyncSeq.toArrayAsync
            let version = match !versionFromStream with Some version -> version | None -> invalidOp "no version encountered in event batch stream"
            return version, events }
        let readSlice log streamPos = Read.loggedReadSlice log conn streamName Direction.Forward batchSize streamPos
        let batches : AsyncSeq<int option * ResolvedEvent[]> = Read.readBatches log readSlice maxPermittedBatchReads startPosition
        return! mergeBatches batches }
    let loadBackwardsUntilCompactionOrStart streamName log isCompactionEventType : Async<int * ResolvedEvent[]> = async {
        let mergeFromCompactionPointOrStartFromBackwardsStream (snapshotPredicate : string -> bool) (batchesBackward : AsyncSeq<int option * ResolvedEvent[]>) : Async<int*ResolvedEvent[]> = async {
            let versionFromStream = ref None
            let! tempBackward =
                batchesBackward
                |> AsyncSeq.map (function None, events -> events | (Some _) as reportedVersion, events -> versionFromStream := reportedVersion; events)
                |> AsyncSeq.concatSeq
                |> AsyncSeq.takeWhileInclusive (fun x -> not (snapshotPredicate x.Event.EventType))
                |> AsyncSeq.toArrayAsync
            let forward = Array.Reverse(tempBackward); tempBackward // sic - in-place reverse of something we own
            let version = match !versionFromStream with Some version -> version | None -> invalidOp "no version encountered in event batch stream"
            return version, forward }
        let startPosition = 0
        let readSlice log streamPos = Read.loggedReadSlice log conn streamName Direction.Backward batchSize streamPos
        let batchesBackward : AsyncSeq<int option * ResolvedEvent[]> = Read.readBatches log readSlice maxPermittedBatchReads startPosition
        return! mergeFromCompactionPointOrStartFromBackwardsStream isCompactionEventType batchesBackward }
    member __.LoadBackwardsStoppingAtCompactionEvent streamName log isCompactionEventType: Async<int * ResolvedEvent[]> =
        loadBackwardsUntilCompactionOrStart streamName log isCompactionEventType
    member __.Load streamName log : Async<int * ResolvedEvent[]> =
        loadForwardsFrom streamName log 0
    member __.LoadFromVersion streamName log version : Async<int * ResolvedEvent[]> =
        loadForwardsFrom streamName log version
    member __.TrySync streamName log version (encodedEvents: EventData array) : Async<Result<int, unit>> = async {
        try
            let! wr = Append.loggedWriteEvents conn log streamName version encodedEvents
            return Ok wr.NextExpectedVersion
        with :? EventStore.ClientAPI.Exceptions.WrongExpectedVersionException as ex ->
            log.Information(ex, "TrySync WrongExpectedVersionException")
            return Error () }

type GesToken = { streamVersion: int }

type GesEventStreamAdapter<'state, 'event>(store : GesStreamStore, codec : EventSum.IEventSumEncoder<'event, byte[]>) =
    let tokenOfVersion version = box { streamVersion = version }

    interface IEventStream<'state, 'event> with
        member __.Load streamName log : Async<StreamState<'state, 'event>> = async {
            let! version, events = store.Load streamName log
            return GesEventSumAdapters.decodeKnownEvents codec events |> StreamState.ofTokenAndEvents (tokenOfVersion version) }
        member __.TrySync streamName log (token, snapshotState) (events : 'event list, proposedState: 'state) = async {
            let encodedEvents : EventData[] = GesEventSumAdapters.encodeEvents codec events
            let! syncRes = store.TrySync streamName log (unbox token).streamVersion encodedEvents
            match syncRes with
            | Error () ->
                let resync = async {
                    let! token, events = store.LoadFromVersion streamName log ((unbox token).streamVersion + 1)
                    let successorEvents = GesEventSumAdapters.decodeKnownEvents codec events |> List.ofSeq
                    return StreamState.ofTokenSnapshotAndEvents (tokenOfVersion token) snapshotState successorEvents }
                return Error resync 
            | Ok version' ->
                return Ok (StreamState.ofTokenAndKnownState (tokenOfVersion version') proposedState) }