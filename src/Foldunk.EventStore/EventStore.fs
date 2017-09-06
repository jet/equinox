module Foldunk.Stores.EventStore

open EventStore.ClientAPI
open Foldunk
open FSharp.Control
open Serilog // NB must shadow EventStore.ClientAPI.ILogger
open System

module private Write =
    let writeEventsAsync (conn : IEventStoreConnection) (streamName : string) (version : int) (events : EventData[]) : Async<WriteResult> =
        conn.AppendToStreamAsync(streamName, version, events) |> Async.AwaitTaskCorrect
    let loggedWriteEvents (conn : IEventStoreConnection) (log : Serilog.ILogger) (streamName : string) (version : int) (events : EventData[])
        : Async<WriteResult> = async {
        let! t, result = writeEventsAsync conn streamName version events |> Stopwatch.Time
        log.Information(
            "{ExternalCall} {Action} {Stream} {Version} {Count} {Latency}",
            true, "AppendToStreamAsync", streamName, version, events.Length, t.Elapsed)
        return result }

[<RequireQualifiedAccess>]
type Direction = Forward | Backward

module private Read =
    let readSliceAsync (conn : IEventStoreConnection) (streamName : string) (direction : Direction) (batchSize : int) (startPos : int)
        : Async<StreamEventsSlice> = async {
        let call = 
            match direction with
            | Direction.Forward ->  conn.ReadStreamEventsForwardAsync(streamName, startPos, batchSize, resolveLinkTos = false)  
            | Direction.Backward -> conn.ReadStreamEventsBackwardAsync(streamName, startPos, batchSize, resolveLinkTos = false)
        return! call |> Async.AwaitTaskCorrect }
    let loggedReadSlice (log : Serilog.ILogger) conn streamName direction batchSize startPos : Async<StreamEventsSlice> = async {
        let! t, slice = readSliceAsync conn streamName direction batchSize startPos |> Stopwatch.Time
        let payloadSize = slice.Events |> Array.sumBy (fun e -> e.Event.Data.Length)
        let action = match direction with Direction.Forward -> "ReadStreamEventsForwardAsync" | Direction.Backward -> "ReadStreamEventsBackwardAsync"
        log.Information(
            "{ExternalCall} {Action} {Stream} {Version} {SliceLength} {TotalPayloadSize} {Latency}",
            true, action, streamName, startPos, batchSize, payloadSize, t.Elapsed)
        return slice }
    let readBatches (log : Serilog.ILogger) (readSlice : Serilog.ILogger -> int -> Async<StreamEventsSlice>) (maxPermittedBatchReads : int option) (startPosition : int)
        : AsyncSeq<int option * ResolvedEvent[]> =
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
    let loadForwardsFrom conn batchSize maxPermittedBatchReads streamName log startPosition
        : Async<int * ResolvedEvent[]> =
        let mergeBatches (batches: AsyncSeq<int option * ResolvedEvent[]>) = async {
            let versionFromStream = ref None
            let! (events : ResolvedEvent[]) =
                batches
                |> AsyncSeq.map (function None, events -> events | (Some _) as reportedVersion, events -> versionFromStream := reportedVersion; events)
                |> AsyncSeq.concatSeq
                |> AsyncSeq.toArrayAsync
            let version = match !versionFromStream with Some version -> version | None -> invalidOp "no version encountered in event batch stream"
            return version, events }
        let readSlice log streamPos = loggedReadSlice log conn streamName Direction.Forward batchSize streamPos
        let batches : AsyncSeq<int option * ResolvedEvent[]> = readBatches log readSlice maxPermittedBatchReads startPosition
        mergeBatches batches

module private EventSumAdapters =
    let private encodedEventOfResolvedEvent (x : ResolvedEvent) : EventSum.EncodedEvent<byte[]> =
        { EventType = x.Event.EventType; Payload = x.Event.Data }
    let private eventDataOfEncodedEvent (x : EventSum.EncodedEvent<byte[]>) =
        EventData(Guid.NewGuid(), x.EventType, (*isJson*) true, x.Payload, [||])
    let encodeEvents (codec : EventSum.IEventSumEncoder<'event, byte[]>) (xs : 'event seq) : EventData[] = 
        xs |> Seq.map codec.Encode |> Seq.map eventDataOfEncodedEvent |> Seq.toArray
    let decodeKnownEvents (codec : EventSum.IEventSumEncoder<'event, byte[]>) (xs : ResolvedEvent[]) : 'event seq =
        xs |> Seq.map encodedEventOfResolvedEvent |> Seq.choose codec.TryDecode

type Token = { streamVersion: int }
module private Token =
    let private create version : StreamToken =
        { value = box { streamVersion = version } }
    let ofVersion version : StreamToken =
        create version

type GesStreamStore(conn, batchSize, ?maxPermittedBatchReads) =
    member __.LoadBatched streamName log : Async<StreamToken * ResolvedEvent[]> = async {
        let! version, events = Read.loadForwardsFrom conn batchSize maxPermittedBatchReads streamName log 0
        return Token.ofVersion version, events }
    member __.LoadFromToken streamName log token : Async<StreamToken * ResolvedEvent[]> = async {
        let! version, events = Read.loadForwardsFrom conn batchSize maxPermittedBatchReads streamName log ((unbox token).streamVersion + 1)
        return Token.ofVersion version, events }
    member __.TrySync streamName log (token : StreamToken) (encodedEvents: EventData array) : Async<Result<StreamToken, unit>> = async {
        try
            let! wr = Write.loggedWriteEvents conn log streamName (unbox token.value).streamVersion encodedEvents
            return Ok (Token.ofVersion wr.NextExpectedVersion)
        with :? EventStore.ClientAPI.Exceptions.WrongExpectedVersionException as ex ->
            log.Information(ex, "TrySync WrongExpectedVersionException")
            return Error () }

type GesEventStreamAdapter<'state, 'event>(store : GesStreamStore, codec : EventSum.IEventSumEncoder<'event, byte[]>) =
    interface Handler.IEventStream<'state,'event> with
        member __.Load streamName log : Async<StreamState<'state, 'event>> = async {
            let! token, events = store.LoadBatched streamName log
            return EventSumAdapters.decodeKnownEvents codec events |> StreamState.ofTokenAndEvents token }
        member __.TrySync streamName log (token, snapshotState) (events : 'event list, proposedState: 'state) = async {
            let encodedEvents : EventData[] = EventSumAdapters.encodeEvents codec events
            let! syncRes = store.TrySync streamName log token encodedEvents
            match syncRes with
            | Error () ->
                let resync = async {
                    let! token', events = store.LoadFromToken streamName log token
                    let successorEvents = EventSumAdapters.decodeKnownEvents codec events |> List.ofSeq
                    return StreamState.ofTokenSnapshotAndEvents token' snapshotState successorEvents }
                return Error resync 
            | Ok token' ->
                return Ok (StreamState.ofTokenAndKnownState token' proposedState) }