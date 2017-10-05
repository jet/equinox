namespace Foldunk.EventStore

open EventStore.ClientAPI
open Foldunk
open FSharp.Control
open Serilog // NB must shadow EventStore.ClientAPI.ILogger
open System

module private Impl =
    let withLoggedRetries<'t> retryPolicy (contextLabel : string) (f : ILogger -> Async<'t>) log: Async<'t> =
        match retryPolicy with
        | None -> f log
        | Some retryPolicy ->
            let withLoggingContextWrapping count =
                let log = if count = 0 then log else log.ForContext(contextLabel, string count)
                f log
            retryPolicy withLoggingContextWrapping

[<NoEquality; NoComparison>]
type EsSyncResult = Written of EventStore.ClientAPI.WriteResult | Conflict

module Metrics =
    [<NoEquality; NoComparison>]
    type Metric = { action: string; stream: string; interval: StopwatchInterval } with
        override __.ToString() = sprintf "%s-Stream=%s %s-Elapsed=%O" __.action __.stream __.action __.interval.Elapsed 
    let (|BlobLen|) = function null -> 0 | (x : byte[]) -> x.Length

module private Write =
    /// Yields `Ok WriteResult` or `Error ()` to signify WrongExpectedVersion
    let private writeEventsAsync (log : ILogger) (conn : IEventStoreConnection) (streamName : string) (version : int) (events : EventData[])
        : Async<EsSyncResult> = async {
        try
            let! wr = conn.AppendToStreamAsync(streamName, version, events) |> Async.AwaitTaskCorrect
            return Written wr
        with :? EventStore.ClientAPI.Exceptions.WrongExpectedVersionException as ex ->
            log.Information(ex, "TrySync WrongExpectedVersionException")
            return Conflict }
    let private writeEventsLogged (conn : IEventStoreConnection) (streamName : string) (version : int) (events : EventData[]) (log : ILogger)
        : Async<EsSyncResult> = async {
        let eventDataLen (x : EventData) = match x.Data, x.Metadata with Metrics.BlobLen bytes, Metrics.BlobLen metaBytes -> bytes + metaBytes
        let log = log.ForContext("count", events.Length).ForContext("bytes", events |> Array.sumBy eventDataLen)
        let! t, result = writeEventsAsync log conn streamName version events |> Stopwatch.Time
        let metric : Metrics.Metric = { Metrics.action = "AppendToStreamAsync"; stream = streamName; interval = t }
        // TODO drop expectedVersion when consumption no longer requires that literal; ditto stream when literal formatting no longer required
        log.ForContext("metric", metric).Information("ES {action:l} expectedVersion={expectedVersion} stream={stream}", "Write", version, streamName)
        return result }
    let writeEvents (log : ILogger) retryPolicy (conn : IEventStoreConnection) (streamName : string) (version : int) (events : EventData[])
        : Async<EsSyncResult> =
        let call = writeEventsLogged conn streamName version events
        Impl.withLoggedRetries retryPolicy "WriteRetry" call log

[<RequireQualifiedAccess>]
type Direction = Forward | Backward

module private Read =
    let private readSliceAsync (conn : IEventStoreConnection) (streamName : string) (direction : Direction) (batchSize : int) (startPos : int)
        : Async<StreamEventsSlice> = async {
        let call =
            match direction with
            | Direction.Forward ->  conn.ReadStreamEventsForwardAsync(streamName, startPos, batchSize, resolveLinkTos = false)
            | Direction.Backward -> conn.ReadStreamEventsBackwardAsync(streamName, startPos, batchSize, resolveLinkTos = false)
        return! call |> Async.AwaitTaskCorrect }
    let (|ResolvedEventLen|) (x : ResolvedEvent) = match x.Event.Data, x.Event.Metadata with Metrics.BlobLen bytes, Metrics.BlobLen metaBytes -> bytes + metaBytes
    let private loggedReadSlice conn streamName direction batchSize startPos (log : ILogger) : Async<StreamEventsSlice> = async {
        let log = log.ForContext("startPos", startPos)
        let! t, slice = readSliceAsync conn streamName direction batchSize startPos |> Stopwatch.Time
        let action = match direction with Direction.Forward -> "ReadStreamEventsForwardAsync" | Direction.Backward -> "ReadStreamEventsBackwardAsync"
        let (metric : Metrics.Metric), bytes = { Metrics.action = action; stream = streamName; interval = t}, slice.Events |> Array.sumBy (|ResolvedEventLen|)
        // TODO drop sliceLength, totalPayloadSize when consumption no longer requires that literal; ditto stream when literal formatting no longer required
        log.ForContext("metric", metric).ForContext("bytes", bytes).Information(
            "ES Slice {action:l} count={count} version={version} sliceLength={sliceLength} totalPayloadSize={totalPayloadSize} stream={stream}",
            "Read", slice.Events.Length, slice.LastEventNumber, batchSize, bytes, streamName)
        return slice }
    let private readBatches (log : ILogger) (readSlice : int -> ILogger -> Async<StreamEventsSlice>)
            (maxPermittedBatchReads : int option) (startPosition : int)
        : AsyncSeq<int option * ResolvedEvent[]> =
        let rec loop batchCount pos = asyncSeq {
            match maxPermittedBatchReads with
            | Some mpbr when batchCount >= mpbr -> log.Information "batch Limit exceeded"; invalidOp "batch Limit exceeded"
            | _ -> ()

            let batchLog = log.ForContext("batchIndex", batchCount)
            let! slice = readSlice pos batchLog
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
    let loadForwardsFrom (log : ILogger) retryPolicy conn batchSize maxPermittedBatchReads streamName startPosition
        : Async<int * ResolvedEvent[]> = async {
        let mergeBatches (batches: AsyncSeq<int option * ResolvedEvent[]>) = async {
            let versionFromStream = ref None
            let! (events : ResolvedEvent[]) =
                batches
                |> AsyncSeq.map (function None, events -> events | (Some _) as reportedVersion, events -> versionFromStream := reportedVersion; events)
                |> AsyncSeq.concatSeq
                |> AsyncSeq.toArrayAsync
            let version = match !versionFromStream with Some version -> version | None -> invalidOp "no version encountered in event batch stream"
            return version, events }
        let call pos = loggedReadSlice conn streamName Direction.Forward batchSize pos
        let retryingLoggingReadSlice pos = Impl.withLoggedRetries retryPolicy "ReadRetry" (call pos)
        let log = log.ForContext("batchSize", batchSize).ForContext("direction", "Forward").ForContext("stream", streamName)
        let batches : AsyncSeq<int option * ResolvedEvent[]> = readBatches log retryingLoggingReadSlice maxPermittedBatchReads startPosition
        let! version, events = mergeBatches batches
        log.Information("ES Batch Read count={count} version={version}", events.Length, version)
        return version, events }

module EventSumAdapters =
    let private encodedEventOfResolvedEvent (x : ResolvedEvent) : EventSum.EncodedEvent<byte[]> =
        { EventType = x.Event.EventType; Payload = x.Event.Data }
    let private eventDataOfEncodedEvent (x : EventSum.EncodedEvent<byte[]>) =
        EventData(Guid.NewGuid(), x.EventType, (*isJson*) true, x.Payload, [||])
    let encodeEvents (codec : EventSum.IEventSumEncoder<'event, byte[]>) (xs : 'event seq) : EventData[] =
        xs |> Seq.map (codec.Encode >> eventDataOfEncodedEvent) |> Seq.toArray
    let decodeKnownEvents (codec : EventSum.IEventSumEncoder<'event, byte[]>) (xs : ResolvedEvent[]) : 'event seq =
        xs |> Seq.map encodedEventOfResolvedEvent |> Seq.choose codec.TryDecode

type Token = { streamVersion: int }

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module private Token =
    let private create version : Storage.StreamToken =
        { value = box { streamVersion = version } }
    let ofVersion version : Storage.StreamToken =
        create version

type GesConnection(connection, ?readRetryPolicy, ?writeRetryPolicy) =
    member __.Connection = connection
    member __.ReadRetryPolicy = readRetryPolicy
    member __.WriteRetryPolicy = writeRetryPolicy

type GesStreamPolicy(getMaxBatchSize : unit -> int, ?batchCountLimit) =
    new (maxBatchSize) = GesStreamPolicy(fun () -> maxBatchSize)
    member __.BatchSize = getMaxBatchSize()
    member __.MaxBatches = batchCountLimit

[<NoComparison; NoEquality>]
type GatewaySyncResult = Written of Storage.StreamToken | Conflict

type GesGateway(conn : GesConnection, config : GesStreamPolicy) =
    member __.LoadBatched streamName (log : ILogger)  : Async<Storage.StreamToken * ResolvedEvent[]> = async {
        let! version, events = Read.loadForwardsFrom log conn.ReadRetryPolicy conn.Connection config.BatchSize config.MaxBatches streamName 0
        return Token.ofVersion version, events }
    member __.LoadFromToken streamName (log : ILogger) (token : Storage.StreamToken) : Async<Storage.StreamToken * ResolvedEvent[]> = async {
        let token : Token = unbox token.value
        let streamPosition = token.streamVersion + 1
        let! version, events = Read.loadForwardsFrom log conn.ReadRetryPolicy conn.Connection config.BatchSize config.MaxBatches streamName streamPosition
        return Token.ofVersion version, events }
    member __.TrySync streamName (log : ILogger) (token : Storage.StreamToken) (encodedEvents: EventData array) : Async<GatewaySyncResult> = async {
        let! wr = Write.writeEvents log conn.WriteRetryPolicy conn.Connection streamName (unbox token.value).streamVersion encodedEvents
        match wr with
        | EsSyncResult.Conflict -> return GatewaySyncResult.Conflict
        | EsSyncResult.Written wr ->

        let token = Token.ofVersion wr.NextExpectedVersion
        return GatewaySyncResult.Written token }

type GesStreamState<'event, 'state>(gateway : GesGateway, codec : EventSum.IEventSumEncoder<'event, byte[]>, ?initialTokenAndState : Storage.StreamToken * 'state) =
    let knownTokenAndState = ref initialTokenAndState
    member __.Load streamName (log : ILogger) : Async<Storage.StreamState<'event, 'state>> = async {
        match !knownTokenAndState with
        | Some (token,state) -> knownTokenAndState := None; return token, Some state, List.empty
        | None ->
            let! token, events = gateway.LoadBatched streamName log
            return EventSumAdapters.decodeKnownEvents codec events |> Storage.StreamState.ofTokenAndEvents token }
    member __.TrySync streamName (log : ILogger) (token, snapshotState) (events : 'event list, proposedState: 'state) = async {
        let encodedEvents : EventData[] = EventSumAdapters.encodeEvents codec events
        let! syncRes = gateway.TrySync streamName log token encodedEvents
        match syncRes with
        | GatewaySyncResult.Conflict ->
            let resync = async {
                let! token', events = gateway.LoadFromToken streamName log token
                let successorEvents = EventSumAdapters.decodeKnownEvents codec events |> List.ofSeq
                return Storage.StreamState.ofTokenSnapshotAndEvents token' snapshotState successorEvents }
            return Storage.SyncResult.Conflict resync
        | GatewaySyncResult.Written token' ->
            return Storage.SyncResult.Written (Storage.StreamState.ofTokenAndKnownState token' proposedState) }

type GesStream<'event, 'state>(store: GesStreamState<'event, 'state>, streamName) =
    interface IStream<'event, 'state> with
        member __.Load (log : ILogger)  : Async<Storage.StreamState<'event, 'state>> =
            store.Load streamName log
        member __.TrySync (log : ILogger) (token, snapshotState) (events : 'event list, proposedState: 'state) =
            store.TrySync streamName log (token, snapshotState) (events, proposedState)