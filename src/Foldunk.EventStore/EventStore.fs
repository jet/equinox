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
    type Metric = { interval: StopwatchInterval; action: string }
    let [<Literal>] ExternalTag = "External"

module private Write =
    /// Yields `Ok WriteResult` or `Error ()` to signify WrongExpectedVersion
    let private writeEventsAsync (log : Serilog.ILogger) (conn : IEventStoreConnection) (streamName : string) (version : int) (events : EventData[]) : Async<EsSyncResult> = async {
        try
            let! wr = conn.AppendToStreamAsync(streamName, version, events) |> Async.AwaitTaskCorrect
            return Written wr
        with :? EventStore.ClientAPI.Exceptions.WrongExpectedVersionException as ex ->
            log.Information(ex, "TrySync WrongExpectedVersionException")
            return Conflict }
    let private writeEventsLogged (conn : IEventStoreConnection) (streamName : string) (version : int) (events : EventData[]) (log : Serilog.ILogger)
        : Async<EsSyncResult> = async {
        let! t, result = writeEventsAsync log conn streamName version events |> Stopwatch.Time
        let metric : Metrics.Metric = { Metrics.interval = t; action = "AppendToStreamAsync" }
        log.Information("{"+Metrics.ExternalTag+"} {Stream} {expectedVersion} {Count} ", metric, streamName, version, events.Length)
        return result }
    let writeEvents (log : Serilog.ILogger) retryPolicy (conn : IEventStoreConnection) (streamName : string) (version : int) (events : EventData[])
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
    let private loggedReadSlice conn streamName direction batchSize startPos (log : Serilog.ILogger) : Async<StreamEventsSlice> = async {
        let! t, slice = readSliceAsync conn streamName direction batchSize startPos |> Stopwatch.Time
        let payloadSize = slice.Events |> Array.sumBy (fun e -> e.Event.Data.Length)
        let action = match direction with Direction.Forward -> "ReadStreamEventsForwardAsync" | Direction.Backward -> "ReadStreamEventsBackwardAsync"
        let metric : Metrics.Metric = { interval = t; action = action }
        log.Information(
            "{"+Metrics.ExternalTag+"} {Stream} {version} {sliceLength} {totalPayloadSize}",
            metric, streamName, slice.LastEventNumber, batchSize, payloadSize)
        return slice }
    let private readBatches (log : Serilog.ILogger) (readSlice : int -> Serilog.ILogger -> Async<StreamEventsSlice>)
            (maxPermittedBatchReads : int option) (startPosition : int)
        : AsyncSeq<int option * ResolvedEvent[]> =
        let rec loop batchCount pos = asyncSeq {
            match maxPermittedBatchReads with
            | Some mpbr when batchCount >= mpbr -> invalidOp "batch Limit exceeded"
            | _ -> ()

            let batchLogger = log.ForContext("BatchIndex", batchCount)
            let! slice = readSlice pos batchLogger
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
    let loadForwardsFrom log retryPolicy conn batchSize maxPermittedBatchReads streamName startPosition
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
        let call pos = loggedReadSlice conn streamName Direction.Forward batchSize pos
        let retryingLoggingReadSlice pos = Impl.withLoggedRetries retryPolicy "ReadRetry" (call pos)
        let batches : AsyncSeq<int option * ResolvedEvent[]> = readBatches log retryingLoggingReadSlice maxPermittedBatchReads startPosition
        mergeBatches batches

module private EventSumAdapters =
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
    member __.LoadBatched streamName (log : Serilog.ILogger)  : Async<Storage.StreamToken * ResolvedEvent[]> = async {
        let! version, events = Read.loadForwardsFrom log conn.ReadRetryPolicy conn.Connection config.BatchSize config.MaxBatches streamName 0
        return Token.ofVersion version, events }
    member __.LoadFromToken streamName (log : Serilog.ILogger)  (token : Storage.StreamToken) : Async<Storage.StreamToken * ResolvedEvent[]> = async {
        let token : Token = unbox token.value
        let streamPosition = token.streamVersion + 1
        let! version, events = Read.loadForwardsFrom log conn.ReadRetryPolicy conn.Connection config.BatchSize config.MaxBatches streamName streamPosition
        return Token.ofVersion version, events }
    member __.TrySync streamName (log : Serilog.ILogger) (token : Storage.StreamToken) (encodedEvents: EventData array) : Async<GatewaySyncResult> = async {
        let! wr = Write.writeEvents log conn.WriteRetryPolicy conn.Connection streamName (unbox token.value).streamVersion encodedEvents
        match wr with
        | EsSyncResult.Conflict -> return GatewaySyncResult.Conflict
        | EsSyncResult.Written wr ->

        let token = Token.ofVersion wr.NextExpectedVersion
        return GatewaySyncResult.Written token }

type GesStreamStore<'event, 'state>(gateway : GesGateway, codec : EventSum.IEventSumEncoder<'event, byte[]>) =
    member __.Load streamName log : Async<Storage.StreamState<'event, 'state>> = async {
        let! token, events = gateway.LoadBatched streamName log
        return EventSumAdapters.decodeKnownEvents codec events |> Storage.StreamState.ofTokenAndEvents token }
    member __.TrySync streamName (log : Serilog.ILogger)  (token, snapshotState) (events : 'event list, proposedState: 'state) = async {
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

type GesStream<'event, 'state>(store: GesStreamStore<'event, 'state>, streamName) =
    interface IStream<'event, 'state> with
        member __.Load (log : Serilog.ILogger)  : Async<Storage.StreamState<'event, 'state>> =
            store.Load streamName log
        member __.TrySync (log : Serilog.ILogger)  (token, snapshotState) (events : 'event list, proposedState: 'state) =
            store.TrySync streamName log (token, snapshotState) (events, proposedState)