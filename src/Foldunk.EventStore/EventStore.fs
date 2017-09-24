module Foldunk.Stores.EventStore

open EventStore.ClientAPI
open Foldunk
open FSharp.Control
open Serilog // NB must shadow EventStore.ClientAPI.ILogger
open System

let private withLoggedRetries<'t> retryPolicy (contextLabel : string) (f : ILogger -> Async<'t>) log: Async<'t> =
    match retryPolicy with
    | None -> f log
    | Some retryPolicy ->
        let withLoggingContextWrapping count =
            let log = if count = 0 then log else log.ForContext(contextLabel, string count)
            f log
        retryPolicy withLoggingContextWrapping

module private Write =
    /// Yields `Ok WriteResult` or `Error ()` to signify WrongExpectedVersion
    let private writeEventsAsync (log : Serilog.ILogger) (conn : IEventStoreConnection) (streamName : string) (version : int) (events : EventData[]) : Async<Result<WriteResult,unit>> = async {
        try
            let! wr = conn.AppendToStreamAsync(streamName, version, events) |> Async.AwaitTaskCorrect
            return Ok wr
        with :? EventStore.ClientAPI.Exceptions.WrongExpectedVersionException as ex ->
            log.Information(ex, "TrySync WrongExpectedVersionException")
            return Error () }
    let private writeEventsLogged (conn : IEventStoreConnection) (streamName : string) (version : int) (events : EventData[]) (log : Serilog.ILogger)
        : Async<Result<WriteResult,unit>> = async {
        let! t, result = writeEventsAsync log conn streamName version events |> Stopwatch.Time
        log.Information(
            "{ExternalCall} {Action} {Stream} {expectedVersion} {Count} {Latency}",
            true, "AppendToStreamAsync", streamName, version, events.Length, t.Elapsed)
        return result }
    let writeEvents (log : Serilog.ILogger) retryPolicy (conn : IEventStoreConnection) (streamName : string) (version : int) (events : EventData[])
        : Async<Result<WriteResult,unit>> =
        let call = writeEventsLogged conn streamName version events
        withLoggedRetries retryPolicy "WriteRetry" call log

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
        log.Information(
            "{ExternalCall} {Action} {Stream} {version} {sliceLength} {totalPayloadSize} {Latency}",
            true, action, streamName, slice.LastEventNumber, batchSize, payloadSize, t.Elapsed)
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
        let retryingLoggingReadSlice pos = withLoggedRetries retryPolicy "ReadRetry" (call pos)
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
    let private create version : Internal.StreamToken =
        { value = box { streamVersion = version } }
    let ofVersion version : Internal.StreamToken =
        create version

type GesConnection(connection, ?readRetryPolicy, ?writeRetryPolicy) =
    member __.Connection = connection
    member __.ReadRetryPolicy = readRetryPolicy
    member __.WriteRetryPolicy = writeRetryPolicy

/// Wraps an async computation in a retry loop, passing the (1-based) count into the computation and,
///   (until `attempts` exhausted) on an exception matching the `filter`, waiting for the timespan chosen by `backoff` before retrying
let retryWithBackoff (maxAttempts : int) (backoff : int -> TimeSpan option) (f : int -> Async<'a>) =
    if maxAttempts < 1 then raise (invalidArg "maxAttempts" "Should be >= 1")
    let rec go attempt = async {
        try
            let! res = f attempt
            return res
        with ex ->
            if attempt = maxAttempts then return raise (exn(sprintf "Retry failed after %i attempts." maxAttempts, ex))
            else
                match backoff attempt with
                | Some timespan -> do! Async.Sleep (int timespan.TotalMilliseconds)
                | None -> ()
                return! go (attempt + 1) }
    go 1

type GesStreamPolicy(getMaxBatchSize : unit -> int, ?batchCountLimit) =
    new (maxBatchSize) = GesStreamPolicy(fun () -> maxBatchSize)
    member __.BatchSize = getMaxBatchSize()
    member __.MaxBatches = batchCountLimit

type GesGateway(conn : GesConnection, config : GesStreamPolicy) =
    member __.LoadBatched streamName log : Async<Internal.StreamToken * ResolvedEvent[]> = async {
        let! version, events = Read.loadForwardsFrom log conn.ReadRetryPolicy conn.Connection config.BatchSize config.MaxBatches streamName 0
        return Token.ofVersion version, events }
    member __.LoadFromToken streamName log (token : Internal.StreamToken) : Async<Internal.StreamToken * ResolvedEvent[]> = async {
        let! version, events = Read.loadForwardsFrom log conn.ReadRetryPolicy conn.Connection config.BatchSize config.MaxBatches streamName ((unbox token).streamVersion + 1)
        return Token.ofVersion version, events }
    member __.TrySync streamName log (token : Internal.StreamToken) (encodedEvents: EventData array) : Async<Result<Internal.StreamToken, unit>> = async {
        let! wr = Write.writeEvents log conn.WriteRetryPolicy conn.Connection streamName (unbox token.value).streamVersion encodedEvents
        match wr with
        | Error () -> return Error ()
        | Ok wr ->

        let token = Token.ofVersion wr.NextExpectedVersion
        return Ok token }

type GesEventStream<'state, 'event>(gateway : GesGateway, codec : EventSum.IEventSumEncoder<'event, byte[]>) =
    interface IEventStream<'state,'event> with
        member __.Load streamName log : Async<Internal.StreamState<'state, 'event>> = async {
            let! token, events = gateway.LoadBatched streamName log
            return EventSumAdapters.decodeKnownEvents codec events |> Internal.StreamState.ofTokenAndEvents token }
        member __.TrySync streamName log (token, snapshotState) (events : 'event list, proposedState: 'state) = async {
            let encodedEvents : EventData[] = EventSumAdapters.encodeEvents codec events
            let! syncRes = gateway.TrySync streamName log token encodedEvents
            match syncRes with
            | Error () ->
                let resync = async {
                    let! token', events = gateway.LoadFromToken streamName log token
                    let successorEvents = EventSumAdapters.decodeKnownEvents codec events |> List.ofSeq
                    return Internal.StreamState.ofTokenSnapshotAndEvents token' snapshotState successorEvents }
                return Error resync
            | Ok token' ->
                return Ok (Internal.StreamState.ofTokenAndKnownState token' proposedState) }