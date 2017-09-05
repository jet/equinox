namespace Foldunk.EventStore

open EventStore.ClientAPI
open Foldunk
open FSharp.Control
open Serilog // NB must shadow EventStore.ClientAPI.ILogger
open System

[<AutoOpen>]
module private Impl =
    let OptionDefaultValue a b = defaultArg b a
    let withLoggedRetries<'t> retryPolicy (contextLabel : string) (f : ILogger -> Async<'t>) log: Async<'t> =
        match retryPolicy with
        | None -> f log
        | Some retryPolicy ->
            let withLoggingContextWrapping count =
                let log = if count = 1 then log else log.ForContext(contextLabel, count)
                f log
            retryPolicy withLoggingContextWrapping
    let lfc name value (log : ILogger) = log.ForContext(name, value)
    let logCount = lfc "count"
    let logBytes = lfc "bytes"
    let logStream = lfc "stream"
    let logBatchSize = lfc "batchSize"
    let logDirection = lfc "direction"

[<NoEquality; NoComparison>]
type EsSyncResult = Written of EventStore.ClientAPI.WriteResult | Conflict

module Metrics =
    [<NoEquality; NoComparison>]
    type Metric = { action: string; stream: string; interval: StopwatchInterval } with
        override __.ToString() = sprintf "%s-Stream=%s %s-Elapsed=%O" __.action __.stream __.action __.interval.Elapsed 
    let (|BlobLen|) = function null -> 0 | (x : byte[]) -> x.Length
    let log action streamName t (log : ILogger) =
        log |> lfc "metric" { action = action; stream = streamName; interval = t }

module private Write =
    /// Yields `EsSyncResult.Written` or `EsSyncResult.Conflict` to signify WrongExpectedVersion
    let private writeEventsAsync (log : ILogger) (conn : IEventStoreConnection) (streamName : string) (version : int) (events : EventData[])
        : Async<EsSyncResult> = async {
        try
            let! wr = conn.AppendToStreamAsync(streamName, version, events) |> Async.AwaitTaskCorrect
            return Written wr
        with :? EventStore.ClientAPI.Exceptions.WrongExpectedVersionException as ex ->
            log.Information(ex, "Ges TrySync WrongExpectedVersionException")
            return Conflict }
    let logEventDataBytes events =
        let eventDataLen (x : EventData) = match x.Data, x.Metadata with Metrics.BlobLen bytes, Metrics.BlobLen metaBytes -> bytes + metaBytes
        events |> Array.sumBy eventDataLen |> logBytes 
    let private writeEventsLogged (conn : IEventStoreConnection) (streamName : string) (version : int) (events : EventData[]) (log : ILogger)
        : Async<EsSyncResult> = async {
        let log = log |> logEventDataBytes events
        let writeLog = log |> logStream streamName |> lfc "expectedVersion" version |> logCount events.Length
        let! t, result = writeEventsAsync writeLog conn streamName version events |> Stopwatch.Time
        let isConflict, resultlog =
            match result, log |> Metrics.log "AppendToStreamAsync" streamName t with
            | EsSyncResult.Conflict, log -> true, log
            | EsSyncResult.Written x, log -> false, log |> lfc "nextExpectedVersion" x.NextExpectedVersion |> lfc "logPosition" x.LogPosition
        // TODO drop expectedVersion when consumption no longer requires that literal; ditto stream when literal formatting no longer required
        resultlog.Information("Ges{action:l} stream={stream} count={count} expectedVersion={expectedVersion} conflict={conflict}",
            "Write", streamName, events.Length, version, isConflict)
        return result }
    let writeEvents (log : ILogger) retryPolicy (conn : IEventStoreConnection) (streamName : string) (version : int) (events : EventData[])
        : Async<EsSyncResult> =
        let call = writeEventsLogged conn streamName version events
        withLoggedRetries retryPolicy "writeAttempt" call log

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
        let log = log |> lfc "startPos" startPos
        let! t, slice = readSliceAsync conn streamName direction batchSize startPos |> Stopwatch.Time
        let bytes = slice.Events |> Array.sumBy (|ResolvedEventLen|)
        let action = match direction with Direction.Forward -> "ReadStreamEventsForwardAsync" | Direction.Backward -> "ReadStreamEventsBackwardAsync"
        (log |> Metrics.log action streamName t |> logBytes bytes).Information(
            // TODO drop sliceLength, totalPayloadSize when consumption no longer requires that literal; ditto stream when literal formatting no longer required
            "Ges{action:l} stream={stream} count={count} version={version} sliceLength={sliceLength} totalPayloadSize={totalPayloadSize}",
            "Read", streamName, slice.Events.Length, slice.LastEventNumber, batchSize, bytes)
        return slice }
    let private readBatches (log : ILogger) (readSlice : int -> ILogger -> Async<StreamEventsSlice>)
            (maxPermittedBatchReads : int option) (startPosition : int)
        : AsyncSeq<int option * ResolvedEvent[]> =
        let rec loop batchCount pos = asyncSeq {
            match maxPermittedBatchReads with
            | Some mpbr when batchCount >= mpbr -> log.Information "batch Limit exceeded"; invalidOp "batch Limit exceeded"
            | _ -> ()

            let batchLog = log |> lfc "batchIndex" batchCount
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
    let logResolvedEventBytes events = events |> Array.sumBy (|ResolvedEventLen|) |> logBytes
    let logBatchRead action streamName t events batchSize version (log : ILogger) =
        (log |> logResolvedEventBytes events |> Metrics.log action streamName t)
            .Information("Ges{action:l} stream={stream} count={count}/{batches} version={version}", action, streamName, events.Length, (events.Length - 1)/batchSize + 1, version)
    let loadForwardsFrom (log : ILogger) retryPolicy conn batchSize maxPermittedBatchReads streamName startPosition
        : Async<int * ResolvedEvent[]> = async {
        let mergeBatches (batches: AsyncSeq<int option * ResolvedEvent[]>) = async {
            let mutable versionFromStream = None
            let! (events : ResolvedEvent[]) =
                batches
                |> AsyncSeq.map (function None, events -> events | (Some _) as reportedVersion, events -> versionFromStream <- reportedVersion; events)
                |> AsyncSeq.concatSeq
                |> AsyncSeq.toArrayAsync
            let version = match versionFromStream with Some version -> version | None -> invalidOp "no version encountered in event batch stream"
            return version, events }
        let call pos = loggedReadSlice conn streamName Direction.Forward batchSize pos
        let retryingLoggingReadSlice pos = withLoggedRetries retryPolicy "readAttempt" (call pos)
        let log = log |> logBatchSize batchSize |> logDirection "Forward" |> logStream streamName
        let batches : AsyncSeq<int option * ResolvedEvent[]> = readBatches log retryingLoggingReadSlice maxPermittedBatchReads startPosition
        let! t, (version, events) = mergeBatches batches |> Stopwatch.Time
        log |> logBatchRead "LoadF" streamName t events batchSize version
        return version, events }
    let loadBackwardsUntilCompactionOrStart (log : ILogger) retryPolicy conn batchSize maxPermittedBatchReads streamName isCompactionEvent : Async<int * ResolvedEvent[]> = async {
        let mergeFromCompactionPointOrStartFromBackwardsStream (batchesBackward : AsyncSeq<int option * ResolvedEvent[]>) : Async<int * ResolvedEvent[]> = async {
            let versionFromStream = ref None
            let! tempBackward =
                batchesBackward
                |> AsyncSeq.map (function None, events -> events | (Some _) as reportedVersion, events -> versionFromStream := reportedVersion; events)
                |> AsyncSeq.concatSeq
                |> AsyncSeq.takeWhileInclusive (not << isCompactionEvent)
                |> AsyncSeq.toArrayAsync
            let eventsForward = Array.Reverse(tempBackward); tempBackward // sic - relatively cheap, in-place reverse of something we own
            let version = match !versionFromStream with Some version -> version | None -> invalidOp "no version encountered in event batch stream"
            return version, eventsForward }
        let call pos = loggedReadSlice conn streamName Direction.Backward batchSize pos
        let retryingLoggingReadSlice pos = Impl.withLoggedRetries retryPolicy "ReadRetry" (call pos)
        let log = log |> logBatchSize batchSize |> logDirection "Backwards" |> logStream streamName
        let startPosition = StreamPosition.End
        let batchesBackward : AsyncSeq<int option * ResolvedEvent[]> = readBatches log retryingLoggingReadSlice maxPermittedBatchReads startPosition
        let! t, (version, events) = mergeFromCompactionPointOrStartFromBackwardsStream batchesBackward |> Stopwatch.Time
        log |> logBatchRead "BatchBackward" streamName t events batchSize version 
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

type Token = { streamVersion: int; compactionEventNumber: int option }
// TODO add tests, finish impls for batchCapacityLimit calcs
[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module private Token =
    let private create compactionEventNumber version : Storage.StreamToken =
        { value = box { streamVersion = version; compactionEventNumber = compactionEventNumber }; batchCapacityLimit = None }
    let ofVersion version : Storage.StreamToken =
        create None version
    let ofCompactionEventNumber compactedEventNumber version : Storage.StreamToken =
        create (Some compactedEventNumber) version
    let ofCompactionResolvedEventAndVersion (compactionEvent: ResolvedEvent) version : Storage.StreamToken =
        ofCompactionEventNumber compactionEvent.Event.EventNumber version
    let ofPreviousTokenAndVersion previousToken version : Storage.StreamToken =
        create previousToken.compactionEventNumber version

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
    let isResolvedEventEventType predicate (x:ResolvedEvent) = predicate x.Event.EventType
    let isEventDataEventType predicate (x:EventData) = predicate x.Type
    let fromCompactionEventOrFallback events isCompactionEventType ofCompactionEventAndVersion =
        match isCompactionEventType with
        | Some predicate ->
            Array.tryHead events
            |> Option.filter predicate
            |> Option.map ofCompactionEventAndVersion
        | None -> None
    member __.LoadBatched streamName (log : ILogger) isCompactionEventType: Async<Storage.StreamToken * ResolvedEvent[]> = async {
        let isCompactionEvent = Option.map isResolvedEventEventType isCompactionEventType
        let! version, events = Read.loadForwardsFrom log conn.ReadRetryPolicy conn.Connection config.BatchSize config.MaxBatches streamName 0
        let tokenAlgo = fromCompactionEventOrFallback events isCompactionEvent Token.ofCompactionResolvedEventAndVersion |> Impl.OptionDefaultValue Token.ofVersion
        return tokenAlgo version, events }
    member __.LoadBackwardsStoppingAtCompactionEvent streamName (log : ILogger) isCompactionEventType: Async<Storage.StreamToken * ResolvedEvent[]> = async {
        let isCompactionEvent = isResolvedEventEventType isCompactionEventType
        let! version, events = Read.loadBackwardsUntilCompactionOrStart log conn.ReadRetryPolicy conn.Connection config.BatchSize config.MaxBatches streamName isCompactionEvent
        let tokenAlgo =
            fromCompactionEventOrFallback events (Some isCompactionEvent) Token.ofCompactionResolvedEventAndVersion
            |> Impl.OptionDefaultValue Token.ofVersion
        return tokenAlgo version, events }
    member __.LoadFromToken streamName log (token : Storage.StreamToken) isCompactionEventType: Async<Storage.StreamToken * ResolvedEvent[]> = async {
        let innerToken : Token = (unbox token.value)
        let streamPosition = innerToken.streamVersion + 1
        let isCompactionEvent = Option.map isResolvedEventEventType isCompactionEventType
        let! version, events = Read.loadForwardsFrom log conn.ReadRetryPolicy conn.Connection config.BatchSize config.MaxBatches streamName streamPosition
        let tokenAlgo =
            fromCompactionEventOrFallback events isCompactionEvent Token.ofCompactionResolvedEventAndVersion
            |> Impl.OptionDefaultValue (Token.ofPreviousTokenAndVersion (unbox token.value))
        return tokenAlgo version, events }
    member __.TrySync streamName log (token: Storage.StreamToken) (encodedEvents: EventData array) isCompactionEventType : Async<GatewaySyncResult> = async {
        let! wr = Write.writeEvents log conn.WriteRetryPolicy conn.Connection streamName (unbox token.value).streamVersion encodedEvents
        match wr with
        | EsSyncResult.Conflict -> return GatewaySyncResult.Conflict
        | EsSyncResult.Written wr ->

        let tokenAlgo =
            match isCompactionEventType with
            | None -> Token.ofVersion
            | Some algorithm ->
                let isCompactionEvent = isEventDataEventType algorithm
                let generateCompactionEventNumber _ignoredEventData = Token.ofCompactionEventNumber wr.NextExpectedVersion
                fromCompactionEventOrFallback encodedEvents (Some isCompactionEvent) generateCompactionEventNumber
                |> Impl.OptionDefaultValue (Token.ofPreviousTokenAndVersion (unbox token.value)) 
        return GatewaySyncResult.Written (tokenAlgo wr.NextExpectedVersion) }

type GesStreamState<'event, 'state>(gateway : GesGateway, codec : EventSum.IEventSumEncoder<'event, byte[]>, ?initialTokenAndState : Storage.StreamToken * 'state, ?compactionEventType) =
    let compactionEventAlgorithm =
        match compactionEventType with
        | Some eventType -> Some (fun x -> x = eventType)
        | None -> None
    let loadAlgorithm streamName log =
        match compactionEventAlgorithm with
        | Some predicate -> gateway.LoadBackwardsStoppingAtCompactionEvent streamName log predicate
        | None -> gateway.LoadBatched streamName log None
    let knownTokenAndState = ref initialTokenAndState
    member __.Load streamName (log : ILogger) : Async<Storage.StreamState<'event, 'state>> = async {
        match !knownTokenAndState with
        | Some (token,state) -> knownTokenAndState := None; return token, Some state, List.empty
        | None ->
            let! token, events = loadAlgorithm streamName log
            return EventSumAdapters.decodeKnownEvents codec events |> Storage.StreamState.ofTokenAndEvents token }
    member __.TrySync streamName (log : ILogger) (token, snapshotState) (events : 'event list, proposedState: 'state) = async {
        let encodedEvents : EventData[] = EventSumAdapters.encodeEvents codec events
        let! syncRes = gateway.TrySync streamName log token encodedEvents compactionEventAlgorithm
        match syncRes with
        | GatewaySyncResult.Conflict ->
            let resync = async {
                let! token', events = gateway.LoadFromToken streamName log token compactionEventAlgorithm
                let successorEvents = EventSumAdapters.decodeKnownEvents codec events |> List.ofSeq
                return Storage.StreamState.ofTokenSnapshotAndEvents token' snapshotState successorEvents }
            return Storage.SyncResult.Conflict resync
        | GatewaySyncResult.Written token' ->
            return Storage.SyncResult.Written (Storage.StreamState.ofTokenAndKnownState token' proposedState) }

type GesStream<'event, 'state>(store: GesStreamState<'event, 'state>, streamName) =
    interface IStream<'event, 'state> with
        member __.Load (log : ILogger) : Async<Storage.StreamState<'event, 'state>> =
            store.Load streamName log
        member __.TrySync (log : ILogger) (token, snapshotState) (events : 'event list, proposedState: 'state) =
            store.TrySync streamName log (token, snapshotState) (events, proposedState)