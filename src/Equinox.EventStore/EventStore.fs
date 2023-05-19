namespace Equinox.EventStore

open Equinox.Core
open EventStore.ClientAPI
open Serilog // NB must shadow EventStore.ClientAPI.ILogger
open System
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks

type EventBody = ReadOnlyMemory<byte>

[<RequireQualifiedAccess>]
type Direction = Forward | Backward with
    override this.ToString() = match this with Forward -> "Forward" | Backward -> "Backward"

module Log =

    /// <summary>Name of Property used for <c>Metric</c> in <c>LogEvent</c>s.</summary>
    let [<Literal>] PropertyTag = "esEvt"

    [<NoEquality; NoComparison>]
    type Measurement = { stream: string; interval: StopwatchInterval; bytes: int; count: int }

    [<NoEquality; NoComparison>]
    type Metric =
        | WriteSuccess of Measurement
        | WriteConflict of Measurement
        | Slice of Direction * Measurement
        | Batch of Direction * slices: int * Measurement
    let [<return: Struct>] (|MetricEvent|_|) (logEvent: Serilog.Events.LogEvent): Metric voption =
        let mutable p = Unchecked.defaultof<_>
        logEvent.Properties.TryGetValue(PropertyTag, &p) |> ignore
        match p with Log.ScalarValue (:? Metric as e) -> ValueSome e | _ -> ValueNone

    /// Attach a property to the captured event record to hold the metric information
    let internal event (value: Metric) = Internal.Log.withScalarProperty PropertyTag value
    let prop name value (log: ILogger) = log.ForContext(name, value)
    let propEvents name (kvps: System.Collections.Generic.KeyValuePair<string, string> seq) (log: ILogger) =
        let items = seq { for kv in kvps do yield sprintf "{\"%s\": %s}" kv.Key kv.Value }
        log.ForContext(name, sprintf "[%s]" (String.concat ",\n\r" items))
    let propEventData name (events: EventData[]) (log: ILogger) =
        log |> propEvents name (seq {
            for x in events do
                if x.IsJson then
                    yield System.Collections.Generic.KeyValuePair<_, _>(x.Type, System.Text.Encoding.UTF8.GetString x.Data) })
    let propResolvedEvents name (events: ResolvedEvent[]) (log: ILogger) =
        log |> propEvents name (seq {
            for x in events do
                let e = x.Event
                if e.IsJson then
                    yield System.Collections.Generic.KeyValuePair<_, _>(e.EventType, System.Text.Encoding.UTF8.GetString e.Data) })

    let withLoggedRetries<'t> retryPolicy (contextLabel: string) (f: ILogger -> Task<'t>) log: Task<'t> =
        match retryPolicy with
        | None -> f log
        | Some retryPolicy ->
            let withLoggingContextWrapping count =
                let log = if count = 1 then log else log |> prop contextLabel count
                f log
            retryPolicy withLoggingContextWrapping

    let (|BlobLen|) = function null -> 0 | (x: byte[]) -> x.Length

    /// NB Caveat emptor; this is subject to unlimited change without the major version changing - while the `dotnet-templates` repo will be kept in step, and
    /// the ChangeLog will mention changes, it's critical to not assume that the presence or nature of these helpers be considered stable
    module InternalMetrics =

        module Stats =
            let inline (|Stats|) ({ interval = i }: Measurement) = int64 i.ElapsedMilliseconds

            let (|Read|Write|Resync|Rollup|) = function
                | Slice (_, Stats s) -> Read s
                | WriteSuccess (Stats s) -> Write s
                | WriteConflict (Stats s) -> Resync s
                // slices are rolled up into batches so be sure not to double-count
                | Batch (_, _, Stats s) -> Rollup s

            type Counter =
                { mutable count: int64; mutable ms: int64 }
                static member Create() = { count = 0L; ms = 0L }
                member x.Ingest(ms) =
                    Interlocked.Increment(&x.count) |> ignore
                    Interlocked.Add(&x.ms, ms) |> ignore

            type LogSink() =
                static let epoch = System.Diagnostics.Stopwatch.StartNew()
                static member val Read = Counter.Create() with get, set
                static member val Write = Counter.Create() with get, set
                static member val Resync = Counter.Create() with get, set
                static member Restart() =
                    LogSink.Read <- Counter.Create()
                    LogSink.Write <- Counter.Create()
                    LogSink.Resync <- Counter.Create()
                    let span = epoch.Elapsed
                    epoch.Restart()
                    span
                interface Serilog.Core.ILogEventSink with
                    member _.Emit logEvent = logEvent |> function
                        | MetricEvent (Read stats) -> LogSink.Read.Ingest stats
                        | MetricEvent (Write stats) -> LogSink.Write.Ingest stats
                        | MetricEvent (Resync stats) -> LogSink.Resync.Ingest stats
                        | MetricEvent (Rollup _) -> ()
                        | _ -> ()

        /// Relies on feeding of metrics from Log through to Stats.LogSink
        /// Use Stats.LogSink.Restart() to reset the start point (and stats) where relevant
        let dump (log: ILogger) =
            let stats =
              [ "Read", Stats.LogSink.Read
                "Write", Stats.LogSink.Write
                "Resync", Stats.LogSink.Resync ]
            let logActivity name count lat =
                log.Information("{name}: {count:n0} requests; Average latency: {lat:n0}ms",
                    name, count, (if count = 0L then Double.NaN else float lat/float count))
            let mutable rows, totalCount, totalMs = 0, 0L, 0L
            for name, stat in stats do
                if stat.count <> 0L then
                    totalCount <- totalCount + stat.count
                    totalMs <- totalMs + stat.ms
                    logActivity name stat.count stat.ms
                    rows <- rows + 1
            // Yes, there's a minor race here between the use of the values and the reset
            let duration = Stats.LogSink.Restart()
            if rows > 1 then logActivity "TOTAL" totalCount totalMs
            let measures: (string * (TimeSpan -> float)) list = [ "s", fun x -> x.TotalSeconds(*; "m", fun x -> x.TotalMinutes; "h", fun x -> x.TotalHours*) ]
            let logPeriodicRate name count = log.Information("rp{name} {count:n0}", name, count)
            for uom, f in measures do let d = f duration in if d <> 0. then logPeriodicRate uom (float totalCount/d |> int64)

[<RequireQualifiedAccess; NoEquality; NoComparison>]
type EsSyncResult = Written of (*EventStore.ClientAPI.*) WriteResult | Conflict of actualVersion: int64

module private Write =
    /// Yields `EsSyncResult.Written` or `EsSyncResult.Conflict` to signify WrongExpectedVersion
    let private writeEventsAsync (log: ILogger) (conn: IEventStoreConnection) (streamName: string) (version: int64) (events: EventData[])
        : Task<EsSyncResult> = task {
        try let! wr = conn.AppendToStreamAsync(streamName, version, events)
            return EsSyncResult.Written wr
        with :? EventStore.ClientAPI.Exceptions.WrongExpectedVersionException as ex ->
            log.Information(ex, "Ges TrySync WrongExpectedVersionException writing {EventTypes}, actual {ActualVersion}",
                [| for x in events -> x.Type |], ex.ActualVersion)
            return EsSyncResult.Conflict (let v = ex.ActualVersion in v.Value) }

    let eventDataBytes events =
        let eventDataLen (x: EventData) = match x.Data, x.Metadata with Log.BlobLen bytes, Log.BlobLen metaBytes -> bytes + metaBytes
        events |> Array.sumBy eventDataLen

    let private writeEventsLogged (conn: IEventStoreConnection) (streamName: string) (version: int64) (events: EventData[]) (log: ILogger)
        : Task<EsSyncResult> = task {
        let log = if (not << log.IsEnabled) Events.LogEventLevel.Debug then log else log |> Log.propEventData "Json" events
        let bytes, count = eventDataBytes events, events.Length
        let log = log |> Log.prop "bytes" bytes
        let writeLog = log |> Log.prop "stream" streamName |> Log.prop "expectedVersion" version |> Log.prop "count" count
        let! t, result = (fun _ct -> writeEventsAsync writeLog conn streamName version events) |> Stopwatch.time CancellationToken.None
        let reqMetric: Log.Measurement = { stream = streamName; interval = t; bytes = bytes; count = count}
        let resultLog, evt =
            match result, reqMetric with
            | EsSyncResult.Written x, m ->
                log |> Log.prop "nextExpectedVersion" x.NextExpectedVersion |> Log.prop "logPosition" x.LogPosition, Log.WriteSuccess m
            | EsSyncResult.Conflict actualVersion, m ->
                log |> Log.prop "actualVersion" actualVersion, Log.WriteConflict m
        (resultLog |> Log.event evt).Information("Ges{action:l} count={count} conflict={conflict}",
            "Write", events.Length, match evt with Log.WriteConflict _ -> true | _ -> false)
        return result }

    let writeEvents (log: ILogger) retryPolicy (conn: IEventStoreConnection) (streamName: string) (version: int64) (events: EventData[])
        : Task<EsSyncResult> =
        let call log = writeEventsLogged conn streamName version events log
        Log.withLoggedRetries retryPolicy "writeAttempt" call log

module private Read =
    open FSharp.Control

    let private readSliceAsync (conn: IEventStoreConnection) (streamName: string) (direction: Direction) (batchSize: int) (startPos: int64)
        : Task<StreamEventsSlice> =
        match direction with
        | Direction.Forward  -> conn.ReadStreamEventsForwardAsync(streamName, startPos, batchSize, resolveLinkTos = false)
        | Direction.Backward -> conn.ReadStreamEventsBackwardAsync(streamName, startPos, batchSize, resolveLinkTos = false)

    let (|ResolvedEventLen|) (x: ResolvedEvent) = match x.Event.Data, x.Event.Metadata with Log.BlobLen bytes, Log.BlobLen metaBytes -> bytes + metaBytes

    let private loggedReadSlice conn streamName direction batchSize startPos (log: ILogger): Task<StreamEventsSlice> = task {
        let! t, slice = (fun _ct -> readSliceAsync conn streamName direction batchSize startPos) |> Stopwatch.time CancellationToken.None
        let bytes, count = slice.Events |> Array.sumBy (|ResolvedEventLen|), slice.Events.Length
        let reqMetric: Log.Measurement = { stream = streamName; interval = t; bytes = bytes; count = count}
        let evt = Log.Slice (direction, reqMetric)
        let log = if (not << log.IsEnabled) Events.LogEventLevel.Debug then log else log |> Log.propResolvedEvents "Json" slice.Events
        (log |> Log.prop "startPos" startPos |> Log.prop "bytes" bytes |> Log.event evt).Information("Ges{action:l} count={count} version={version}",
            "Read", count, slice.LastEventNumber)
        return slice }

    let private readBatches (log: ILogger) (readSlice: int64 -> ILogger -> Task<StreamEventsSlice>)
            (maxPermittedBatchReads: int option) (startPosition: int64)
        : IAsyncEnumerable<int64 option * ResolvedEvent[]> =
        let rec loop batchCount pos: IAsyncEnumerable<int64 option * ResolvedEvent[]> = taskSeq {
            match maxPermittedBatchReads with
            | Some mpbr when batchCount >= mpbr -> log.Information "batch Limit exceeded"; invalidOp "batch Limit exceeded"
            | _ -> ()

            let batchLog = log |> Log.prop "batchIndex" batchCount
            let! slice = readSlice pos batchLog
            match slice.Status with
            | SliceReadStatus.StreamDeleted -> raise <| EventStore.ClientAPI.Exceptions.StreamDeletedException(slice.Stream)
            | SliceReadStatus.StreamNotFound -> yield Some (int64 ExpectedVersion.NoStream), Array.empty // NoStream must = -1
            | SliceReadStatus.Success ->
                let version = if batchCount = 0 then Some slice.LastEventNumber else None
                yield version, slice.Events
                if not slice.IsEndOfStream then
                    yield! loop (batchCount + 1) slice.NextEventNumber
            | x -> raise <| ArgumentOutOfRangeException("SliceReadStatus", x, "Unknown result value") }
        loop 0 startPosition

    let resolvedEventBytes events = events |> Array.sumBy (|ResolvedEventLen|)

    let logBatchRead direction streamName t events batchSize version (log: ILogger) =
        let bytes, count = resolvedEventBytes events, events.Length
        let reqMetric: Log.Measurement = { stream = streamName; interval = t; bytes = bytes; count = count}
        let batches = (events.Length - 1) / batchSize + 1
        let action = match direction with Direction.Forward -> "LoadF" | Direction.Backward -> "LoadB"
        let evt = Log.Metric.Batch (direction, batches, reqMetric)
        (log |> Log.prop "bytes" bytes |> Log.event evt).Information(
            "Ges{action:l} stream={stream} count={count}/{batches} version={version}",
            action, streamName, count, batches, version)

    let loadForwardsFrom (log: ILogger) retryPolicy conn batchSize maxPermittedBatchReads streamName startPosition: Task<int64 * ResolvedEvent[]> = task {
        let mergeBatches (batches: IAsyncEnumerable<int64 option * ResolvedEvent[]>) = task {
            let mutable versionFromStream = None
            let! (events: ResolvedEvent[]) =
                batches
                |> TaskSeq.collectSeq (function None, events -> events | Some _ as reportedVersion, events -> versionFromStream <- reportedVersion; events)
                |> TaskSeq.toArrayAsync
            let version = match versionFromStream with Some version -> version | None -> invalidOp "no version encountered in event batch stream"
            return version, events }

        let call = loggedReadSlice conn streamName Direction.Forward batchSize
        let retryingLoggingReadSlice pos = Log.withLoggedRetries retryPolicy "readAttempt" (call pos)
        let direction = Direction.Forward
        let log = log |> Log.prop "batchSize" batchSize |> Log.prop "direction" direction |> Log.prop "stream" streamName
        let batches _ct: IAsyncEnumerable<int64 option * ResolvedEvent[]> = readBatches log retryingLoggingReadSlice maxPermittedBatchReads startPosition
        let! t, (version, events) = (batches >> mergeBatches) |> Stopwatch.time CancellationToken.None
        log |> logBatchRead direction streamName t events batchSize version
        return version, events }

    let partitionPayloadFrom firstUsedEventNumber: ResolvedEvent[] -> int * int =
        let acc (tu, tr) (ResolvedEventLen bytes as y) = if y.Event.EventNumber < firstUsedEventNumber then tu, tr + bytes else tu + bytes, tr
        Array.fold acc (0, 0)

    let loadBackwardsUntilCompactionOrStart (log: ILogger) retryPolicy conn batchSize maxPermittedBatchReads streamName (tryDecode, isOrigin)
        : Task<int64 * struct (ResolvedEvent * 'event voption)[]> = task {
        let mergeFromCompactionPointOrStartFromBackwardsStream (log: ILogger) (batchesBackward: IAsyncEnumerable<int64 option * ResolvedEvent[]>)
            : Task<int64 * struct (ResolvedEvent * 'event voption)[]> = task {
            let versionFromStream, lastBatch = ref None, ref None
            let! tempBackward =
                batchesBackward
                |> TaskSeq.collectSeq (fun batch ->
                    match batch with
                    | None, events -> lastBatch.Value <- Some events; events
                    | Some _ as reportedVersion, events -> versionFromStream.Value <- reportedVersion; lastBatch.Value <- Some events; events
                    |> Array.map (fun e -> struct (e, tryDecode e)))
                |> TaskSeq.takeWhileInclusive (function
                    | x, ValueSome e when isOrigin e ->
                        match lastBatch.Value with
                        | None -> log.Information("GesStop stream={stream} at={eventNumber}", streamName, x.Event.EventNumber)
                        | Some batch ->
                            let used, residual = batch |> partitionPayloadFrom x.Event.EventNumber
                            log.Information("GesStop stream={stream} at={eventNumber} used={used} residual={residual}", streamName, x.Event.EventNumber, used, residual)
                        false
                    | _ -> true) // continue the search
                |> TaskSeq.toArrayAsync
            let eventsForward = Array.Reverse(tempBackward); tempBackward // sic - relatively cheap, in-place reverse of something we own
            let version = match versionFromStream.Value with Some version -> version | None -> invalidOp "no version encountered in event batch stream"
            return version, eventsForward }

        let call = loggedReadSlice conn streamName Direction.Backward batchSize
        let retryingLoggingReadSlice pos = Log.withLoggedRetries retryPolicy "readAttempt" (call pos)
        let log = log |> Log.prop "batchSize" batchSize |> Log.prop "stream" streamName
        let startPosition = int64 StreamPosition.End
        let direction = Direction.Backward
        let readlog = log |> Log.prop "direction" direction
        let batchesBackward _ct: IAsyncEnumerable<int64 option * ResolvedEvent[]> = readBatches readlog retryingLoggingReadSlice maxPermittedBatchReads startPosition
        let! t, (version, events) = (batchesBackward >> mergeFromCompactionPointOrStartFromBackwardsStream log) |> Stopwatch.time CancellationToken.None
        log |> logBatchRead direction streamName t (Array.map ValueTuple.fst events) batchSize version
        return version, events }

module UnionEncoderAdapters =
    let encodedEventOfResolvedEvent (x: ResolvedEvent): FsCodec.ITimelineEvent<EventBody> =
        let e = x.Event
        // Inspecting server code shows both Created and CreatedEpoch are set; taking this as it's less ambiguous than DateTime in the general case
        let ts = DateTimeOffset.FromUnixTimeMilliseconds(e.CreatedEpoch)
        // TOCONSIDER wire e.Metadata.["$correlationId"] and .["$causationId"] into correlationId and causationId
        // https://eventstore.org/docs/server/metadata-and-reserved-names/index.html#event-metadata
        FsCodec.Core.TimelineEvent.Create(e.EventNumber, e.EventType, e.Data, e.Metadata, e.EventId, correlationId = null, causationId = null, timestamp = ts)

    let eventDataOfEncodedEvent (x: FsCodec.IEventData<EventBody>) =
        // TOCONSIDER wire x.CorrelationId, x.CausationId into x.Meta.["$correlationId"] and .["$causationId"]
        // https://eventstore.org/docs/server/metadata-and-reserved-names/index.html#event-metadata
        let map (x: EventBody): byte[] = x.ToArray()
        EventData(x.EventId, x.EventType, isJson = true, data = map x.Data, metadata = map x.Meta)

type Position = { streamVersion: int64; compactionEventNumber: int64 option; batchCapacityLimit: int option }
type Token = { pos: Position }

module Token =
    let private create compactionEventNumber batchCapacityLimit streamVersion: StreamToken =
        {   value = box {
                pos = { streamVersion = streamVersion; compactionEventNumber = compactionEventNumber; batchCapacityLimit = batchCapacityLimit } }
            // In this impl, the StreamVersion matches the EventStore StreamVersion in being -1-based
            // Version however is the representation that needs to align with ISyncContext.Version
            version = streamVersion + 1L
            // TOCONSIDER Could implement accumulating the size as it's loaded (but should stop counting when it hits the origin event)
            streamBytes = -1 }

    /// No batching / compaction; we only need to retain the StreamVersion
    let ofNonCompacting streamVersion: StreamToken =
        create None None streamVersion

    // headroom before compaction is necessary given the stated knowledge of the last (if known) `compactionEventNumberOption`
    let private batchCapacityLimit compactedEventNumberOption unstoredEventsPending (batchSize: int) (streamVersion: int64): int =
        match compactedEventNumberOption with
        | Some (compactionEventNumber: int64) -> (batchSize - unstoredEventsPending) - int (streamVersion - compactionEventNumber + 1L) |> max 0
        | None -> (batchSize - unstoredEventsPending) - (int streamVersion + 1) - 1 |> max 0

    let (*private*) ofCompactionEventNumber compactedEventNumberOption unstoredEventsPending batchSize streamVersion: StreamToken =
        let batchCapacityLimit = batchCapacityLimit compactedEventNumberOption unstoredEventsPending batchSize streamVersion
        create compactedEventNumberOption (Some batchCapacityLimit) streamVersion

    /// Assume we have not seen any compaction events; use the batchSize and version to infer headroom
    let ofUncompactedVersion batchSize streamVersion: StreamToken =
        ofCompactionEventNumber None 0 batchSize streamVersion

    let (|Unpack|) (x: StreamToken): Position = let t = unbox<Token> x.value in t.pos

    /// Use previousToken plus the data we are adding and the position we are adding it to infer a headroom
    let ofPreviousTokenAndEventsLength (Unpack previousToken) eventsLength batchSize streamVersion: StreamToken =
        let compactedEventNumber = previousToken.compactionEventNumber
        ofCompactionEventNumber compactedEventNumber eventsLength batchSize streamVersion

    /// Use an event just read from the stream to infer headroom
    let ofCompactionResolvedEventAndVersion (compactionEvent: ResolvedEvent) batchSize streamVersion: StreamToken =
        ofCompactionEventNumber (Some compactionEvent.Event.EventNumber) 0 batchSize streamVersion

    /// Use an event we are about to write to the stream to infer headroom
    let ofPreviousStreamVersionAndCompactionEventDataIndex (Unpack token) compactionEventDataIndex eventsLength batchSize streamVersion': StreamToken =
        ofCompactionEventNumber (Some (token.streamVersion + 1L + int64 compactionEventDataIndex)) eventsLength batchSize streamVersion'

    /// Like other .NET CompareTo operators: negative if current is superseded by candidate, 0 if equivalent, positive if candidate stale
    let compare struct (current, candidate) = current.version - candidate.version

type EventStoreConnection(readConnection, [<O; D(null)>] ?writeConnection, [<O; D(null)>] ?readRetryPolicy, [<O; D(null)>] ?writeRetryPolicy) =
    member _.ReadConnection = readConnection
    member _.ReadRetryPolicy = readRetryPolicy
    member _.WriteConnection = defaultArg writeConnection readConnection
    member _.WriteRetryPolicy = writeRetryPolicy

type BatchOptions(getBatchSize: Func<int>, [<O; D(null)>]?batchCountLimit) =
    new (batchSize) = BatchOptions(fun () -> batchSize)
    member _.BatchSize = getBatchSize.Invoke()
    member _.MaxBatches = batchCountLimit

[<RequireQualifiedAccess; NoComparison; NoEquality>]
type GatewaySyncResult = Written of StreamToken | ConflictUnknown of StreamToken

type EventStoreContext(connection: EventStoreConnection, batchOptions: BatchOptions) =
    let isResolvedEventEventType (tryDecode, predicate) (x: ResolvedEvent) = predicate (tryDecode x.Event.Data)
    let tryIsResolvedEventEventType predicateOption = predicateOption |> Option.map isResolvedEventEventType
    let conn requireLeader = if requireLeader then connection.WriteConnection else connection.ReadConnection
    new (   connection: EventStoreConnection,
            // Max number of Events to retrieve in a single batch. Also affects frequency of RollingSnapshots. Default: 500.
            [<O; D null>] ?batchSize) =
        EventStoreContext(connection, BatchOptions(batchSize = defaultArg batchSize 500))
    member val BatchOptions = batchOptions

    member _.TokenEmpty = Token.ofUncompactedVersion batchOptions.BatchSize -1L
    member _.LoadBatched(log, streamName, requireLeader, tryDecode, isCompactionEventType): Task<StreamToken * 'event[]> = task {
        let! version, events = Read.loadForwardsFrom log connection.ReadRetryPolicy (conn requireLeader) batchOptions.BatchSize batchOptions.MaxBatches streamName 0L
        match tryIsResolvedEventEventType isCompactionEventType with
        | None -> return Token.ofNonCompacting version, Array.chooseV tryDecode events
        | Some isCompactionEvent ->
            match events |> Array.tryFindBack isCompactionEvent with
            | None -> return Token.ofUncompactedVersion batchOptions.BatchSize version, Array.chooseV tryDecode events
            | Some resolvedEvent -> return Token.ofCompactionResolvedEventAndVersion resolvedEvent batchOptions.BatchSize version, Array.chooseV tryDecode events }
    member _.LoadBackwardsStoppingAtCompactionEvent(log, streamName, requireLeader, tryDecode, isOrigin): Task<StreamToken * 'event []> = task {
        let! version, events =
            Read.loadBackwardsUntilCompactionOrStart log connection.ReadRetryPolicy (conn requireLeader) batchOptions.BatchSize batchOptions.MaxBatches streamName (tryDecode, isOrigin)
        match Array.tryHead events |> Option.filter (function _, ValueSome e -> isOrigin e | _ -> false) with
        | None -> return Token.ofUncompactedVersion batchOptions.BatchSize version, Array.chooseV ValueTuple.snd events
        | Some (resolvedEvent, _) -> return Token.ofCompactionResolvedEventAndVersion resolvedEvent batchOptions.BatchSize version, Array.chooseV ValueTuple.snd events }
    member _.Reload(log, streamName, requireLeader, (Token.Unpack token as streamToken), tryDecode, isCompactionEventType)
        : Task<StreamToken * 'event[]> = task {
        let streamPosition = token.streamVersion + 1L
        let! version, events = Read.loadForwardsFrom log connection.ReadRetryPolicy (conn requireLeader) batchOptions.BatchSize batchOptions.MaxBatches streamName streamPosition
        match isCompactionEventType with
        | None -> return Token.ofNonCompacting version, Array.chooseV tryDecode events
        | Some isCompactionEvent ->
            match events |> Array.tryFindBack (fun re -> match tryDecode re with ValueSome e -> isCompactionEvent e | _ -> false) with
            | None -> return Token.ofPreviousTokenAndEventsLength streamToken events.Length batchOptions.BatchSize version, Array.chooseV tryDecode events
            | Some resolvedEvent -> return Token.ofCompactionResolvedEventAndVersion resolvedEvent batchOptions.BatchSize version, Array.chooseV tryDecode events }

    member _.TrySync(log, streamName, (Token.Unpack token as streamToken), (events, encodedEvents: EventData[]), isCompactionEventType): Task<GatewaySyncResult> = task {
        let streamVersion = token.streamVersion
        match! Write.writeEvents log connection.WriteRetryPolicy connection.WriteConnection streamName streamVersion encodedEvents with
        | EsSyncResult.Written wr ->
            let version' = wr.NextExpectedVersion
            let token =
                match isCompactionEventType with
                | None -> Token.ofNonCompacting version'
                | Some isCompactionEvent ->
                    match events |> Array.tryFindIndexBack isCompactionEvent with
                    | None -> Token.ofPreviousTokenAndEventsLength streamToken encodedEvents.Length batchOptions.BatchSize version'
                    | Some compactionEventIndex ->
                        Token.ofPreviousStreamVersionAndCompactionEventDataIndex streamToken compactionEventIndex encodedEvents.Length batchOptions.BatchSize version'
            return GatewaySyncResult.Written token
        | EsSyncResult.Conflict actualVersion ->
            return GatewaySyncResult.ConflictUnknown (Token.ofNonCompacting actualVersion) }
    // Used by Propulsion.EventStore.EventStoreSink
    member _.Sync(log, streamName, streamVersion, events: FsCodec.IEventData<EventBody>[]): Task<GatewaySyncResult> = task {
        let encodedEvents: EventData[] = events |> Array.map UnionEncoderAdapters.eventDataOfEncodedEvent
        match! Write.writeEvents log connection.WriteRetryPolicy connection.WriteConnection streamName streamVersion encodedEvents with
        | EsSyncResult.Written wr ->
            let version' = wr.NextExpectedVersion
            let token = Token.ofNonCompacting version'
            return GatewaySyncResult.Written token
        | EsSyncResult.Conflict actualVersion ->
            return GatewaySyncResult.ConflictUnknown (Token.ofNonCompacting actualVersion) }

[<NoComparison; NoEquality; RequireQualifiedAccess>]
type AccessStrategy<'event, 'state> =
    /// Load only the single most recent event defined in <c>'event</c> and trust that doing a <c>fold</c> from any such event
    /// will yield a correct and complete state
    /// In other words, the <c>fold</c> function should not need to consider either the preceding <c>'state</state> or <c>'event</c>s.
    | LatestKnownEvent
    /// Ensures a snapshot/compaction event from which the state can be reconstituted upon decoding is always present
    /// (embedded in the stream as an event), generated every <c>batchSize</c> events using the supplied <c>toSnapshot</c> function
    /// Scanning for events concludes when any event passes the <c>isOrigin</c> test.
    /// See https://eventstore.org/docs/event-sourcing-basics/rolling-snapshots/index.html
    | RollingSnapshots of isOrigin: ('event -> bool) * toSnapshot: ('state -> 'event)

type private CompactionContext(eventsLen: int, capacityBeforeCompaction: int) =
    /// Determines whether writing a Compaction event is warranted (based on the existing state and the current accumulated changes)
    member _.IsCompactionDue = eventsLen > capacityBeforeCompaction

type private Category<'event, 'state, 'context>(context: EventStoreContext, codec: FsCodec.IEventCodec<_, _, 'context>, fold, initial, access) =
    let tryDecode (e: ResolvedEvent) = e |> UnionEncoderAdapters.encodedEventOfResolvedEvent |> codec.TryDecode
    let isOrigin =
        match access with
        | None | Some AccessStrategy.LatestKnownEvent -> fun _ -> true
        | Some (AccessStrategy.RollingSnapshots (isValid, _)) -> isValid
    let loadAlgorithm log streamName requireLeader =
        match access with
        | None -> context.LoadBatched(log, streamName, requireLeader, tryDecode, None)
        | Some AccessStrategy.LatestKnownEvent
        | Some (AccessStrategy.RollingSnapshots _) -> context.LoadBackwardsStoppingAtCompactionEvent(log, streamName, requireLeader, tryDecode, isOrigin)
    let compactionPredicate =
        match access with
        | None -> None
        | Some AccessStrategy.LatestKnownEvent -> Some (fun _ -> true)
        | Some (AccessStrategy.RollingSnapshots (isValid, _)) -> Some isValid
    let fetch state f = task { let! token', events = f in return struct (token', fold state (Seq.ofArray events)) }
    let reload (log, sn, leader, token, state) = fetch state (context.Reload(log, sn, leader, token, tryDecode, compactionPredicate))
    interface ICategory<'event, 'state, 'context> with
        member _.Load(log, _categoryName, _streamId, streamName, _maxAge, requireLeader, _ct) =
            fetch initial (loadAlgorithm log streamName requireLeader)
        member _.TrySync(log, _categoryName, _streamId, streamName, ctx, _maybeInit, (Token.Unpack token as streamToken), state, events, _ct) = task {
            let events =
                match access with
                | None | Some AccessStrategy.LatestKnownEvent -> events
                | Some (AccessStrategy.RollingSnapshots (_, compact)) ->
                    let cc = CompactionContext(Array.length events, token.batchCapacityLimit.Value)
                    if cc.IsCompactionDue then Array.append events (fold state events |> compact |> Array.singleton) else events
            let encode e = codec.Encode(ctx, e)
            let encodedEvents: EventData[] = events |> Array.map (encode >> UnionEncoderAdapters.eventDataOfEncodedEvent)
            match! context.TrySync(log, streamName, streamToken, (events, encodedEvents), compactionPredicate) with
            | GatewaySyncResult.Written token' ->    return SyncResult.Written  (token', fold state events)
            | GatewaySyncResult.ConflictUnknown _ -> return SyncResult.Conflict (fun _ct -> reload (log, streamName, true, streamToken, state)) }
    interface Caching.IReloadable<'state> with member _.Reload(log, sn, leader, token, state, _ct) = reload (log, sn, leader, token, state)

type EventStoreCategory<'event, 'state, 'context> internal (resolveInner, empty) =
    inherit Equinox.Category<'event, 'state, 'context>(resolveInner, empty)
    new(context: EventStoreContext, codec: FsCodec.IEventCodec<_, _, 'context>, fold, initial,
        // Caching can be overkill for EventStore esp considering the degree to which its intrinsic caching is a first class feature
        // e.g., A key benefit is that reads of streams more than a few pages long get completed in constant time after the initial load
        [<O; D(null)>] ?caching,
        [<O; D(null)>] ?access) =
        do  match access with
            | Some AccessStrategy.LatestKnownEvent when Option.isSome caching ->
                "Equinox.EventStore does not support (and it would make things _less_ efficient even if it did)"
                + "mixing AccessStrategy.LatestKnownEvent with Caching at present."
                |> invalidOp
            | _ -> ()
        let cat = Category<'event, 'state, 'context>(context, codec, fold, initial, access) |> Caching.apply Token.compare caching
        let resolveInner categoryName streamId = struct (cat, StreamName.render categoryName streamId, ValueNone)
        let empty = struct (context.TokenEmpty, initial)
        EventStoreCategory(resolveInner, empty)

type private SerilogAdapter(log: ILogger) =
    interface EventStore.ClientAPI.ILogger with
        member _.Debug(format: string, args: obj []) =           log.Debug(format, args)
        member _.Debug(ex: exn, format: string, args: obj []) =  log.Debug(ex, format, args)
        member _.Info(format: string, args: obj []) =            log.Information(format, args)
        member _.Info(ex: exn, format: string, args: obj []) =   log.Information(ex, format, args)
        member _.Error(format: string, args: obj []) =           log.Error(format, args)
        member _.Error(ex: exn, format: string, args: obj []) =  log.Error(ex, format, args)

[<RequireQualifiedAccess; NoComparison; NoEquality>]
type Logger =
    | SerilogVerbose of ILogger
    | SerilogNormal of ILogger
    | CustomVerbose of EventStore.ClientAPI.ILogger
    | CustomNormal of EventStore.ClientAPI.ILogger
    member log.Configure(b: ConnectionSettingsBuilder) =
        match log with
        | SerilogVerbose logger -> b.EnableVerboseLogging().UseCustomLogger(SerilogAdapter(logger))
        | SerilogNormal logger -> b.UseCustomLogger(SerilogAdapter(logger))
        | CustomVerbose logger -> b.EnableVerboseLogging().UseCustomLogger(logger)
        | CustomNormal logger -> b.UseCustomLogger(logger)

[<RequireQualifiedAccess; NoComparison>]
type NodePreference =
    /// Track master via gossip, writes direct, reads should immediately reflect writes, resync without backoff (highest load on master, good write perf)
    | Master
    /// Take the first connection that comes along, ideally a master, but do not track master changes
    | PreferMaster
    /// Prefer slave node, writes normally need forwarding, often can't read writes, resync requires backoff (kindest to master, writes and resyncs expensive)
    | PreferSlave
    /// Take random node, writes may need forwarding, sometimes can't read writes, resync requires backoff (balanced load on master, balanced write perf)
    | Random

[<RequireQualifiedAccess; NoComparison>]
type Discovery =
    // Allow Uri-based connection definition (discovery://, tcp:// or
    | Uri of Uri
    /// Supply a set of pre-resolved EndPoints instead of letting Gossip resolution derive from the DNS outcome
    | GossipSeeded of seedManagerEndpoints: System.Net.EndPoint []
    // Standard Gossip-based discovery based on Dns query and standard manager port
    | GossipDns of clusterDns: string
    // Standard Gossip-based discovery based on Dns query (with manager port overriding default 2113)
    | GossipDnsCustomPort of clusterDns: string * managerPortOverride: int

module private Discovery =
    let buildDns np (f: DnsClusterSettingsBuilder -> DnsClusterSettingsBuilder) =
        ClusterSettings.Create().DiscoverClusterViaDns().KeepDiscovering()
        |> fun s -> match np with NodePreference.Random -> s.PreferRandomNode() | NodePreference.PreferSlave -> s.PreferFollowerNode() | _ -> s
        |> f |> fun s -> s.Build()

    let buildSeeded np (f: GossipSeedClusterSettingsBuilder -> GossipSeedClusterSettingsBuilder) =
        ClusterSettings.Create().DiscoverClusterViaGossipSeeds().KeepDiscovering()
        |> fun s -> match np with NodePreference.Random -> s.PreferRandomNode() | NodePreference.PreferSlave -> s.PreferFollowerNode() | _ -> s
        |> f |> fun s -> s.Build()

    let configureDns clusterDns maybeManagerPort (x: DnsClusterSettingsBuilder) =
        x.SetClusterDns(clusterDns)
        |> fun s -> match maybeManagerPort with Some port -> s.SetClusterGossipPort(port) | None -> s

    let inline configureSeeded (seedEndpoints: System.Net.EndPoint []) (x: GossipSeedClusterSettingsBuilder) =
        x.SetGossipSeedEndPoints(seedEndpoints)

    // converts a Discovery mode to a ClusterSettings or a Uri as appropriate
    let (|DiscoverViaUri|DiscoverViaGossip|): Discovery * NodePreference -> Choice<Uri, ClusterSettings> = function
        | Discovery.Uri uri, _ ->                           DiscoverViaUri    uri
        | Discovery.GossipSeeded seedEndpoints, np ->       DiscoverViaGossip (buildSeeded np   (configureSeeded seedEndpoints))
        | Discovery.GossipDns clusterDns, np ->             DiscoverViaGossip (buildDns np      (configureDns clusterDns None))
        | Discovery.GossipDnsCustomPort (dns, port), np ->  DiscoverViaGossip (buildDns np      (configureDns dns (Some port)))

// see https://github.com/EventStore/EventStore/issues/1652
[<RequireQualifiedAccess; NoComparison>]
type ConnectionStrategy =
    /// Pair of master and slave connections, writes direct, often can't read writes, resync without backoff (kind to master, writes+resyncs optimal)
    | ClusterTwinPreferSlaveReads
    /// Single connection, with resync backoffs appropriate to the NodePreference
    | ClusterSingle of NodePreference

type EventStoreConnector
    (   username, password, reqTimeout: TimeSpan, reqRetries: int,
        [<O; D(null)>] ?log: Logger, [<O; D(null)>] ?heartbeatTimeout: TimeSpan, [<O; D(null)>] ?concurrentOperationsLimit,
        [<O; D(null)>] ?readRetryPolicy, [<O; D(null)>] ?writeRetryPolicy,
        [<O; D(null)>] ?gossipTimeout, [<O; D(null)>] ?clientConnectionTimeout,
        // Additional strings identifying the context of this connection; should provide enough context to disambiguate all potential connections to a cluster
        // NB as this will enter server and client logs, it should not contain sensitive information
        [<O; D(null)>] ?tags: (string*string) seq,
        // Facilitates arbitrary customization of settings that are not explicitly addressed herein and/or general post-processing of the configuration.
        [<O; D(null)>] ?custom: ConnectionSettingsBuilder -> ConnectionSettingsBuilder) =
    let connSettings node =
      ConnectionSettings.Create().SetDefaultUserCredentials(SystemData.UserCredentials(username, password))
        .KeepReconnecting() // ES default: .LimitReconnectionsTo(10)
        .SetQueueTimeoutTo(reqTimeout) // ES default: Zero/unlimited
        .FailOnNoServerResponse() // ES default: DoNotFailOnNoServerResponse() => wait forever; retry and/or log
        .SetOperationTimeoutTo(reqTimeout) // ES default: 7s
        .LimitRetriesForOperationTo(reqRetries) // ES default: 10
        |> fun s ->
            match node with
            | NodePreference.Master         -> s.PerformOnLeaderOnly()                  // explicitly use ES default of requiring master, use default Node preference of Master
            | NodePreference.PreferMaster   -> s.PerformOnAnyNode()                     // override default [implied] PerformOnMasterOnly(), use default Node preference of Master
            // NB .PreferSlaveNode/.PreferRandomNode setting is ignored if using EventStoreConnection.Create(ConnectionSettings, ClusterSettings) overload but
            // this code is necessary for cases where people are using the discover:// and related URI schemes
            | NodePreference.PreferSlave    -> s.PerformOnAnyNode().PreferFollowerNode()// override default PerformOnMasterOnly(), override Master Node preference
            | NodePreference.Random         -> s.PerformOnAnyNode().PreferRandomNode()  // override default PerformOnMasterOnly(), override Master Node preference
        |> fun s -> match concurrentOperationsLimit with Some col -> s.LimitConcurrentOperationsTo(col) | None -> s // ES default: 5000
        |> fun s -> match heartbeatTimeout with Some v -> s.SetHeartbeatTimeout v | None -> s // default: 1500 ms
        |> fun s -> match gossipTimeout with Some v -> s.SetGossipTimeout v | None -> s // default: 1000 ms
        |> fun s -> match clientConnectionTimeout with Some v -> s.WithConnectionTimeoutOf v | None -> s // default: 1000 ms
        |> fun s -> match log with Some log -> log.Configure s | None -> s
        |> fun s -> match custom with Some c -> c s | None -> s
        |> fun s -> s.Build()

    /// Yields an IEventStoreConnection configured and Connect()ed to a node (or the cluster) per the supplied `discovery` and `clusterNodePreference` preference
    member _.Connect
        (   // Name should be sufficient to uniquely identify this connection within a single app instance's logs
            name,
            discovery: Discovery, [<O; D null>] ?clusterNodePreference): Async<IEventStoreConnection> = async {
        if name = null then nullArg "name"
        let clusterNodePreference = defaultArg clusterNodePreference NodePreference.Master
        let name = String.concat ";" <| seq {
            yield name
            yield string clusterNodePreference
            match tags with None -> () | Some tags -> for key, value in tags do yield sprintf "%s=%s" key value }
        let sanitizedName = name.Replace('\'', '_').Replace(':', '_') // ES internally uses `:` and `'` as separators in log messages and ... people regex logs
        let connection =
            match discovery, clusterNodePreference with
            | Discovery.DiscoverViaUri uri ->
                // This overload picks up the discovery settings via ConnectionSettingsBuilder.PreferSlaveNode/.PreferRandomNode
                EventStoreConnection.Create(connSettings clusterNodePreference, uri, sanitizedName)
            | Discovery.DiscoverViaGossip clusterSettings ->
                // NB This overload's implementation ignores the calls to ConnectionSettingsBuilder.PreferSlaveNode/.PreferRandomNode and
                // requires equivalent ones on the GossipSeedClusterSettingsBuilder or ClusterSettingsBuilder
                EventStoreConnection.Create(connSettings clusterNodePreference, clusterSettings, sanitizedName)
        do! connection.ConnectAsync() |> Async.AwaitTaskCorrect
        return connection }

    /// Yields a Connection (which may internally be twin connections) configured per the specified strategy
    member x.Establish
        (   // Name should be sufficient to uniquely identify this (aggregate) connection within a single app instance's logs
            name,
            discovery: Discovery, strategy: ConnectionStrategy): Async<EventStoreConnection> = async {
        match strategy with
        | ConnectionStrategy.ClusterSingle nodePreference ->
            let! conn = x.Connect(name, discovery, nodePreference)
            return EventStoreConnection(conn, ?readRetryPolicy = readRetryPolicy, ?writeRetryPolicy = writeRetryPolicy)
        | ConnectionStrategy.ClusterTwinPreferSlaveReads ->
            let! masterInParallel = Async.StartChild (x.Connect(name + "-TwinW", discovery, NodePreference.Master))
            let! slave = x.Connect(name + "-TwinR", discovery, NodePreference.PreferSlave)
            let! master = masterInParallel
            return EventStoreConnection(readConnection = slave, writeConnection = master, ?readRetryPolicy = readRetryPolicy, ?writeRetryPolicy = writeRetryPolicy) }
