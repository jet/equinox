namespace Equinox.EventStoreDb

open Equinox.Core
open EventStore.Client
open Serilog
open System

type EventBody = ReadOnlyMemory<byte>

[<RequireQualifiedAccess>]
type Direction = Forward | Backward with
    override this.ToString() = match this with Forward -> "Forward" | Backward -> "Backward"

module Log =

    /// <summary>Name of Property used for <c>Metric</c> in <c>LogEvent</c>s.</summary>
    let [<Literal>] PropertyTag = "esdbEvt"

    [<NoEquality; NoComparison>]
    type Measurement = { stream: string; interval: StopwatchInterval; bytes: int; count: int }

    [<NoEquality; NoComparison>]
    type Metric =
        | WriteSuccess of Measurement
        | WriteConflict of Measurement
        | Batch of Direction * slices: int * Measurement
    let [<return: Struct>] (|MetricEvent|_|) (logEvent: Serilog.Events.LogEvent): Metric voption =
        let mutable p = Unchecked.defaultof<_>
        logEvent.Properties.TryGetValue(PropertyTag, &p) |> ignore
        match p with Log.ScalarValue (:? Metric as e) -> ValueSome e | _ -> ValueNone

    /// Attach a property to the captured event record to hold the metric information
    let internal event (value: Metric) = Internal.Log.withScalarProperty PropertyTag value
    let prop name value (log: ILogger) = log.ForContext(name, value)
    let propEvents name (kvps: System.Collections.Generic.KeyValuePair<string,string> seq) (log: ILogger) =
        let items = seq { for kv in kvps do yield sprintf "{\"%s\": %s}" kv.Key kv.Value }
        log.ForContext(name, sprintf "[%s]" (String.concat ",\n\r" items))
    let propEventData name (events: EventData[]) (log: ILogger) =
        log |> propEvents name (seq {
            for x in events do
                if x.ContentType = "application/json" then
                    yield let d = x.Data in System.Collections.Generic.KeyValuePair<_,_>(x.Type, System.Text.Encoding.UTF8.GetString d.Span) })
    let propResolvedEvents name (events: ResolvedEvent[]) (log: ILogger) =
        log |> propEvents name (seq {
            for x in events do
                let e = x.Event
                if e.ContentType = "application/json" then
                    yield let d = e.Data in System.Collections.Generic.KeyValuePair<_,_>(e.EventType, System.Text.Encoding.UTF8.GetString d.Span) })

    let withLoggedRetries<'t> retryPolicy (contextLabel: string) (f: ILogger -> CancellationToken -> Task<'t>) log ct: Task<'t> =
        match retryPolicy with
        | None -> f log ct
        | Some retryPolicy ->
            let withLoggingContextWrapping count =
                let log = if count = 1 then log else log |> prop contextLabel count
                f log
            retryPolicy withLoggingContextWrapping

    let (|BlobLen|) (x: ReadOnlyMemory<byte>) = x.Length

    /// NB Caveat emptor; this is subject to unlimited change without the major version changing - while the `dotnet-templates` repo will be kept in step, and
    /// the ChangeLog will mention changes, it's critical to not assume that the presence or nature of these helpers be considered stable
    module InternalMetrics =

        module Stats =
            let inline (|Stats|) ({ interval = i }: Measurement) = int64 i.ElapsedMilliseconds

            let (|Read|Write|Resync|) = function
                | WriteSuccess (Stats s) -> Write s
                | WriteConflict (Stats s) -> Resync s
                // slices are rolled up into batches in other stores, but we don't log slices in this impl
                | Batch (_, _, Stats s) -> Read s

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
type EsSyncResult = Written of ConditionalWriteResult | Conflict of actualVersion: int64

module private Write =

    let private writeEventsAsync (log: ILogger) (conn: EventStoreClient) (streamName: string) version (events: EventData[]) ct: Task<EsSyncResult> = task {
        let! wr = conn.ConditionalAppendToStreamAsync(streamName, StreamRevision.FromInt64 version, events, cancellationToken = ct)
        if wr.Status = ConditionalWriteStatus.VersionMismatch then
            log.Information("Esdb Sync VersionMismatch writing {EventTypes}, actual {ActualVersion}",
                [| for x in events -> x.Type |], wr.NextExpectedVersion)
            return EsSyncResult.Conflict wr.NextExpectedVersion
        elif wr.Status = ConditionalWriteStatus.StreamDeleted then return failwith $"Unexpected write to deleted stream %s{streamName}"
        elif wr.Status = ConditionalWriteStatus.Succeeded then return EsSyncResult.Written wr
        else return failwith $"Unexpected write response code {wr.Status}" }

    let private eventDataBytes events =
        let eventDataLen (x: EventData) = match x.Data, x.Metadata with Log.BlobLen bytes, Log.BlobLen metaBytes -> bytes + metaBytes
        events |> Array.sumBy eventDataLen

    let private writeEventsLogged (conn: EventStoreClient) (streamName: string) (version: int64) (events: EventData[]) (log: ILogger) ct
        : Task<EsSyncResult> = task {
        let log = if (not << log.IsEnabled) Events.LogEventLevel.Debug then log else log |> Log.propEventData "Json" events
        let bytes, count = eventDataBytes events, events.Length
        let log = log |> Log.prop "bytes" bytes |> Log.prop "expectedVersion" version
        let writeLog = log |> Log.prop "stream" streamName |> Log.prop "count" count
        let! t, result = writeEventsAsync writeLog conn streamName version events |> Stopwatch.time ct
        let reqMetric: Log.Measurement = { stream = streamName; interval = t; bytes = bytes; count = count}
        let resultLog, evt =
            match result, reqMetric with
            | EsSyncResult.Written x, m ->
                log |> Log.prop "nextExpectedVersion" x.NextExpectedVersion |> Log.prop "logPosition" x.LogPosition, Log.WriteSuccess m
            | EsSyncResult.Conflict actualVersion, m ->
                log |> Log.prop "actualVersion" actualVersion, Log.WriteConflict m
        (resultLog |> Log.event evt).Information("Esdb{action:l} count={count} conflict={conflict}",
            "Write", events.Length, match evt with Log.WriteConflict _ -> true | _ -> false)
        return result }

    let writeEvents (log: ILogger) retryPolicy (conn: EventStoreClient) (streamName: string) (version: int64) (events: EventData[]) ct
        : Task<EsSyncResult> =
        let call = writeEventsLogged conn streamName version events
        Log.withLoggedRetries retryPolicy "writeAttempt" call log ct

module private Read =
    open FSharp.Control
    let resolvedEventBytes (x: ResolvedEvent) = let Log.BlobLen bytes, Log.BlobLen metaBytes = x.Event.Data, x.Event.Metadata in bytes + metaBytes
    let resolvedEventsBytes events = events |> Array.sumBy resolvedEventBytes
    let private logBatchRead direction streamName t events batchSize version (log: ILogger) =
        let bytes, count = resolvedEventsBytes events, events.Length
        let reqMetric: Log.Measurement = { stream = streamName; interval = t; bytes = bytes; count = count}
        let batches = match batchSize with Some batchSize -> (events.Length - 1) / batchSize + 1 | None -> -1
        let action = if direction = Direction.Forward then "LoadF" else "LoadB"
        let log = if (not << log.IsEnabled) Events.LogEventLevel.Debug then log else log |> Log.propResolvedEvents "Json" events
        let evt = Log.Metric.Batch (direction, batches, reqMetric)
        (log |> Log.prop "bytes" bytes |> Log.event evt).Information(
            "Esdb{action:l} stream={stream} count={count}/{batches} version={version}",
            action, streamName, count, batches, version)
    let private loadBackwardsUntilOrigin (log: ILogger) (conn: EventStoreClient) batchSize streamName (tryDecode, isOrigin) ct
        : Task<int64 * struct (ResolvedEvent * 'event voption)[]> = task {
        let res = conn.ReadStreamAsync(Direction.Backwards, streamName, StreamPosition.End, int64 batchSize, resolveLinkTos = false, cancellationToken = ct)
        match! res.ReadState with
        | ReadState.StreamNotFound -> return (-1L, [||])
        | _ ->

        let! events =
            res
            |> TaskSeq.map (fun x -> struct (x, tryDecode x))
            |> TaskSeq.takeWhileInclusive (function
                | x, ValueSome e when isOrigin e ->
                    log.Information("EsdbStop stream={stream} at={eventNumber}", streamName, let en = x.Event.EventNumber in en.ToInt64())
                    false
                | _ -> true)
            |> TaskSeq.toArrayAsync
        let v = match Seq.tryHead events with Some (r, _) -> let en = r.Event.EventNumber in en.ToInt64() | None -> -1
        Array.Reverse events
        return v, events }

    let loadBackwards (log: ILogger) (conn: EventStoreClient) batchSize streamName (tryDecode, isOrigin) ct
        : Task<int64 * struct (ResolvedEvent * 'event voption)[]> = task {
        let! t, (version, events) = loadBackwardsUntilOrigin log conn batchSize streamName (tryDecode, isOrigin) |> Stopwatch.time ct
        let log = log |> Log.prop "batchSize" batchSize |> Log.prop "stream" streamName
        log |> logBatchRead Direction.Backward streamName t (Array.map ValueTuple.fst events) (Some batchSize) version
        return version, events }

    let private loadForward (conn: EventStoreClient) streamName startPosition ct: Task<int64 * ResolvedEvent[]> = task {
        let res = conn.ReadStreamAsync(Direction.Forwards, streamName, startPosition, Int64.MaxValue, resolveLinkTos = false, cancellationToken = ct)
        match! res.ReadState with
        | ReadState.StreamNotFound -> return (-1L, [||])
        | _ ->

        let! events = TaskSeq.toArrayAsync res
        let v = match Seq.tryLast events with Some r -> let en = r.Event.EventNumber in en.ToInt64() | None -> startPosition.ToInt64() - 1L
        return v, events }

    let loadForwards log conn streamName startPosition ct: Task<int64 * ResolvedEvent[]> = task {
        let direction = Direction.Forward
        let! t, (version, events) = loadForward conn streamName startPosition |> Stopwatch.time ct
        let log = log |> Log.prop "startPos" startPosition |> Log.prop "direction" direction |> Log.prop "stream" streamName
        log |> logBatchRead direction streamName t events None version
        return version, events }

module ClientCodec =

    let timelineEvent (x: EventRecord): FsCodec.ITimelineEvent<EventBody> =
        // TOCONSIDER wire e.Metadata["$correlationId"] and ["$causationId"] into correlationId and causationId
        // https://developers.eventstore.com/server/v21.10/streams.html#reserved-names
        let n, eu, ts = x.EventNumber, x.EventId, DateTimeOffset x.Created
        let et, data, meta = x.EventType, x.Data, x.Metadata
        let size = et.Length + data.Length + meta.Length
        FsCodec.Core.TimelineEvent.Create(n.ToInt64(), et, data, meta, eu.ToGuid(), correlationId = null, causationId = null, timestamp = ts, size = size)
    let isSystemEvent (x: EventRecord) = x.EventStreamId.StartsWith '$'
    let isJson (x: EventRecord) = x.ContentType = "application/json"

    let eventData (x: FsCodec.IEventData<EventBody>) =
        // TOCONSIDER wire x.CorrelationId, x.CausationId into x.Meta.["$correlationId"] and .["$causationId"]
        // https://developers.eventstore.com/server/v21.10/streams.html#reserved-names
        EventData(Uuid.FromGuid x.EventId, x.EventType, contentType = "application/json", data = x.Data, metadata = x.Meta)

type Position = { streamVersion: int64; compactionEventNumber: int64 option; batchCapacityLimit: int option }
type Token = { pos: Position }

module Token =
    let private create compactionEventNumber batchCapacityLimit streamVersion: StreamToken =
        {   value = box { pos = { streamVersion = streamVersion; compactionEventNumber = compactionEventNumber; batchCapacityLimit = batchCapacityLimit } }
            // In this impl, the StreamVersion matches the EventStore StreamVersion in being -1-based
            // Version however is the representation that needs to align with ISyncContext.Version
            version = streamVersion + 1L
            // TOCONSIDER Could implement accumulating the size as it's loaded (but should stop counting when it hits the origin event)
            streamBytes = -1 }

    /// No batching / compaction; we only need to retain the StreamVersion
    let ofNonCompacting streamVersion: StreamToken =
        create None None streamVersion

    // headroom before compaction is necessary given the stated knowledge of the last (if known) `compactionEventNumberOption`
    let private batchCapacityLimit compactedEventNumberOption eventsPending (batchSize: int) (streamVersion: int64): int =
        match compactedEventNumberOption with
        | Some (compactionEventNumber: int64) -> (batchSize - eventsPending) - int (streamVersion - compactionEventNumber + 1L) |> max 0
        | None -> (batchSize - eventsPending) - (int streamVersion + 1) - 1 |> max 0

    let (*private*) ofCompactionEventNumber compactedEventNumberOption eventsPending batchSize streamVersion: StreamToken =
        let batchCapacityLimit = batchCapacityLimit compactedEventNumberOption eventsPending batchSize streamVersion
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
        let e = compactionEvent.Event.EventNumber in ofCompactionEventNumber (Some (e.ToInt64())) 0 batchSize streamVersion

    /// Use an event we are about to write to the stream to infer headroom
    let ofPreviousStreamVersionAndCompactionEventDataIndex (Unpack token) compactionEventDataIndex eventsLength batchSize streamVersion': StreamToken =
        ofCompactionEventNumber (Some (token.streamVersion + 1L + int64 compactionEventDataIndex)) eventsLength batchSize streamVersion'

    let isStale current candidate = current.version > candidate.version

type EventStoreConnection(readConnection, [<O; D(null)>] ?writeConnection, [<O; D(null)>] ?readRetryPolicy, [<O; D(null)>] ?writeRetryPolicy) =
    member _.ReadConnection = readConnection
    member _.ReadRetryPolicy = readRetryPolicy
    member _.WriteConnection = defaultArg writeConnection readConnection
    member _.WriteRetryPolicy = writeRetryPolicy

type BatchOptions(getBatchSize: Func<int>) =
    new(batchSize) = BatchOptions(fun () -> batchSize)
    member _.BatchSize = getBatchSize.Invoke()

[<RequireQualifiedAccess; NoComparison; NoEquality>]
type GatewaySyncResult = Written of StreamToken | ConflictUnknown of StreamToken

type EventStoreContext(connection: EventStoreConnection, batchOptions: BatchOptions) =

    let isResolvedEventEventType (tryDecode, predicate) (x: ResolvedEvent) = predicate (tryDecode x.Event.Data)
    let tryIsResolvedEventEventType predicateOption = predicateOption |> Option.map isResolvedEventEventType
    let conn requireLeader = if requireLeader then connection.WriteConnection else connection.ReadConnection
    new(connection: EventStoreConnection,
        // Max number of Events to retrieve in a single batch. Also affects frequency of RollingSnapshots. Default: 500.
        [<O; D null>] ?batchSize) =
        EventStoreContext(connection, BatchOptions(batchSize = defaultArg batchSize 500))
    member val BatchOptions = batchOptions

    member internal _.TokenEmpty = Token.ofUncompactedVersion batchOptions.BatchSize -1L
    member internal _.LoadBatched(log, streamName, requireLeader, tryDecode, isCompactionEventType, ct): Task<struct (StreamToken * 'event[])> = task {
        let! version, events = Read.loadForwards log (conn requireLeader) streamName StreamPosition.Start ct
        match tryIsResolvedEventEventType isCompactionEventType with
        | None -> return struct (Token.ofNonCompacting version, Array.chooseV tryDecode events)
        | Some isCompactionEvent ->
            match events |> Array.tryFindBack isCompactionEvent with
            | None -> return Token.ofUncompactedVersion batchOptions.BatchSize version, Array.chooseV tryDecode events
            | Some resolvedEvent -> return Token.ofCompactionResolvedEventAndVersion resolvedEvent batchOptions.BatchSize version, Array.chooseV tryDecode events }
    member internal _.LoadBackwardsStoppingAtCompactionEvent(log, streamName, requireLeader, limit, tryDecode, isOrigin, ct): Task<struct (StreamToken * 'event [])> = task {
        let! version, events = Read.loadBackwards log (conn requireLeader) (defaultArg limit Int32.MaxValue) streamName (tryDecode, isOrigin) ct
        match Array.tryHead events |> Option.filter (function _, ValueSome e -> isOrigin e | _ -> false) with
        | None -> return struct (Token.ofUncompactedVersion batchOptions.BatchSize version, Array.chooseV ValueTuple.snd events)
        | Some (resolvedEvent, _) -> return Token.ofCompactionResolvedEventAndVersion resolvedEvent batchOptions.BatchSize version, Array.chooseV ValueTuple.snd events }
    member internal _.Reload(log, streamName, requireLeader, (Token.Unpack token as streamToken), tryDecode, isCompactionEventType, ct)
        : Task<struct (StreamToken * 'event[])> = task {
        let streamPosition = StreamPosition.FromInt64(token.streamVersion + 1L)
        let! version, events = Read.loadForwards log (conn requireLeader) streamName streamPosition ct
        match isCompactionEventType with
        | None -> return struct (Token.ofNonCompacting version, Array.chooseV tryDecode events)
        | Some isCompactionEvent ->
            match events |> Array.tryFindBack (fun re -> match tryDecode re with ValueSome e -> isCompactionEvent e | _ -> false) with
            | None -> return Token.ofPreviousTokenAndEventsLength streamToken events.Length batchOptions.BatchSize version, Array.chooseV tryDecode events
            | Some resolvedEvent -> return Token.ofCompactionResolvedEventAndVersion resolvedEvent batchOptions.BatchSize version, Array.chooseV tryDecode events }

    member internal _.Sync(log, streamName, streamToken, events, encodedEvents: EventData[], isCompactionEventType, ct): Task<GatewaySyncResult> = task {
        let streamVersion = let (Token.Unpack token) = streamToken in token.streamVersion
        let! wr = Write.writeEvents log connection.WriteRetryPolicy connection.WriteConnection streamName streamVersion encodedEvents ct
        match wr with
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
    // Used by Propulsion.EventStoreDb.EventStoreSink
    member _.Sync(log, streamName, streamVersion, events: FsCodec.IEventData<EventBody>[], ct): Task<GatewaySyncResult> = task {
        let encodedEvents: EventData[] = events |> Array.map ClientCodec.eventData
        match! Write.writeEvents log connection.WriteRetryPolicy connection.WriteConnection streamName streamVersion encodedEvents ct with
        | EsSyncResult.Written wr ->
            let version' = wr.NextExpectedVersion
            let token = Token.ofNonCompacting version'
            return GatewaySyncResult.Written token
        | EsSyncResult.Conflict actualVersion ->
            return GatewaySyncResult.ConflictUnknown (Token.ofNonCompacting actualVersion) }

[<NoComparison; NoEquality; RequireQualifiedAccess>]
type AccessStrategy<'event, 'state> =
    /// Read events forward, in batches.
    | Unoptimized
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

type private StoreCategory<'event, 'state, 'req>(context: EventStoreContext, codec: FsCodec.IEventCodec<_, _, 'req>, fold, initial, access) =
    let fold s xs = (fold : System.Func<'state, 'event[], 'state>).Invoke(s, xs)
    let tryDecode (e: ResolvedEvent) = e.Event |> ClientCodec.timelineEvent |> codec.TryDecode
    let isOrigin =
        match access with
        | AccessStrategy.Unoptimized | AccessStrategy.LatestKnownEvent -> fun _ -> true
        | AccessStrategy.RollingSnapshots (isValid, _) -> isValid
    let loadAlgorithm log streamName requireLeader ct =
        let compacted limit = context.LoadBackwardsStoppingAtCompactionEvent(log, streamName, requireLeader, limit, tryDecode, isOrigin, ct)
        match access with
        | AccessStrategy.Unoptimized -> context.LoadBatched(log, streamName, requireLeader, tryDecode, None, ct)
        | AccessStrategy.LatestKnownEvent -> compacted (Some 1)
        | AccessStrategy.RollingSnapshots _ -> compacted None
    let compactionPredicate =
        match access with
        | AccessStrategy.Unoptimized -> None
        | AccessStrategy.LatestKnownEvent -> Some (fun _ -> true)
        | AccessStrategy.RollingSnapshots (isValid, _) -> Some isValid
    let fetch state f = task { let! struct (token', events) = f in return struct (token', fold state events) }
    let reload (log, sn, leader, token, state) ct = fetch state (context.Reload(log, sn, leader, token, tryDecode, compactionPredicate, ct))
    interface Caching.IReloadable<'state> with member _.Reload(log, sn, leader, token, state, ct) = reload (log, sn, leader, token, state) ct
    interface ICategory<'event, 'state, 'req> with
        member _.Empty = context.TokenEmpty, initial
        member _.Load(log, _categoryName, _streamId, streamName, _maxAge, requireLeader, ct) =
            fetch initial (loadAlgorithm log streamName requireLeader ct)
        member _.Sync(log, _categoryName, _streamId, streamName, req, (Token.Unpack token as streamToken), state, events, ct) = task {
            let events =
                match access with
                | AccessStrategy.Unoptimized | AccessStrategy.LatestKnownEvent -> events
                | AccessStrategy.RollingSnapshots (_, compact) ->
                    let cc = CompactionContext(Array.length events, token.batchCapacityLimit.Value)
                    if cc.IsCompactionDue then Array.append events (fold state events |> compact |> Array.singleton) else events
            let encode e = codec.Encode(req, e)
            let encodedEvents: EventData[] = events |> Array.map (encode >> ClientCodec.eventData)
            match! context.Sync(log, streamName, streamToken, events, encodedEvents, compactionPredicate, ct) with
            | GatewaySyncResult.Written token' ->    return SyncResult.Written  (token', fold state events)
            | GatewaySyncResult.ConflictUnknown _ -> return SyncResult.Conflict (reload (log, streamName, (*requireLeader*)true, streamToken, state)) }

type EventStoreCategory<'event, 'state, 'req> =
    inherit Equinox.Category<'event, 'state, 'req>
    new(context: EventStoreContext, name, codec: FsCodec.IEventCodec<_, _, 'req>, fold, initial, access,
        // Caching can be overkill for EventStore esp considering the degree to which its intrinsic caching is a first class feature
        // e.g., a key benefit is that reads of streams more than a few pages long get completed in constant time after the initial load
        caching) =
        match access, caching with
        | AccessStrategy.LatestKnownEvent, Equinox.CachingStrategy.NoCaching -> ()
        | AccessStrategy.LatestKnownEvent, _ ->
            invalidOp "Equinox.EventStoreDb does not support mixing AccessStrategy.LatestKnownEvent with Caching at present."
        | _ -> ()
        { inherit Equinox.Category<'event, 'state, 'req>(name,
            StoreCategory<'event, 'state, 'req>(context, codec, fold, initial, access)
            |> Caching.apply Token.isStale caching); __ = () }; val private __: unit // __ can be removed after Rider 2023.2

[<RequireQualifiedAccess; NoComparison>]
type Discovery =
    // Allow Uri-based connection definition (esdb://, etc)
    | ConnectionString of string

// see https://github.com/EventStore/EventStore/issues/1652
[<RequireQualifiedAccess; NoComparison>]
type ConnectionStrategy =
    /// Pair of master and slave connections, writes direct, often can't read writes, resync without backoff (kind to master, writes+resyncs optimal)
    | ClusterTwinPreferSlaveReads
    /// Single connection, with resync backoffs appropriate to the NodePreference
    | ClusterSingle of NodePreference

type EventStoreConnector
    (   reqTimeout: TimeSpan,
        [<O; D null>] ?readRetryPolicy, [<O; D null>] ?writeRetryPolicy, [<O; D null>] ?tags,
        [<O; D null>] ?customize: Action<EventStoreClientSettings>) =

    member _.Connect
        (   // Name should be sufficient to uniquely identify this connection within a single app instance's logs
            name, discovery: Discovery, ?clusterNodePreference): EventStoreClient =
        if name = null then nullArg "name"
        let name = String.concat ";" <| seq {
            name
            string clusterNodePreference
            match tags with None -> () | Some tags -> for key, value in tags do sprintf "%s=%s" key value }
        let sanitizedName = name.Replace('\'','_').Replace(':','_') // ES internally uses `:` and `'` as separators in log messages and ... people regex logs
        let settings = discovery |> function Discovery.ConnectionString s -> EventStoreClientSettings.Create s
        settings.ConnectionName <- sanitizedName
        match clusterNodePreference with None -> () | Some np -> settings.ConnectivitySettings.NodePreference <- np
        match customize with None -> () | Some f -> f.Invoke settings
        settings.DefaultDeadline <- reqTimeout
        // TODO implement reqRetries
        new EventStoreClient(settings)

    /// Yields a Connection (which may internally be twin connections) configured per the specified strategy
    member x.Establish
        (   // Name should be sufficient to uniquely identify this (aggregate) connection within a single app instance's logs
            name,
            discovery: Discovery, strategy: ConnectionStrategy): EventStoreConnection =
        match strategy with
        | ConnectionStrategy.ClusterSingle nodePreference ->
            let client = x.Connect(name, discovery, nodePreference)
            EventStoreConnection(client, ?readRetryPolicy = readRetryPolicy, ?writeRetryPolicy = writeRetryPolicy)
        | ConnectionStrategy.ClusterTwinPreferSlaveReads ->
            let leader = x.Connect(name + "-TwinW", discovery, NodePreference.Leader)
            let follower = x.Connect(name + "-TwinR", discovery, NodePreference.Follower)
            EventStoreConnection(readConnection = follower, writeConnection = leader, ?readRetryPolicy = readRetryPolicy, ?writeRetryPolicy = writeRetryPolicy)
