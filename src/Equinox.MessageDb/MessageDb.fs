﻿namespace Equinox.MessageDb

open Equinox.Core
open Equinox.Core.Tracing
open Equinox.MessageDb.Core
open FsCodec
open Serilog
open System
open System.Diagnostics
open System.Text.Json
open System.Threading
open System.Threading.Tasks

type EventBody = ReadOnlyMemory<byte>

module Log =

    /// <summary>Name of Property used for <c>Metric</c> in <c>LogEvent</c>s.</summary>
    let [<Literal>] PropertyTag = "mdbEvt"

    [<NoEquality; NoComparison>]
    type Measurement = { stream: string; interval: StopwatchInterval; bytes: int; count: int }
    [<NoEquality; NoComparison>]
    type Metric =
        | Slice of Measurement
        | Batch of slices: int * Measurement
        | ReadLast of Measurement
        | WriteSuccess of Measurement
        | WriteConflict of Measurement
    let [<return: Struct>] (|MetricEvent|_|) (logEvent: Serilog.Events.LogEvent): Metric voption =
        let mutable p = Unchecked.defaultof<_>
        logEvent.Properties.TryGetValue(PropertyTag, &p) |> ignore
        match p with Log.ScalarValue (:? Metric as e) -> ValueSome e | _ -> ValueNone

    /// Attach a property to the captured event record to hold the metric information
    let internal event (value: Metric) = Internal.Log.withScalarProperty PropertyTag value
    let prop name value (log: ILogger) = log.ForContext(name, value)
    let bytesToString (bytes: EventBody) = System.Text.Encoding.UTF8.GetString bytes.Span
    let propEvents name (kvps: System.Collections.Generic.KeyValuePair<string, string> seq) (log: ILogger) =
        let items = seq { for kv in kvps do yield sprintf "{\"%s\": %s}" kv.Key kv.Value }
        log.ForContext(name, sprintf "[%s]" (String.concat ",\n\r" items))
    let propEventData name (events: IEventData<EventBody>[]) (log: ILogger) =
        log |> propEvents name (seq {
            for x in events do
                yield System.Collections.Generic.KeyValuePair<_, _>(x.EventType, bytesToString x.Data) })
    let propResolvedEvents name (events: ITimelineEvent<EventBody>[]) (log: ILogger) =
        log |> propEvents name (seq {
            for x in events do
                yield System.Collections.Generic.KeyValuePair<_, _>(x.EventType, bytesToString x.Data) })
    let withLoggedRetries<'t> retryPolicy (contextLabel: string) (f: ILogger -> CancellationToken -> Task<'t>) log ct: Task<'t> =
        match retryPolicy with
        | None -> f log ct
        | Some retryPolicy ->
            let withLoggingContextWrapping count =
                let log = if count = 1 then log else log |> prop contextLabel count
                let act = Activity.Current in if act <> null then act.AddRetryAttempt(count) |> ignore
                f log
            retryPolicy withLoggingContextWrapping

    /// NB Caveat emptor; this is subject to unlimited change without the major version changing - while the `dotnet-templates` repo will be kept in step, and
    /// the ChangeLog will mention changes, it's critical to not assume that the presence or nature of these helpers be considered stable
    module InternalMetrics =

        module Stats =
            let inline (|Stats|) ({ interval = i }: Measurement) = int64 i.ElapsedMilliseconds

            let (|Read|ReadL|Write|Resync|Rollup|) = function
                | Slice (Stats s) -> Read s
                // slices are rolled up into batches so be sure not to double-count
                | Batch (_, Stats s) -> Rollup s
                | ReadLast (Stats s) -> ReadL s
                | WriteSuccess (Stats s) -> Write s
                | WriteConflict (Stats s) -> Resync s
            type Counter =
                { mutable count: int64; mutable ms: int64 }
                static member Create() = { count = 0L; ms = 0L }
                member x.Ingest(ms) =
                    Interlocked.Increment(&x.count) |> ignore
                    Interlocked.Add(&x.ms, ms) |> ignore
            type LogSink() =
                static let epoch = Stopwatch.StartNew()
                static member val Read = Counter.Create() with get, set
                static member val ReadL = Counter.Create() with get, set
                static member val Write = Counter.Create() with get, set
                static member val Resync = Counter.Create() with get, set
                static member Restart() =
                    LogSink.Read <- Counter.Create()
                    LogSink.ReadL <- Counter.Create()
                    LogSink.Write <- Counter.Create()
                    LogSink.Resync <- Counter.Create()
                    let span = epoch.Elapsed
                    epoch.Restart()
                    span
                interface Serilog.Core.ILogEventSink with
                    member _.Emit logEvent = logEvent |> function
                        | MetricEvent (Read stats) -> LogSink.Read.Ingest stats
                        | MetricEvent (ReadL stats) -> LogSink.ReadL.Ingest stats
                        | MetricEvent (Write stats) -> LogSink.Write.Ingest stats
                        | MetricEvent (Resync stats) -> LogSink.Resync.Ingest stats
                        | MetricEvent (Rollup _) -> ()
                        | _ -> ()

        /// Relies on feeding of metrics from Log through to Stats.LogSink
        /// Use Stats.LogSink.Restart() to reset the start point (and stats) where relevant
        let dump (log: ILogger) =
            let stats =
              [ "Read", Stats.LogSink.Read
                "ReadL", Stats.LogSink.ReadL
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

module private Write =

    let private writeEventsAsync (writer: MessageDbWriter) streamName version events ct: Task<MdbSyncResult> =
        writer.WriteMessages(streamName, events, version, ct)
    let inline len (bytes: EventBody) = bytes.Length
    let private eventDataLen (x: IEventData<EventBody>) = len x.Data + len x.Meta
    let private eventDataBytes events = events |> Array.sumBy eventDataLen
    let private writeEventsLogged writer streamName version events (act: Activity) (log: ILogger) ct: Task<MdbSyncResult> = task {
        let log = if not (log.IsEnabled Events.LogEventLevel.Debug) then log else log |> Log.propEventData "Json" events
        let bytes, count = eventDataBytes events, events.Length
        let log = log |> Log.prop "bytes" bytes
        if act <> null then act.AddExpectedVersion(version).IncMetric(count, bytes) |> ignore
        let! t, result = writeEventsAsync writer streamName version events |> Stopwatch.time ct
        let reqMetric: Log.Measurement = { stream = streamName; interval = t; bytes = bytes; count = count}
        let resultLog, evt =
            match result with
            | MdbSyncResult.Written x ->
                if act <> null then
                    act.SetStatus(ActivityStatusCode.Ok).AddTag("eqx.new_version", x) |> ignore
                log |> Log.prop "currentPosition" x, Log.WriteSuccess reqMetric
            | MdbSyncResult.ConflictUnknown ->
                let eventTypes = [| for x in events -> x.EventType |]
                if act <> null then act.RecordConflict().AddTag("eqx.event_types", eventTypes) |> ignore
                let writeLog = log |> Log.prop "stream" streamName |> Log.prop "count" count
                writeLog.Information("MdbTrySync WrongExpectedVersionException writing {eventTypes}, expected {expectedVersion}", eventTypes, version)
                log, Log.WriteConflict reqMetric
        (resultLog |> Log.event evt).Information("Mdb{action:l} count={count} conflict={conflict}",
                                                 "Write", count, match evt with Log.WriteConflict _ -> true | _ -> false)
        return result }
    let writeEvents log retryPolicy writer (category, streamId, streamName) version events ct: Task<MdbSyncResult> = task {
        use act = source.StartActivity("WriteEvents", ActivityKind.Client)
        if act <> null then act.AddStream(category, streamId, streamName) |> ignore
        let call = writeEventsLogged writer streamName version events act
        return! Log.withLoggedRetries retryPolicy "writeAttempt" call log ct }

module Read =

    [<NoComparison;NoEquality>]
    type StreamEventsSlice = { Messages: ITimelineEvent<EventBody>[]; IsEnd: bool; LastVersion: int64 }

    let private toSlice (events: ITimelineEvent<EventBody>[]) isLast: StreamEventsSlice=
        let lastVersion = match Array.tryLast events with Some ev -> ev.Index | None -> -1L
        { Messages = events; IsEnd = isLast; LastVersion = lastVersion }

    let private readSliceAsync (reader: MessageDbReader) (streamName: string) (batchSize: int64) (startPos: int64) (requiresLeader: bool) ct = task {
        let! page = reader.ReadStream(streamName, startPos, batchSize, requiresLeader, ct)
        let isLast = int64 page.Length < batchSize
        return toSlice page isLast }

    let private readLastEventAsync (reader: MessageDbReader) (streamName: string) (requiresLeader: bool) (eventType: string option) ct = task {
        let! events = reader.ReadLastEvent(streamName, requiresLeader, ct, ?eventType = eventType)
        return toSlice events false }

    let inline len (bytes: EventBody) = bytes.Length
    let private resolvedEventLen (x: ITimelineEvent<EventBody>) = len x.Data + len x.Meta
    let private resolvedEventBytes events = events |> Array.sumBy resolvedEventLen
    let private loggedReadSlice reader streamName batchSize requiresLeader startPos batchIndex (log: ILogger) ct: Task<_> = task {
        let parentAct = Activity.Current
        use act = source.StartActivity("ReadSlice", ActivityKind.Client)
        if act <> null then act.AddStreamFromParent(parentAct).AddBatch(batchSize, batchIndex).AddStartPosition(startPos).AddLeader(requiresLeader) |> ignore
        let! t, slice = readSliceAsync reader streamName batchSize startPos requiresLeader |> Stopwatch.time ct
        let bytes, count = slice.Messages |> resolvedEventBytes, slice.Messages.Length
        let reqMetric: Log.Measurement = { stream = streamName; interval = t; bytes = bytes; count = count}
        let evt = Log.Slice reqMetric
        if act <> null then act.IncMetric(count, bytes).AddLastVersion(slice.LastVersion) |> ignore
        if parentAct <> null then parentAct.IncMetric(count, bytes) |> ignore
        let log = if not (log.IsEnabled Events.LogEventLevel.Debug) then log else log |> Log.propResolvedEvents "Json" slice.Messages
        (log |> Log.prop "startPos" startPos |> Log.prop "bytes" bytes |> Log.event evt).Information("Mdb{action:l} count={count} version={version}",
            "Read", count, slice.LastVersion)
        return slice }

    let private readBatches (log: ILogger) batchSize (readSlice: int64 -> int -> ILogger -> CancellationToken -> Task<StreamEventsSlice>)
            (maxPermittedBatchReads: int option) (startPosition: int64) ct
        : Task<int64 * ITimelineEvent<EventBody>[]> = task {
        let mutable batchCount, pos = 0, startPosition
        let mutable version = -1L
        let result = ResizeArray(int batchSize) // pre allocate batchSize as the vast majority of reads will only have a single batch
        let rec loop () : Task<unit> = task {
            match maxPermittedBatchReads with
            | Some mpbr when batchCount >= mpbr -> log.Information "batch Limit exceeded"; invalidOp "batch Limit exceeded"
            | _ -> ()

            let batchLog = log |> Log.prop "batchIndex" batchCount
            let! slice = readSlice pos batchCount batchLog ct |> Async.AwaitTaskCorrect
            version <- max version slice.LastVersion
            result.AddRange(slice.Messages)
            if not slice.IsEnd then
                batchCount <- batchCount + 1
                pos <- slice.LastVersion  + 1L
                do! loop () }

        do! loop ()
        return version, Array.ofSeq result }

    let private logBatchRead (act: Activity) streamName t events (batchSize: int64) version (log: ILogger) =
        let bytes, count = resolvedEventBytes events, events.Length
        let reqMetric: Log.Measurement = { stream = streamName; interval = t; bytes = bytes; count = count}
        let batches = (events.Length - 1)/ (int batchSize) + 1
        let action = "Load"
        let evt = Log.Metric.Batch (batches, reqMetric)
        if act <> null then act.AddBatches(batches).AddLastVersion(version) |> ignore
        (log |> Log.prop "bytes" bytes |> Log.event evt).Information(
            "Mdb{action:l} stream={stream} count={count}/{batches} version={version}",
            action, streamName, count, batches, version)

    let private logLastEventRead (parent: Activity) (act: Activity) streamName t events (version: int64) (log: ILogger) =
        let bytes = resolvedEventBytes events
        let count = events.Length
        let reqMetric: Log.Measurement = { stream = streamName; interval = t; bytes = bytes; count = count}
        let evt = Log.Metric.ReadLast reqMetric
        if act <> null then act.IncMetric(count, bytes).AddLastVersion(version) |> ignore
        if parent <> null then parent.IncMetric(count, bytes) |> ignore
        (log |> Log.prop "bytes" bytes |> Log.event evt).Information(
            "Mdb{action:l} stream={stream} count={count} version={version}",
            "ReadL", streamName, count, version)

    let internal loadLastEvent (log: ILogger) retryPolicy (reader: MessageDbReader) requiresLeader streamName eventType ct
        : Task<int64 * ITimelineEvent<EventBody>[]> = task {
        let parentAct = Activity.Current
        if parentAct <> null then parentAct.AddLoadMethod("Last") |> ignore
        use act = source.StartActivity("ReadLast", ActivityKind.Client)
        if act <> null then act.AddStreamFromParent(parentAct).AddLeader(requiresLeader) |> ignore
        let read _ = readLastEventAsync reader streamName requiresLeader eventType

        let! t, page = Log.withLoggedRetries retryPolicy "readAttempt" read log |> Stopwatch.time ct

        log |> logLastEventRead parentAct act streamName t page.Messages page.LastVersion
        return page.LastVersion, page.Messages }

    let internal loadForwardsFrom (log: ILogger) retryPolicy reader batchSize maxPermittedBatchReads streamName startPosition requiresLeader ct
        : Task<int64 * ITimelineEvent<EventBody>[]> = task {
        let act = Activity.Current
        if act <> null then act.AddBatchSize(batchSize).AddStartPosition(startPosition).AddLoadMethod("BatchForward") |> ignore
        let call = loggedReadSlice reader streamName batchSize requiresLeader
        let retryingLoggingReadSlice pos batchIndex = Log.withLoggedRetries retryPolicy "readAttempt" (call pos batchIndex)
        let log = log |> Log.prop "batchSize" batchSize |> Log.prop "stream" streamName
        let! t, (version, events) = readBatches log batchSize retryingLoggingReadSlice maxPermittedBatchReads startPosition |> Stopwatch.time ct
        log |> logBatchRead act streamName t events batchSize version
        return version, events }

module private Token =

    // NOTE MessageDB's streamVersion is -1 based, similar to ESDB and SSS
    let create streamVersion: StreamToken =
        { value = box streamVersion
          // The `Version` exposed on the `ISyncContext` is 0-based
          version = streamVersion + 1L
          streamBytes = -1 }
    let inline streamVersion (token: StreamToken) = unbox<int64> token.value
    let shouldSnapshot batchSize prev next =
        let previousVersion = prev.version
        let nextVersion = next.version
        let estimatedSnapshotPos = previousVersion - (previousVersion % batchSize)
        nextVersion - estimatedSnapshotPos >= batchSize

    let supersedes struct (current, x) =
        x.version > current.version

module private Snapshot =

    let inline snapshotCategory original = original + ":snapshot"
    let inline streamName category (streamId: string) = Equinox.Core.StreamName.render (snapshotCategory category) streamId
    type Meta = {| streamVersion: int64 |} // STJ doesn't want to serialize it unless its anonymous
    let private streamVersion (evt: ITimelineEvent<EventBody>) =
        let meta = evt.Meta // avoid defensive copy
        JsonSerializer.Deserialize<Meta>(meta.Span).streamVersion

    let meta token = {| streamVersion = Token.streamVersion token |} |> JsonSerializer.SerializeToUtf8Bytes |> ReadOnlyMemory
    let decode tryDecode (events: ITimelineEvent<EventBody>[]) =
        match events |> Array.tryFirstV |> ValueOption.bind tryDecode with
        | ValueSome decoded -> ValueSome struct(events[0] |> streamVersion |> Token.create, decoded)
        | ValueNone -> ValueNone

type MessageDbClient internal (reader, writer, ?readRetryPolicy, ?writeRetryPolicy) =
    member val internal Reader = reader
    member val ReadRetryPolicy = readRetryPolicy
    member val internal Writer = writer
    member val WriteRetryPolicy = writeRetryPolicy
    new(connectionString: string,
        // Can be used to divert reads to a replica
        // Conflicts detected on write trigger a resync, reading via the `connectionString` to maximize the freshness of the data for the retry
        [<O; D(null)>]?readConnectionString: string,
        [<O; D(null)>]?readRetryPolicy, [<O; D(null)>]?writeRetryPolicy) =

        let readConnectionString = defaultArg readConnectionString connectionString
        let reader = MessageDbReader(readConnectionString, connectionString)
        let writer = MessageDbWriter(connectionString)
        MessageDbClient(reader, writer, ?readRetryPolicy = readRetryPolicy, ?writeRetryPolicy = writeRetryPolicy)

type BatchOptions(getBatchSize: Func<int>, [<O; D(null)>]?batchCountLimit) =
    new (batchSize) = BatchOptions(fun () -> batchSize)
    member _.BatchSize = getBatchSize.Invoke()
    member val MaxBatches = batchCountLimit

[<RequireQualifiedAccess; NoComparison; NoEquality>]
type internal GatewaySyncResult = Written of StreamToken | ConflictUnknown

type MessageDbContext(client: MessageDbClient, batchOptions: BatchOptions) =
    new(client: MessageDbClient,
        // Max number of Events to retrieve in a single batch. Also affects frequency of Snapshots. Default: 500.
        [<O; D null>] ?batchSize) =
        MessageDbContext(client, BatchOptions(batchSize = defaultArg batchSize 500))
    member val BatchOptions = batchOptions

    member _.TokenEmpty = Token.create -1L
    member _.LoadBatched(log, streamName, requireLeader, tryDecode, ct): Task<StreamToken * 'event[]> = task {
        let! version, events = Read.loadForwardsFrom log client.ReadRetryPolicy client.Reader batchOptions.BatchSize batchOptions.MaxBatches streamName 0L requireLeader ct
        return Token.create version, Array.chooseV tryDecode events }
    member _.LoadLast(log, streamName, requireLeader, tryDecode, ct): Task<StreamToken * 'event[]> = task {
        let! version, events = Read.loadLastEvent log client.ReadRetryPolicy client.Reader requireLeader streamName None ct
        return Token.create version, Array.chooseV tryDecode events }
    member _.LoadSnapshot(log, category, streamId, requireLeader, tryDecode, eventType, ct) = task {
        let snapshotStream = Snapshot.streamName category streamId
        let! _, events = Read.loadLastEvent log client.ReadRetryPolicy client.Reader requireLeader snapshotStream (Some eventType) ct
        return Snapshot.decode tryDecode events }

    member _.Reload(log, streamName, requireLeader, token, tryDecode, ct): Task<StreamToken * 'event[]> = task {
        let streamVersion = Token.streamVersion token
        let startPos = streamVersion + 1L // Reading a stream uses {inclusive} positions, but the streamVersion is `-1`-based
        let! version, events = Read.loadForwardsFrom log client.ReadRetryPolicy client.Reader batchOptions.BatchSize batchOptions.MaxBatches streamName startPos requireLeader ct
        return Token.create (max streamVersion version), Array.chooseV tryDecode events }

    member internal _.TrySync(log, category, streamId, streamName, token, encodedEvents: IEventData<EventBody>[], ct): Task<GatewaySyncResult> = task {
        let streamVersion = Token.streamVersion token
        match! Write.writeEvents log client.WriteRetryPolicy client.Writer (category, streamId, streamName) (StreamVersion streamVersion) encodedEvents ct with
        | MdbSyncResult.Written version' ->
            let token = Token.create version'
            return GatewaySyncResult.Written token
        | MdbSyncResult.ConflictUnknown ->
            return GatewaySyncResult.ConflictUnknown }

    member _.StoreSnapshot(log, category, streamId, event, ct) = task {
        let snapshotStream = Snapshot.streamName category streamId
        let category = Snapshot.snapshotCategory category
        do! Write.writeEvents log None client.Writer (category, streamId, snapshotStream) Any [| event |] ct :> Task }

[<NoComparison; NoEquality; RequireQualifiedAccess>]
type AccessStrategy<'event, 'state> =
    /// Load only the single most recent event defined in in a stream and trust that it'll be decoded and
    /// doing a <c>fold</c> from any such event will yield a correct and complete state
    /// In other words, the <c>fold</c> function should not need to consider either the preceding <c>'state</c> or <c>'event</c>s.
    | LatestKnownEvent
    /// <summary>
    /// Generates and stores a snapshot event in an adjacent <c>{category}:snapshot-{stream_id}</c> stream
    /// The generation happens every <c>batchSize</c> events.
    /// This means the state of the stream can be reconstructed with exactly 2 round-trips to the database.
    /// The first round-trip fetches the most recent event of type <c>snapshotEventCaseName</c> from the snapshot stream.
    /// The second round-trip fetches <c>batchSize</c> events from the position of the snapshot
    /// The <c>toSnapshot</c> function is used to generate the event to store in the snapshot stream.
    /// It should return the event case whose name matches <c>snapshotEventCaseName</c>
    /// </summary>
    | AdjacentSnapshots of snapshotEventCaseName: string * toSnapshot: ('state -> 'event)

type private Category<'event, 'state, 'context>(context: MessageDbContext, codec: IEventCodec<_, _, 'context>, fold, initial, access) =
    let loadAlgorithm log category streamId streamName requireLeader ct =
        match access with
        | None -> context.LoadBatched(log, streamName, requireLeader, codec.TryDecode, ct)
        | Some AccessStrategy.LatestKnownEvent -> context.LoadLast(log, streamName, requireLeader, codec.TryDecode, ct)
        | Some (AccessStrategy.AdjacentSnapshots (snapshotType, _)) -> task {
            match! context.LoadSnapshot(log, category, streamId, requireLeader, codec.TryDecode, snapshotType, ct) with
            | ValueSome (pos, snapshotEvent) ->
                let! token, rest = context.Reload(log, streamName, requireLeader, pos, codec.TryDecode, ct)
                return token, Array.insertAt 0 snapshotEvent rest
            | ValueNone -> return! context.LoadBatched(log, streamName, requireLeader, codec.TryDecode, ct) }
    let fetch state f = task { let! token', events = f in return struct (token', fold state (Seq.ofArray events)) }
    let reload (log, sn, leader, token, state) ct = fetch state (context.Reload(log, sn, leader, token, codec.TryDecode, ct))
    interface Caching.IReloadableCategory<'event, 'state, 'context> with
        member _.Load(log, categoryName, streamId, streamName, _allowStale, requireLeader, ct) =
            fetch initial (loadAlgorithm log categoryName streamId streamName requireLeader ct)
        member _.Reload(log, streamName, requireLeader, streamToken, state, ct) =
            reload (log, streamName, requireLeader, streamToken, state) ct
        member x.TrySync(log, categoryName, streamId, streamName, ctx, _maybeInit, token, state, events, ct) = task {
            let encode e = codec.Encode(ctx, e)
            let encodedEvents: IEventData<EventBody>[] = events |> Array.map encode
            match! context.TrySync(log, categoryName, streamId, streamName, token, encodedEvents, ct) with
            | GatewaySyncResult.Written token' ->
                let state' = fold state (Seq.ofArray events)
                match access with
                | None | Some AccessStrategy.LatestKnownEvent -> ()
                | Some (AccessStrategy.AdjacentSnapshots(_, toSnap)) ->
                    if Token.shouldSnapshot context.BatchOptions.BatchSize token token' then
                        do! x.StoreSnapshot(log, categoryName, streamId, ctx, token', toSnap state', ct)
                return SyncResult.Written (token', state')
            | GatewaySyncResult.ConflictUnknown ->
                return SyncResult.Conflict (reload (log, streamName, (*requireLeader*)true, token, state)) }

    member _.StoreSnapshot(log, category, streamId, ctx, token, snapshotEvent, ct) =
        let encodedWithMeta =
            let rawEvent = codec.Encode(ctx, snapshotEvent)
            FsCodec.Core.EventData.Create(rawEvent.EventType, rawEvent.Data, meta = Snapshot.meta token)
        context.StoreSnapshot(log, category, streamId, encodedWithMeta, ct)

type MessageDbCategory<'event, 'state, 'context> internal (resolveInner, empty) =
    inherit Equinox.Category<'event, 'state, 'context>(resolveInner, empty)
    new(context: MessageDbContext, codec: IEventCodec<_, _, 'context>, fold, initial,
        // For MessageDb, caching is less critical than it is for e.g. CosmosDB
        // As such, it can often be omitted, particularly if streams are short, or events are small and/or database latency aligns with request latency requirements
        [<O; D(null)>]?caching,
        [<O; D(null)>]?access) =
        let cat = Category<'event, 'state, 'context>(context, codec, fold, initial, access) |> Caching.apply Token.supersedes caching
        let resolveInner categoryName streamId = struct (cat, StreamName.render categoryName streamId, ValueNone)
        let empty = struct (context.TokenEmpty, initial)
        MessageDbCategory(resolveInner, empty)
