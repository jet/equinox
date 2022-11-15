namespace Equinox.MessageDb

open Equinox.Core
open Equinox.MessageDb.Core
open FsCodec
open Serilog
open System
open System.Diagnostics

type EventBody = ReadOnlyMemory<byte>

module Log =

    /// <summary>Name of Property used for <c>Metric</c> in <c>LogEvent</c>s.</summary>
    let [<Literal>] PropertyTag = "mdbEvt"

    [<NoEquality; NoComparison>]
    type Measurement = { stream : string; interval : StopwatchInterval; bytes : int; count : int }
    [<NoEquality; NoComparison>]
    type Metric =
        | Slice of Measurement
        | Batch of slices : int * Measurement
        | ReadLast of Measurement
        | WriteSuccess of Measurement
        | WriteConflict of Measurement
    let [<return: Struct>] (|MetricEvent|_|) (logEvent : Serilog.Events.LogEvent) : Metric voption =
        let mutable p = Unchecked.defaultof<_>
        logEvent.Properties.TryGetValue(PropertyTag, &p) |> ignore
        match p with Log.ScalarValue (:? Metric as e) -> ValueSome e | _ -> ValueNone

    /// Attach a property to the captured event record to hold the metric information
    let internal event (value : Metric) = Internal.Log.withScalarProperty PropertyTag value
    let prop name value (log : ILogger) = log.ForContext(name, value)
    let bytesToString (bytes: EventBody) = System.Text.Encoding.UTF8.GetString bytes.Span
    let propEvents name (kvps : System.Collections.Generic.KeyValuePair<string, string> seq) (log : ILogger) =
        let items = seq { for kv in kvps do yield sprintf "{\"%s\": %s}" kv.Key kv.Value }
        log.ForContext(name, sprintf "[%s]" (String.concat ",\n\r" items))
    let propEventData name (events : IEventData<EventBody> array) (log : ILogger) =
        log |> propEvents name (seq {
            for x in events do
                yield System.Collections.Generic.KeyValuePair<_, _>(x.EventType, bytesToString x.Data) })
    let propResolvedEvents name (events : ITimelineEvent<EventBody> array) (log : ILogger) =
        log |> propEvents name (seq {
            for x in events do
                yield System.Collections.Generic.KeyValuePair<_, _>(x.EventType, bytesToString x.Data) })
    let withLoggedRetries<'t> retryPolicy (contextLabel : string) (f : ILogger -> Async<'t>) log : Async<'t> =
        match retryPolicy with
        | None -> f log
        | Some retryPolicy ->
            let withLoggingContextWrapping count =
                let log = if count = 1 then log else log |> prop contextLabel count
                Tracing.addRetryAttempt count Activity.Current
                f log
            retryPolicy withLoggingContextWrapping

    /// NB Caveat emptor; this is subject to unlimited change without the major version changing - while the `dotnet-templates` repo will be kept in step, and
    /// the ChangeLog will mention changes, it's critical to not assume that the presence or nature of these helpers be considered stable
    module InternalMetrics =

        module Stats =
            let inline (|Stats|) ({ interval = i } : Measurement) = int64 i.ElapsedMilliseconds

            let (|Read|ReadL|Write|Resync|Rollup|) = function
                | Slice (Stats s) -> Read s
                // slices are rolled up into batches so be sure not to double-count
                | Batch (_, Stats s) -> Rollup s
                | ReadLast (Stats s) -> ReadL s
                | WriteSuccess (Stats s) -> Write s
                | WriteConflict (Stats s) -> Resync s
            type Counter =
                { mutable count : int64; mutable ms : int64 }
                static member Create() = { count = 0L; ms = 0L }
                member x.Ingest(ms) =
                    System.Threading.Interlocked.Increment(&x.count) |> ignore
                    System.Threading.Interlocked.Add(&x.ms, ms) |> ignore
            type LogSink() =
                static let epoch = System.Diagnostics.Stopwatch.StartNew()
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
        let dump (log : ILogger) =
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
            let measures : (string * (TimeSpan -> float)) list = [ "s", fun x -> x.TotalSeconds(*; "m", fun x -> x.TotalMinutes; "h", fun x -> x.TotalHours*) ]
            let logPeriodicRate name count = log.Information("rp{name} {count:n0}", name, count)
            for uom, f in measures do let d = f duration in if d <> 0. then logPeriodicRate uom (float totalCount/d |> int64)

module private Write =

    let private writeEventsAsync (writer: MessageDbWriter) (streamName : string) (version : int64) (events : IEventData<EventBody> array)
        : Async<MdbSyncResult> =
        writer.WriteMessages(streamName, events, version, Async.DefaultCancellationToken) |> Async.AwaitTaskCorrect

    let inline len (bytes: EventBody) = bytes.Length
    let private eventDataLen (x : IEventData<EventBody>) = len x.Data + len x.Meta
    let private eventDataBytes events = events |> Array.sumBy eventDataLen
    let private writeEventsLogged (writer: MessageDbWriter) (streamName : string) (version : int64) (events : IEventData<EventBody> array) (act: Activity) (log : ILogger)
        : Async<MdbSyncResult> = async {
        let log = if not (log.IsEnabled Events.LogEventLevel.Debug) then log else log |> Log.propEventData "Json" events
        let bytes, count = eventDataBytes events, events.Length
        let log = log |> Log.prop "bytes" bytes
        if act <> null then act.AddStreamName(streamName).AddExpectedVersion(version).AddMetric(count, bytes) |> ignore
        let! t, result = writeEventsAsync writer streamName version events |> Stopwatch.Time
        let reqMetric : Log.Measurement = { stream = streamName; interval = t; bytes = bytes; count = count}
        let resultLog, evt =
            match result with
            | MdbSyncResult.ConflictUnknown ->
                let eventTypes = [| for x in events -> x.EventType |]
                if act <> null then act.RecordConflict().AddTag("eqx.event_types", eventTypes) |> ignore
                let writeLog = log |> Log.prop "stream" streamName |> Log.prop "count" count
                writeLog.Information("MdbTrySync WrongExpectedVersionException writing {eventTypes}, expected {expectedVersion}", eventTypes, version)
                log, Log.WriteConflict reqMetric
            | MdbSyncResult.Written x ->
                if act <> null then
                    act.SetStatus(ActivityStatusCode.Ok).AddTag("eqx.new_version", x) |> ignore
                log |> Log.prop "currentPosition" x, Log.WriteSuccess reqMetric
        (resultLog |> Log.event evt).Information("Mdb{action:l} count={count} conflict={conflict}",
                                                 "Write", count, match evt with Log.WriteConflict _ -> true | _ -> false)
        return result }
    let writeEvents (log : ILogger) retryPolicy (writer : MessageDbWriter) (streamName : string) (version : int64) (events : IEventData<EventBody> array)
        : Async<MdbSyncResult> = async {
        use act = source.StartActivity("AppendEvents", ActivityKind.Client)
        let call = writeEventsLogged writer streamName version events act
        return! Log.withLoggedRetries retryPolicy "writeAttempt" call log }

module Read =

    [<NoComparison;NoEquality>]
    type StreamEventsSlice = { Messages : ITimelineEvent<EventBody> array; IsEnd : bool; LastVersion : int64 }

    open FSharp.Control

    let private toSlice (events : ITimelineEvent<EventBody> array) isLast : StreamEventsSlice=
        let lastVersion = match Array.tryLast events with Some ev -> ev.Index | None -> -1L
        { Messages = events; IsEnd = isLast; LastVersion = lastVersion }

    let private readSliceAsync (reader : MessageDbReader) (streamName : string) (batchSize : int64) (startPos : int64) (requiresLeader : bool) ct = task {
        let! page = reader.ReadStream(streamName, startPos, batchSize, requiresLeader, ct)
        let isLast = int64 page.Length < batchSize
        return toSlice page isLast }

    let private readLastEventAsync (reader : MessageDbReader) (streamName : string) (requiresLeader : bool) ct = task {
        let! events = reader.ReadLastEvent(streamName, requiresLeader, ct)
        return toSlice events false }

    let inline len (bytes : EventBody) = bytes.Length
    let private resolvedEventLen (x : ITimelineEvent<EventBody>) = len x.Data + len x.Meta
    let private resolvedEventBytes events = events |> Array.sumBy resolvedEventLen
    let private loggedReadSlice reader streamName batchSize batchIndex startPos requiresLeader (log : ILogger) : Async<_> = async {
        use act = source.StartActivity("ReadSlice", ActivityKind.Client)
        if act <> null then
            act.AddStreamName(streamName).AddBatch(batchSize, batchIndex)
                .AddStartPosition(startPos).AddLeader(requiresLeader) |> ignore
        let! ct = Async.CancellationToken
        let! t, slice = readSliceAsync reader streamName batchSize startPos requiresLeader ct |> Async.AwaitTaskCorrect |> Stopwatch.Time
        let bytes, count = slice.Messages |> resolvedEventBytes, slice.Messages.Length
        let reqMetric : Log.Measurement = { stream = streamName; interval = t; bytes = bytes; count = count}
        let evt = Log.Slice reqMetric
        if act <> null then act.AddMetric(count, bytes).AddVersion(slice.LastVersion) |> ignore
        let log = if not (log.IsEnabled Events.LogEventLevel.Debug) then log else log |> Log.propResolvedEvents "Json" slice.Messages
        (log |> Log.prop "startPos" startPos |> Log.prop "bytes" bytes |> Log.event evt).Information("Mdb{action:l} count={count} version={version}",
            "Read", count, slice.LastVersion)
        return slice }

    let private readBatches (log : ILogger) (readSlice : int64 -> int -> ILogger -> Async<StreamEventsSlice>)
            (maxPermittedBatchReads : int option) (startPosition : int64)
        : AsyncSeq<int64 * ITimelineEvent<EventBody> array> =
        let rec loop batchCount pos : AsyncSeq<int64 * ITimelineEvent<EventBody> array> = asyncSeq {
            match maxPermittedBatchReads with
            | Some mpbr when batchCount >= mpbr -> log.Information "batch Limit exceeded"; invalidOp "batch Limit exceeded"
            | _ -> ()

            let batchLog = log |> Log.prop "batchIndex" batchCount
            let! slice = readSlice pos batchCount batchLog
            yield slice.LastVersion, slice.Messages
            if not slice.IsEnd then
                yield! loop (batchCount + 1) (slice.LastVersion + 1L) }
        loop 0 startPosition

    let private logBatchRead (act: Activity) streamName t events (batchSize: int64) version (log : ILogger) =
        let bytes, count = resolvedEventBytes events, events.Length
        let reqMetric : Log.Measurement = { stream = streamName; interval = t; bytes = bytes; count = count}
        let batches = (events.Length - 1)/ (int batchSize) + 1
        let action = "Load"
        let evt = Log.Metric.Batch (batches, reqMetric)
        if act <> null then act.AddMetric(count, bytes).AddBatches(batches, count).AddVersion(version) |> ignore
        (log |> Log.prop "bytes" bytes |> Log.event evt).Information(
            "Mdb{action:l} stream={stream} count={count}/{batches} version={version}",
            action, streamName, count, batches, version)

    let private logLastEventRead (act: Activity) streamName t events (version: int64) (log: ILogger) =
        let bytes = resolvedEventBytes events
        let count = events.Length
        let reqMetric : Log.Measurement = { stream = streamName; interval = t; bytes = bytes; count = count}
        let evt = Log.Metric.ReadLast reqMetric
        if act <> null then act.AddMetric(count, bytes).AddVersion(version) |> ignore
        (log |> Log.prop "bytes" bytes |> Log.event evt).Information(
            "Mdb{action:l} stream={stream} count={count} version={version}",
            "ReadL", streamName, count, version)

    let internal loadLastEvent (log : ILogger) retryPolicy (reader : MessageDbReader) requiresLeader streamName
        : Async<int64 * ITimelineEvent<EventBody> array> = async {
        use act = source.StartActivity("LoadLast", ActivityKind.Client)
        if act <> null then act.AddStreamName(streamName).AddLeader(requiresLeader) |> ignore
        let! ct = Async.CancellationToken
        let read _ = readLastEventAsync reader streamName requiresLeader ct |> Async.AwaitTaskCorrect

        let! t, page = Log.withLoggedRetries retryPolicy "readAttempt" read log |> Stopwatch.Time

        log |> logLastEventRead act streamName t page.Messages page.LastVersion
        return page.LastVersion, page.Messages }

    let internal loadForwardsFrom (log : ILogger) retryPolicy reader batchSize maxPermittedBatchReads streamName startPosition requiresLeader
        : Async<int64 * ITimelineEvent<EventBody> array> = async {
        let mergeBatches (batches : AsyncSeq<int64 * ITimelineEvent<EventBody> array>) = async {
            let mutable versionFromStream = -1L
            let! (events : ITimelineEvent<EventBody> array) =
                batches
                |> AsyncSeq.map (fun (reportedVersion, events)-> versionFromStream <- max reportedVersion versionFromStream; events)
                |> AsyncSeq.concatSeq
                |> AsyncSeq.toArrayAsync
            let version = versionFromStream
            return version, events }
        use act = source.StartActivity("ReadStream", ActivityKind.Client)
        if act <> null then act.AddStreamName(streamName).AddBatchSize(batchSize).AddStartPosition(startPosition) |> ignore
        let call pos batchIndex = loggedReadSlice reader streamName batchSize batchIndex pos requiresLeader
        let retryingLoggingReadSlice pos batchIndex = Log.withLoggedRetries retryPolicy "readAttempt" (call pos batchIndex)
        let log = log |> Log.prop "batchSize" batchSize |> Log.prop "stream" streamName
        let batches : AsyncSeq<int64 * ITimelineEvent<EventBody> array> = readBatches log retryingLoggingReadSlice maxPermittedBatchReads startPosition
        let! t, (version, events) = mergeBatches batches |> Stopwatch.Time
        log |> logBatchRead act streamName t events batchSize version
        return version, events }

module private Token =
    // NOTE MessageDB's streamVersion is -1 based, similar to ESDB and SSS
    let create streamVersion : StreamToken =
        { value = box streamVersion
          // The `Version` exposed on the `ISyncContext` is 0-based
          version = streamVersion + 1L
          streamBytes = -1 }
    let inline streamVersion (token : StreamToken) = unbox<int64> token.value

    let supersedes struct (current, x) =
        x.version > current.version

type MessageDbConnection(reader, writer, [<O; D(null)>]?readRetryPolicy, [<O; D(null)>]?writeRetryPolicy) =
    member val Reader = reader
    member val ReadRetryPolicy = readRetryPolicy
    member val Writer = writer
    member val WriteRetryPolicy = writeRetryPolicy

type BatchOptions(getBatchSize : Func<int>, [<O; D(null)>]?batchCountLimit) =
    new (batchSize) = BatchOptions(fun () -> batchSize)
    member _.BatchSize = getBatchSize.Invoke()
    member val MaxBatches = batchCountLimit

[<RequireQualifiedAccess; NoComparison; NoEquality>]
type GatewaySyncResult = Written of StreamToken | ConflictUnknown

type MessageDbContext(connection : MessageDbConnection, batchOptions : BatchOptions) =
    new (   connection : MessageDbConnection,
            // Max number of Events to retrieve in a single batch. Also affects frequency of RollingSnapshots. Default: 500.
            [<O; D null>] ?batchSize) =
        MessageDbContext(connection, BatchOptions(batchSize = defaultArg batchSize 500))
    member val BatchOptions = batchOptions

    member _.TokenEmpty = Token.create -1L
    member _.LoadBatched(streamName, requireLeader, log, tryDecode) : Async<StreamToken * 'event array> = async {
        let! version, events = Read.loadForwardsFrom log connection.ReadRetryPolicy connection.Reader batchOptions.BatchSize batchOptions.MaxBatches streamName 0L requireLeader
        return Token.create version, Array.chooseV tryDecode events }
    member _.LoadLast(streamName, requireLeader, log, tryDecode) : Async<StreamToken * 'event array> = async {
        let! version, events = Read.loadLastEvent log connection.ReadRetryPolicy connection.Reader requireLeader streamName
        return Token.create version, Array.chooseV tryDecode events }
    member _.Reload(streamName, requireLeader, log, token, tryDecode)
        : Async<StreamToken * 'event array> = async {
        let streamVersion = Token.streamVersion token
        let startPos = streamVersion + 1L // Reading a stream uses {inclusive} positions, but the streamVersion is `-1`-based
        let! version, events = Read.loadForwardsFrom log connection.ReadRetryPolicy connection.Reader batchOptions.BatchSize batchOptions.MaxBatches streamName startPos requireLeader
        return Token.create (max streamVersion version), Array.chooseV tryDecode events }

    member _.TrySync(log, streamName, token, encodedEvents : IEventData<EventBody> array): Async<GatewaySyncResult> = async {
        let streamVersion = Token.streamVersion token
        match! Write.writeEvents log connection.WriteRetryPolicy connection.Writer streamName streamVersion encodedEvents with
        | MdbSyncResult.ConflictUnknown ->
            return GatewaySyncResult.ConflictUnknown
        | MdbSyncResult.Written version' ->
            let token = Token.create version'
            return GatewaySyncResult.Written token }

[<NoComparison; NoEquality; RequireQualifiedAccess>]
type AccessStrategy =
    /// Load only the single most recent event defined in in a stream and trust that it'll be decoded and
    /// doing a <c>fold</c> from any such event will yield a correct and complete state
    /// In other words, the <c>fold</c> function should not need to consider either the preceding <c>'state</c> or <c>'event</c>s.
    | LatestKnownEvent

type private Category<'event, 'state, 'context>(context : MessageDbContext, codec : IEventCodec<_, _, 'context>, ?access) =

    let loadAlgorithm streamName requireLeader log =
        match access with
        | None -> context.LoadBatched(streamName, requireLeader, log, codec.TryDecode)
        | Some AccessStrategy.LatestKnownEvent -> context.LoadLast(streamName, requireLeader, log, codec.TryDecode)

    let load (fold : 'state -> 'event seq -> 'state) initial f : Async<struct (StreamToken * 'state)> = async {
        let! token, events = f
        return struct (token, fold initial events) }

    member _.Load(fold : 'state -> 'event seq -> 'state, initial : 'state, streamName : string, requireLeader, log : ILogger) =
        load fold initial (loadAlgorithm streamName requireLeader log)
    member _.Reload(fold : 'state -> 'event seq -> 'state, state : 'state, streamName : string, requireLeader, token, log : ILogger) =
        load fold state (context.Reload(streamName, requireLeader, log, token, codec.TryDecode))

    member x.TrySync<'context>
        (   log : ILogger, fold : 'state -> 'event seq -> 'state,
            streamName, token, state : 'state, events : 'event array, ctx : 'context) : Async<SyncResult<'state>> = async {
        let encode e = codec.Encode(ctx, e)
        let encodedEvents : IEventData<EventBody> array = events |> Array.map encode
        match! context.TrySync(log, streamName, token, encodedEvents) with
        | GatewaySyncResult.ConflictUnknown ->
            return SyncResult.Conflict  (fun ct -> x.Reload(fold, state, streamName, (*requireLeader*)true, token, log) |> Async.startAsTask ct)
        | GatewaySyncResult.Written token' ->
            return SyncResult.Written   (token', fold state (Seq.ofArray events)) }

type private Folder<'event, 'state, 'context>(category : Category<'event, 'state, 'context>, fold : 'state -> 'event seq -> 'state, initial : 'state, ?readCache) =
    let batched log streamName requireLeader ct = category.Load(fold, initial, streamName, requireLeader, log) |> Async.startAsTask ct
    interface ICategory<'event, 'state, 'context> with
        member _.Load(log, _categoryName, _streamId, streamName, allowStale, requireLeader, ct) = task {
            match readCache with
            | None -> return! batched log streamName requireLeader ct
            | Some (cache : ICache, prefix : string) ->
                match! cache.TryGet(prefix + streamName) with
                | ValueNone -> return! batched log streamName requireLeader ct
                | ValueSome tokenAndState when allowStale -> return tokenAndState
                | ValueSome (token, state) -> return! category.Reload(fold, state, streamName, requireLeader, token, log) }
        member _.TrySync(log, _categoryName, _streamId, streamName, context, _init, token, originState, events, _ct) = task {
            match! category.TrySync(log, fold, streamName, token, originState, events, context) with
            | SyncResult.Conflict resync ->          return SyncResult.Conflict resync
            | SyncResult.Written (token', state') -> return SyncResult.Written (token', state') }

/// For MessageDb, caching is less critical than it is for e.g. CosmosDB
/// As such, it can often be omitted, particularly if streams are short, or events are small and/or database latency aligns with request latency requirements
[<NoComparison; NoEquality; RequireQualifiedAccess>]
type CachingStrategy =
    /// Retain a single 'state per streamName.
    /// Each cache hit for a stream renews the retention period for the defined <c>window</c>.
    /// Upon expiration of the defined <c>window</c> from the point at which the cache was entry was last used, a full reload is triggered.
    /// Unless <c>LoadOption.AllowStale</c> is used, each cache hit still incurs a roundtrip to load any subsequently-added events.
    | SlidingWindow of ICache * window : TimeSpan
    /// Retain a single 'state per streamName
    /// Upon expiration of the defined <c>period</c>, a full reload is triggered.
    /// Unless <c>LoadOption.AllowStale</c> is used, each cache hit still incurs a roundtrip to load any subsequently-added events.
    | FixedTimeSpan of ICache * period : TimeSpan
    /// Prefix is used to segregate multiple folds per stream when they are stored in the cache.
    /// Semantics are identical to <c>SlidingWindow</c>.
    | SlidingWindowPrefixed of ICache * window : TimeSpan * prefix : string

type MessageDbCategory<'event, 'state, 'context>(resolveInner, empty) =
    inherit Equinox.Category<'event, 'state, 'context>(resolveInner, empty)
    new (   context : MessageDbContext, codec : IEventCodec<_, _, 'context>, fold, initial,
            [<O; D(null)>]?caching,
            [<O; D(null)>]?access) =
        let inner = Category<'event, 'state, 'context>(context, codec, ?access = access)
        let readCacheOption =
            match caching with
            | None -> None
            | Some (CachingStrategy.SlidingWindow (cache, _))
            | Some (CachingStrategy.FixedTimeSpan (cache, _)) -> Some (cache, null)
            | Some (CachingStrategy.SlidingWindowPrefixed (cache, _, prefix)) -> Some (cache, prefix)
        let folder = Folder<'event, 'state, 'context>(inner, fold, initial, ?readCache = readCacheOption)
        let category : ICategory<_, _, 'context> =
            match caching with
            | None -> folder :> _
            | Some (CachingStrategy.SlidingWindow (cache, window)) ->
                Caching.applyCacheUpdatesWithSlidingExpiration cache null window folder Token.supersedes
            | Some (CachingStrategy.FixedTimeSpan (cache, period)) ->
                Caching.applyCacheUpdatesWithFixedTimeSpan cache null period folder Token.supersedes
            | Some (CachingStrategy.SlidingWindowPrefixed (cache, window, prefix)) ->
                Caching.applyCacheUpdatesWithSlidingExpiration cache prefix window folder Token.supersedes
        let resolveInner streamIds = struct (category, StreamName.Internal.ofCategoryAndStreamId streamIds, ValueNone)
        let empty = struct (context.TokenEmpty, initial)
        MessageDbCategory(resolveInner, empty)

type MessageDbConnector
    (   connectionString : string,
        // Can be used to divert reads to a replica
        // Conflicts detected on write trigger a resync, reading via the `connectionString` to maximize the freshness of the data for the retry
        [<O; D(null)>]?readConnectionString : string,
        [<O; D(null)>]?readRetryPolicy, [<O; D(null)>]?writeRetryPolicy) =

    let readConnectionString = defaultArg readConnectionString connectionString

    member _.Establish() : MessageDbConnection =
        let reader = MessageDbReader(readConnectionString, connectionString)
        let writer = MessageDbWriter(connectionString)
        MessageDbConnection(reader, writer, ?readRetryPolicy = readRetryPolicy, ?writeRetryPolicy = writeRetryPolicy)
