namespace Equinox.MessageDb

open Equinox.Core
open Equinox.MessageDb.Core
open FsCodec
open FSharp.Control
open Npgsql
open Serilog
open System
open System.Threading.Tasks

type EventBody = ReadOnlyMemory<byte>

[<NoComparison;NoEquality>]
type StreamEventsSlice = { Messages: ITimelineEvent<EventBody> array; IsEnd: bool; LastVersion: int64 }

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
    let private writeEventsAsync (log : ILogger) (conn : MessageDbClient) (streamName : string) (version : int64) (events : IEventData<EventBody> array)
        : Async<MdbSyncResult> = async {
            let! wr = conn.WriteMessages(streamName, events, version, Async.DefaultCancellationToken) |> Async.AwaitTaskCorrect
            match wr with
            | MdbSyncResult.ConflictUnknown ->
                log.Information("MdbTrySync WrongExpectedVersionException writing {EventTypes}, expected {ExpectedVersion}",
                                [| for x in events -> x.EventType |], version)
                return wr
            | _ -> return wr }

    let inline len (bytes: EventBody) = bytes.Length
    let eventDataBytes events =
        let eventDataLen (x : IEventData<EventBody>) = len x.Data + len x.Meta
        events |> Array.sumBy eventDataLen
    let private writeEventsLogged (conn : MessageDbClient) (streamName : string) (version : int64) (events : IEventData<EventBody> array) (log : ILogger)
        : Async<MdbSyncResult> = async {
        let log = if (not << log.IsEnabled) Events.LogEventLevel.Debug then log else log |> Log.propEventData "Json" events
        let bytes, count = eventDataBytes events, events.Length
        let log = log |> Log.prop "bytes" bytes
        let writeLog = log |> Log.prop "stream" streamName |> Log.prop "expectedVersion" version |> Log.prop "count" count
        let! t, result = writeEventsAsync writeLog conn streamName version events |> Stopwatch.Time
        let reqMetric : Log.Measurement = { stream = streamName; interval = t; bytes = bytes; count = count}
        let resultLog, evt =
            match result with
            | MdbSyncResult.ConflictUnknown ->
                log, Log.WriteConflict reqMetric
            | MdbSyncResult.Written x ->
                log |> Log.prop "currentPosition" x, Log.WriteSuccess reqMetric
        (resultLog |> Log.event evt).Information("Mdb{action:l} count={count} conflict={conflict}",
            "Write", events.Length, match evt with Log.WriteConflict _ -> true | _ -> false)
        return result }
    let writeEvents (log : ILogger) retryPolicy (conn : MessageDbClient) (streamName : string) (version : int64) (events : IEventData<EventBody> array)
        : Async<MdbSyncResult> =
        let call = writeEventsLogged conn streamName version events
        Log.withLoggedRetries retryPolicy "writeAttempt" call log

module private Read =
    let private toSlice (events : ITimelineEvent<EventBody> array) isLast =
        let lastVersion = match Array.tryLast events with Some ev -> ev.Index | None -> -1L
        { Messages = events; IsEnd = isLast; LastVersion = lastVersion }

    let private readSliceAsync (conn : MessageDbClient) (streamName : string) (batchSize : int64) (startPos : int64) ct
        : Task<_> = task {
        let! page = conn.ReadStream(streamName, startPos, batchSize, ct)
        let isLast = int64 page.Length < batchSize
        return toSlice page isLast }

    let private readLastEventAsync (conn : MessageDbClient) (streamName : string) ct : Task<_> = task {
        let! events = conn.ReadLastEvent(streamName, ct)
        return toSlice events false }

    let inline len (bytes: EventBody) = bytes.Length
    let resolvedEventLen (x : ITimelineEvent<EventBody>) = len x.Data + len x.Meta
    let private loggedReadSlice conn streamName batchSize startPos (log : ILogger) : Async<_> = async {
        let! ct = Async.CancellationToken
        let! t, slice = readSliceAsync conn streamName batchSize startPos ct |> Async.AwaitTaskCorrect |> Stopwatch.Time
        let bytes, count = slice.Messages |> Array.sumBy resolvedEventLen, slice.Messages.Length
        let reqMetric : Log.Measurement = { stream = streamName; interval = t; bytes = bytes; count = count}
        let evt = Log.Slice reqMetric
        let log = if not (log.IsEnabled Events.LogEventLevel.Debug) then log else log |> Log.propResolvedEvents "Json" slice.Messages
        (log |> Log.prop "startPos" startPos |> Log.prop "bytes" bytes |> Log.event evt).Information("Mdb{action:l} count={count} version={version}",
            "Read", count, slice.LastVersion)
        return slice }

    let private readBatches (log : ILogger) (readSlice : int64 -> ILogger -> Async<StreamEventsSlice>)
            (maxPermittedBatchReads : int option) (startPosition : int64)
        : AsyncSeq<int64 * ITimelineEvent<EventBody> array> =
        let rec loop batchCount pos : AsyncSeq<int64 * ITimelineEvent<EventBody> array> = asyncSeq {
            match maxPermittedBatchReads with
            | Some mpbr when batchCount >= mpbr -> log.Information "batch Limit exceeded"; invalidOp "batch Limit exceeded"
            | _ -> ()

            let batchLog = log |> Log.prop "batchIndex" batchCount
            let! slice = readSlice pos batchLog
            yield slice.LastVersion, slice.Messages
            if not slice.IsEnd then
                yield! loop (batchCount + 1) (slice.LastVersion + 1L) }
        loop 0 startPosition

    let resolvedEventBytes events = events |> Array.sumBy resolvedEventLen
    let logBatchRead streamName t events (batchSize: int64) version (log : ILogger) =
        let bytes, count = resolvedEventBytes events, events.Length
        let reqMetric : Log.Measurement = { stream = streamName; interval = t; bytes = bytes; count = count}
        let batches = (events.Length - 1)/ (int batchSize) + 1
        let action = "Load"
        let evt = Log.Metric.Batch (batches, reqMetric)
        (log |> Log.prop "bytes" bytes |> Log.event evt).Information(
            "Mdb{action:l} stream={stream} count={count}/{batches} version={version}",
            action, streamName, count, batches, version)

    let logLastEventRead streamName t events version (log: ILogger) =
        let bytes = resolvedEventBytes events
        let count = events.Length
        let reqMetric : Log.Measurement = { stream = streamName; interval = t; bytes = bytes; count = count}
        let evt = Log.Metric.ReadLast reqMetric
        (log |> Log.prop "bytes" bytes |> Log.event evt).Information(
            "Mdb{action:l} stream={stream} count={count} version={version}",
            "ReadL", streamName, count, version)

    let loadLastEvent (log : ILogger) retryPolicy (conn : MessageDbClient) streamName
        : Async<int64 * ITimelineEvent<EventBody> array> = async {
        let! ct = Async.CancellationToken
        let read _ = readLastEventAsync conn streamName ct |> Async.AwaitTaskCorrect

        let! t, page = Log.withLoggedRetries retryPolicy "readAttempt" read log |> Stopwatch.Time

        log |> logLastEventRead streamName t page.Messages page.LastVersion
        return page.LastVersion, page.Messages }

    let loadForwardsFrom (log : ILogger) retryPolicy conn batchSize maxPermittedBatchReads streamName startPosition
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
        let call pos = loggedReadSlice conn streamName batchSize pos
        let retryingLoggingReadSlice pos = Log.withLoggedRetries retryPolicy "readAttempt" (call pos)
        let log = log |> Log.prop "batchSize" batchSize |> Log.prop "stream" streamName
        let batches : AsyncSeq<int64 * ITimelineEvent<EventBody> array> = readBatches log retryingLoggingReadSlice maxPermittedBatchReads startPosition
        let! t, (version, events) = mergeBatches batches |> Stopwatch.Time
        log |> logBatchRead streamName t events batchSize version
        return version, events }

module private Token =
    let create streamVersion : StreamToken =
        { value = box streamVersion
          version = streamVersion
          streamBytes = -1 }
    let supersedes struct (current, x) =
        x.version > current.version

type MessageDbConnection(readConnection, [<O; D(null)>]?writeConnection, [<O; D(null)>]?readRetryPolicy, [<O; D(null)>]?writeRetryPolicy) =
    member _.ReadConnection = readConnection
    member _.ReadRetryPolicy = readRetryPolicy
    member _.WriteConnection = defaultArg writeConnection readConnection
    member _.WriteRetryPolicy = writeRetryPolicy

type BatchOptions(getBatchSize : Func<int>, [<O; D(null)>]?batchCountLimit) =
    new (batchSize) = BatchOptions(fun () -> batchSize)
    member _.BatchSize = getBatchSize.Invoke()
    member _.MaxBatches = batchCountLimit

[<RequireQualifiedAccess; NoComparison; NoEquality>]
type GatewaySyncResult = Written of StreamToken | ConflictUnknown

type MessageDbContext(connection : MessageDbConnection, batchOptions : BatchOptions) =
    new (   connection : MessageDbConnection,
            // Max number of Events to retrieve in a single batch. Also affects frequency of RollingSnapshots. Default: 500.
            [<O; D null>] ?batchSize) =
        MessageDbContext(connection, BatchOptions(batchSize = defaultArg batchSize 500))
    member val BatchOptions = batchOptions

    member _.TokenEmpty = Token.create -1L
    member _.LoadBatched(streamName, log, tryDecode) : Async<StreamToken * 'event array> = async {
        let! version, events = Read.loadForwardsFrom log connection.ReadRetryPolicy connection.ReadConnection batchOptions.BatchSize batchOptions.MaxBatches streamName 0L
        return Token.create version, Array.chooseV tryDecode events }
    member _.LoadLast(streamName, log, tryDecode) : Async<StreamToken * 'event array> = async {
        let! version, events = Read.loadLastEvent log connection.ReadRetryPolicy connection.ReadConnection streamName
        return Token.create version, Array.chooseV tryDecode events }
    member _.LoadFromToken(useWriteConn, streamName, log, token, tryDecode)
        : Async<StreamToken * 'event array> = async {
        let streamPosition = token.version + 1L
        let connToUse = if useWriteConn then connection.WriteConnection else connection.ReadConnection
        let! version, events = Read.loadForwardsFrom log connection.ReadRetryPolicy connToUse batchOptions.BatchSize batchOptions.MaxBatches streamName streamPosition
        return Token.create (max token.version version), Array.chooseV tryDecode events }

    member _.TrySync(log, streamName, token, encodedEvents : IEventData<EventBody> array): Async<GatewaySyncResult> = async {
        match! Write.writeEvents log connection.WriteRetryPolicy connection.WriteConnection streamName token.version encodedEvents with
        | MdbSyncResult.ConflictUnknown ->
            return GatewaySyncResult.ConflictUnknown
        | MdbSyncResult.Written version' ->
            let token = Token.create version'
            return GatewaySyncResult.Written token }

    member _.Sync(log, streamName, streamVersion, events : IEventData<EventBody> array) : Async<GatewaySyncResult> = async {
        match! Write.writeEvents log connection.WriteRetryPolicy connection.WriteConnection streamName streamVersion events with
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
    let tryDecode = codec.TryDecode
    let loadAlgorithm streamName log =
        match access with
        | None -> context.LoadBatched(streamName, log, tryDecode)
        | Some AccessStrategy.LatestKnownEvent -> context.LoadLast(streamName, log, tryDecode)
    let load (fold : 'state -> 'event seq -> 'state) initial f = async {
        let! token, events = f
        return struct (token, fold initial events) }
    member _.Load(fold : 'state -> 'event seq -> 'state) (initial : 'state) (streamName : string) (log : ILogger) : Async<struct (StreamToken * 'state)> =
        (load fold initial) (loadAlgorithm streamName log)

    member _.LoadFromToken (fold : 'state -> 'event seq -> 'state) (state : 'state) (streamName : string) token (log : ILogger) : Async<struct (StreamToken * 'state)> =
        (load fold state) (context.LoadFromToken(false, streamName, log, token, tryDecode))

    member _.TrySync<'context>
        (   log : ILogger, fold : 'state -> 'event seq -> 'state,
            streamName, token, state : 'state, events : 'event array, ctx : 'context) : Async<SyncResult<'state>> = async {
        let encode e = codec.Encode(ctx, e)
        let encodedEvents : IEventData<EventBody> array = events |> Array.map encode
        match! context.TrySync(log, streamName, token, encodedEvents) with
        | GatewaySyncResult.ConflictUnknown ->
            return SyncResult.Conflict  (fun ct -> load fold state (context.LoadFromToken(true, streamName, log, token, tryDecode)) |> Async.startAsTask ct)
        | GatewaySyncResult.Written token' ->
            return SyncResult.Written   (token', fold state (Seq.ofArray events)) }

type private Folder<'event, 'state, 'context>(category : Category<'event, 'state, 'context>, fold : 'state -> 'event seq -> 'state, initial : 'state, ?readCache) =
    let batched log streamName ct = category.Load fold initial streamName log |> Async.startAsTask ct
    interface ICategory<'event, 'state, 'context> with
        member _.Load(log, _categoryName, _streamId, streamName, allowStale, ct) = task {
            match readCache with
            | None -> return! batched log streamName ct
            | Some (cache : ICache, prefix : string) ->
                match! cache.TryGet(prefix + streamName) with
                | ValueNone -> return! batched log streamName ct
                | ValueSome tokenAndState when allowStale -> return tokenAndState
                | ValueSome (token, state) -> return! category.LoadFromToken fold state streamName token log }
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


type MessageDbConnector(
    connectionString : string, ?readOnlyConnectionString : string,
    [<O; D(null)>]?readRetryPolicy, [<O; D(null)>]?writeRetryPolicy) =
        let readOnlyConnectionString = defaultArg readOnlyConnectionString connectionString
        member _.Connect() =
            let connectToDb connectionString ct = task {
                let conn = new NpgsqlConnection(connectionString)
                do! conn.OpenAsync(ct)
                return conn }
            let writeConnection = connectToDb connectionString
            let readConnection = connectToDb readOnlyConnectionString
            MessageDbClient(writeConnection), MessageDbClient(readConnection)

        member x.Establish() : MessageDbConnection =
            let write, read = x.Connect()
            MessageDbConnection(readConnection = read, writeConnection = write, ?readRetryPolicy = readRetryPolicy, ?writeRetryPolicy = writeRetryPolicy)
