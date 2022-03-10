namespace Equinox.EventStoreDb

open Equinox.Core
open EventStore.Client
open Serilog
open System

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

    let prop name value (log : ILogger) = log.ForContext(name, value)

    let propEvents name (kvps : System.Collections.Generic.KeyValuePair<string,string> seq) (log : ILogger) =
        let items = seq { for kv in kvps do yield sprintf "{\"%s\": %s}" kv.Key kv.Value }
        log.ForContext(name, sprintf "[%s]" (String.concat ",\n\r" items))

    let propEventData name (events : EventData[]) (log : ILogger) =
        log |> propEvents name (seq {
            for x in events do
                if x.ContentType = "application/json" then
                    yield let d = x.Data in System.Collections.Generic.KeyValuePair<_,_>(x.Type, System.Text.Encoding.UTF8.GetString d.Span) })

    let propResolvedEvents name (events : ResolvedEvent[]) (log : ILogger) =
        log |> propEvents name (seq {
            for x in events do
                let e = x.Event
                if e.ContentType = "application/json" then
                    yield let d = e.Data in System.Collections.Generic.KeyValuePair<_,_>(e.EventType, System.Text.Encoding.UTF8.GetString d.Span) })

    open Serilog.Events

    /// Attach a property to the log context to hold the metrics
    // Sidestep Log.ForContext converting to a string; see https://github.com/serilog/serilog/issues/1124
    let event (value : Metric) (log : ILogger) =
        let enrich (e : LogEvent) = e.AddPropertyIfAbsent(LogEventProperty(PropertyTag, ScalarValue(value)))
        log.ForContext({ new Serilog.Core.ILogEventEnricher with member _.Enrich(evt, _) = enrich evt })

    let withLoggedRetries<'t> retryPolicy (contextLabel : string) (f : ILogger -> Async<'t>) log : Async<'t> =
        match retryPolicy with
        | None -> f log
        | Some retryPolicy ->
            let withLoggingContextWrapping count =
                let log = if count = 1 then log else log |> prop contextLabel count
                f log
            retryPolicy withLoggingContextWrapping

    let (|BlobLen|) (x : ReadOnlyMemory<byte>) = x.Length

    /// NB Caveat emptor; this is subject to unlimited change without the major version changing - while the `dotnet-templates` repo will be kept in step, and
    /// the ChangeLog will mention changes, it's critical to not assume that the presence or nature of these helpers be considered stable
    module InternalMetrics =

        module Stats =
            let inline (|Stats|) ({ interval = i } : Measurement) = let e = i.Elapsed in int64 e.TotalMilliseconds

            let (|Read|Write|Resync|) = function
                | WriteSuccess (Stats s) -> Write s
                | WriteConflict (Stats s) -> Resync s
                // slices are rolled up into batches in other stores, but we don't log slices in this impl
                | Batch (_, _, Stats s) -> Read s

            let (|SerilogScalar|_|) : LogEventPropertyValue -> obj option = function
                | :? ScalarValue as x -> Some x.Value
                | _ -> None

            let (|EsMetric|_|) (logEvent : LogEvent) : Metric option =
                match logEvent.Properties.TryGetValue("esEvt") with
                | true, SerilogScalar (:? Metric as e) -> Some e
                | _ -> None

            type Counter =
                { mutable count : int64; mutable ms : int64 }
                static member Create() = { count = 0L; ms = 0L }
                member x.Ingest(ms) =
                    System.Threading.Interlocked.Increment(&x.count) |> ignore
                    System.Threading.Interlocked.Add(&x.ms, ms) |> ignore

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
                        | EsMetric (Read stats) -> LogSink.Read.Ingest stats
                        | EsMetric (Write stats) -> LogSink.Write.Ingest stats
                        | EsMetric (Resync stats) -> LogSink.Resync.Ingest stats
                        | _ -> ()

        /// Relies on feeding of metrics from Log through to Stats.LogSink
        /// Use Stats.LogSink.Restart() to reset the start point (and stats) where relevant
        let dump (log : ILogger) =
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
            let measures : (string * (TimeSpan -> float)) list = [ "s", fun x -> x.TotalSeconds(*; "m", fun x -> x.TotalMinutes; "h", fun x -> x.TotalHours*) ]
            let logPeriodicRate name count = log.Information("rp{name} {count:n0}", name, count)
            for uom, f in measures do let d = f duration in if d <> 0. then logPeriodicRate uom (float totalCount/d |> int64)

[<RequireQualifiedAccess; NoEquality; NoComparison>]
type EsSyncResult = Written of ConditionalWriteResult | Conflict of actualVersion: int64

module private Write =

    let private writeEventsAsync (log : ILogger) (conn : EventStoreClient) (streamName : string) version (events : EventData[])
        : Async<EsSyncResult> = async {
        let! ct = Async.CancellationToken
        let! wr = conn.ConditionalAppendToStreamAsync(streamName, StreamRevision.FromInt64 version, events, cancellationToken = ct) |> Async.AwaitTaskCorrect
        if wr.Status = ConditionalWriteStatus.VersionMismatch then
            log.Information("Esdb TrySync VersionMismatch writing {EventTypes}, actual {ActualVersion}",
                [| for x in events -> x.Type |], wr.NextExpectedVersion)
            return EsSyncResult.Conflict wr.NextExpectedVersion
        elif wr.Status = ConditionalWriteStatus.StreamDeleted then return failwithf "Unexpected write to deleted stream %s" streamName
        elif wr.Status = ConditionalWriteStatus.Succeeded then return EsSyncResult.Written wr
        else return failwithf "Unexpected write response code %O" wr.Status }

    let eventDataBytes events =
        let eventDataLen (x : EventData) = match x.Data, x.Metadata with Log.BlobLen bytes, Log.BlobLen metaBytes -> bytes + metaBytes
        events |> Array.sumBy eventDataLen

    let private writeEventsLogged (conn : EventStoreClient) (streamName : string) (version : int64) (events : EventData[]) (log : ILogger)
        : Async<EsSyncResult> = async {
        let log = if (not << log.IsEnabled) Events.LogEventLevel.Debug then log else log |> Log.propEventData "Json" events
        let bytes, count = eventDataBytes events, events.Length
        let log = log |> Log.prop "bytes" bytes |> Log.prop "expectedVersion" version
        let writeLog = log |> Log.prop "stream" streamName |> Log.prop "count" count
        let! t, result = writeEventsAsync writeLog conn streamName version events |> Stopwatch.Time
        let reqMetric : Log.Measurement = { stream = streamName; interval = t; bytes = bytes; count = count}
        let resultLog, evt =
            match result, reqMetric with
            | EsSyncResult.Conflict actualVersion, m ->
                log |> Log.prop "actualVersion" actualVersion, Log.WriteConflict m
            | EsSyncResult.Written x, m ->
                log |> Log.prop "nextExpectedVersion" x.NextExpectedVersion |> Log.prop "logPosition" x.LogPosition, Log.WriteSuccess m
        (resultLog |> Log.event evt).Information("Esdb{action:l} count={count} conflict={conflict}",
            "Write", events.Length, match evt with Log.WriteConflict _ -> true | _ -> false)
        return result }

    let writeEvents (log : ILogger) retryPolicy (conn : EventStoreClient) (streamName : string) (version : int64) (events : EventData[])
        : Async<EsSyncResult> =
        let call = writeEventsLogged conn streamName version events
        Log.withLoggedRetries retryPolicy "writeAttempt" call log

module private Read =
    open FSharp.Control
    let resolvedEventBytes (x : ResolvedEvent) = let Log.BlobLen bytes, Log.BlobLen metaBytes = x.Event.Data, x.Event.Metadata in bytes + metaBytes
    let resolvedEventsBytes events = events |> Array.sumBy resolvedEventBytes
    let logBatchRead direction streamName t events batchSize version (log : ILogger) =
        let bytes, count = resolvedEventsBytes events, events.Length
        let reqMetric : Log.Measurement = { stream = streamName; interval = t; bytes = bytes; count = count}
        let batches = match batchSize with Some batchSize -> (events.Length - 1) / batchSize + 1 | None -> -1
        let action = if direction = Direction.Forward then "LoadF" else "LoadB"
        let log = if (not << log.IsEnabled) Events.LogEventLevel.Debug then log else log |> Log.propResolvedEvents "Json" events
        let evt = Log.Metric.Batch (direction, batches, reqMetric)
        (log |> Log.prop "bytes" bytes |> Log.event evt).Information(
            "Esdb{action:l} stream={stream} count={count}/{batches} version={version}",
            action, streamName, count, batches, version)
    let loadBackwardsUntilOrigin (log : ILogger) (conn : EventStoreClient) batchSize streamName (tryDecode, isOrigin)
        : Async<int64 * (ResolvedEvent * 'event option)[]> = async {
        let! ct = Async.CancellationToken
        let res = conn.ReadStreamAsync(Direction.Backwards, streamName, StreamPosition.End, int64 batchSize, resolveLinkTos = false, cancellationToken = ct)
        try let! events =
                AsyncSeq.ofAsyncEnum res
                |> AsyncSeq.map (fun x -> x, tryDecode x)
                |> AsyncSeq.takeWhileInclusive (function
                    | x, Some e when isOrigin e ->
                        log.Information("EsdbStop stream={stream} at={eventNumber}", streamName, let en = x.Event.EventNumber in en.ToInt64())
                        false
                    | _ -> true)
                |> AsyncSeq.toArrayAsync
            let v = match Seq.tryHead events with Some (r, _) -> let en = r.Event.EventNumber in en.ToInt64() | None -> -1
            Array.Reverse events
            return v, events
        with :? AggregateException as e when (e.InnerExceptions.Count = 1 && e.InnerExceptions[0] :? StreamNotFoundException) ->
            return -1L, [||] }
    let loadBackwards (log : ILogger) (conn : EventStoreClient) batchSize streamName (tryDecode, isOrigin)
        : Async<int64 * (ResolvedEvent * 'event option)[]> = async {
        let! t, (version, events) = loadBackwardsUntilOrigin log conn batchSize streamName (tryDecode, isOrigin) |> Stopwatch.Time
        let log = log |> Log.prop "batchSize" batchSize |> Log.prop "stream" streamName
        log |> logBatchRead Direction.Backward streamName t (Array.map fst events) (Some batchSize) version
        return version, events }

    let loadForward (conn : EventStoreClient) streamName startPosition
        : Async<int64 * ResolvedEvent[]> = async {
        let! ct = Async.CancellationToken
        let res = conn.ReadStreamAsync(Direction.Forwards, streamName, startPosition, Int64.MaxValue, resolveLinkTos = false, cancellationToken = ct)
        try let! events = AsyncSeq.ofAsyncEnum res |> AsyncSeq.toArrayAsync
            let v = match Seq.tryLast events with Some r -> let en = r.Event.EventNumber in en.ToInt64() | None -> startPosition.ToInt64() - 1L
            return v, events
        with :? AggregateException as e when (e.InnerExceptions.Count = 1 && e.InnerExceptions[0] :? StreamNotFoundException) ->
            return -1L, [||] }
    let loadForwards log conn streamName startPosition
        : Async<int64 * ResolvedEvent[]> = async {
        let direction = Direction.Forward
        let! t, (version, events) = loadForward conn streamName startPosition |> Stopwatch.Time
        let log = log |> Log.prop "startPos" startPosition |> Log.prop "direction" direction |> Log.prop "stream" streamName
        log |> logBatchRead direction streamName t events None version
        return version, events }

module UnionEncoderAdapters =
    let encodedEventOfResolvedEvent (x : ResolvedEvent) : FsCodec.ITimelineEvent<byte[]> =
        let e = x.Event
        // Inspecting server code shows both Created and CreatedEpoch are set; taking this as it's less ambiguous than DateTime in the general case
        let ts = DateTimeOffset(e.Created)
        // TODO something like let ts = DateTimeOffset.FromUnixTimeMilliseconds(e.CreatedEpoch)
        // TOCONSIDER wire e.Metadata.["$correlationId"] and .["$causationId"] into correlationId and causationId
        // https://eventstore.org/docs/server/metadata-and-reserved-names/index.html#event-metadata
        let n, d, m = e.EventNumber, e.Data, e.Metadata
        FsCodec.Core.TimelineEvent.Create(n.ToInt64(), e.EventType, d.ToArray(), m.ToArray(), correlationId = null, causationId = null, timestamp = ts)

    let eventDataOfEncodedEvent (x : FsCodec.IEventData<byte[]>) =
        // TOCONSIDER wire x.CorrelationId, x.CausationId into x.Meta.["$correlationId"] and .["$causationId"]
        // https://eventstore.org/docs/server/metadata-and-reserved-names/index.html#event-metadata
        EventData(Uuid.NewUuid(), x.EventType, contentType = "application/json", data = ReadOnlyMemory(x.Data), metadata = ReadOnlyMemory(x.Meta))

type Position = { streamVersion : int64; compactionEventNumber : int64 option; batchCapacityLimit : int option }
type Token = { pos : Position }

module Token =
    let private create compactionEventNumber batchCapacityLimit streamVersion : StreamToken =
        {   value = box { pos = { streamVersion = streamVersion; compactionEventNumber = compactionEventNumber; batchCapacityLimit = batchCapacityLimit } }
            // In this impl, the StreamVersion matches the EventStore StreamVersion in being -1-based
            // Version however is the representation that needs to align with ISyncContext.Version
            version = streamVersion + 1L }

    /// No batching / compaction; we only need to retain the StreamVersion
    let ofNonCompacting streamVersion : StreamToken =
        create None None streamVersion

    // headroom before compaction is necessary given the stated knowledge of the last (if known) `compactionEventNumberOption`
    let private batchCapacityLimit compactedEventNumberOption unstoredEventsPending (batchSize : int) (streamVersion : int64) : int =
        match compactedEventNumberOption with
        | Some (compactionEventNumber : int64) -> (batchSize - unstoredEventsPending) - int (streamVersion - compactionEventNumber + 1L) |> max 0
        | None -> (batchSize - unstoredEventsPending) - (int streamVersion + 1) - 1 |> max 0

    let (*private*) ofCompactionEventNumber compactedEventNumberOption unstoredEventsPending batchSize streamVersion : StreamToken =
        let batchCapacityLimit = batchCapacityLimit compactedEventNumberOption unstoredEventsPending batchSize streamVersion
        create compactedEventNumberOption (Some batchCapacityLimit) streamVersion

    /// Assume we have not seen any compaction events; use the batchSize and version to infer headroom
    let ofUncompactedVersion batchSize streamVersion : StreamToken =
        ofCompactionEventNumber None 0 batchSize streamVersion

    let (|Unpack|) (x : StreamToken) : Position = let t = unbox<Token> x.value in t.pos

    /// Use previousToken plus the data we are adding and the position we are adding it to infer a headroom
    let ofPreviousTokenAndEventsLength (Unpack previousToken) eventsLength batchSize streamVersion : StreamToken =
        let compactedEventNumber = previousToken.compactionEventNumber
        ofCompactionEventNumber compactedEventNumber eventsLength batchSize streamVersion

    /// Use an event just read from the stream to infer headroom
    let ofCompactionResolvedEventAndVersion (compactionEvent : ResolvedEvent) batchSize streamVersion : StreamToken =
        let e = compactionEvent.Event.EventNumber in ofCompactionEventNumber (Some (e.ToInt64())) 0 batchSize streamVersion

    /// Use an event we are about to write to the stream to infer headroom
    let ofPreviousStreamVersionAndCompactionEventDataIndex (Unpack token) compactionEventDataIndex eventsLength batchSize streamVersion' : StreamToken =
        ofCompactionEventNumber (Some (token.streamVersion + 1L + int64 compactionEventDataIndex)) eventsLength batchSize streamVersion'

    let supersedes (Unpack current) (Unpack x) =
        let currentVersion, newVersion = current.streamVersion, x.streamVersion
        newVersion > currentVersion

type EventStoreConnection(readConnection, [<O; D(null)>] ?writeConnection, [<O; D(null)>] ?readRetryPolicy, [<O; D(null)>] ?writeRetryPolicy) =
    member _.ReadConnection = readConnection
    member _.ReadRetryPolicy = readRetryPolicy
    member _.WriteConnection = defaultArg writeConnection readConnection
    member _.WriteRetryPolicy = writeRetryPolicy

type BatchingPolicy(getMaxBatchSize : unit -> int, [<O; D(null)>] ?batchCountLimit) = // TODO batchCountLimit
    new (maxBatchSize) = BatchingPolicy(fun () -> maxBatchSize)
// TOCONSIDER remove if Client does not start to expose it
    member _.BatchSize = getMaxBatchSize()
//TODO    member _.MaxBatches = batchCountLimit

[<RequireQualifiedAccess; NoComparison; NoEquality>]
type GatewaySyncResult = Written of StreamToken | ConflictUnknown of StreamToken

type EventStoreContext(conn : EventStoreConnection, batching : BatchingPolicy) =
    let isResolvedEventEventType (tryDecode, predicate) (x : ResolvedEvent) = predicate (tryDecode x.Event.Data)
    let tryIsResolvedEventEventType predicateOption = predicateOption |> Option.map isResolvedEventEventType

    member _.TokenEmpty = Token.ofUncompactedVersion batching.BatchSize -1L
    member _.LoadBatched(streamName, log, tryDecode, isCompactionEventType) : Async<StreamToken * 'event[]> = async {
        let! version, events = Read.loadForwards log conn.ReadConnection streamName StreamPosition.Start
        match tryIsResolvedEventEventType isCompactionEventType with
        | None -> return Token.ofNonCompacting version, Array.choose tryDecode events
        | Some isCompactionEvent ->
            match events |> Array.tryFindBack isCompactionEvent with
            | None -> return Token.ofUncompactedVersion batching.BatchSize version, Array.choose tryDecode events
            | Some resolvedEvent -> return Token.ofCompactionResolvedEventAndVersion resolvedEvent batching.BatchSize version, Array.choose tryDecode events }

    member _.LoadBackwardsStoppingAtCompactionEvent(streamName, log, limit, (tryDecode, isOrigin)) : Async<StreamToken * 'event []> = async {
        let! version, events = Read.loadBackwards log conn.ReadConnection (defaultArg limit Int32.MaxValue) streamName (tryDecode, isOrigin)
        match Array.tryHead events |> Option.filter (function _, Some e -> isOrigin e | _ -> false) with
        | None -> return Token.ofUncompactedVersion batching.BatchSize version, Array.choose snd events
        | Some (resolvedEvent, _) -> return Token.ofCompactionResolvedEventAndVersion resolvedEvent batching.BatchSize version, Array.choose snd events }

    member _.LoadFromToken(useWriteConn, streamName, log, (Token.Unpack token as streamToken), tryDecode, isCompactionEventType)
        : Async<StreamToken * 'event[]> = async {
        let streamPosition = StreamPosition.FromInt64(token.streamVersion + 1L)
        let connToUse = if useWriteConn then conn.WriteConnection else conn.ReadConnection
        let! version, events = Read.loadForwards log connToUse streamName streamPosition
        match isCompactionEventType with
        | None -> return Token.ofNonCompacting version, Array.choose tryDecode events
        | Some isCompactionEvent ->
            match events |> Array.tryFindBack (fun re -> match tryDecode re with Some e -> isCompactionEvent e | _ -> false) with
            | None -> return Token.ofPreviousTokenAndEventsLength streamToken events.Length batching.BatchSize version, Array.choose tryDecode events
            | Some resolvedEvent -> return Token.ofCompactionResolvedEventAndVersion resolvedEvent batching.BatchSize version, Array.choose tryDecode events }

    member _.TrySync(log, streamName, (Token.Unpack token as streamToken), events, encodedEvents : EventData array, isCompactionEventType): Async<GatewaySyncResult> = async {
        let streamVersion = token.streamVersion
        let! wr = Write.writeEvents log conn.WriteRetryPolicy conn.WriteConnection streamName streamVersion encodedEvents
        match wr with
        | EsSyncResult.Conflict actualVersion ->
            return GatewaySyncResult.ConflictUnknown (Token.ofNonCompacting actualVersion)
        | EsSyncResult.Written wr ->
            let version' = wr.NextExpectedVersion
            let token =
                match isCompactionEventType with
                | None -> Token.ofNonCompacting version'
                | Some isCompactionEvent ->
                    match events |> Array.ofList |> Array.tryFindIndexBack isCompactionEvent with
                    | None -> Token.ofPreviousTokenAndEventsLength streamToken encodedEvents.Length batching.BatchSize version'
                    | Some compactionEventIndex ->
                        Token.ofPreviousStreamVersionAndCompactionEventDataIndex streamToken compactionEventIndex encodedEvents.Length batching.BatchSize version'
            return GatewaySyncResult.Written token }

    member _.Sync(log, streamName, streamVersion, events : FsCodec.IEventData<byte[]>[]) : Async<GatewaySyncResult> = async {
        let encodedEvents : EventData[] = events |> Array.map UnionEncoderAdapters.eventDataOfEncodedEvent
        let! wr = Write.writeEvents log conn.WriteRetryPolicy conn.WriteConnection streamName streamVersion encodedEvents
        match wr with
        | EsSyncResult.Conflict actualVersion ->
            return GatewaySyncResult.ConflictUnknown (Token.ofNonCompacting actualVersion)
        | EsSyncResult.Written wr ->
            let version' = wr.NextExpectedVersion
            let token = Token.ofNonCompacting version'
            return GatewaySyncResult.Written token }

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
    | RollingSnapshots of isOrigin : ('event -> bool) * toSnapshot : ('state -> 'event)

type private CompactionContext(eventsLen : int, capacityBeforeCompaction : int) =
    /// Determines whether writing a Compaction event is warranted (based on the existing state and the current accumulated changes)
    member _.IsCompactionDue = eventsLen > capacityBeforeCompaction

type private Category<'event, 'state, 'context>(context : EventStoreContext, codec : FsCodec.IEventCodec<_, _, 'context>, ?access : AccessStrategy<'event, 'state>) =
    let tryDecode (e : ResolvedEvent) = e |> UnionEncoderAdapters.encodedEventOfResolvedEvent |> codec.TryDecode

    let compactionPredicate =
        match access with
        | None -> None
        | Some AccessStrategy.LatestKnownEvent -> Some (fun _ -> true)
        | Some (AccessStrategy.RollingSnapshots (isValid, _)) -> Some isValid

    let isOrigin =
        match access with
        | None | Some AccessStrategy.LatestKnownEvent -> fun _ -> true
        | Some (AccessStrategy.RollingSnapshots (isValid, _)) -> isValid

    let loadAlgorithm load streamName initial log =
        let batched = load initial (context.LoadBatched(streamName, log, tryDecode, None))
        let compacted limit = load initial (context.LoadBackwardsStoppingAtCompactionEvent(streamName, log, limit, (tryDecode, isOrigin)))
        match access with
        | None -> batched
        | Some AccessStrategy.LatestKnownEvent -> compacted (Some 1)
        | Some (AccessStrategy.RollingSnapshots _) -> compacted None

    let load (fold : 'state -> 'event seq -> 'state) initial f = async {
        let! token, events = f
        return token, fold initial events }

    member _.Load(fold : 'state -> 'event seq -> 'state, initial : 'state, streamName : string, log : ILogger) : Async<StreamToken * 'state> =
        loadAlgorithm (load fold) streamName initial log

    member _.LoadFromToken(fold : 'state -> 'event seq -> 'state, state : 'state, streamName : string, token, log : ILogger) : Async<StreamToken * 'state> =
        (load fold) state (context.LoadFromToken(false, streamName, log, token, tryDecode, compactionPredicate))

    member _.TrySync<'context>
        (   log : ILogger, fold : 'state -> 'event seq -> 'state,
            streamName, (Token.Unpack token as streamToken), state : 'state, events : 'event list, ctx : 'context option) : Async<SyncResult<'state>> = async {
        let encode e = codec.Encode(ctx, e)
        let events =
            match access with
            | None | Some AccessStrategy.LatestKnownEvent -> events
            | Some (AccessStrategy.RollingSnapshots (_, compact)) ->
                let cc = CompactionContext(List.length events, token.batchCapacityLimit.Value)
                if cc.IsCompactionDue then events @ [fold state events |> compact] else events

        let encodedEvents : EventData[] = events |> Seq.map (encode >> UnionEncoderAdapters.eventDataOfEncodedEvent) |> Array.ofSeq
        let! syncRes = context.TrySync(log, streamName, streamToken, events, encodedEvents, compactionPredicate)
        match syncRes with
        | GatewaySyncResult.ConflictUnknown _ ->
            return SyncResult.Conflict  (load fold state (context.LoadFromToken(true, streamName, log, streamToken, tryDecode, compactionPredicate)))
        | GatewaySyncResult.Written token' ->
            return SyncResult.Written   (token', fold state (Seq.ofList events)) }

type private Folder<'event, 'state, 'context>(category : Category<'event, 'state, 'context>, fold : 'state -> 'event seq -> 'state, initial : 'state, ?readCache) =
    let batched log streamName = category.Load(fold, initial, streamName, log)
    interface ICategory<'event, 'state, string, 'context> with
        member _.Load(log, streamName, allowStale) : Async<StreamToken * 'state> =
            match readCache with
            | None -> batched log streamName
            | Some (cache : ICache, prefix : string) -> async {
                match! cache.TryGet(prefix + streamName) with
                | None -> return! batched log streamName
                | Some tokenAndState when allowStale -> return tokenAndState
                | Some (token, state) -> return! category.LoadFromToken(fold, state, streamName, token, log) }

        member _.TrySync(log : ILogger, streamName, token, initialState, events : 'event list, context) : Async<SyncResult<'state>> = async {
            let! syncRes = category.TrySync(log, fold, streamName, token, initialState, events, context)
            match syncRes with
            | SyncResult.Conflict resync ->         return SyncResult.Conflict resync
            | SyncResult.Written (token', state') -> return SyncResult.Written (token', state') }

/// For EventStoreDB, caching is less critical than it is for e.g. CosmosDB
/// As such, it can often be omitted, particularly if streams are short or there are snapshots being maintained
[<NoComparison; NoEquality; RequireQualifiedAccess>]
type CachingStrategy =
    /// Retain a single 'state per streamName.
    /// Each cache hit for a stream renews the retention period for the defined <c>window</c>.
    /// Upon expiration of the defined <c>window</c> from the point at which the cache was entry was last used, a full reload is triggered.
    /// Unless <c>LoadOption.AllowStale</c> is used, each cache hit still incurs a roundtrip to load any subsequently-added events.
    | SlidingWindow of ICache * window : TimeSpan
    /// Retain a single 'state per streamName.
    /// Upon expiration of the defined <c>period</c>, a full reload is triggered.
    /// Unless <c>LoadOption.AllowStale</c> is used, each cache hit still incurs a roundtrip to load any subsequently-added events.
    | FixedTimeSpan of ICache * period : TimeSpan
    /// Prefix is used to segregate multiple folds per stream when they are stored in the cache.
    /// Semantics are identical to <c>SlidingWindow</c>.
    | SlidingWindowPrefixed of ICache * window : TimeSpan * prefix : string

type EventStoreCategory<'event, 'state, 'context>
    (   context : EventStoreContext, codec : FsCodec.IEventCodec<_, _, 'context>, fold, initial,
        // Caching can be overkill for EventStore esp considering the degree to which its intrinsic caching is a first class feature
        // e.g., A key benefit is that reads of streams more than a few pages long get completed in constant time after the initial load
        [<O; D(null)>] ?caching,
        [<O; D(null)>] ?access) =

    do  match access with
        | Some AccessStrategy.LatestKnownEvent when Option.isSome caching ->
            "Equinox.EventStoreDb does not support (and it would make things _less_ efficient even if it did)"
            + "mixing AccessStrategy.LatestKnownEvent with Caching at present."
            |> invalidOp
        | _ -> ()
    let inner = Category<'event, 'state, 'context>(context, codec, ?access = access)
    let readCacheOption =
        match caching with
        | None -> None
        | Some (CachingStrategy.SlidingWindow (cache, _))
        | Some (CachingStrategy.FixedTimeSpan (cache, _)) -> Some (cache, null)
        | Some (CachingStrategy.SlidingWindowPrefixed (cache, _, prefix)) -> Some (cache, prefix)
    let folder = Folder<'event, 'state, 'context>(inner, fold, initial, ?readCache = readCacheOption)
    let category : ICategory<_, _, _, 'context> =
        match caching with
        | None -> folder :> _
        | Some (CachingStrategy.SlidingWindow (cache, window)) ->
            Caching.applyCacheUpdatesWithSlidingExpiration cache null window folder Token.supersedes
        | Some (CachingStrategy.FixedTimeSpan (cache, period)) ->
            Caching.applyCacheUpdatesWithFixedTimeSpan cache null period folder Token.supersedes
        | Some (CachingStrategy.SlidingWindowPrefixed (cache, window, prefix)) ->
            Caching.applyCacheUpdatesWithSlidingExpiration cache prefix window folder Token.supersedes
    let resolve streamName = category, FsCodec.StreamName.toString streamName, None
    let empty = context.TokenEmpty, initial
    let storeCategory = StoreCategory(resolve, empty)
    member _.Resolve(streamName : FsCodec.StreamName, [<O; D null>] ?context) = storeCategory.Resolve(streamName, ?context = context)

(* TODO
type private SerilogAdapter(log : ILogger) =
    interface EventStore.ClientAPI.ILogger with
        member _.Debug(format : string, args : obj []) =           log.Debug(format, args)
        member _.Debug(ex : exn, format : string, args : obj []) = log.Debug(ex, format, args)
        member _.Info(format : string, args : obj []) =            log.Information(format, args)
        member _.Info(ex : exn, format : string, args : obj []) =  log.Information(ex, format, args)
        member _.Error(format : string, args : obj []) =           log.Error(format, args)
        member _.Error(ex : exn, format : string, args : obj []) = log.Error(ex, format, args)

[<RequireQualifiedAccess; NoComparison; NoEquality>]
type Logger =
    | SerilogVerbose of ILogger
    | SerilogNormal of ILogger
    | CustomVerbose of EventStore.ClientAPI.ILogger
    | CustomNormal of EventStore.ClientAPI.ILogger
    member log.Configure(b : ConnectionSettingsBuilder) =
        match log with
        | SerilogVerbose logger -> b.EnableVerboseLogging().UseCustomLogger(SerilogAdapter(logger))
        | SerilogNormal logger -> b.UseCustomLogger(SerilogAdapter(logger))
        | CustomVerbose logger -> b.EnableVerboseLogging().UseCustomLogger(logger)
        | CustomNormal logger -> b.UseCustomLogger(logger)
*)

[<RequireQualifiedAccess; NoComparison>]
type Discovery =
    // Allow Uri-based connection definition (esdb://, etc)
    | ConnectionString of string
(* TODO
    /// Supply a set of pre-resolved EndPoints instead of letting Gossip resolution derive from the DNS outcome
    | GossipSeeded of seedManagerEndpoints : System.Net.IPEndPoint []
    // Standard Gossip-based discovery based on Dns query and standard manager port
    | GossipDns of clusterDns : string
    // Standard Gossip-based discovery based on Dns query (with manager port overriding default 2113)
    | GossipDnsCustomPort of clusterDns : string * managerPortOverride : int

module private Discovery =
    let buildDns np (f : DnsClusterSettingsBuilder -> DnsClusterSettingsBuilder) =
        ClusterSettings.Create().DiscoverClusterViaDns().KeepDiscovering()
        |> fun s -> match np with NodePreference.Random -> s.PreferRandomNode() | NodePreference.PreferSlave -> s.PreferSlaveNode() | _ -> s
        |> f |> fun s -> s.Build()

    let buildSeeded np (f : GossipSeedClusterSettingsBuilder -> GossipSeedClusterSettingsBuilder) =
        ClusterSettings.Create().DiscoverClusterViaGossipSeeds().KeepDiscovering()
        |> fun s -> match np with NodePreference.Random -> s.PreferRandomNode() | NodePreference.PreferSlave -> s.PreferSlaveNode() | _ -> s
        |> f |> fun s -> s.Build()

    let configureDns clusterDns maybeManagerPort (x : DnsClusterSettingsBuilder) =
        x.SetClusterDns(clusterDns)
        |> fun s -> match maybeManagerPort with Some port -> s.SetClusterGossipPort(port) | None -> s

    let inline configureSeeded (seedEndpoints : System.Net.IPEndPoint []) (x : GossipSeedClusterSettingsBuilder) =
        x.SetGossipSeedEndPoints(seedEndpoints)

    // converts a Discovery mode to a ClusterSettings or a Uri as appropriate
    let (|DiscoverViaUri|DiscoverViaGossip|) : Discovery * NodePreference -> Choice<Uri,ClusterSettings> = function
        | (Discovery.Uri uri), _ ->                         DiscoverViaUri    uri
        | (Discovery.GossipSeeded seedEndpoints), np ->     DiscoverViaGossip (buildSeeded np   (configureSeeded seedEndpoints))
        | (Discovery.GossipDns clusterDns), np ->           DiscoverViaGossip (buildDns np      (configureDns clusterDns None))
        | (Discovery.GossipDnsCustomPort (dns, port)), np ->DiscoverViaGossip (buildDns np      (configureDns dns (Some port)))
*)

// see https://github.com/EventStore/EventStore/issues/1652
[<RequireQualifiedAccess; NoComparison>]
type ConnectionStrategy =
    /// Pair of master and slave connections, writes direct, often can't read writes, resync without backoff (kind to master, writes+resyncs optimal)
    | ClusterTwinPreferSlaveReads
    /// Single connection, with resync backoffs appropriate to the NodePreference
    | ClusterSingle of NodePreference

type EventStoreConnector
    (   reqTimeout : TimeSpan, reqRetries : int,
        ?readRetryPolicy, ?writeRetryPolicy, ?tags,
        ?customize : EventStoreClientSettings -> unit) =
(* TODO port
    let connSettings node =
      ConnectionSettings.Create().SetDefaultUserCredentials(SystemData.UserCredentials(username, password))
        .KeepReconnecting() // ES default: .LimitReconnectionsTo(10)
        .SetQueueTimeoutTo(reqTimeout) // ES default: Zero/unlimited
        .FailOnNoServerResponse() // ES default: DoNotFailOnNoServerResponse() => wait forever; retry and/or log
        .SetOperationTimeoutTo(reqTimeout) // ES default: 7s
        .LimitRetriesForOperationTo(reqRetries) // ES default: 10
        |> fun s ->
            match node with
            | NodePreference.Master         -> s.PerformOnMasterOnly()                  // explicitly use ES default of requiring master, use default Node preference of Master
            | NodePreference.PreferMaster   -> s.PerformOnAnyNode()                     // override default [implied] PerformOnMasterOnly(), use default Node preference of Master
            // NB .PreferSlaveNode/.PreferRandomNode setting is ignored if using EventStoreConneciton.Create(ConnectionSettings, ClusterSettings) overload but
            // this code is necessary for cases where people are using the discover :// and related URI schemes
            | NodePreference.PreferSlave    -> s.PerformOnAnyNode().PreferSlaveNode()   // override default PerformOnMasterOnly(), override Master Node preference
            | NodePreference.Random         -> s.PerformOnAnyNode().PreferRandomNode()  // override default PerformOnMasterOnly(), override Master Node preference
        |> fun s -> match concurrentOperationsLimit with Some col -> s.LimitConcurrentOperationsTo(col) | None -> s // ES default: 5000
        |> fun s -> match heartbeatTimeout with Some v -> s.SetHeartbeatTimeout v | None -> s // default: 1500 ms
        |> fun s -> match gossipTimeout with Some v -> s.SetGossipTimeout v | None -> s // default: 1000 ms
        |> fun s -> match clientConnectionTimeout with Some v -> s.WithConnectionTimeoutOf v | None -> s // default: 1000 ms
        |> fun s -> match log with Some log -> log.Configure s | None -> s
        |> fun s -> s.Build()
*)
    member _.Connect
        (   // Name should be sufficient to uniquely identify this connection within a single app instance's logs
            name, discovery : Discovery, ?clusterNodePreference) : EventStoreClient =
        let settings =
            match discovery with
            | Discovery.ConnectionString s -> EventStoreClientSettings.Create(s)
(* TODO
            | Discovery.DiscoverViaGossip clusterSettings ->
                // NB This overload's implementation ignores the calls to ConnectionSettingsBuilder.PreferSlaveNode/.PreferRandomNode and
                // requires equivalent ones on the GossipSeedClusterSettingsBuilder or ClusterSettingsBuilder
                EventStoreConnection.Create(connSettings clusterNodePreference, clusterSettings, sanitizedName) *)
        if name = null then nullArg "name"
        let name = String.concat ";" <| seq {
            name
            string clusterNodePreference
            match tags with None -> () | Some tags -> for key, value in tags do sprintf "%s=%s" key value }
        let sanitizedName = name.Replace('\'','_').Replace(':','_') // ES internally uses `:` and `'` as separators in log messages and ... people regex logs
        settings.ConnectionName <- sanitizedName
        match clusterNodePreference with None -> () | Some np -> settings.ConnectivitySettings.NodePreference <- np
        match customize with None -> () | Some f -> f settings
        settings.DefaultDeadline <- reqTimeout
        // TODO implement reqRetries
        new EventStoreClient(settings)

    /// Yields a Connection (which may internally be twin connections) configured per the specified strategy
    member x.Establish
        (   // Name should be sufficient to uniquely identify this (aggregate) connection within a single app instance's logs
            name,
            discovery : Discovery, strategy : ConnectionStrategy) : EventStoreConnection =
        match strategy with
        | ConnectionStrategy.ClusterSingle nodePreference ->
            let client = x.Connect(name, discovery, nodePreference)
            EventStoreConnection(client, ?readRetryPolicy = readRetryPolicy, ?writeRetryPolicy = writeRetryPolicy)
        | ConnectionStrategy.ClusterTwinPreferSlaveReads ->
            let leader = x.Connect(name + "-TwinW", discovery, NodePreference.Leader)
            let follower = x.Connect(name + "-TwinR", discovery, NodePreference.Follower)
            EventStoreConnection(readConnection = follower, writeConnection = leader, ?readRetryPolicy = readRetryPolicy, ?writeRetryPolicy = writeRetryPolicy)
