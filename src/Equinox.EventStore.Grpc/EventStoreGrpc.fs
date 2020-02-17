namespace Equinox.EventStore

open Equinox
open Equinox.Core
open EventStore.Client
open Serilog
open System

module Log =
    [<NoEquality; NoComparison>]
    type Measurement = { stream: string; interval: StopwatchInterval; bytes: int; count: int }

    [<NoEquality; NoComparison>]
    type Event =
        | WriteSuccess of Measurement
        | WriteConflict of Measurement
        | Slice of Direction * Measurement
        | Batch of Direction * slices: int * Measurement

    let prop name value (log : ILogger) = log.ForContext(name, value)

    let propEvents name (kvps : System.Collections.Generic.KeyValuePair<string,string> seq) (log : ILogger) =
        let items = seq { for kv in kvps do yield sprintf "{\"%s\": %s}" kv.Key kv.Value }
        log.ForContext(name, sprintf "[%s]" (String.concat ",\n\r" items))

    let propEventData name (events : EventData[]) (log : ILogger) =
        log |> propEvents name (seq {
            for x in events do
                if x.IsJson then
                    yield System.Collections.Generic.KeyValuePair<_,_>(x.Type, System.Text.Encoding.UTF8.GetString x.Data) })

    let propResolvedEvents name (events : ResolvedEvent[]) (log : ILogger) =
        log |> propEvents name (seq {
            for x in events do
                let e = x.Event
                if e.IsJson then
                    yield System.Collections.Generic.KeyValuePair<_,_>(e.EventType, System.Text.Encoding.UTF8.GetString e.Data) })

    open Serilog.Events

    /// Attach a property to the log context to hold the metrics
    // Sidestep Log.ForContext converting to a string; see https://github.com/serilog/serilog/issues/1124
    let event (value : Event) (log : ILogger) =
        let enrich (e : LogEvent) = e.AddPropertyIfAbsent(LogEventProperty("esEvt", ScalarValue(value)))
        log.ForContext({ new Serilog.Core.ILogEventEnricher with member __.Enrich(evt,_) = enrich evt })

    let withLoggedRetries<'t> retryPolicy (contextLabel : string) (f : ILogger -> Async<'t>) log : Async<'t> =
        match retryPolicy with
        | None -> f log
        | Some retryPolicy ->
            let withLoggingContextWrapping count =
                let log = if count = 1 then log else log |> prop contextLabel count
                f log
            retryPolicy withLoggingContextWrapping

    let (|BlobLen|) = function null -> 0 | (x : byte[]) -> x.Length

    /// NB Caveat emptor; this is subject to unlimited change without the major version changing - while the `dotnet-templates` repo will be kept in step, and
    /// the ChangeLog will mention changes, it's critical to not assume that the presence or nature of these helpers be considered stable
    module InternalMetrics =

        module Stats =
            let inline (|Stats|) ({ interval = i } : Measurement) = let e = i.Elapsed in int64 e.TotalMilliseconds

            let (|Read|Write|Resync|Rollup|) = function
                | Slice (_, (Stats s)) -> Read s
                | WriteSuccess (Stats s) -> Write s
                | WriteConflict (Stats s) -> Resync s
                // slices are rolled up into batches so be sure not to double-count
                | Batch (_, _, (Stats s)) -> Rollup s

            let (|SerilogScalar|_|) : LogEventPropertyValue -> obj option = function
                | (:? ScalarValue as x) -> Some x.Value
                | _ -> None

            let (|EsMetric|_|) (logEvent : LogEvent) : Event option =
                match logEvent.Properties.TryGetValue("esEvt") with
                | true, SerilogScalar (:? Event as e) -> Some e
                | _ -> None

            type Counter =
                { mutable count : int64; mutable ms : int64 }
                static member Create() = { count = 0L; ms = 0L }
                member __.Ingest(ms) =
                    System.Threading.Interlocked.Increment(&__.count) |> ignore
                    System.Threading.Interlocked.Add(&__.ms, ms) |> ignore

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
                    member __.Emit logEvent = logEvent |> function
                        | EsMetric (Read stats) -> LogSink.Read.Ingest stats
                        | EsMetric (Write stats) -> LogSink.Write.Ingest stats
                        | EsMetric (Resync stats) -> LogSink.Resync.Ingest stats
                        | EsMetric (Rollup _) -> ()
                        | _ -> ()

        /// Relies on feeding of metrics from Log through to Stats.LogSink
        /// Use Stats.LogSink.Restart() to reset the start point (and stats) where relevant
        let dump (log : Serilog.ILogger) =
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
type EsSyncResult = Written of EventStore.Client.WriteResult | Conflict of actualVersion: int64

module private Write =
    /// Yields `EsSyncResult.Written` or `EsSyncResult.Conflict` to signify WrongExpectedVersion
    let private writeEventsAsync (log : ILogger) (conn : EventStoreClient) (streamName : string) (version : int64) (events : EventData[])
        : Async<EsSyncResult> = async {
        try let! ct = Async.CancellationToken
            let! wr = conn.AppendToStreamAsync(streamName, StreamRevision.FromInt64 version, events, cancellationToken=ct) |> Async.AwaitTaskCorrect
            return EsSyncResult.Written wr
        with :? EventStore.Client.WrongExpectedVersionException as ex ->
            log.Information(ex, "Ges TrySync WrongExpectedVersionException writing {EventTypes}, actual {ActualVersion}",
                [| for x in events -> x.Type |], ex.ActualVersion)
            return EsSyncResult.Conflict (let v = ex.ActualVersion in v.Value) }

    let eventDataBytes events =
        let eventDataLen (x : EventData) = match x.Data, x.Metadata with Log.BlobLen bytes, Log.BlobLen metaBytes -> bytes + metaBytes
        events |> Array.sumBy eventDataLen

    let private writeEventsLogged (conn : EventStoreClient) (streamName : string) (version : int64) (events : EventData[]) (log : ILogger)
        : Async<EsSyncResult> = async {
        let log = if (not << log.IsEnabled) Events.LogEventLevel.Debug then log else log |> Log.propEventData "Json" events
        let bytes, count = eventDataBytes events, events.Length
        let log = log |> Log.prop "bytes" bytes
        let writeLog = log |> Log.prop "stream" streamName |> Log.prop "expectedVersion" version |> Log.prop "count" count
        let! t, result = writeEventsAsync writeLog conn streamName version events |> Stopwatch.Time
        let reqMetric : Log.Measurement = { stream = streamName; interval = t; bytes = bytes; count = count}
        let resultLog, evt =
            match result, reqMetric with
            | EsSyncResult.Conflict actualVersion, m ->
                log |> Log.prop "actualVersion" actualVersion, Log.WriteConflict m
            | EsSyncResult.Written x, m ->
                log |> Log.prop "nextExpectedVersion" x.NextExpectedVersion |> Log.prop "logPosition" x.LogPosition, Log.WriteSuccess m
        (resultLog |> Log.event evt).Information("Ges{action:l} count={count} conflict={conflict}",
            "Write", events.Length, match evt with Log.WriteConflict _ -> true | _ -> false)
        return result }

    let writeEvents (log : ILogger) retryPolicy (conn : EventStoreClient) (streamName : string) (version : int64) (events : EventData[])
        : Async<EsSyncResult> =
        let call = writeEventsLogged conn streamName version events
        Log.withLoggedRetries retryPolicy "writeAttempt" call log

module private Read =
    open FSharp.Control

    let private readAsyncEnum (conn : EventStoreClient) (streamName : string) (direction : Direction) (batchSize : int) (startPos : StreamRevision)
        : Async<AsyncSeq<ResolvedEvent>> = async {
        let! ct = Async.CancellationToken
        return conn.ReadStreamAsync(direction, streamName, startPos, uint64 batchSize, resolveLinkTos = false, cancellationToken = ct)
            |> AsyncSeq.ofAsyncEnum }

    let readAsyncEnumVer (conn : EventStoreClient) (streamName : string) (direction : Direction) (batchSize : int) (startPos : StreamRevision) : Async<int64*ResolvedEvent[]> = async {
        try let! res = readAsyncEnum conn streamName direction batchSize startPos
            let! events = res |> AsyncSeq.toArrayAsync
            let version =
                match events with
                | [||] when direction = Direction.Backwards -> -1L // When reading backwards, the startPos is End, which is not directly convertible
                | [||] -> startPos.ToInt64()
                | xs when direction = Direction.Backwards -> xs.[0].Event.EventNumber.ToInt64()
                | xs -> xs.[xs.Length - 1].Event.EventNumber.ToInt64()
            return version, events
        with :? StreamNotFoundException -> return -1L, [||] }

    let (|ResolvedEventLen|) (x : ResolvedEvent) = match x.Event.Data, x.Event.Metadata with Log.BlobLen bytes, Log.BlobLen metaBytes -> bytes + metaBytes

    let private loggedReadAsyncEnumVer conn streamName direction batchSize startPos (log : ILogger) : Async<int64 * ResolvedEvent[]> = async {
        let! t, (version, events) = readAsyncEnumVer conn streamName direction batchSize startPos |> Stopwatch.Time
        let bytes, count = events |> Array.sumBy (|ResolvedEventLen|), events.Length
        let reqMetric : Log.Measurement = { stream = streamName; interval = t; bytes = bytes; count = count}
        let evt = Log.Slice (direction, reqMetric)
        let log = if (not << log.IsEnabled) Events.LogEventLevel.Debug then log else log |> Log.propResolvedEvents "Json" events
        (log |> Log.prop "startPos" startPos |> Log.prop "bytes" bytes |> Log.event evt).Information("Ges{action:l} count={count} version={version}",
            "Read", count, version)
        return version, events }

    let resolvedEventBytes events = events |> Array.sumBy (|ResolvedEventLen|)

    let logBatchRead direction streamName t events batchSize version (log : ILogger) =
        let bytes, count = resolvedEventBytes events, events.Length
        let reqMetric : Log.Measurement = { stream = streamName; interval = t; bytes = bytes; count = count}
        let batches = (events.Length - 1) / batchSize + 1
        let action = match direction with Direction.Forwards -> "LoadF" | Direction.Backwards -> "LoadB"
        let evt = Log.Event.Batch (direction, batches, reqMetric)
        (log |> Log.prop "bytes" bytes |> Log.event evt).Information(
            "Ges{action:l} stream={stream} count={count}/{batches} version={version}",
            action, streamName, count, batches, version)

    let loadForwardsFrom (log : ILogger) retryPolicy conn batchSize streamName startPosition
        : Async<int64 * ResolvedEvent[]> = async {
        let call pos = loggedReadAsyncEnumVer conn streamName Direction.Forwards batchSize pos
        let retryingLoggingRead pos = Log.withLoggedRetries retryPolicy "readAttempt" (call pos)
        let direction = Direction.Forwards
        let log = log |> Log.prop "batchSize" batchSize |> Log.prop "direction" direction |> Log.prop "stream" streamName
        let! t, (version, events) = retryingLoggingRead startPosition log |> Stopwatch.Time
        log |> logBatchRead direction streamName t events batchSize version
        return version, events }

    let takeWhileInclusive (predicate: 'T -> bool) (source: 'T seq) = seq {
        let enumerator = source.GetEnumerator()
        let mutable fin = false
        while not fin && enumerator.MoveNext() do
            yield enumerator.Current
            if not (predicate enumerator.Current) then
                fin <- true }
    let mergeFromCompactionPointOrStartFromBackwardsStream streamName (tryDecode, isOrigin) (log : ILogger) (itemsBackward : ResolvedEvent[])
        : (ResolvedEvent * 'event option)[] =
            itemsBackward
            |> Seq.map (fun x -> x, tryDecode x)
            |> takeWhileInclusive (function
                | x, Some e when isOrigin e ->
                    log.Information("GesStop stream={stream} at={eventNumber}", streamName, x.Event.EventNumber)
                    false
                | _ -> true) // continue the search
            |> Seq.rev
            |> Seq.toArray
    let loadBackwardsUntilCompactionOrStart (log : ILogger) retryPolicy conn batchSize streamName (tryDecode, isOrigin)
        : Async<int64 * (ResolvedEvent * 'event option)[]> = async {
        let call pos = loggedReadAsyncEnumVer conn streamName Direction.Backwards batchSize pos
        let retryingLoggingRead pos = Log.withLoggedRetries retryPolicy "readAttempt" (call pos)
        let log = log |> Log.prop "batchSize" batchSize |> Log.prop "stream" streamName
        let startPosition = StreamRevision.End
        let direction = Direction.Backwards
        let readLog = log |> Log.prop "direction" direction
        let! t, (version, eventsBackward) = retryingLoggingRead startPosition readLog |> Stopwatch.Time
        let events = mergeFromCompactionPointOrStartFromBackwardsStream streamName (tryDecode, isOrigin) log eventsBackward
        log |> logBatchRead direction streamName t (Array.map fst events) batchSize version
        return version, events }

module UnionEncoderAdapters =
    let encodedEventOfResolvedEvent (x : ResolvedEvent) : FsCodec.ITimelineEvent<byte[]> =
        let e = x.Event
        // Inspecting server code shows both Created and CreatedEpoch are set; taking this as it's less ambiguous than DateTime in the general case
        let ts = DateTimeOffset(e.Created)
        // TODO something like let ts = DateTimeOffset.FromUnixTimeMilliseconds(e.CreatedEpoch)
        // TOCONSIDER wire e.Metadata.["$correlationId"] and .["$causationId"] into correlationId and causationId
        // https://eventstore.org/docs/server/metadata-and-reserved-names/index.html#event-metadata
        FsCodec.Core.TimelineEvent.Create(e.EventNumber.ToInt64(), e.EventType, e.Data, e.Metadata, correlationId = null, causationId = null, timestamp = ts)

    let eventDataOfEncodedEvent (x : FsCodec.IEventData<byte[]>) =
        // TOCONSIDER wire x.CorrelationId, x.CausationId into x.Meta.["$correlationId"] and .["$causationId"]
        // https://eventstore.org/docs/server/metadata-and-reserved-names/index.html#event-metadata
        EventData(Uuid.NewUuid(), x.EventType, isJson = true, data = x.Data, metadata = x.Meta)

type Stream = { name : string }
type Position = { streamVersion : int64; compactionEventNumber : int64 option; batchCapacityLimit : int option }
type Token = { stream : Stream; pos : Position }

module Token =
    let private create compactionEventNumber batchCapacityLimit streamName streamVersion : StreamToken =
        {   value = box {
                stream = { name = streamName}
                pos = { streamVersion = streamVersion; compactionEventNumber = compactionEventNumber; batchCapacityLimit = batchCapacityLimit } }
            version = streamVersion }

    /// No batching / compaction; we only need to retain the StreamVersion
    let ofNonCompacting streamName streamVersion : StreamToken =
        create None None streamName streamVersion

    // headroom before compaction is necessary given the stated knowledge of the last (if known) `compactionEventNumberOption`
    let private batchCapacityLimit compactedEventNumberOption unstoredEventsPending (batchSize : int) (streamVersion : int64) : int =
        match compactedEventNumberOption with
        | Some (compactionEventNumber : int64) -> (batchSize - unstoredEventsPending) - int (streamVersion - compactionEventNumber + 1L) |> max 0
        | None -> (batchSize - unstoredEventsPending) - (int streamVersion + 1) - 1 |> max 0

    let (*private*) ofCompactionEventNumber compactedEventNumberOption unstoredEventsPending batchSize streamName streamVersion : StreamToken =
        let batchCapacityLimit = batchCapacityLimit compactedEventNumberOption unstoredEventsPending batchSize streamVersion
        create compactedEventNumberOption (Some batchCapacityLimit) streamName streamVersion

    /// Assume we have not seen any compaction events; use the batchSize and version to infer headroom
    let ofUncompactedVersion batchSize streamName streamVersion : StreamToken =
        ofCompactionEventNumber None 0 batchSize streamName streamVersion

    let (|Unpack|) (x : StreamToken) : Token = unbox<Token> x.value

    /// Use previousToken plus the data we are adding and the position we are adding it to infer a headroom
    let ofPreviousTokenAndEventsLength (Unpack previousToken) eventsLength batchSize streamVersion : StreamToken =
        let compactedEventNumber = previousToken.pos.compactionEventNumber
        ofCompactionEventNumber compactedEventNumber eventsLength batchSize previousToken.stream.name streamVersion

    /// Use an event just read from the stream to infer headroom
    let ofCompactionResolvedEventAndVersion (compactionEvent: ResolvedEvent) batchSize streamName streamVersion : StreamToken =
        ofCompactionEventNumber (Some (compactionEvent.Event.EventNumber.ToInt64())) 0 batchSize streamName streamVersion

    /// Use an event we are about to write to the stream to infer headroom
    let ofPreviousStreamVersionAndCompactionEventDataIndex (Unpack token) compactionEventDataIndex eventsLength batchSize streamVersion' : StreamToken =
        ofCompactionEventNumber (Some (token.pos.streamVersion + 1L + int64 compactionEventDataIndex)) eventsLength batchSize token.stream.name streamVersion'

    let (|StreamPos|) (Unpack token) : Stream * Position = token.stream, token.pos

    let supersedes (Unpack current) (Unpack x) =
        let currentVersion, newVersion = current.pos.streamVersion, x.pos.streamVersion
        newVersion > currentVersion

type Connection(readConnection, [<O; D(null)>] ?writeConnection, [<O; D(null)>] ?readRetryPolicy, [<O; D(null)>] ?writeRetryPolicy) =
    member __.ReadConnection = readConnection
    member __.ReadRetryPolicy = readRetryPolicy
    member __.WriteConnection = defaultArg writeConnection readConnection
    member __.WriteRetryPolicy = writeRetryPolicy

type BatchingPolicy(getMaxBatchSize : unit -> int) =
    new (maxBatchSize) = BatchingPolicy(fun () -> maxBatchSize)
    member __.BatchSize = getMaxBatchSize()

[<RequireQualifiedAccess; NoComparison; NoEquality>]
type GatewaySyncResult = Written of StreamToken | ConflictUnknown of StreamToken

type Context(conn : Connection, batching : BatchingPolicy) =
    let isResolvedEventEventType (tryDecode, predicate) (x : ResolvedEvent) = predicate (tryDecode (x.Event.Data))
    let tryIsResolvedEventEventType predicateOption = predicateOption |> Option.map isResolvedEventEventType

    member internal __.LoadEmpty streamName = Token.ofUncompactedVersion batching.BatchSize streamName -1L

    member __.LoadBatched streamName log (tryDecode, isCompactionEventType) : Async<StreamToken * 'event[]> = async {
        let! version, events = Read.loadForwardsFrom log conn.ReadRetryPolicy conn.ReadConnection batching.BatchSize streamName StreamRevision.Start
        match tryIsResolvedEventEventType isCompactionEventType with
        | None -> return Token.ofNonCompacting streamName version, Array.choose tryDecode events
        | Some isCompactionEvent ->
            match events |> Array.tryFindBack isCompactionEvent with
            | None -> return Token.ofUncompactedVersion batching.BatchSize streamName version, Array.choose tryDecode events
            | Some resolvedEvent -> return Token.ofCompactionResolvedEventAndVersion resolvedEvent batching.BatchSize streamName version, Array.choose tryDecode events }

    member __.LoadBackwardsStoppingAtCompactionEvent streamName log (tryDecode, isOrigin) : Async<StreamToken * 'event []> = async {
        let! version, events =
            Read.loadBackwardsUntilCompactionOrStart log conn.ReadRetryPolicy conn.ReadConnection batching.BatchSize streamName (tryDecode, isOrigin)
        match Array.tryHead events |> Option.filter (function _, Some e -> isOrigin e | _ -> false) with
        | None -> return Token.ofUncompactedVersion batching.BatchSize streamName version, Array.choose snd events
        | Some (resolvedEvent, _) -> return Token.ofCompactionResolvedEventAndVersion resolvedEvent batching.BatchSize streamName version, Array.choose snd events }

    member __.LoadFromToken useWriteConn streamName log (Token.Unpack token as streamToken) (tryDecode, isCompactionEventType)
        : Async<StreamToken * 'event[]> = async {
        let streamPosition = StreamRevision.FromInt64(token.pos.streamVersion + 1L)
        let connToUse = if useWriteConn then conn.WriteConnection else conn.ReadConnection
        let! version, events = Read.loadForwardsFrom log conn.ReadRetryPolicy connToUse batching.BatchSize streamName streamPosition
        match isCompactionEventType with
        | None -> return Token.ofNonCompacting streamName version, Array.choose tryDecode events
        | Some isCompactionEvent ->
            match events |> Array.tryFindBack (fun re -> match tryDecode re with Some e -> isCompactionEvent e | _ -> false) with
            | None -> return Token.ofPreviousTokenAndEventsLength streamToken events.Length batching.BatchSize version, Array.choose tryDecode events
            | Some resolvedEvent -> return Token.ofCompactionResolvedEventAndVersion resolvedEvent batching.BatchSize streamName version, Array.choose tryDecode events }

    member __.TrySync log (Token.Unpack token as streamToken) (events, encodedEvents: EventData array) (isCompactionEventType) : Async<GatewaySyncResult> = async {
        let streamVersion = token.pos.streamVersion
        let! wr = Write.writeEvents log conn.WriteRetryPolicy conn.WriteConnection token.stream.name streamVersion encodedEvents
        match wr with
        | EsSyncResult.Conflict actualVersion ->
            return GatewaySyncResult.ConflictUnknown (Token.ofNonCompacting token.stream.name actualVersion)
        | EsSyncResult.Written wr ->
            let version' = wr.NextExpectedVersion
            let token =
                match isCompactionEventType with
                | None -> Token.ofNonCompacting token.stream.name version'
                | Some isCompactionEvent ->
                    match events |> Array.ofList |> Array.tryFindIndexBack isCompactionEvent with
                    | None -> Token.ofPreviousTokenAndEventsLength streamToken encodedEvents.Length batching.BatchSize version'
                    | Some compactionEventIndex ->
                        Token.ofPreviousStreamVersionAndCompactionEventDataIndex streamToken compactionEventIndex encodedEvents.Length batching.BatchSize version'
            return GatewaySyncResult.Written token }

    member __.Sync(log, streamName, streamVersion, events: FsCodec.IEventData<byte[]>[]) : Async<GatewaySyncResult> = async {
        let encodedEvents : EventData[] = events |> Array.map UnionEncoderAdapters.eventDataOfEncodedEvent
        let! wr = Write.writeEvents log conn.WriteRetryPolicy conn.WriteConnection streamName streamVersion encodedEvents
        match wr with
        | EsSyncResult.Conflict actualVersion ->
            return GatewaySyncResult.ConflictUnknown (Token.ofNonCompacting streamName actualVersion)
        | EsSyncResult.Written wr ->
            let version' = wr.NextExpectedVersion
            let token = Token.ofNonCompacting streamName version'
            return GatewaySyncResult.Written token }

[<NoComparison; NoEquality; RequireQualifiedAccess>]
type AccessStrategy<'event,'state> =
    /// Load only the single most recent event defined in <c>'event`</c> and trust that doing a <c>fold</c> from any such event
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
    member __.IsCompactionDue = eventsLen > capacityBeforeCompaction

type private Category<'event, 'state, 'context>(context : Context, codec : FsCodec.IEventCodec<_, _, 'context>, ?access : AccessStrategy<'event, 'state>) =
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
        let batched = load initial (context.LoadBatched streamName log (tryDecode,None))
        let compacted = load initial (context.LoadBackwardsStoppingAtCompactionEvent streamName log (tryDecode, isOrigin))
        match access with
        | None -> batched
        | Some AccessStrategy.LatestKnownEvent
        | Some (AccessStrategy.RollingSnapshots _) -> compacted

    let load (fold : 'state -> 'event seq -> 'state) initial f = async {
        let! token, events = f
        return token, fold initial events }

    member __.Load (fold : 'state -> 'event seq -> 'state) (initial : 'state) (streamName : string) (log : ILogger) : Async<StreamToken * 'state> =
        loadAlgorithm (load fold) streamName initial log

    member __.LoadFromToken (fold : 'state -> 'event seq -> 'state) (state : 'state) (streamName : string) token (log : ILogger) : Async<StreamToken * 'state> =
        (load fold) state (context.LoadFromToken false streamName log token (tryDecode, compactionPredicate))

    member __.TrySync<'context>
        (   log : ILogger, fold: 'state -> 'event seq -> 'state,
            (Token.StreamPos (stream, pos) as streamToken), state : 'state, events : 'event list, ctx : 'context option) : Async<SyncResult<'state>> = async {
        let encode e = codec.Encode(ctx, e)
        let events =
            match access with
            | None | Some AccessStrategy.LatestKnownEvent -> events
            | Some (AccessStrategy.RollingSnapshots (_, compact)) ->
                let cc = CompactionContext(List.length events, pos.batchCapacityLimit.Value)
                if cc.IsCompactionDue then events @ [fold state events |> compact] else events

        let encodedEvents : EventData[] = events |> Seq.map (encode >> UnionEncoderAdapters.eventDataOfEncodedEvent) |> Array.ofSeq
        let! syncRes = context.TrySync log streamToken (events, encodedEvents) compactionPredicate
        match syncRes with
        | GatewaySyncResult.ConflictUnknown _ ->
            return SyncResult.Conflict  (load fold state (context.LoadFromToken true stream.name log streamToken (tryDecode, compactionPredicate)))
        | GatewaySyncResult.Written token' ->
            return SyncResult.Written   (token', fold state (Seq.ofList events)) }

module Caching =
    /// Forwards all state changes in all streams of an ICategory to a `tee` function
    type CategoryTee<'event, 'state, 'context>(inner: ICategory<'event, 'state, string, 'context>, tee : string -> StreamToken * 'state -> Async<unit>) =
        let intercept streamName tokenAndState = async {
            let! _ = tee streamName tokenAndState
            return tokenAndState }

        let loadAndIntercept load streamName = async {
            let! tokenAndState = load
            return! intercept streamName tokenAndState }

        interface ICategory<'event, 'state, string, 'context> with
            member __.Load(log, streamName : string, opt) : Async<StreamToken * 'state> =
                loadAndIntercept (inner.Load(log, streamName, opt)) streamName

            member __.TrySync(log : ILogger, (Token.StreamPos (stream,_) as token), state, events : 'event list, context) : Async<SyncResult<'state>> = async {
                let! syncRes = inner.TrySync(log, token, state, events, context)
                match syncRes with
                | SyncResult.Conflict resync -> return SyncResult.Conflict (loadAndIntercept resync stream.name)
                | SyncResult.Written (token', state') ->
                    let! intercepted = intercept stream.name (token', state')
                    return SyncResult.Written intercepted }

    let applyCacheUpdatesWithSlidingExpiration
            (cache : ICache)
            (prefix : string)
            (slidingExpiration : TimeSpan)
            (category : ICategory<'event, 'state, string, 'context>)
            : ICategory<'event, 'state, string, 'context> =
        let mkCacheEntry (initialToken : StreamToken, initialState : 'state) = new CacheEntry<'state>(initialToken, initialState, Token.supersedes)
        let options = CacheItemOptions.RelativeExpiration slidingExpiration
        let addOrUpdateSlidingExpirationCacheEntry streamName value = cache.UpdateIfNewer(prefix + streamName, options, mkCacheEntry value)
        CategoryTee<'event, 'state, 'context>(category, addOrUpdateSlidingExpirationCacheEntry) :> _

type private Folder<'event, 'state, 'context>(category : Category<'event, 'state, 'context>, fold: 'state -> 'event seq -> 'state, initial: 'state, ?readCache) =
    let batched log streamName = category.Load fold initial streamName log
    interface ICategory<'event, 'state, string, 'context> with
        member __.Load(log, streamName, opt) : Async<StreamToken * 'state> =
            match readCache with
            | None -> batched log streamName
            | Some (cache : ICache, prefix : string) -> async {
                match! cache.TryGet(prefix + streamName) with
                | None -> return! batched log streamName
                | Some tokenAndState when opt = Some AllowStale -> return tokenAndState
                | Some (token, state) -> return! category.LoadFromToken fold state streamName token log }

        member __.TrySync(log : ILogger, token, initialState, events : 'event list, context) : Async<SyncResult<'state>> = async {
            let! syncRes = category.TrySync(log, fold, token, initialState, events, context)
            match syncRes with
            | SyncResult.Conflict resync ->         return SyncResult.Conflict resync
            | SyncResult.Written (token',state') -> return SyncResult.Written (token',state') }

[<NoComparison; NoEquality; RequireQualifiedAccess>]
type CachingStrategy =
    | SlidingWindow of ICache * window : TimeSpan
    /// Prefix is used to segregate multiple folds per stream when they are stored in the cache
    | SlidingWindowPrefixed of ICache * window : TimeSpan * prefix : string

type Resolver<'event, 'state, 'context>
    (   context : Context, codec : FsCodec.IEventCodec<_, _, 'context>, fold, initial,
        /// Caching can be overkill for EventStore esp considering the degree to which its intrinsic caching is a first class feature
        /// e.g., A key benefit is that reads of streams more than a few pages long get completed in constant time after the initial load
        [<O; D(null)>] ?caching,
        [<O; D(null)>] ?access) =

    do  match access with
        | Some AccessStrategy.LatestKnownEvent when Option.isSome caching ->
            "Equinox.EventStore does not support (and it would make things _less_ efficient even if it did)"
            + "mixing AccessStrategy.LatestKnownEvent with Caching at present."
            |> invalidOp
        | _ -> ()

    let inner = Category<'event, 'state, 'context>(context, codec, ?access = access)
    let readCacheOption =
        match caching with
        | None -> None
        | Some (CachingStrategy.SlidingWindow(cache, _)) -> Some(cache, null)
        | Some (CachingStrategy.SlidingWindowPrefixed(cache, _, prefix)) -> Some(cache, prefix)

    let folder = Folder<'event, 'state, 'context>(inner, fold, initial, ?readCache = readCacheOption)

    let category : ICategory<_, _, _, 'context> =
        match caching with
        | None -> folder :> _
        | Some (CachingStrategy.SlidingWindow(cache, window)) ->
            Caching.applyCacheUpdatesWithSlidingExpiration cache null window folder
        | Some (CachingStrategy.SlidingWindowPrefixed(cache, window, prefix)) ->
            Caching.applyCacheUpdatesWithSlidingExpiration cache prefix window folder

    let resolveStream = Stream.create category
    let loadEmpty sn = context.LoadEmpty sn, initial

    member __.Resolve(streamName : FsCodec.StreamName, [<O; D null>] ?option, [<O; D null>] ?context) =
        match FsCodec.StreamName.toString streamName, option with
        | sn, (None|Some AllowStale) -> resolveStream sn option context
        | sn, Some AssumeEmpty -> Stream.ofMemento (loadEmpty sn) (resolveStream sn option context)

    /// Resolve from a Memento being used in a Continuation [based on position and state typically from Stream.CreateMemento]
    member __.FromMemento(Token.Unpack token as streamToken, state, ?context) =
        Stream.ofMemento (streamToken, state) (resolveStream token.stream.name context None)

#if PREVIEW3 // - there's no logging implemented in V3 as yet, so disabling for now
type private SerilogAdapter(log : ILogger) =
    interface EventStore.ClientAPI.ILogger with
        member __.Debug(format : string, args : obj []) =           log.Debug(format, args)
        member __.Debug(ex : exn, format : string, args : obj []) = log.Debug(ex, format, args)
        member __.Info(format : string, args : obj []) =            log.Information(format, args)
        member __.Info(ex : exn, format : string, args : obj []) =  log.Information(ex, format, args)
        member __.Error(format : string, args : obj []) =           log.Error(format, args)
        member __.Error(ex : exn, format : string, args : obj []) = log.Error(ex, format, args)

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
#endif

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
    | GossipSeeded of seedManagerEndpoints : System.Net.IPEndPoint []
    // Standard Gossip-based discovery based on Dns query and standard manager port
    | GossipDns of clusterDns : string
    // Standard Gossip-based discovery based on Dns query (with manager port overriding default 30778)
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

// see https://github.com/EventStore/EventStore/issues/1652
[<RequireQualifiedAccess; NoComparison>]
type ConnectionStrategy =
    /// Pair of master and slave connections, writes direct, often can't read writes, resync without backoff (kind to master, writes+resyncs optimal)
    | ClusterTwinPreferSlaveReads
    /// Single connection, with resync backoffs appropriate to the NodePreference
    | ClusterSingle of NodePreference

type Connector
    (   username, password, reqTimeout: TimeSpan, reqRetries: int,
        [<O; D(null)>] ?log : Logger, [<O; D(null)>] ?heartbeatTimeout: TimeSpan, [<O; D(null)>] ?concurrentOperationsLimit,
        [<O; D(null)>] ?readRetryPolicy, [<O; D(null)>] ?writeRetryPolicy,
        [<O; D(null)>] ?gossipTimeout, [<O; D(null)>] ?clientConnectionTimeout,
        /// Additional strings identifying the context of this connection; should provide enough context to disambiguate all potential connections to a cluster
        /// NB as this will enter server and client logs, it should not contain sensitive information
        [<O; D(null)>] ?tags : (string*string) seq) =
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

    /// Yields an IEventStoreConnection configured and Connect()ed to a node (or the cluster) per the supplied `discovery` and `clusterNodePrefence` preference
    member __.Connect
        (   /// Name should be sufficient to uniquely identify this connection within a single app instance's logs
            name,
            discovery : Discovery, ?clusterNodePreference) : Async<IEventStoreConnection> = async {
        if name = null then nullArg "name"
        let clusterNodePreference = defaultArg clusterNodePreference NodePreference.Master
        let name = String.concat ";" <| seq {
            yield name
            yield string clusterNodePreference
            match tags with None -> () | Some tags -> for key, value in tags do yield sprintf "%s=%s" key value }
        let sanitizedName = name.Replace('\'','_').Replace(':','_') // ES internally uses `:` and `'` as separators in log messages and ... people regex logs
        let conn =
            match discovery, clusterNodePreference with
            | Discovery.DiscoverViaUri uri ->
                // This overload picks up the discovery settings via ConnectionSettingsBuilder.PreferSlaveNode/.PreferRandomNode
                EventStoreConnection.Create(connSettings clusterNodePreference, uri, sanitizedName)
            | Discovery.DiscoverViaGossip clusterSettings ->
                // NB This overload's implementation ignores the calls to ConnectionSettingsBuilder.PreferSlaveNode/.PreferRandomNode and
                // requires equivalent ones on the GossipSeedClusterSettingsBuilder or ClusterSettingsBuilder
                EventStoreConnection.Create(connSettings clusterNodePreference, clusterSettings, sanitizedName)
        do! conn.ConnectAsync() |> Async.AwaitTaskCorrect
        return conn }

    /// Yields a Connection (which may internally be twin connections) configured per the specified strategy
    member __.Establish
        (   /// Name should be sufficient to uniquely identify this (aggregate) connection within a single app instance's logs
            name,
            discovery : Discovery, strategy : ConnectionStrategy) : Async<Connection> = async {
        match strategy with
        | ConnectionStrategy.ClusterSingle nodePreference ->
            let! conn = __.Connect(name, discovery, nodePreference)
            return Connection(conn, ?readRetryPolicy=readRetryPolicy, ?writeRetryPolicy=writeRetryPolicy)
        | ConnectionStrategy.ClusterTwinPreferSlaveReads ->
            let! masterInParallel = Async.StartChild (__.Connect(name + "-TwinW", discovery, NodePreference.Master))
            let! slave = __.Connect(name + "-TwinR", discovery, NodePreference.PreferSlave)
            let! master = masterInParallel
            return Connection(readConnection = slave, writeConnection = master, ?readRetryPolicy = readRetryPolicy, ?writeRetryPolicy = writeRetryPolicy) }
