namespace Equinox.EventStore

open Equinox
open Equinox.Store
open Serilog
open System
open SqlStreamStore
open SqlStreamStore.Streams

[<RequireQualifiedAccess>]
type Direction = Forward | Backward with
    override this.ToString() = match this with Forward -> "Forward" | Backward -> "Backward"

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
    let propEventData name (events : NewStreamMessage[]) (log : ILogger) =
        log |> propEvents name (seq {
            for x in events do
                yield System.Collections.Generic.KeyValuePair<_,_>(x.Type, x.JsonData) })
    let propResolvedEvents name (events : StreamMessage[]) (log : ILogger) =
        log |> propEvents name (seq {
            for x in events do
                let data = x.GetJsonData() |> Async.AwaitTask |> Async.RunSynchronously
                yield System.Collections.Generic.KeyValuePair<_,_>(x.Type, data) })

    open Serilog.Events
    /// Attach a property to the log context to hold the metrics
    // Sidestep Log.ForContext converting to a string; see https://github.com/serilog/serilog/issues/1124
    let event (value : Event) (log : ILogger) =
        let enrich (e : LogEvent) = e.AddPropertyIfAbsent(LogEventProperty("esEvt", ScalarValue(value)))
        log.ForContext({ new Serilog.Core.ILogEventEnricher with member __.Enrich(evt,_) = enrich evt })
    let withLoggedRetries<'t> retryPolicy (contextLabel : string) (f : ILogger -> Async<'t>) log: Async<'t> =
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
            let inline (|Stats|) ({ interval = i }: Measurement) = let e = i.Elapsed in int64 e.TotalMilliseconds

            let (|Read|Write|Resync|Rollup|) = function
                | Slice (_,(Stats s)) -> Read s
                | WriteSuccess (Stats s) -> Write s
                | WriteConflict (Stats s) -> Resync s
                // slices are rolled up into batches so be sure not to double-count
                | Batch (_,_,(Stats s)) -> Rollup s
            let (|SerilogScalar|_|) : LogEventPropertyValue -> obj option = function
                | (:? ScalarValue as x) -> Some x.Value
                | _ -> None
            let (|CosmosMetric|_|) (logEvent : LogEvent) : Event option =
                match logEvent.Properties.TryGetValue("esEvt") with
                | true, SerilogScalar (:? Event as e) -> Some e
                | _ -> None
            type Counter =
                { mutable count: int64; mutable ms: int64 }
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
                        | CosmosMetric (Read stats) -> LogSink.Read.Ingest stats
                        | CosmosMetric (Write stats) -> LogSink.Write.Ingest stats
                        | CosmosMetric (Resync stats) -> LogSink.Resync.Ingest stats
                        | CosmosMetric (Rollup _) -> ()
                        | _ -> ()

        /// Relies on feeding of metrics from Log through to Stats.LogSink
        /// Use Stats.LogSink.Restart() to reset the start point (and stats) where relevant
        let dump (log: Serilog.ILogger) =
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
type EsSyncResult = Written of AppendResult | Conflict of expectedVersion: int64

module private Write =
    /// Yields `EsSyncResult.Written` or `EsSyncResult.Conflict` to signify WrongExpectedVersion
    let private writeEventsAsync (log : ILogger) (conn : IStreamStore) (streamName : string) (version : int) (events : NewStreamMessage[])
        : Async<EsSyncResult> = async {
        try
            let! wr = conn.AppendToStream(StreamId(streamName), version, events) |> Async.AwaitTaskCorrect
            return EsSyncResult.Written wr
        with :? WrongExpectedVersionException as ex ->
            let expectedVersion =
                match ex.ExpectedVersion with
                | value when value.HasValue -> ex.ExpectedVersion.Value
                | _ -> ExpectedVersion.Any

            log.Information(ex, "Es TrySync WrongExpectedVersionException writing {EventTypes}, expected {ExpectedVersion}",
                [| for x in events -> x.Type |], expectedVersion)
            return EsSyncResult.Conflict (expectedVersion |> int64) }
    let eventDataBytes events =
        let eventDataLen (x : NewStreamMessage) = match x.JsonData |> System.Text.Encoding.UTF8.GetBytes, x.JsonMetadata |> System.Text.Encoding.UTF8.GetBytes with Log.BlobLen bytes, Log.BlobLen metaBytes -> bytes + metaBytes
        events |> Array.sumBy eventDataLen
    let private writeEventsLogged (conn : IStreamStore) (streamName : string) (version : int) (events : NewStreamMessage[]) (log : ILogger)
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
                log |> Log.prop "currentVersion" x.CurrentVersion |> Log.prop "currentPosition" x.CurrentPosition, Log.WriteSuccess m
        (resultLog |> Log.event evt).Information("Es{action:l} count={count} conflict={conflict}",
            "Write", events.Length, match evt with Log.WriteConflict _ -> true | _ -> false)
        return result }
    let writeEvents (log : ILogger) retryPolicy (conn : IStreamStore) (streamName : string) (version : int) (events : NewStreamMessage[])
        : Async<EsSyncResult> =
        let call = writeEventsLogged conn streamName version events
        Log.withLoggedRetries retryPolicy "writeAttempt" call log

module private Read =
    open FSharp.Control
    let private readSliceAsync (conn : IStreamStore) (streamName : string) (direction : Direction) (batchSize : int) (startPos : int)
        : Async<ReadStreamPage> = async {
        let call =
            match direction with
            | Direction.Forward ->  conn.ReadStreamForwards(streamName, startPos, batchSize)
            | Direction.Backward -> conn.ReadStreamBackwards(streamName, startPos, batchSize)
        return! call |> Async.AwaitTaskCorrect }
    let (|ResolvedEventLen|) (x : StreamMessage) = 
        let data = x.GetJsonData() |> Async.AwaitTask |> Async.RunSynchronously
        match data |> System.Text.Encoding.UTF8.GetBytes, x.JsonMetadata |> System.Text.Encoding.UTF8.GetBytes with Log.BlobLen bytes, Log.BlobLen metaBytes -> bytes + metaBytes
    let private loggedReadSlice conn streamName direction batchSize startPos (log : ILogger) : Async<ReadStreamPage> = async {
        let! t, slice = readSliceAsync conn streamName direction batchSize startPos |> Stopwatch.Time
        let bytes, count = slice.Messages |> Array.sumBy (|ResolvedEventLen|), slice.Messages.Length
        let reqMetric : Log.Measurement ={ stream = streamName; interval = t; bytes = bytes; count = count}
        let evt = Log.Slice (direction, reqMetric)
        let log = if (not << log.IsEnabled) Events.LogEventLevel.Debug then log else log |> Log.propResolvedEvents "Json" slice.Messages
        (log |> Log.prop "startPos" startPos |> Log.prop "bytes" bytes |> Log.event evt).Information("Es{action:l} count={count} version={version}",
            "Read", count, slice.LastStreamPosition)
        return slice }
    let private readBatches (log : ILogger) (readSlice : int -> ILogger -> Async<ReadStreamPage>)
            (maxPermittedBatchReads : int option) (startPosition : int)
        : AsyncSeq<int64 option * StreamMessage[]> =
        let rec loop batchCount pos : AsyncSeq<int64 option * StreamMessage[]> = asyncSeq {
            match maxPermittedBatchReads with
            | Some mpbr when batchCount >= mpbr -> log.Information "batch Limit exceeded"; invalidOp "batch Limit exceeded"
            | _ -> ()

            let batchLog = log |> Log.prop "batchIndex" batchCount
            let! slice = readSlice pos batchLog
            match slice.Status with
            | PageReadStatus.StreamNotFound -> yield Some (int64 ExpectedVersion.NoStream), Array.empty
            | PageReadStatus.Success ->
                let version = if batchCount = 0 then Some slice.LastStreamPosition else None
                yield version, slice.Messages
                if not slice.IsEnd then
                    yield! loop (batchCount + 1) slice.NextStreamVersion
            | x -> raise <| System.ArgumentOutOfRangeException("SliceReadStatus", x, "Unknown result value") }
        loop 0 startPosition
    let resolvedEventBytes events = events |> Array.sumBy (|ResolvedEventLen|)
    let logBatchRead direction streamName t events batchSize version (log : ILogger) =
        let bytes, count = resolvedEventBytes events, events.Length
        let reqMetric : Log.Measurement = { stream = streamName; interval = t; bytes = bytes; count = count}
        let batches = (events.Length - 1)/batchSize + 1
        let action = match direction with Direction.Forward -> "LoadF" | Direction.Backward -> "LoadB"
        let evt = Log.Event.Batch (direction, batches, reqMetric)
        (log |> Log.prop "bytes" bytes |> Log.event evt).Information(
            "Es{action:l} stream={stream} count={count}/{batches} version={version}",
            action, streamName, count, batches, version)
    let loadForwardsFrom (log : ILogger) retryPolicy conn batchSize maxPermittedBatchReads streamName startPosition
        : Async<int64 * StreamMessage[]> = async {
        let mergeBatches (batches: AsyncSeq<int64 option * StreamMessage[]>) = async {
            let mutable versionFromStream = None
            let! (events : StreamMessage[]) =
                batches
                |> AsyncSeq.map (function None, events -> events | (Some _) as reportedVersion, events -> versionFromStream <- reportedVersion; events)
                |> AsyncSeq.concatSeq
                |> AsyncSeq.toArrayAsync
            let version = match versionFromStream with Some version -> version | None -> invalidOp "no version encountered in event batch stream"
            return version, events }
        let call pos = loggedReadSlice conn streamName Direction.Forward batchSize pos
        let retryingLoggingReadSlice pos = Log.withLoggedRetries retryPolicy "readAttempt" (call pos)
        let direction = Direction.Forward
        let log = log |> Log.prop "batchSize" batchSize |> Log.prop "direction" direction |> Log.prop "stream" streamName
        let batches : AsyncSeq<int64 option * StreamMessage[]> = readBatches log retryingLoggingReadSlice maxPermittedBatchReads startPosition
        let! t, (version, events) = mergeBatches batches |> Stopwatch.Time
        log |> logBatchRead direction streamName t events batchSize version
        return version, events }
    let partitionPayloadFrom firstUsedEventNumber : StreamMessage[] -> int * int =
        let acc (tu,tr) ((ResolvedEventLen bytes) as y) = if y.Position < firstUsedEventNumber then tu, tr + bytes else tu + bytes, tr
        Array.fold acc (0,0)
    let loadBackwardsUntilCompactionOrStart (log : ILogger) retryPolicy conn batchSize maxPermittedBatchReads streamName (tryDecode,isOrigin)
        : Async<int64 * (StreamMessage * 'event option)[]> = async {
        let mergeFromCompactionPointOrStartFromBackwardsStream (log : ILogger) (batchesBackward : AsyncSeq<int64 option * StreamMessage[]>)
            : Async<int64 * (StreamMessage*'event option)[]> = async {
            let versionFromStream, lastBatch = ref None, ref None
            let! tempBackward =
                batchesBackward
                |> AsyncSeq.map (fun batch ->
                    match batch with
                    | None, events -> lastBatch := Some events; events
                    | (Some _) as reportedVersion, events -> versionFromStream := reportedVersion; lastBatch := Some events; events
                    |> Array.map (fun e -> e, tryDecode e))
                |> AsyncSeq.concatSeq
                |> AsyncSeq.takeWhileInclusive (function
                    | x, Some e when isOrigin e ->
                        match !lastBatch with
                        | None -> log.Information("GesStop stream={stream} at={eventNumber}", streamName, x.Position)
                        | Some batch ->
                            let used, residual = batch |> partitionPayloadFrom x.Position
                            log.Information("GesStop stream={stream} at={eventNumber} used={used} residual={residual}", streamName, x.Position, used, residual)
                        false
                    | _ -> true) // continue the search
                |> AsyncSeq.toArrayAsync
            let eventsForward = Array.Reverse(tempBackward); tempBackward // sic - relatively cheap, in-place reverse of something we own
            let version = match !versionFromStream with Some version -> version | None -> invalidOp "no version encountered in event batch stream"
            return version, eventsForward }
        let call pos = loggedReadSlice conn streamName Direction.Backward batchSize pos
        let retryingLoggingReadSlice pos = Log.withLoggedRetries retryPolicy "readAttempt" (call pos)
        let log = log |> Log.prop "batchSize" batchSize |> Log.prop "stream" streamName
        let startPosition = int Position.End
        let direction = Direction.Backward
        let readlog = log |> Log.prop "direction" direction
        let batchesBackward : AsyncSeq<int64 option * StreamMessage[]> = readBatches readlog retryingLoggingReadSlice maxPermittedBatchReads startPosition
        let! t, (version, events) = mergeFromCompactionPointOrStartFromBackwardsStream log batchesBackward |> Stopwatch.Time
        log |> logBatchRead direction streamName t (Array.map fst events) batchSize version
        return version, events }

module UnionEncoderAdapters =
    let encodedEventOfResolvedEvent (e : StreamMessage) : FsCodec.IIndexedEvent<byte[]> =
        // Inspecting server code shows both Created and CreatedEpoch are set; taking this as it's less ambiguous than DateTime in the general case
        let ts = (e.CreatedUtc.ToUniversalTime().Ticks - (new DateTimeOffset(1970, 1, 1, 0, 0, 0, TimeSpan.Zero)).Ticks) / TimeSpan.TicksPerSecond |> DateTimeOffset.FromUnixTimeMilliseconds

        let data = e.GetJsonData() |> Async.AwaitTask |> Async.RunSynchronously |> System.Text.Encoding.UTF8.GetBytes

        FsCodec.Core.IndexedEventData(e.Position, (*isUnfold*)false, e.Type, data, e.JsonMetadata |> System.Text.Encoding.UTF8.GetBytes, ts) :> _
    let eventDataOfEncodedEvent (x : FsCodec.IEvent<byte[]>) =
        NewStreamMessage(Guid.NewGuid(), x.EventType, x.Data |> System.Text.Encoding.UTF8.GetString, x.Meta |> System.Text.Encoding.UTF8.GetString)

type Stream = { name: string }
type Position = { streamVersion: int64; compactionEventNumber: int64 option; batchCapacityLimit: int option }
type Token = { stream: Stream; pos: Position }

module Token =
    let private create compactionEventNumber batchCapacityLimit streamName streamVersion : Store.StreamToken =
        {   value = box {
                stream = { name = streamName}
                pos = { streamVersion = streamVersion; compactionEventNumber = compactionEventNumber; batchCapacityLimit = batchCapacityLimit } }
            version = streamVersion }
    /// No batching / compaction; we only need to retain the StreamVersion
    let ofNonCompacting streamName streamVersion : Store.StreamToken =
        create None None streamName streamVersion
    // headroom before compaction is necessary given the stated knowledge of the last (if known) `compactionEventNumberOption`
    let private batchCapacityLimit compactedEventNumberOption unstoredEventsPending (batchSize : int) (streamVersion : int64) : int =
        match compactedEventNumberOption with
        | Some (compactionEventNumber : int64) -> (batchSize - unstoredEventsPending) - int (streamVersion - compactionEventNumber + 1L) |> max 0
        | None -> (batchSize - unstoredEventsPending) - (int streamVersion + 1) - 1 |> max 0
    let (*private*) ofCompactionEventNumber compactedEventNumberOption unstoredEventsPending batchSize streamName streamVersion : Store.StreamToken =
        let batchCapacityLimit = batchCapacityLimit compactedEventNumberOption unstoredEventsPending batchSize streamVersion
        create compactedEventNumberOption (Some batchCapacityLimit) streamName streamVersion
    /// Assume we have not seen any compaction events; use the batchSize and version to infer headroom
    let ofUncompactedVersion batchSize streamName streamVersion : Store.StreamToken =
        ofCompactionEventNumber None 0 batchSize streamName streamVersion
    let (|Unpack|) (x : Store.StreamToken) : Token = unbox<Token> x.value
    /// Use previousToken plus the data we are adding and the position we are adding it to infer a headroom
    let ofPreviousTokenAndEventsLength (Unpack previousToken) eventsLength batchSize streamVersion : Store.StreamToken =
        let compactedEventNumber = previousToken.pos.compactionEventNumber
        ofCompactionEventNumber compactedEventNumber eventsLength batchSize previousToken.stream.name streamVersion
    /// Use an event just read from the stream to infer headroom
    let ofCompactionResolvedEventAndVersion (compactionEvent: StreamMessage) batchSize streamName streamVersion : Store.StreamToken =
        ofCompactionEventNumber (Some compactionEvent.Position) 0 batchSize streamName streamVersion
    /// Use an event we are about to write to the stream to infer headroom
    let ofPreviousStreamVersionAndCompactionEventDataIndex (Unpack token) compactionEventDataIndex eventsLength batchSize streamVersion' : Store.StreamToken =
        ofCompactionEventNumber (Some (token.pos.streamVersion + 1L + int64 compactionEventDataIndex)) eventsLength batchSize token.stream.name streamVersion'
    let (|StreamPos|) (Unpack token) : Stream * Position = token.stream, token.pos
    let supersedes (Unpack current) (Unpack x) =
        let currentVersion, newVersion = current.pos.streamVersion, x.pos.streamVersion
        newVersion > currentVersion

type Connection(readConnection, [<O; D(null)>]?writeConnection, [<O; D(null)>]?readRetryPolicy, [<O; D(null)>]?writeRetryPolicy) =
    member __.ReadConnection = readConnection
    member __.ReadRetryPolicy = readRetryPolicy
    member __.WriteConnection = defaultArg writeConnection readConnection
    member __.WriteRetryPolicy = writeRetryPolicy

type BatchingPolicy(getMaxBatchSize : unit -> int, [<O; D(null)>]?batchCountLimit) =
    new (maxBatchSize) = BatchingPolicy(fun () -> maxBatchSize)
    member __.BatchSize = getMaxBatchSize()
    member __.MaxBatches = batchCountLimit

[<RequireQualifiedAccess; NoComparison; NoEquality>]
type GatewaySyncResult = Written of Store.StreamToken | ConflictUnknown of Store.StreamToken

type Context(conn : Connection, batching : BatchingPolicy) =
    let isResolvedEventEventType (tryDecode,predicate) (e:StreamMessage) = 
        let data = e.GetJsonData() |> Async.AwaitTask |> Async.RunSynchronously
        predicate (tryDecode data)
    let tryIsResolvedEventEventType predicateOption = predicateOption |> Option.map isResolvedEventEventType
    member __.LoadEmpty streamName = Token.ofUncompactedVersion batching.BatchSize streamName -1L
    member __.LoadBatched streamName log (tryDecode,isCompactionEventType): Async<Store.StreamToken * 'event[]> = async {
        let! version, events = Read.loadForwardsFrom log conn.ReadRetryPolicy conn.ReadConnection batching.BatchSize batching.MaxBatches streamName 0
        match tryIsResolvedEventEventType isCompactionEventType with
        | None -> return Token.ofNonCompacting streamName version, Array.choose tryDecode events
        | Some isCompactionEvent ->
            match events |> Array.tryFindBack isCompactionEvent with
            | None -> return Token.ofUncompactedVersion batching.BatchSize streamName version, Array.choose tryDecode events
            | Some resolvedEvent -> return Token.ofCompactionResolvedEventAndVersion resolvedEvent batching.BatchSize streamName version, Array.choose tryDecode events }
    member __.LoadBackwardsStoppingAtCompactionEvent streamName log (tryDecode,isOrigin): Async<Store.StreamToken * 'event []> = async {
        let! version, events =
            Read.loadBackwardsUntilCompactionOrStart log conn.ReadRetryPolicy conn.ReadConnection batching.BatchSize batching.MaxBatches streamName (tryDecode,isOrigin)
        match Array.tryHead events |> Option.filter (function _, Some e -> isOrigin e | _ -> false) with
        | None -> return Token.ofUncompactedVersion batching.BatchSize streamName version, Array.choose snd events
        | Some (resolvedEvent,_) -> return Token.ofCompactionResolvedEventAndVersion resolvedEvent batching.BatchSize streamName version, Array.choose snd events }
    member __.LoadFromToken useWriteConn streamName log (Token.Unpack token as streamToken) (tryDecode,isCompactionEventType)
        : Async<Store.StreamToken * 'event[]> = async {
        let streamPosition = token.pos.streamVersion + 1L
        let connToUse = if useWriteConn then conn.WriteConnection else conn.ReadConnection
        let! version, events = Read.loadForwardsFrom log conn.ReadRetryPolicy connToUse batching.BatchSize batching.MaxBatches streamName (streamPosition |> int)
        match isCompactionEventType with
        | None -> return Token.ofNonCompacting streamName version, Array.choose tryDecode events
        | Some isCompactionEvent ->
            match events |> Array.tryFindBack (fun re -> match tryDecode re with Some e -> isCompactionEvent e | _ -> false) with
            | None -> return Token.ofPreviousTokenAndEventsLength streamToken events.Length batching.BatchSize version, Array.choose tryDecode events
            | Some resolvedEvent -> return Token.ofCompactionResolvedEventAndVersion resolvedEvent batching.BatchSize streamName version, Array.choose tryDecode events }
    member __.TrySync log (Token.Unpack token as streamToken) (events, encodedEvents: NewStreamMessage array) (isCompactionEventType) : Async<GatewaySyncResult> = async {
        let streamVersion = token.pos.streamVersion
        let! wr = Write.writeEvents log conn.WriteRetryPolicy conn.WriteConnection token.stream.name (streamVersion |> int) encodedEvents
        match wr with
        | EsSyncResult.Conflict expectedVersion ->
            return GatewaySyncResult.ConflictUnknown (Token.ofNonCompacting token.stream.name expectedVersion)
        | EsSyncResult.Written wr ->

        let version' = wr.CurrentVersion |> int64
        let token =
            match isCompactionEventType with
            | None -> Token.ofNonCompacting token.stream.name version'
            | Some isCompactionEvent ->
                match events |> Array.ofList |> Array.tryFindIndexBack isCompactionEvent with
                | None -> Token.ofPreviousTokenAndEventsLength streamToken encodedEvents.Length batching.BatchSize version'
                | Some compactionEventIndex ->
                    Token.ofPreviousStreamVersionAndCompactionEventDataIndex streamToken compactionEventIndex encodedEvents.Length batching.BatchSize version'
        return GatewaySyncResult.Written token }
    member __.Sync(log, streamName, streamVersion, events: FsCodec.IEvent<byte[]>[]) : Async<GatewaySyncResult> = async {
        let encodedEvents : NewStreamMessage[] = events |> Array.map UnionEncoderAdapters.eventDataOfEncodedEvent
        let! wr = Write.writeEvents log conn.WriteRetryPolicy conn.WriteConnection streamName streamVersion encodedEvents
        match wr with
        | EsSyncResult.Conflict actualVersion ->
            return GatewaySyncResult.ConflictUnknown (Token.ofNonCompacting streamName actualVersion)
        | EsSyncResult.Written wr ->
            let version' = wr.CurrentVersion |> int64
            let token = Token.ofNonCompacting streamName version'
            return GatewaySyncResult.Written token }

[<NoComparison; NoEquality; RequireQualifiedAccess>]
type AccessStrategy<'event,'state> =
    | EventsAreState
    | RollingSnapshots of isValid: ('event -> bool) * compact: ('state -> 'event)

type private CompactionContext(eventsLen : int, capacityBeforeCompaction : int) =
    /// Determines whether writing a Compaction event is warranted (based on the existing state and the current `Accumulated` changes)
    member __.IsCompactionDue = eventsLen > capacityBeforeCompaction

type private Category<'event, 'state>(context : Context, codec : FsCodec.IUnionEncoder<'event,byte[]>, ?access : AccessStrategy<'event,'state>) =
    let tryDecode (e: StreamMessage) = e |> UnionEncoderAdapters.encodedEventOfResolvedEvent |> codec.TryDecode
    let compactionPredicate =
        match access with
        | None -> None
        | Some AccessStrategy.EventsAreState -> Some (fun _ -> true)
        | Some (AccessStrategy.RollingSnapshots (isValid,_)) -> Some isValid
    let isOrigin =
        match access with
        | None | Some AccessStrategy.EventsAreState -> fun _ -> true
        | Some (AccessStrategy.RollingSnapshots (isValid,_)) -> isValid
    let loadAlgorithm load streamName initial log =
        let batched = load initial (context.LoadBatched streamName log (tryDecode,None))
        let compacted = load initial (context.LoadBackwardsStoppingAtCompactionEvent streamName log (tryDecode,isOrigin))
        match access with
        | None -> batched
        | Some AccessStrategy.EventsAreState
        | Some (AccessStrategy.RollingSnapshots _) -> compacted
    let load (fold: 'state -> 'event seq -> 'state) initial f = async {
        let! token, events = f
        return token, fold initial events }
    member __.Load (fold: 'state -> 'event seq -> 'state) (initial: 'state) (streamName : string) (log : ILogger) : Async<Store.StreamToken * 'state> =
        loadAlgorithm (load fold) streamName initial log
    member __.LoadFromToken (fold: 'state -> 'event seq -> 'state) (state: 'state) (streamName : string) token (log : ILogger) : Async<Store.StreamToken * 'state> =
        (load fold) state (context.LoadFromToken false streamName log token (tryDecode,compactionPredicate))
    member __.TrySync (fold: 'state -> 'event seq -> 'state) (log : ILogger)
            ((Token.StreamPos (stream,pos) as streamToken), state : 'state)
            (events : 'event list) : Async<Store.SyncResult<'state>> = async {
        let events =
            match access with
            | None | Some AccessStrategy.EventsAreState -> events
            | Some (AccessStrategy.RollingSnapshots (_,compact)) ->
                let cc = CompactionContext(List.length events, pos.batchCapacityLimit.Value)
                if cc.IsCompactionDue then events @ [fold state events |> compact] else events

        let encodedEvents : NewStreamMessage[] = events |> Seq.map (codec.Encode >> UnionEncoderAdapters.eventDataOfEncodedEvent) |> Array.ofSeq
        let! syncRes = context.TrySync log streamToken (events,encodedEvents) compactionPredicate
        match syncRes with
        | GatewaySyncResult.ConflictUnknown _ ->
            return Store.SyncResult.Conflict  (load fold state (context.LoadFromToken true stream.name log streamToken (tryDecode,compactionPredicate)))
        | GatewaySyncResult.Written token' ->
            return Store.SyncResult.Written   (token', fold state (Seq.ofList events)) }

module Caching =
    open System.Runtime.Caching
    [<AllowNullLiteral>]
    type CacheEntry<'state>(initialToken : Store.StreamToken, initialState :'state) =
        let mutable currentToken, currentState = initialToken, initialState
        member __.UpdateIfNewer (other : CacheEntry<'state>) =
            lock __ <| fun () ->
                let otherToken, otherState = other.Value
                if otherToken |> Token.supersedes currentToken then
                    currentToken <- otherToken
                    currentState <- otherState
        member __.Value : Store.StreamToken  * 'state =
            lock __ <| fun () ->
                currentToken, currentState

    type Cache(name, sizeMb : int) =
        let cache =
            let config = System.Collections.Specialized.NameValueCollection(1)
            config.Add("cacheMemoryLimitMegabytes", string sizeMb);
            new MemoryCache(name, config)
        member __.UpdateIfNewer (policy : CacheItemPolicy) (key : string) entry =
            match cache.AddOrGetExisting(key, box entry, policy) with
            | null -> ()
            | :? CacheEntry<'state> as existingEntry -> existingEntry.UpdateIfNewer entry
            | x -> failwithf "UpdateIfNewer Incompatible cache entry %A" x
        member __.TryGet (key : string) =
            match cache.Get key with
            | null -> None
            | :? CacheEntry<'state> as existingEntry -> Some existingEntry.Value
            | x -> failwithf "TryGet Incompatible cache entry %A" x

    /// Forwards all state changes in all streams of an ICategory to a `tee` function
    type CategoryTee<'event, 'state>(inner: ICategory<'event, 'state, string>, tee : string -> Store.StreamToken * 'state -> unit) =
        let intercept streamName tokenAndState =
            tee streamName tokenAndState
            tokenAndState
        let interceptAsync load streamName = async {
            let! tokenAndState = load
            return intercept streamName tokenAndState }
        interface ICategory<'event, 'state, string> with
            member __.Load (streamName : string) (log : ILogger) : Async<Store.StreamToken * 'state> =
                interceptAsync (inner.Load streamName log) streamName
            member __.TrySync (log : ILogger) ((Token.StreamPos (stream,_) as token), state) (events : 'event list) : Async<Store.SyncResult<'state>> = async {
                let! syncRes = inner.TrySync log (token, state) events
                match syncRes with
                | Store.SyncResult.Conflict resync ->             return Store.SyncResult.Conflict (interceptAsync resync stream.name)
                | Store.SyncResult.Written (token', state') ->    return Store.SyncResult.Written (token', state') }

    let applyCacheUpdatesWithSlidingExpiration
            (cache: Cache)
            (prefix: string)
            (slidingExpiration : TimeSpan)
            (category: ICategory<'event, 'state, string>)
            : ICategory<'event, 'state, string> =
        let policy = new CacheItemPolicy(SlidingExpiration = slidingExpiration)
        let addOrUpdateSlidingExpirationCacheEntry streamName = CacheEntry >> cache.UpdateIfNewer policy (prefix + streamName)
        CategoryTee<'event,'state>(category, addOrUpdateSlidingExpirationCacheEntry) :> _

type private Folder<'event, 'state>(category : Category<'event, 'state>, fold: 'state -> 'event seq -> 'state, initial: 'state, ?readCache) =
    let loadAlgorithm streamName initial log =
        let batched = category.Load fold initial streamName log
        let cached token state = category.LoadFromToken fold state streamName token log
        match readCache with
        | None -> batched
        | Some (cache : Caching.Cache, prefix : string) ->
            match cache.TryGet(prefix + streamName) with
            | None -> batched
            | Some (token, state) -> cached token state
    interface ICategory<'event, 'state, string> with
        member __.Load (streamName : string) (log : ILogger) : Async<Store.StreamToken * 'state> =
            loadAlgorithm streamName initial log
        member __.TrySync (log : ILogger) (token, initialState) (events : 'event list) : Async<Store.SyncResult<'state>> = async {
            let! syncRes = category.TrySync fold log (token, initialState) events
            match syncRes with
            | Store.SyncResult.Conflict resync ->         return Store.SyncResult.Conflict resync
            | Store.SyncResult.Written (token',state') -> return Store.SyncResult.Written (token',state') }

[<NoComparison; NoEquality; RequireQualifiedAccess>]
type CachingStrategy =
    | SlidingWindow of Caching.Cache * window: TimeSpan
    /// Prefix is used to segregate multiple folds per stream when they are stored in the cache
    | SlidingWindowPrefixed of Caching.Cache * window: TimeSpan * prefix: string

type Resolver<'event,'state>
    (   context : Context, codec, fold, initial,
        /// Caching can be overkill for EventStore esp considering the degree to which its intrinsic caching is a first class feature
        /// e.g., A key benefit is that reads of streams more than a few pages long get completed in constant time after the initial load
        [<O; D(null)>]?caching,
        [<O; D(null)>]?access) =
    do  match access with
        | Some (AccessStrategy.EventsAreState) when Option.isSome caching ->
            "Equinox.EventStore does not support (and it would make things _less_ efficient even if it did)"
            + "mixing AccessStrategy.EventsAreState with Caching at present."
            |> invalidOp
        | _ -> ()

    let inner = Category<'event, 'state>(context, codec, ?access = access)
    let readCacheOption =
        match caching with
        | None -> None
        | Some (CachingStrategy.SlidingWindow(cache, _)) -> Some(cache, null)
        | Some (CachingStrategy.SlidingWindowPrefixed(cache, _, prefix)) -> Some(cache, prefix)
    let folder = Folder<'event, 'state>(inner, fold, initial, ?readCache = readCacheOption)
    let category : Equinox.Store.ICategory<_,_,_> =
        match caching with
        | None -> folder :> _
        | Some (CachingStrategy.SlidingWindow(cache, window)) ->
            Caching.applyCacheUpdatesWithSlidingExpiration cache null window folder
        | Some (CachingStrategy.SlidingWindowPrefixed(cache, window, prefix)) ->
            Caching.applyCacheUpdatesWithSlidingExpiration cache prefix window folder
    let mkStreamName categoryName streamId = sprintf "%s-%s" categoryName streamId
    let resolve = Stream.create category

    member __.Resolve = function
        | Target.AggregateId (categoryName,streamId) ->
            resolve <| mkStreamName categoryName streamId
        | Target.AggregateIdEmpty (categoryName,streamId) ->
            let streamName = mkStreamName categoryName streamId
            Stream.ofMemento (context.LoadEmpty streamName,initial) <| resolve streamName
        | Target.StreamName streamName ->
            resolve streamName

    /// Resolve from a Memento being used in a Continuation [based on position and state typically from Stream.CreateMemento]
    member __.FromMemento(Token.Unpack token as streamToken, state) =
        Stream.ofMemento (streamToken,state) <| resolve token.stream.name