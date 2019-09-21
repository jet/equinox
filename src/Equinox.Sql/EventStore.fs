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
type EsSyncResult = Written of AppendResult | Conflict of expectedVersion: Nullable<int>

module private Write =
    /// Yields `EsSyncResult.Written` or `EsSyncResult.Conflict` to signify WrongExpectedVersion
    let private writeEventsAsync (log : ILogger) (conn : IStreamStore) (streamName : string) (version : int) (events : NewStreamMessage[])
        : Async<EsSyncResult> = async {
        try
            let! wr = conn.AppendToStream(StreamId(streamName), version, events) |> Async.AwaitTaskCorrect
            return EsSyncResult.Written wr
        with :? WrongExpectedVersionException as ex ->
            log.Information(ex, "Es TrySync WrongExpectedVersionException writing {EventTypes}, expected {ExpectedVersion}",
                [| for x in events -> x.Type |], ex.ExpectedVersion)
            return EsSyncResult.Conflict ex.ExpectedVersion }
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
