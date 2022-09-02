namespace Equinox.CosmosStore.Core

open Equinox.Core
open FsCodec
open FSharp.Control
open Microsoft.Azure.Cosmos
open Serilog
open System
open System.Text.Json
open System.Threading
open System.Threading.Tasks

type EventBody = JsonElement

/// A single Domain Event from the array held in a Batch
[<NoEquality; NoComparison>]
type Event = // TODO for STJ v5: All fields required unless explicitly optional
    {   /// Creation datetime (as opposed to system-defined _lastUpdated which is touched by triggers, replication etc.)
        t: DateTimeOffset // ISO 8601

        /// The Case (Event Type); used to drive deserialization
        c: string // required

        /// Event body, as UTF-8 encoded json ready to be injected into the Json being rendered for CosmosDB
        d: EventBody // TODO for STJ v5: Required, but can be null so Nullary cases can work

        /// Optional metadata, as UTF-8 encoded json, ready to emit directly
        m: EventBody // TODO for STJ v5: Optional, not serialized if missing

        /// Optional correlationId
        correlationId : string // TODO for STJ v5: Optional, not serialized if missing

        /// Optional causationId
        causationId : string // TODO for STJ v5: Optional, not serialized if missing
    }

    interface IEventData<EventBody> with
        member x.EventType = x.c
        member x.Data = x.d
        member x.Meta = x.m
        member _.EventId = Guid.Empty
        member x.CorrelationId = x.correlationId
        member x.CausationId = x.causationId
        member x.Timestamp = x.t

/// A 'normal' (frozen, not Tip) Batch of Events (without any Unfolds)
[<NoEquality; NoComparison>]
type Batch = // TODO for STJ v5: All fields required unless explicitly optional
    {   /// CosmosDB-mandated Partition Key, must be maintained within the document
        /// Not actually required if running in single partition mode, but for simplicity, we always write it
        p: string // "{streamName}" TODO for STJ v5: Optional, not requested in queries

        /// CosmosDB-mandated unique row key; needs to be unique within any partition it is maintained; must be string
        /// At the present time, one can't perform an ORDER BY on this field, hence we also have i shadowing it
        /// NB Tip uses a well known value here while it's actively 'open'
        id: string // "{index}"

        /// When we read, we need to capture the value so we can retain it for caching purposes
        /// NB this is not relevant to fill in when we pass it to the writing stored procedure
        /// as it will do: 1. read 2. merge 3. write merged version contingent on the _etag not having changed
        _etag: string // TODO for STJ v5: Optional, not serialized if missing

        /// base 'i' value for the Events held herein
        i: int64 // {index}

        // `i` value for successor batch (to facilitate identifying which Batch a given startPos is within)
        n: int64 // {index}

        /// The Domain Events (as opposed to Unfolded Events, see Tip) at this offset in the stream
        e: Event[] }
    /// Unless running in single partition mode (which would restrict us to 10GB per container)
    /// we need to nominate a partition key that will be in every document
    static member internal PartitionKeyField = "p"
    /// As one cannot sort by the implicit `id` field, we have an indexed `i` field for sort and range query use
    static member internal IndexedFields = [Batch.PartitionKeyField; "i"; "n"]

/// Compaction/Snapshot/Projection Event based on the state at a given point in time `i`
[<NoEquality; NoComparison>]
type Unfold =
    {   /// Base: Stream Position (Version) of State from which this Unfold Event was generated
        i: int64

        /// Generation datetime
        t: DateTimeOffset // ISO 8601 // Not written by versions <= 2.0.0-rc9

        /// The Case (Event Type) of this compaction/snapshot, used to drive deserialization
        c: string // required

        /// Event body - Json -> Deflate -> Base64 -> JsonElement
        [<Serialization.JsonConverter(typeof<JsonCompressedBase64Converter>)>]
        d: EventBody // required

        /// Optional metadata, same encoding as `d` (can be null; not written if missing)
        [<Serialization.JsonConverter(typeof<JsonCompressedBase64Converter>)>]
        m: EventBody // TODO for STJ v5: Optional, not serialized if missing
    }

/// The special-case 'Pending' Batch Format used to read the currently active (and mutable) document
/// Stored representation has the following diffs vs a 'normal' (frozen/completed) Batch: a) `id` = `-1` b) contains unfolds (`u`)
/// NB the type does double duty as a) model for when we read it b) encoding a batch being sent to the stored proc
[<NoEquality; NoComparison>]
type Tip = // TODO for STJ v5: All fields required unless explicitly optional
    {
        /// Partition key, as per Batch
        p: string // "{streamName}" TODO for STJ v5: Optional, not requested in queries

        /// Document Id within partition, as per Batch
        id: string // "{-1}" - Well known IdConstant used while this remains the pending batch

        /// When we read, we need to capture the value so we can retain it for caching purposes
        /// NB this is not relevant to fill in when we pass it to the writing stored procedure
        /// as it will do: 1. read 2. merge 3. write merged version contingent on the _etag not having changed
        _etag: string // TODO for STJ v5: Optional, not serialized if missing

        /// base 'i' value for the Events held herein
        i: int64

        /// `i` value for successor batch (to facilitate identifying which Batch a given startPos is within)
        n: int64 // {index}

        /// Domain Events, will eventually move out to a Batch
        e: Event[]

        /// Compaction/Snapshot/Projection events - owned and managed by the sync stored proc
        u: Unfold[] }
    static member internal WellKnownDocumentId = "-1"

/// Position and Etag to which an operation is relative
[<NoComparison>]
type Position = { index : int64; etag : string option }

module internal Position =
    /// NB very inefficient compared to FromDocument or using one already returned to you
    let fromI (i : int64) = { index = i; etag = None }
    /// If we have strong reason to suspect a stream is empty, we won't have an etag (and Writer Stored Procedure special cases this)
    let fromKnownEmpty = fromI 0L
    /// Just Do It mode
    let fromAppendAtEnd = fromI -1L // sic - needs to yield -1
    let fromEtag (value : string) = { fromI -2L with etag = Some value }
    /// Create Position from Tip record context (facilitating 1 RU reads)
    let fromTip (x : Tip) = { index = x.n; etag = match x._etag with null -> None | x -> Some x }
    /// If we encounter the tip (id=-1) item/document, we're interested in its etag so we can re-sync for 1 RU
    let tryFromBatch (x : Batch) =
        if x.id <> Tip.WellKnownDocumentId then None
        else Some { index = x.n; etag = match x._etag with null -> None | x -> Some x }

[<RequireQualifiedAccess>]
type Direction = Forward | Backward override this.ToString() = match this with Forward -> "Forward" | Backward -> "Backward"

type internal Enum() =
    static member Events(i : int64, e : Event[], ?minIndex, ?maxIndex) : ITimelineEvent<EventBody> seq = seq {
        let indexMin, indexMax = defaultArg minIndex 0L, defaultArg maxIndex Int64.MaxValue
        for offset in 0..e.Length-1 do
            let index = i + int64 offset
            // If we're loading from a nominated position, we need to discard items in the batch before/after the start on the start page
            if index >= indexMin && index < indexMax then
                let x = e[offset]
                yield FsCodec.Core.TimelineEvent.Create(index, x.c, x.d, x.m, Guid.Empty, x.correlationId, x.causationId, x.t) }
    static member internal Events(t : Tip, ?minIndex, ?maxIndex) : ITimelineEvent<EventBody> seq =
        Enum.Events(t.i, t.e, ?minIndex = minIndex, ?maxIndex = maxIndex)
    static member internal Events(b : Batch, ?minIndex, ?maxIndex) =
        Enum.Events(b.i, b.e, ?minIndex = minIndex, ?maxIndex = maxIndex)
    static member Unfolds(xs : Unfold[]) : ITimelineEvent<EventBody> seq = seq {
        for x in xs -> FsCodec.Core.TimelineEvent.Create(x.i, x.c, x.d, x.m, Guid.Empty, null, null, x.t, isUnfold = true) }
    static member EventsAndUnfolds(x : Tip, ?minIndex, ?maxIndex) : ITimelineEvent<EventBody> seq =
        Enum.Events(x, ?minIndex = minIndex, ?maxIndex = maxIndex)
        |> Seq.append (Enum.Unfolds x.u)
        // where Index is equal, unfolds get delivered after the events so the fold semantics can be 'idempotent'
        |> Seq.sortBy (fun x -> x.Index, x.IsUnfold)

type IRetryPolicy = abstract member Execute : (int -> Async<'T>) -> Async<'T>

module Log =

    /// <summary>Name of Property used for <c>Metric</c> in <c>LogEvent</c>s.</summary>
    let [<Literal>] PropertyTag = "cosmosEvt"

    [<NoEquality; NoComparison>]
    type Measurement =
       {   database : string; container : string; stream : string
           interval : StopwatchInterval; bytes : int; count : int; ru : float }
    [<RequireQualifiedAccess; NoEquality; NoComparison>]
    type Metric =
        /// Individual read request for the Tip
        | Tip of Measurement
        /// Individual read request for the Tip, not found
        | TipNotFound of Measurement
        /// Tip read with Single RU Request Charge due to correct use of etag in cache
        | TipNotModified of Measurement

        /// Summarizes a set of Responses for a given Read request
        | Query of Direction * responses : int * Measurement
        /// Individual read request in a Batch
        /// Charges are rolled up into Query Metric (so do not double count)
        | QueryResponse of Direction * Measurement

        | SyncSuccess of Measurement
        | SyncResync of Measurement
        | SyncConflict of Measurement

        /// Summarizes outcome of request to trim batches from head of a stream and events in Tip
        /// Count in Measurement is number of batches (documents) deleted
        /// Bytes in Measurement is number of events deleted
        | Prune of responsesHandled : int * Measurement
        /// Handled response from listing of batches in a stream
        /// Charges are rolled up into the Prune Metric (so do not double count)
        | PruneResponse of Measurement
        /// Deleted an individual Batch
        | Delete of Measurement
        /// Trimmed the Tip
        | Trim of Measurement
    let [<return: Struct>] (|MetricEvent|_|) (logEvent : Serilog.Events.LogEvent) : Metric voption =
        let mutable p = Unchecked.defaultof<_>
        logEvent.Properties.TryGetValue(PropertyTag, &p) |> ignore
        match p with Log.ScalarValue (:? Metric as e) -> ValueSome e | _ -> ValueNone

    /// Attach a property to the captured event record to hold the metric information
    let internal event (value : Metric) = Internal.Log.withScalarProperty PropertyTag value
    let internal prop name value (log : ILogger) = log.ForContext(name, value)
    let internal propData name (events : #IEventData<EventBody> seq) (log : ILogger) =
        let render (body : EventBody) = body.GetRawText()
        let items = seq { for e in events do yield sprintf "{\"%s\": %s}" e.EventType (render e.Data) }
        log.ForContext(name, sprintf "[%s]" (String.concat ",\n\r" items))
    let internal propEvents = propData "events"
    let internal propDataUnfolds = Enum.Unfolds >> propData "unfolds"
    let internal propStartPos (value : Position) log = prop "startPos" value.index log
    let internal propStartEtag (value : Position) log = prop "startEtag" value.etag log

    let internal withLoggedRetries<'t> (retryPolicy : IRetryPolicy option) (contextLabel : string) (f : ILogger -> Async<'t>) log : Async<'t> =
        match retryPolicy with
        | None -> f log
        | Some retryPolicy ->
            let withLoggingContextWrapping count =
                let log = if count = 1 then log else log |> prop contextLabel count
                f log
            retryPolicy.Execute withLoggingContextWrapping

    let internal (|BlobLen|) (x : EventBody) = if x.ValueKind = JsonValueKind.Null then 0 else x.GetRawText().Length
    let internal eventLen (x: #IEventData<_>) = let BlobLen bytes, BlobLen metaBytes = x.Data, x.Meta in bytes + metaBytes + 80
    let internal batchLen = Seq.sumBy eventLen
    [<RequireQualifiedAccess>]
    type Operation = Tip | Tip404 | Tip304 | Query | Write | Resync | Conflict | Prune | Delete | Trim
    let (|Op|QueryRes|PruneRes|) = function
        | Metric.Tip s                        -> Op (Operation.Tip, s)
        | Metric.TipNotFound s                -> Op (Operation.Tip404, s)
        | Metric.TipNotModified s             -> Op (Operation.Tip304, s)

        | Metric.Query (_, _, s)              -> Op (Operation.Query, s)
        | Metric.QueryResponse (direction, s) -> QueryRes (direction, s)

        | Metric.SyncSuccess s                -> Op (Operation.Write, s)
        | Metric.SyncResync s                 -> Op (Operation.Resync, s)
        | Metric.SyncConflict s               -> Op (Operation.Conflict, s)

        | Metric.Prune (_, s)                 -> Op (Operation.Prune, s)
        | Metric.PruneResponse s              -> PruneRes s
        | Metric.Delete s                     -> Op (Operation.Delete, s)
        | Metric.Trim s                       -> Op (Operation.Trim, s)

    /// NB Caveat emptor; this is subject to unlimited change without the major version changing - while the `dotnet-templates` repo will be kept in step, and
    /// the ChangeLog will mention changes, it's critical to not assume that the presence or nature of these helpers be considered stable
    module InternalMetrics =

        module Stats =

            type internal Counter =
                 { mutable rux100 : int64; mutable count : int64; mutable ms : int64 }
                 static member Create() = { rux100 = 0L; count = 0L; ms = 0L }
                 member x.Ingest(ru, ms) =
                     Interlocked.Increment(&x.count) |> ignore
                     Interlocked.Add(&x.rux100, int64 (ru*100.)) |> ignore
                     Interlocked.Add(&x.ms, ms) |> ignore
            let inline private (|RcMs|) ({ interval = i; ru = ru } : Measurement) =
                ru, let e = i.Elapsed in int64 e.TotalMilliseconds
            type LogSink() =
                static let epoch = System.Diagnostics.Stopwatch.StartNew()
                static member val internal Read = Counter.Create() with get, set
                static member val internal Write = Counter.Create() with get, set
                static member val internal Resync = Counter.Create() with get, set
                static member val internal Conflict = Counter.Create() with get, set
                static member val internal Prune = Counter.Create() with get, set
                static member val internal Delete = Counter.Create() with get, set
                static member val internal Trim = Counter.Create() with get, set
                static member Restart() =
                    LogSink.Read <- Counter.Create()
                    LogSink.Write <- Counter.Create()
                    LogSink.Resync <- Counter.Create()
                    LogSink.Conflict <- Counter.Create()
                    LogSink.Prune <- Counter.Create()
                    LogSink.Delete <- Counter.Create()
                    LogSink.Trim <- Counter.Create()
                    let span = epoch.Elapsed
                    epoch.Restart()
                    span
                interface Serilog.Core.ILogEventSink with
                    member _.Emit logEvent =
                        match logEvent with
                        | MetricEvent cm ->
                            match cm with
                            | Op ((Operation.Tip | Operation.Tip404 | Operation.Tip304 | Operation.Query), RcMs m)  ->
                                                                          LogSink.Read.Ingest m
                            | QueryRes (_direction,          _)        -> ()
                            | Op (Operation.Write,            RcMs m)  -> LogSink.Write.Ingest m
                            | Op (Operation.Conflict,         RcMs m)  -> LogSink.Conflict.Ingest m
                            | Op (Operation.Resync,           RcMs m)  -> LogSink.Resync.Ingest m
                            | Op (Operation.Prune,            RcMs m)  -> LogSink.Prune.Ingest m
                            | PruneRes                        _        -> ()
                            | Op (Operation.Delete,           RcMs m)  -> LogSink.Delete.Ingest m
                            | Op (Operation.Trim,             RcMs m)  -> LogSink.Trim.Ingest m
                        | _ -> ()

        /// Relies on feeding of metrics from Log through to Stats.LogSink
        /// Use Stats.LogSink.Restart() to reset the start point (and stats) where relevant
        let dump (log : ILogger) =
            let stats =
              [ "Read", Stats.LogSink.Read
                "Write", Stats.LogSink.Write
                "Resync", Stats.LogSink.Resync
                "Conflict", Stats.LogSink.Conflict
                "Prune", Stats.LogSink.Prune
                "Delete", Stats.LogSink.Delete
                "Trim", Stats.LogSink.Trim ]
            let mutable rows, totalCount, totalRRu, totalWRu, totalMs = 0, 0L, 0., 0., 0L
            let logActivity name count ru lat =
                let aru, ams = (if count = 0L then Double.NaN else ru/float count), (if count = 0L then Double.NaN else float lat/float count)
                let rut = name |> function
                    | "TOTAL" -> "" | "Read" | "Prune" -> totalRRu <- totalRRu + ru; "R"
                    | _ ->                                totalWRu <- totalWRu + ru; "W"
                log.Information("{name}: {count:n0}r {ru:n0}{rut:l}RU Average {avgRu:n1}RU {lat:n0}ms", name, count, ru, rut, aru, ams)
            for name, stat in stats do
                if stat.count <> 0L then
                    let ru = float stat.rux100 / 100.
                    totalCount <- totalCount + stat.count
                    totalMs <- totalMs + stat.ms
                    logActivity name stat.count ru stat.ms
                    rows <- rows + 1
            // Yes, there's a minor race here between the use of the values and the reset
            let duration = Stats.LogSink.Restart()
            if rows > 1 then logActivity "TOTAL" totalCount (totalRRu + totalWRu) totalMs
            let measures : (string * (TimeSpan -> float)) list = [ "s", fun x -> x.TotalSeconds(*; "m", fun x -> x.TotalMinutes; "h", fun x -> x.TotalHours*) ]
            let logPeriodicRate name count rru wru = log.Information("{rru:n1}R/{wru:n1}W CU @ {count:n0} rp{unit}", rru, wru, count, name)
            for uom, f in measures do let d = f duration in if d <> 0. then logPeriodicRate uom (float totalCount/d |> int64) (totalRRu/d) (totalWRu/d)

[<AutoOpen>]
module private MicrosoftAzureCosmosWrappers =

    type ReadResult<'T> = Found of 'T | NotFound | NotModified
    type Container with
        member private container.DeserializeResponseBody<'T>(rm : ResponseMessage) : 'T =
            rm.EnsureSuccessStatusCode().Content
            |> container.Database.Client.ClientOptions.Serializer.FromStream<'T>
        member container.TryReadItem(partitionKey : PartitionKey, id : string, ?options : ItemRequestOptions) : Async<float * ReadResult<'T>> = async {
            let! ct = Async.CancellationToken
            use! rm = container.ReadItemStreamAsync(id, partitionKey, requestOptions = Option.toObj options, cancellationToken = ct) |> Async.AwaitTaskCorrect
            return rm.Headers.RequestCharge, rm.StatusCode |> function
                | System.Net.HttpStatusCode.NotFound -> NotFound
                | System.Net.HttpStatusCode.NotModified -> NotModified
                | _ -> container.DeserializeResponseBody<'T>(rm) |> Found }

// NB don't nest in a private module, or serialization will fail miserably ;)
[<CLIMutable; NoEquality; NoComparison>]
type SyncResponse = { etag : string; n : int64; conflicts : Unfold[]; e : Event[] }

module internal SyncStoredProc =
    let [<Literal>] name = "EquinoxEventsInTip4"  // NB need to rename/number for any breaking change
    let [<Literal>] body = """
// Manages the merging of the supplied Request Batch into the stream, potentially storing events in the Tip

// 0 perform concurrency check (index=-1 -> always append; index=-2 -> check based on .etag; _ -> check .n=.index)
// High level end-states:
// 1a if there is a Tip, but are only changes to the `u`nfolds (and no `e`vents) -> update Tip only
// 1b if there is a Tip, but incoming request includes an event -> generate a batch document + create empty Tip
// 2a if stream empty, but incoming request includes an event -> generate a batch document + create empty Tip
// 2b if no current Tip, and no events being written -> the incoming `req` becomes the Tip batch

function sync(req, expIndex, expEtag, maxEventsInTip, maxStringifyLen) {
    if (!req) throw new Error("Missing req argument");
    const collectionLink = __.getSelfLink();
    const response = getContext().getResponse();
    // Locate the Tip (-1) batch for this stream (which may not exist)
    const tipDocId = __.getAltLink() + "/docs/" + req.id;
    const isAccepted = __.readDocument(tipDocId, {}, function (err, current) {
        // Verify we dont have a conflicting write
        if (expIndex === -1) {
            // For Any mode, we always do an append operation
            executeUpsert(current);
        } else if (!current && ((expIndex === -2 && expEtag !== null) || expIndex > 0)) {
            // If there is no Tip page, the writer has no possible reason for writing at an index other than zero, and an etag exp must be fulfilled
            response.setBody({ etag: null, n: 0, conflicts: [], e: [] });
        } else if (current && ((expIndex === -2 && expEtag !== current._etag) || (expIndex !== -2 && expIndex !== current.n))) {
            // Where possible, we extract conflicting events from e and/or u in order to avoid another read cycle;
            // yielding [] triggers the client to go loading the events itself
            // if we're working based on etags, the `u`nfolds likely bear relevant info as state-bearing unfolds
            const recentEvents = expIndex < current.i ? [] : current.e.slice(expIndex - current.i);
            response.setBody({ etag: current._etag, n: current.n, conflicts: current.u || [], e: recentEvents });
        } else {
            executeUpsert(current);
        }
    });
    if (!isAccepted) throw new Error("readDocument not Accepted");
    function executeUpsert(tip) {
        function callback(err, doc) {
            if (err) throw err;
            response.setBody({ etag: doc._etag, n: doc.n, conflicts: null, e: [] });
        }
        function shouldCalveBatch(events) {
            return events.length > maxEventsInTip || JSON.stringify(events).length > maxStringifyLen;
        }
        if (tip) {
            Array.prototype.push.apply(tip.e, req.e);
            tip.n = tip.i + tip.e.length;
            // If there are events, calve them to their own batch (this behavior is to simplify CFP consumer impl)
            if (shouldCalveBatch(tip.e)) {
                const batch = { id: tip.i.toString(), p: tip.p, i: tip.i, n: tip.n, e: tip.e }
                const batchAccepted = __.createDocument(collectionLink, batch, { disableAutomaticIdGeneration: true });
                if (!batchAccepted) throw new Error("Unable to remove Tip markings.");
                tip.i = tip.n;
                tip.e = [];
            }
            // TODO Carry forward `u` items not present in `batch`, together with supporting catchup events from preceding batches
            // Replace all the unfolds
            // TODO: should remove only unfolds being superseded
            tip.u = req.u;
            // As we've mutated the document in a manner that can conflict with other writers, our write needs to be contingent on no competing updates having taken place
            const isAccepted = __.replaceDocument(tip._self, tip, { etag: tip._etag }, callback);
            if (!isAccepted) throw new Error("Unable to replace Tip batch.");
        } else {
            // NOTE we write the batch first (more consistent RU cost than writing tip first)
            if (shouldCalveBatch(req.e)) {
                const batch = { id: "0", p: req.p, i: 0, n: req.e.length, e: req.e };
                const batchAccepted = __.createDocument(collectionLink, batch, { disableAutomaticIdGeneration: true });
                if (!batchAccepted) throw new Error("Unable to create Batch 0.");
                req.i = batch.n;
                req.e = [];
            } else {
                req.i = 0;
            }
            req.n = req.i + req.e.length;
            const isAccepted = __.createDocument(collectionLink, req, { disableAutomaticIdGeneration: true }, callback);
            if (!isAccepted) throw new Error("Unable to create Tip batch.");
        }
    }
}"""

[<RequireQualifiedAccess>]
type internal SyncExp = Version of int64 | Etag of string | Any

module internal Sync =

    // NB don't nest in a private module, or serialization will fail miserably ;)
    [<RequireQualifiedAccess; NoEquality; NoComparison>]
    type Result =
        | Written of Position
        | Conflict of Position * events : ITimelineEvent<EventBody>[]
        | ConflictUnknown of Position

    let private run (container : Container, stream : string) (maxEventsInTip, maxStringifyLen) (exp, req : Tip)
        : Async<float*Result> = async {
        let ep =
            match exp with
            | SyncExp.Version ev -> Position.fromI ev
            | SyncExp.Etag et -> Position.fromEtag et
            | SyncExp.Any -> Position.fromAppendAtEnd
        let! ct = Async.CancellationToken
        let args = [| box req; box ep.index; box (Option.toObj ep.etag); box maxEventsInTip; box maxStringifyLen |]
        let! (res : Scripts.StoredProcedureExecuteResponse<SyncResponse>) =
            container.Scripts.ExecuteStoredProcedureAsync<SyncResponse>(SyncStoredProc.name, PartitionKey stream, args, cancellationToken = ct) |> Async.AwaitTaskCorrect
        let newPos = { index = res.Resource.n; etag = Option.ofObj res.Resource.etag }
        match res.Resource.conflicts with
        | null -> return res.RequestCharge, Result.Written newPos
        // ConflictUnknown is to be yielded if we believe querying is going to be necessary (as there are no unfolds, and no relevant events in the Tip)
        | [||] when res.Resource.e.Length = 0 && newPos.index > ep.index ->
            return res.RequestCharge, Result.ConflictUnknown newPos
        | unfolds -> // stored proc only returns events and unfolds with index >= req.i - no need to trim to a minIndex
            let events = (Enum.Events(ep.index, res.Resource.e), Enum.Unfolds unfolds) ||> Seq.append |> Array.ofSeq
            return res.RequestCharge, Result.Conflict (newPos, events) }

    let private logged (container, stream) (maxEventsInTip, maxStringifyLen) (exp : SyncExp, req : Tip) (log : ILogger)
        : Async<Result> = async {
        let! t, (ru, result) = run (container, stream) (maxEventsInTip, maxStringifyLen) (exp, req) |> Stopwatch.Time
        let verbose = log.IsEnabled Serilog.Events.LogEventLevel.Debug
        let count, bytes = req.e.Length, if verbose then Enum.Events req |> Log.batchLen else 0
        let log =
            let inline mkMetric ru : Log.Measurement =
                { database = container.Database.Id; container = container.Id; stream = stream; interval = t; bytes = bytes; count = count; ru = ru }
            let inline propConflict log = log |> Log.prop "conflict" true |> Log.prop "eventTypes" (Seq.truncate 5 (seq { for x in req.e -> x.c }))
            if verbose then log |> Log.propEvents (Enum.Events req) |> Log.propDataUnfolds req.u else log
            |> match exp with
                | SyncExp.Etag et ->     Log.prop "expectedEtag" et
                | SyncExp.Version ev ->  Log.prop "expectedVersion" ev
                | SyncExp.Any ->         Log.prop "expectedVersion" -1
            |> match result with
                | Result.Written pos ->
                    Log.prop "nextExpectedVersion" pos >> Log.event (Log.Metric.SyncSuccess (mkMetric ru))
                | Result.ConflictUnknown pos' ->
                    Log.prop "nextExpectedVersion" pos' >> propConflict >> Log.event (Log.Metric.SyncConflict (mkMetric ru))
                | Result.Conflict (pos', xs) ->
                    if verbose then Log.propData "conflicts" xs else id
                    >> Log.prop "nextExpectedVersion" pos' >> propConflict >> Log.event (Log.Metric.SyncResync (mkMetric ru))
        log.Information("EqxCosmos {action:l} {stream} {count}+{ucount} {ms:f1}ms {ru}RU {bytes:n0}b {exp}",
            "Sync", stream, count, req.u.Length, (let e = t.Elapsed in e.TotalMilliseconds), ru, bytes, exp)
        return result }

    let batch (log : ILogger) (retryPolicy, maxEventsInTip, maxStringifyLen) containerStream expBatch : Async<Result> =
        let call = logged containerStream (maxEventsInTip, maxStringifyLen) expBatch
        Log.withLoggedRetries retryPolicy "writeAttempt" call log

    let private mkEvent (e : IEventData<_>) =
        {   t = e.Timestamp
            c = e.EventType; d = JsonElement.undefinedToNull e.Data; m = JsonElement.undefinedToNull e.Meta
            correlationId = e.CorrelationId; causationId = e.CausationId }
    let mkBatch (stream : string) (events : IEventData<_>[]) unfolds : Tip =
        {   p = stream; id = Tip.WellKnownDocumentId; n = -1L(*Server-managed*); i = -1L(*Server-managed*); _etag = null
            e = Array.map mkEvent events; u = Array.ofSeq unfolds }
    let mkUnfold compressor baseIndex (x : IEventData<_>) : Unfold =
        {   i = baseIndex; t = x.Timestamp
            c = x.EventType; d = compressor x.Data; m = compressor x.Meta }

module Initialization =

    // Note: the Cosmos SDK does not (currently) support changing the Throughput mode of an existing Database or Container.
    type [<RequireQualifiedAccess>] Throughput = Manual of rus : int | Autoscale of maxRus : int
    type [<RequireQualifiedAccess>] Provisioning = Container of Throughput | Database of Throughput | Serverless
    let private (|ThroughputProperties|) = function
        | Throughput.Manual rus -> ThroughputProperties.CreateManualThroughput(rus)
        | Throughput.Autoscale maxRus -> ThroughputProperties.CreateAutoscaleThroughput(maxRus)
    let private createOrProvisionDatabase (client : CosmosClient) dName mode = async {
        let! ct = Async.CancellationToken
        let createDatabaseIfNotExists maybeTp = async {
            let! r = client.CreateDatabaseIfNotExistsAsync(dName, throughputProperties = Option.toObj maybeTp, cancellationToken = ct) |> Async.AwaitTaskCorrect
            return r.Database }
        match mode with
        | Provisioning.Container _ | Provisioning.Serverless -> return! createDatabaseIfNotExists None
        | Provisioning.Database (ThroughputProperties tp) ->
            let! d = createDatabaseIfNotExists (Some tp)
            let! _ = d.ReplaceThroughputAsync(tp, cancellationToken = ct) |> Async.AwaitTaskCorrect
            return d }
    let private createOrProvisionContainer (d : Database) (cName, pkPath, customizeContainer) mode = async {
        let cp = ContainerProperties(id = cName, partitionKeyPath = pkPath)
        customizeContainer cp
        let! ct = Async.CancellationToken
        let createContainerIfNotExists maybeTp = async {
            let! r = d.CreateContainerIfNotExistsAsync(cp, throughputProperties = Option.toObj maybeTp, cancellationToken = ct) |> Async.AwaitTaskCorrect
            return r.Container }
        match mode with
        | Provisioning.Database _ | Provisioning.Serverless -> return! createContainerIfNotExists None
        | Provisioning.Container (ThroughputProperties throughput) ->
            let! c = createContainerIfNotExists (Some throughput)
            let! _ = c.ReplaceThroughputAsync(throughput, cancellationToken = ct) |> Async.AwaitTaskCorrect
            return c }

    let private createStoredProcIfNotExists (c : Container) (name, body) : Async<float> = async {
        let! ct = Async.CancellationToken
        try let! r = c.Scripts.CreateStoredProcedureAsync(Scripts.StoredProcedureProperties(id = name, body = body), cancellationToken = ct) |> Async.AwaitTaskCorrect
            return r.RequestCharge
        with :? CosmosException as ce when ce.StatusCode = System.Net.HttpStatusCode.Conflict -> return ce.RequestCharge }
    let private applyBatchAndTipContainerProperties (cp : ContainerProperties) =
        cp.IndexingPolicy.IndexingMode <- IndexingMode.Consistent
        cp.IndexingPolicy.Automatic <- true
        // Can either do a blacklist or a whitelist
        // Given how long and variable the blacklist would be, we whitelist instead
        cp.IndexingPolicy.ExcludedPaths.Add(ExcludedPath(Path="/*"))
        // NB its critical to index the nominated PartitionKey field defined above or there will be runtime errors
        for k in Batch.IndexedFields do cp.IndexingPolicy.IncludedPaths.Add(IncludedPath(Path = sprintf "/%s/?" k))
    let createSyncStoredProcIfNotExists (log : ILogger option) container = async {
        let! t, ru = createStoredProcIfNotExists container (SyncStoredProc.name, SyncStoredProc.body) |> Stopwatch.Time
        match log with
        | None -> ()
        | Some log -> log.Information("Created stored procedure {procName} in {ms:f1}ms {ru}RU", SyncStoredProc.name, (let e = t.Elapsed in e.TotalMilliseconds), ru) }
    let init log (client : CosmosClient) (dName, cName) mode skipStoredProc = async {
        let! d = createOrProvisionDatabase client dName mode
        let! c = createOrProvisionContainer d (cName, sprintf "/%s" Batch.PartitionKeyField, applyBatchAndTipContainerProperties) mode // as per Cosmos team, Partition Key must be "/id"
        if not skipStoredProc then
            do! createSyncStoredProcIfNotExists (Some log) c }

    let private applyAuxContainerProperties (cp : ContainerProperties) =
        // TL;DR no indexing of any kind; see https://github.com/Azure/azure-documentdb-changefeedprocessor-dotnet/issues/142
        cp.IndexingPolicy.Automatic <- false
        cp.IndexingPolicy.IndexingMode <- IndexingMode.None

    let initAux (client : CosmosClient) (dName, cName) mode = async {
        let! d = createOrProvisionDatabase client dName mode
        return! createOrProvisionContainer d (cName, "/id", applyAuxContainerProperties) mode } // as per Cosmos team, Partition Key must be "/id"

    /// Holds Container state, coordinating initialization activities
    type internal ContainerInitializerGuard(container : Container, fallback : Container option, ?initContainer : Container -> CancellationToken -> Task<unit>) =
        let initGuard = initContainer |> Option.map (fun init -> AsyncCacheCell<unit>(init container))

        member _.Container = container
        member _.Fallback = fallback
        member internal _.InitializationGate = match initGuard with Some g when not (g.IsValid())  -> ValueSome g.Await | _ -> ValueNone

module internal Tip =

    let private get (container : Container, stream : string) (maybePos : Position option) =
        let ro = match maybePos with Some { etag = Some etag } -> ItemRequestOptions(IfNoneMatchEtag = etag) |> Some | _ -> None
        container.TryReadItem<Tip>(PartitionKey stream, Tip.WellKnownDocumentId, ?options = ro)
    let private loggedGet (get : Container * string -> Position option -> Async<_>) (container, stream) (maybePos : Position option) (log : ILogger) = async {
        let log = log |> Log.prop "stream" stream
        let! t, (ru, res : ReadResult<Tip>) = get (container, stream) maybePos |> Stopwatch.Time
        let verbose = log.IsEnabled Events.LogEventLevel.Debug
        let log bytes count (f : Log.Measurement -> _) = log |> Log.event (f { database = container.Database.Id; container = container.Id; stream = stream; interval = t; bytes = bytes; count = count; ru = ru })
        match res with
        | ReadResult.NotModified ->
            (log 0 0 Log.Metric.TipNotModified).Information("EqxCosmos {action:l} {stream} {res} {ms:f1}ms {ru}RU", "Tip", stream, 304, (let e = t.Elapsed in e.TotalMilliseconds), ru)
        | ReadResult.NotFound ->
            (log 0 0 Log.Metric.TipNotFound).Information("EqxCosmos {action:l} {stream} {res} {ms:f1}ms {ru}RU", "Tip", stream, 404, (let e = t.Elapsed in e.TotalMilliseconds), ru)
        | ReadResult.Found tip ->
            let log =
                let count, bytes = tip.u.Length, if verbose then Enum.Unfolds tip.u |> Log.batchLen else 0
                log bytes count Log.Metric.Tip
            let log = if verbose then log |> Log.propDataUnfolds tip.u else log
            let log = match maybePos with Some p -> log |> Log.propStartPos p |> Log.propStartEtag p | None -> log
            let log = log |> Log.prop "_etag" tip._etag |> Log.prop "n" tip.n
            log.Information("EqxCosmos {action:l} {stream} {res} {ms:f1}ms {ru}RU", "Tip", stream, 200, (let e = t.Elapsed in e.TotalMilliseconds), ru)
        return ru, res }
    type [<RequireQualifiedAccess; NoComparison; NoEquality>] Result = NotModified | NotFound | Found of Position * i : int64 * ITimelineEvent<EventBody>[]
    /// `pos` being Some implies that the caller holds a cached value and hence is ready to deal with Result.NotModified
    let tryLoad (log : ILogger) retryPolicy containerStream (maybePos : Position option, maxIndex) : Async<Result> = async {
        let! _rc, res = Log.withLoggedRetries retryPolicy "readAttempt" (loggedGet get containerStream maybePos) log
        match res with
        | ReadResult.NotModified -> return Result.NotModified
        | ReadResult.NotFound -> return Result.NotFound
        | ReadResult.Found tip ->
            let minIndex = maybePos |> Option.map (fun x -> x.index)
            return Result.Found (Position.fromTip tip, tip.i, Enum.EventsAndUnfolds(tip, ?maxIndex = maxIndex, ?minIndex = minIndex) |> Array.ofSeq) }

module internal Query =

    let feedIteratorMapTi (map : int -> StopwatchInterval -> FeedResponse<'t> -> 'u) (query : FeedIterator<'t>) : AsyncSeq<'u> =
        let rec loop i : AsyncSeq<'u> = asyncSeq {
            if not query.HasMoreResults then return None else
            let! ct = Async.CancellationToken
            let! t, (res : FeedResponse<'t>) = query.ReadNextAsync(ct) |> Async.AwaitTaskCorrect |> Stopwatch.Time
            yield map i t res
            if query.HasMoreResults then
                yield! loop (i + 1) }
        // earlier versions, such as 3.9.0, do not implement IDisposable; see linked issue for detail on when SDK team added it
        use _ = query // see https://github.com/jet/equinox/issues/225 - in the Cosmos V4 SDK, all this is managed IAsyncEnumerable
        loop 0
    let private mkQuery (log : ILogger) (container : Container, stream : string) includeTip (maxItems : int) (direction : Direction, minIndex, maxIndex) : FeedIterator<Batch> =
        let order = if direction = Direction.Forward then "ASC" else "DESC"
        let query =
            let args = [
                 match minIndex with None -> () | Some x -> yield "c.n > @minPos", fun (q : QueryDefinition) -> q.WithParameter("@minPos", x)
                 match maxIndex with None -> () | Some x -> yield "c.i < @maxPos", fun (q : QueryDefinition) -> q.WithParameter("@maxPos", x) ]
            let whereClause =
                let notTip = sprintf "c.id!=\"%s\"" Tip.WellKnownDocumentId
                let conditions = Seq.map fst args
                if List.isEmpty args && includeTip then null
                else "WHERE " + String.Join(" AND ", if includeTip then conditions else Seq.append conditions (Seq.singleton notTip))
            let queryString = sprintf "SELECT c.id, c.i, c._etag, c.n, c.e FROM c %s ORDER BY c.i %s" whereClause order
            let prams = Seq.map snd args
            (QueryDefinition queryString, prams) ||> Seq.fold (fun q wp -> q |> wp)
        log.Debug("EqxCosmos Query {stream} {query}; n>{minIndex} i<{maxIndex}", stream, query.QueryText, Option.toNullable minIndex, Option.toNullable maxIndex)
        container.GetItemQueryIterator<Batch>(query, requestOptions = QueryRequestOptions(PartitionKey = PartitionKey stream, MaxItemCount = maxItems))

    // Unrolls the Batches in a response
    // NOTE when reading backwards, the events are emitted in reverse Index order to suit the takeWhile consumption
    let private mapPage direction (container : Container, streamName : string) (minIndex, maxIndex) (maxRequests : int option)
            (log : ILogger) i t (res : FeedResponse<Batch>)
        : ITimelineEvent<EventBody>[] * Position option * float =
        let log = log |> Log.prop "batchIndex" i
        match maxRequests with
        | Some mr when i >= mr -> log.Information "batch Limit exceeded"; invalidOp "batch Limit exceeded"
        | _ -> ()
        let batches, ru = Array.ofSeq res, res.RequestCharge
        let unwrapBatch (b : Batch) =
            Enum.Events(b, ?minIndex = minIndex, ?maxIndex = maxIndex)
            |> if direction = Direction.Backward then Seq.rev else id
        let events = batches |> Seq.collect unwrapBatch |> Array.ofSeq
        let verbose = log.IsEnabled Events.LogEventLevel.Debug
        let count, bytes = events.Length, if verbose then events |> Log.batchLen else 0
        let reqMetric : Log.Measurement = { database = container.Database.Id; container = container.Id; stream = streamName; interval = t; bytes = bytes; count = count; ru = ru }
        let log = let evt = Log.Metric.QueryResponse (direction, reqMetric) in log |> Log.event evt
        let log = if verbose then log |> Log.propEvents events else log
        let index = if count = 0 then Nullable () else Nullable <| Seq.min (seq { for x in batches -> x.i })
        (log|> Log.prop "bytes" bytes
            |> match minIndex with None -> id | Some i -> Log.prop "minIndex" i
            |> match maxIndex with None -> id | Some i -> Log.prop "maxIndex" i)
            .Information("EqxCosmos {action:l} {count}/{batches} {direction} {ms:f1}ms i={index} {ru}RU",
                "Response", count, batches.Length, direction, (let e = t.Elapsed in e.TotalMilliseconds), index, ru)
        let maybePosition = batches |> Array.tryPick Position.tryFromBatch
        events, maybePosition, ru

    let private logQuery direction (container : Container, streamName) interval (responsesCount, events : ITimelineEvent<EventBody>[]) n (ru : float) (log : ILogger) =
        let verbose = log.IsEnabled Events.LogEventLevel.Debug
        let count, bytes = events.Length, if verbose then events |> Log.batchLen else 0
        let reqMetric : Log.Measurement = { database = container.Database.Id; container = container.Id; stream = streamName; interval = interval; bytes = bytes; count = count; ru = ru }
        let evt = Log.Metric.Query (direction, responsesCount, reqMetric)
        let action = match direction with Direction.Forward -> "QueryF" | Direction.Backward -> "QueryB"
        (log |> Log.prop "bytes" bytes |> Log.event evt).Information(
            "EqxCosmos {action:l} {stream} v{n} {count}/{responses} {ms:f1}ms {ru}RU",
            action, streamName, n, count, responsesCount, (let e = interval.Elapsed in e.TotalMilliseconds), ru)

    let private calculateUsedVersusDroppedPayload stopIndex (xs : ITimelineEvent<EventBody>[]) : int * int =
        let mutable used, dropped = 0, 0
        let mutable found = false
        for x in xs do
            let bytes = Log.eventLen x
            if found then dropped <- dropped + bytes
            else used <- used + bytes
            if x.Index = stopIndex then found <- true
        used, dropped

    [<RequireQualifiedAccess; NoComparison; NoEquality>]
    type ScanResult<'event> = { found : bool; minIndex : int64; next : int64; maybeTipPos : Position option; events : 'event[] }

    let scanTip (tryDecode : #IEventData<EventBody> -> 'event voption, isOrigin : 'event -> bool) (pos : Position, i : int64, xs : #ITimelineEvent<EventBody>[]) : ScanResult<'event> =
        let items = ResizeArray()
        let isOrigin' e =
            match tryDecode e with
            | ValueNone -> false
            | ValueSome e ->
                items.Insert(0, e) // WalkResult always renders events ordered correctly - here we're aiming to align with Enum.EventsAndUnfolds
                isOrigin e
        let f, e = xs |> Seq.tryFindBack isOrigin' |> Option.isSome, items.ToArray()
        { found = f; maybeTipPos = Some pos; minIndex = i; next = pos.index + 1L; events = e }

    // Yields events in ascending Index order
    let scan<'event> (log : ILogger) (container, stream) includeTip (maxItems : int) maxRequests direction
        (tryDecode : ITimelineEvent<EventBody> -> 'event voption, isOrigin : 'event -> bool)
        (minIndex, maxIndex)
        : Async<ScanResult<'event> option> = async {
        let mutable found = false
        let mutable responseCount = 0
        let mergeBatches (log : ILogger) (batchesBackward : AsyncSeq<ITimelineEvent<EventBody>[] * Position option * float>) = async {
            let mutable lastResponse, maybeTipPos, ru = None, None, 0.
            let! events =
                batchesBackward
                |> AsyncSeq.map (fun (events, maybePos, r) ->
                    if maybeTipPos = None then maybeTipPos <- maybePos
                    lastResponse <- Some events; ru <- ru + r
                    responseCount <- responseCount + 1
                    seq { for x in events -> struct (x, tryDecode x) })
                |> AsyncSeq.concatSeq
                |> AsyncSeq.takeWhileInclusive (function
                    | struct (x, ValueSome e) when isOrigin e ->
                        found <- true
                        match lastResponse with
                        | None -> log.Information("EqxCosmos Stop stream={stream} at={index} {case}", stream, x.Index, x.EventType)
                        | Some batch ->
                            let used, residual = batch |> calculateUsedVersusDroppedPayload x.Index
                            log.Information("EqxCosmos Stop stream={stream} at={index} {case} used={used} residual={residual}",
                                stream, x.Index, x.EventType, used, residual)
                        false
                    | _ -> true)
                |> AsyncSeq.toArrayAsync
            return events, maybeTipPos, ru }
        let log = log |> Log.prop "batchSize" maxItems |> Log.prop "stream" stream
        let readLog = log |> Log.prop "direction" direction
        let batches : AsyncSeq<ITimelineEvent<EventBody>[] * Position option * float> =
            mkQuery readLog (container, stream) includeTip maxItems (direction, minIndex, maxIndex)
            |> feedIteratorMapTi (mapPage direction (container, stream) (minIndex, maxIndex) maxRequests readLog)
        let! t, (events, maybeTipPos, ru) = mergeBatches log batches |> Stopwatch.Time
        let raws = Array.map ValueTuple.fst events
        let decoded = if direction = Direction.Forward then Array.chooseV ValueTuple.snd events else Seq.chooseV ValueTuple.snd events |> Seq.rev |> Array.ofSeq
        let minMax = (None, raws) ||> Array.fold (fun acc x -> let i = x.Index in Some (match acc with None -> i, i | Some (n, x) -> min n i, max x i))
        let version =
            match maybeTipPos, minMax with
            | Some { index = max }, _
            | _, Some (_, max) -> max + 1L
            | None, None -> 0L
        log |> logQuery direction (container, stream) t (responseCount, raws) version ru
        match minMax, maybeTipPos with
        | Some (i, m), _ -> return Some { found = found; minIndex = i; next = m + 1L; maybeTipPos = maybeTipPos; events = decoded }
        | None, Some { index = tipI } -> return Some { found = found; minIndex = tipI; next = tipI; maybeTipPos = maybeTipPos; events = [||] }
        | None, _ -> return None }

    let walkLazy<'event> (log : ILogger) (container, stream) maxItems maxRequests
        (tryDecode : ITimelineEvent<EventBody> -> 'event option, isOrigin : 'event -> bool)
        (direction, minIndex, maxIndex)
        : AsyncSeq<'event[]> = asyncSeq {
        let query = mkQuery log (container, stream) true maxItems (direction, minIndex, maxIndex)

        let readPage = mapPage direction (container, stream) (minIndex, maxIndex) maxRequests
        let log = log |> Log.prop "batchSize" maxItems |> Log.prop "stream" stream
        let readLog = log |> Log.prop "direction" direction
        let query = query |> feedIteratorMapTi (readPage readLog)
        let startTicks = System.Diagnostics.Stopwatch.GetTimestamp()
        let allEvents = ResizeArray()
        let mutable i, ru = 0, 0.
        try let mutable ok = true
            let e = query.GetEnumerator()
            while ok do
                let batchLog = readLog |> Log.prop "batchIndex" i
                match maxRequests with
                | Some mr when i + 1 >= mr -> batchLog.Information "batch Limit exceeded"; invalidOp "batch Limit exceeded"
                | _ -> ()

                match! e.MoveNext() with
                | None -> ok <- false // rest of block does not happen, while exits
                | Some (events, _pos, rus) ->

                ru <- ru + rus
                allEvents.AddRange(events)

                let acc = ResizeArray()
                for x in events do
                    match tryDecode x with
                    | Some e when isOrigin e ->
                        let used, residual = events |> calculateUsedVersusDroppedPayload x.Index
                        log.Information("EqxCosmos Stop stream={stream} at={index} {case} used={used} residual={residual}",
                            stream, x.Index, x.EventType, used, residual)
                        ok <- false
                        acc.Add e
                    | Some e -> acc.Add e
                    | None -> ()
                i <- i + 1
                yield acc.ToArray()
        finally
            let endTicks = System.Diagnostics.Stopwatch.GetTimestamp()
            let t = StopwatchInterval(startTicks, endTicks)
            log |> logQuery direction (container, stream) t (i, allEvents.ToArray()) -1L ru }

    /// Manages coalescing of spans of events obtained from various sources:
    /// 1) Tip Data and/or Conflicting events
    /// 2) Querying Primary for predecessors of what's obtained from 1
    /// 3) Querying Archive for predecessors of what's obtained from 2
    let load (log : ILogger) (minIndex, maxIndex) (tip : ScanResult<'event> option)
            (primary : int64 option * int64 option -> Async<ScanResult<'event> option>)
            // Choice1Of2 -> indicates whether it's acceptable to ignore missing events; Choice2Of2 -> Fallback store
            (fallback : Choice<bool, int64 option * int64 option -> Async<ScanResult<'event> option>>)
            : Async<Position * 'event[]> = async {
        let minI = defaultArg minIndex 0L
        match tip with
        | Some { found = true; maybeTipPos = Some p; events = e } -> return p, e
        | Some { minIndex = i; maybeTipPos = Some p; events = e } when i <= minI -> return p, e
        | _ ->

        let i, events, pos =
            match tip with
            | Some { minIndex = i; maybeTipPos = p; events = e } -> Some i, e, p
            | None -> maxIndex, Array.empty, None
        let! primary = primary (minIndex, i)
        let events, pos =
            match primary with
            | None -> events, pos |> Option.defaultValue Position.fromKnownEmpty
            | Some p -> Array.append p.events events, pos |> Option.orElse p.maybeTipPos |> Option.defaultValue (Position.fromI p.next)
        let inline logMissing (minIndex, maxIndex) message =
            if log.IsEnabled Events.LogEventLevel.Debug then
                (log|> fun log -> match minIndex with None -> log | Some mi -> log |> Log.prop "minIndex" mi
                    |> fun log -> match maxIndex with None -> log | Some mi -> log |> Log.prop "maxIndex" mi)
                    .Debug(message)

        match primary, fallback with
        | Some { found = true }, _ -> return pos, events // origin found in primary, no need to look in fallback
        | Some { minIndex = i }, _ when i <= minI -> return pos, events // primary had required earliest event Index, no need to look at fallback
        | None, _ when Option.isNone tip -> return pos, events // initial load where no documents present in stream
        | _, Choice1Of2 allowMissing ->
            logMissing (minIndex, i) "Origin event not found; no Archive Container supplied"
            if allowMissing then return pos, events
            else return failwithf "Origin event not found; no Archive Container supplied"
        | _, Choice2Of2 fallback ->

        let maxIndex = match primary with Some p -> Some p.minIndex | None -> maxIndex // if no batches in primary, high water mark from tip is max
        let! fallback = fallback (minIndex, maxIndex)
        let events =
            match fallback with
            | Some s -> Array.append s.events events
            | None -> events
        match fallback with
        | Some { minIndex = i } when i <= minI -> ()
        | Some { found = true } -> ()
        | _ -> logMissing (minIndex, maxIndex) "Origin event not found in Archive Container"
        return pos, events }

// Manages deletion of (full) Batches, and trimming of events in Tip, maintaining ordering guarantees by never updating non-Tip batches
// Additionally, the nature of the fallback algorithm requires that deletions be carried out in sequential order so as not to leave gaps
// NOTE: module is public so BatchIndices can be deserialized into
module Prune =

    type BatchIndices = { id : string; i : int64; n : int64 }

    let until (log : ILogger) (container : Container, stream : string) (maxItems : int) indexInclusive : Async<int * int * int64> = async {
        let log = log |> Log.prop "stream" stream
        let! ct = Async.CancellationToken
        let deleteItem id count : Async<float> = async {
            let ro = ItemRequestOptions(EnableContentResponseOnWrite = false) // https://devblogs.microsoft.com/cosmosdb/enable-content-response-on-write/
            let! t, res = container.DeleteItemAsync(id, PartitionKey stream, ro, ct) |> Async.AwaitTaskCorrect |> Stopwatch.Time
            let rc, ms = res.RequestCharge, (let e = t.Elapsed in e.TotalMilliseconds)
            let reqMetric : Log.Measurement = { database = container.Database.Id; container = container.Id; stream = stream; interval = t; bytes = -1; count = count; ru = rc }
            let log = let evt = Log.Metric.Delete reqMetric in log |> Log.event evt
            log.Information("EqxCosmos {action:l} {id} {ms:f1}ms {ru}RU", "Delete", id, ms, rc)
            return rc
        }
        let trimTip expectedI count = async {
            match! container.TryReadItem<Tip>(PartitionKey stream, Tip.WellKnownDocumentId) with
            | _, ReadResult.NotModified -> return failwith "unexpected NotModified; no etag supplied"
            | _, ReadResult.NotFound -> return failwith "unexpected NotFound"
            | _, ReadResult.Found tip when tip.i <> expectedI -> return failwithf "Concurrent write detected; Expected i=%d actual=%d" expectedI tip.i
            | tipRu, ReadResult.Found tip ->

            let tip = { tip with i = tip.i + int64 count; e = Array.skip count tip.e }
            let ro = ItemRequestOptions(EnableContentResponseOnWrite = false, IfMatchEtag = tip._etag)
            let! t, updateRes = container.ReplaceItemAsync(tip, tip.id, PartitionKey stream, ro, ct) |> Async.AwaitTaskCorrect |> Stopwatch.Time
            let rc, ms = tipRu + updateRes.RequestCharge, (let e = t.Elapsed in e.TotalMilliseconds)
            let reqMetric : Log.Measurement = { database = container.Database.Id; container = container.Id; stream = stream; interval = t; bytes = -1; count = count; ru = rc }
            let log = let evt = Log.Metric.Trim reqMetric in log |> Log.event evt
            log.Information("EqxCosmos {action:l} {count} {ms:f1}ms {ru}RU", "Trim", count, ms, rc)
            return rc
        }
        let log = log |> Log.prop "index" indexInclusive
        let query : FeedIterator<BatchIndices> =
             let qro = QueryRequestOptions(PartitionKey = PartitionKey stream, MaxItemCount = maxItems)
             // sort by i to guarantee we don't ever leave an observable gap in the sequence
             container.GetItemQueryIterator<_>("SELECT c.id, c.i, c.n FROM c ORDER by c.i", requestOptions = qro)
        let mapPage i (t : StopwatchInterval) (page : FeedResponse<BatchIndices>) =
            let batches, rc, ms = Array.ofSeq page, page.RequestCharge, (let e = t.Elapsed in e.TotalMilliseconds)
            let next = Array.tryLast batches |> Option.map (fun x -> x.n)
            let reqMetric : Log.Measurement = { database = container.Database.Id; container = container.Id; stream = stream; interval = t; bytes = -1; count = batches.Length; ru = rc }
            let log = let evt = Log.Metric.PruneResponse reqMetric in log |> Log.prop "batchIndex" i |> Log.event evt
            log.Information("EqxCosmos {action:l} {batches} {ms:f1}ms n={next} {ru}RU", "PruneResponse", batches.Length, ms, Option.toNullable next, rc)
            batches, rc
        let! pt, outcomes =
            let isTip (x : BatchIndices) = x.id = Tip.WellKnownDocumentId
            let isRelevant x = x.i <= indexInclusive || isTip x
            let handle (batches : BatchIndices[], rc) = async {
                let mutable delCharges, batchesDeleted, trimCharges, batchesTrimmed, eventsDeleted, eventsDeferred = 0., 0, 0., 0, 0, 0
                let mutable lwm = None
                for x in batches |> Seq.takeWhile (fun x -> isRelevant x || lwm = None) do
                    let batchSize = x.n - x.i |> int
                    let eligibleEvents = max 0 (min batchSize (int (indexInclusive + 1L - x.i)))
                    if isTip x then // Even if we remove the last event from the Tip, we need to retain a) unfolds b) position (n)
                        if eligibleEvents <> 0 then
                            let! charge = trimTip x.i eligibleEvents
                            trimCharges <- trimCharges + charge
                            batchesTrimmed <- batchesTrimmed + 1
                            eventsDeleted <- eventsDeleted + eligibleEvents
                        if lwm = None then
                            lwm <- Some (x.i + int64 eligibleEvents)
                    elif x.n <= indexInclusive + 1L then
                        let! charge = deleteItem x.id batchSize
                        delCharges <- delCharges + charge
                        batchesDeleted <- batchesDeleted + 1
                        eventsDeleted <- eventsDeleted + batchSize
                    else // can't update a non-Tip batch, or it'll be ordered wrong from a CFP perspective
                        eventsDeferred <- eventsDeferred + eligibleEvents
                        if lwm = None then
                            lwm <- Some x.i
                return (rc, delCharges, trimCharges), lwm, (batchesDeleted + batchesTrimmed, eventsDeleted, eventsDeferred)
            }
            let hasRelevantItems (batches, _rc) = batches |> Array.exists isRelevant
            query
            |> Query.feedIteratorMapTi mapPage
            |> AsyncSeq.takeWhile hasRelevantItems
            |> AsyncSeq.mapAsync handle
            |> AsyncSeq.toArrayAsync
            |> Stopwatch.Time
        let mutable queryCharges, delCharges, trimCharges, responses, batches, eventsDeleted, eventsDeferred = 0., 0., 0., 0, 0, 0, 0
        let mutable lwm = None
        for (qc, dc, tc), bLwm, (bCount, eDel, eDef) in outcomes do
            lwm <- max lwm bLwm
            queryCharges <- queryCharges + qc
            delCharges <- delCharges + dc
            trimCharges <- trimCharges + tc
            responses <- responses + 1
            batches <- batches + bCount
            eventsDeleted <- eventsDeleted + eDel
            eventsDeferred <- eventsDeferred + eDef
        let reqMetric : Log.Measurement = { database = container.Database.Id; container = container.Id; stream = stream; interval = pt; bytes = eventsDeleted; count = batches; ru = queryCharges }
        let log = let evt = Log.Metric.Prune (responses, reqMetric) in log |> Log.event evt
        let lwm = lwm |> Option.defaultValue 0L // If we've seen no batches at all, then the write position is 0L
        log.Information("EqxCosmos {action:l} {events}/{batches} lwm={lwm} {ms:f1}ms queryRu={queryRu} deleteRu={deleteRu} trimRu={trimRu}",
                "Prune", eventsDeleted, batches, lwm, (let e = pt.Elapsed in e.TotalMilliseconds), queryCharges, delCharges, trimCharges)
        return eventsDeleted, eventsDeferred, lwm
    }

type [<NoComparison>] Token = { pos : Position }
module Token =

    // TOCONSIDER Could implement accumulating the streamBytes as it's loaded (but should stop counting when it hits the origin event)
    //            Should probably be optional as computing the UTF8/16 size from the JsonElement is not cheap
    //            Alternately, can mirror DynamoStore scheme where the size is maintained in the Tip, and updated as batches are calved
    let create pos : StreamToken = { value = box { pos = pos }; version = pos.index; streamBytes = -1 }
    let (|Unpack|) (token : StreamToken) : Position = let t = unbox<Token> token.value in t.pos
    let supersedes struct (Unpack currentPos, Unpack xPos) =
        let currentVersion, newVersion = currentPos.index, xPos.index
        let currentETag, newETag = currentPos.etag, xPos.etag
        newVersion > currentVersion || currentETag <> newETag

[<AutoOpen>]
module Internal =

    [<RequireQualifiedAccess; NoComparison; NoEquality>]
    type InternalSyncResult = Written of StreamToken | ConflictUnknown of StreamToken | Conflict of Position * ITimelineEvent<EventBody>[]

    [<RequireQualifiedAccess; NoComparison; NoEquality>]
    type LoadFromTokenResult<'event> = Unchanged | Found of StreamToken * 'event[]

/// Defines the policies in force regarding how to split up calls when loading Event Batches via queries
type QueryOptions
    (   // Max number of Batches to return per paged query response. Default: 10.
        [<O; D null>] ?maxItems : int,
        // Dynamic version of `maxItems`, allowing one to react to dynamic configuration changes. Default: use `maxItems` value.
        [<O; D null>] ?getMaxItems : unit -> int,
        // Maximum number of trips to permit when slicing the work into multiple responses based on `MaxItems`. Default: unlimited.
        [<O; D null>] ?maxRequests) =
    let getMaxItems = defaultArg getMaxItems (fun () -> defaultArg maxItems 10)
    /// Limit for Maximum number of `Batch` records in a single query batch response
    member _.MaxItems = getMaxItems ()
    /// Maximum number of trips to permit when slicing the work into multiple responses based on `MaxItems`
    member _.MaxRequests = maxRequests

/// Defines the policies in force regarding
/// - accumulation/retention of Events in Tip
/// - retrying read and write operations for the Tip
type TipOptions
    (   // Maximum number of events permitted in Tip. When this is exceeded, events are moved out to a standalone Batch.
        maxEvents,
        // Maximum serialized size (length of JSON.stringify representation) to permit to accumulate in Tip before they get moved out to a standalone Batch. Default: 30_000.
        [<O; D null>] ?maxJsonLength,
        // Inhibit throwing when events are missing, but no Archive Container has been supplied. Default: false.
        [<O; D null>] ?ignoreMissingEvents,
        [<O; D null>] ?readRetryPolicy,
        [<O; D null>] ?writeRetryPolicy) =
    /// Maximum number of events permitted in Tip. When this is exceeded, events are moved out to a standalone Batch.
    member val MaxEvents : int = maxEvents
    /// Maximum serialized size (length of JSON.stringify representation) to permit to accumulate in Tip before they get moved out to a standalone Batch. Default: 30_000.
    member val MaxJsonLength = defaultArg maxJsonLength 30_000
    /// Whether to inhibit throwing when events are missing, but no Archive Container has been supplied
    member val IgnoreMissingEvents = defaultArg ignoreMissingEvents false

    member val ReadRetryPolicy = readRetryPolicy
    member val WriteRetryPolicy = writeRetryPolicy

type StoreClient(container : Container, archive : Container option, query : QueryOptions, tip : TipOptions) =

    let loadTip log stream pos = Tip.tryLoad log tip.ReadRetryPolicy (container, stream) (pos, None)
    let ignoreMissing = tip.IgnoreMissingEvents

    // Always yields events forward, regardless of direction
    member internal _.Read(log, stream, direction, (tryDecode, isOrigin), ?minIndex, ?maxIndex, ?tip) : Async<StreamToken * 'event[]> = async {
        let tip = tip |> Option.map (Query.scanTip (tryDecode, isOrigin))
        let includeTip = Option.isNone tip
        let walk log container = Query.scan log (container, stream) includeTip query.MaxItems query.MaxRequests direction (tryDecode, isOrigin)
        let walkFallback =
            match archive with
            | None -> Choice1Of2 ignoreMissing
            | Some f -> Choice2Of2 (walk (log |> Log.prop "fallback" true) f)

        let log = log |> Log.prop "stream" stream
        let! pos, events = Query.load log (minIndex, maxIndex) tip (walk log container) walkFallback
        return Token.create pos, events }
    member _.ReadLazy(log, batching : QueryOptions, stream, direction, (tryDecode, isOrigin), ?minIndex, ?maxIndex) : AsyncSeq<'event[]> =
        Query.walkLazy log (container, stream) batching.MaxItems batching.MaxRequests (tryDecode, isOrigin) (direction, minIndex, maxIndex)

    member store.Load(log, (stream, maybePos), (tryDecode, isOrigin), checkUnfolds : bool) : Async<StreamToken * 'event[]> =
        if not checkUnfolds then store.Read(log, stream, Direction.Backward, (tryDecode, isOrigin))
        else async {
            match! loadTip log stream maybePos with
            | Tip.Result.NotFound -> return Token.create Position.fromKnownEmpty, Array.empty
            | Tip.Result.NotModified -> return invalidOp "Not applicable"
            | Tip.Result.Found (pos, i, xs) -> return! store.Read(log, stream, Direction.Backward, (tryDecode, isOrigin), tip = (pos, i, xs)) }
    member _.GetPosition(log, stream, ?pos) : Async<StreamToken> = async {
        match! loadTip log stream pos with
        | Tip.Result.NotFound -> return Token.create Position.fromKnownEmpty
        | Tip.Result.NotModified -> return Token.create pos.Value
        | Tip.Result.Found (pos, _i, _unfoldsAndEvents) -> return Token.create pos }
    member store.Reload(log, (stream, pos), (tryDecode, isOrigin), ?preview): Async<LoadFromTokenResult<'event>> =
        let read tipContent = async {
            let! res = store.Read(log, stream, Direction.Backward, (tryDecode, isOrigin), minIndex = pos.index, tip = tipContent)
            return LoadFromTokenResult.Found res }
        match preview with
        | Some (pos, i, xs) -> read (pos, i, xs)
        | None -> async {
            match! loadTip log stream (Some pos) with
            | Tip.Result.NotFound -> return LoadFromTokenResult.Found (Token.create Position.fromKnownEmpty, Array.empty)
            | Tip.Result.NotModified -> return LoadFromTokenResult.Unchanged
            | Tip.Result.Found (pos, i, xs) -> return! read (pos, i, xs) }

    member internal _.Sync(log, stream, exp, batch : Tip) : Async<InternalSyncResult> = async {
        if Array.isEmpty batch.e && Array.isEmpty batch.u then invalidOp "Must write either events or unfolds."
        match! Sync.batch log (tip.WriteRetryPolicy, tip.MaxEvents, tip.MaxJsonLength) (container, stream) (exp, batch) with
        | Sync.Result.Conflict (pos', events) -> return InternalSyncResult.Conflict (pos', events)
        | Sync.Result.ConflictUnknown pos' -> return InternalSyncResult.ConflictUnknown (Token.create pos')
        | Sync.Result.Written pos' -> return InternalSyncResult.Written (Token.create pos') }

    member _.Prune(log, stream, index) =
        Prune.until log (container, stream) query.MaxItems index

type internal Category<'event, 'state, 'context>(store : StoreClient, codec : IEventCodec<'event, EventBody, 'context>) =
    member _.Load(log, stream, initial, checkUnfolds, fold, isOrigin, ct) : Task<struct (StreamToken * 'state)> = task {
        let! token, events = store.Load(log, (stream, None), (codec.TryDecode, isOrigin), checkUnfolds) |> Async.startAsTask ct
        return struct (token, fold initial events) }
    member _.Reload(log, streamName, (Token.Unpack pos as streamToken), state, fold, isOrigin, ct, ?preloaded) : Task<struct (StreamToken * 'state)> = task {
        match! store.Reload(log, (streamName, pos), (codec.TryDecode, isOrigin), ?preview = preloaded) |> Async.startAsTask ct with
        | LoadFromTokenResult.Unchanged -> return struct (streamToken, state)
        | LoadFromTokenResult.Found (token', events) -> return token', fold state events }
    member cat.Sync(log, streamName, (Token.Unpack pos as streamToken), state, events, mapUnfolds, fold, isOrigin, context, compressUnfolds) : Async<SyncResult<'state>> = async {
        let state' = fold state (Seq.ofList events)
        let encode e = codec.Encode(context, e)
        let exp, events, eventsEncoded, projectionsEncoded =
            match mapUnfolds with
            | Choice1Of3 () ->     SyncExp.Version pos.index, events, Seq.map encode events |> Array.ofSeq, Seq.empty
            | Choice2Of3 unfold -> SyncExp.Version pos.index, events, Seq.map encode events |> Array.ofSeq, Seq.map encode (unfold events state')
            | Choice3Of3 transmute ->
                let events', unfolds = transmute events state'
                SyncExp.Etag (defaultArg pos.etag null), events', Seq.map encode events' |> Array.ofSeq, Seq.map encode unfolds
        let baseIndex = pos.index + int64 (List.length events)
        let renderElement = if compressUnfolds then JsonElement.undefinedToNull >> JsonElement.deflate else JsonElement.undefinedToNull
        let projections = projectionsEncoded |> Seq.map (Sync.mkUnfold renderElement baseIndex)
        let batch = Sync.mkBatch streamName eventsEncoded projections
        match! store.Sync(log, streamName, exp, batch) with
        | InternalSyncResult.Conflict (pos', tipEvents) -> return SyncResult.Conflict (fun ct -> cat.Reload(log, streamName, streamToken, state, fold, isOrigin, ct, (pos', pos.index, tipEvents)))
        | InternalSyncResult.ConflictUnknown _token' -> return SyncResult.Conflict (fun ct -> cat.Reload(log, streamName, streamToken, state, fold, isOrigin, ct))
        | InternalSyncResult.Written token' -> return SyncResult.Written (token', state') }

module internal Caching =

    let applyCacheUpdatesWithSlidingExpiration (cache : ICache, prefix : string) (slidingExpiration : TimeSpan) =
        let mkCacheEntry struct (initialToken : StreamToken, initialState : 'state) = CacheEntry<'state>(initialToken, initialState)
        let options = CacheItemOptions.RelativeExpiration slidingExpiration
        fun streamName value ->
            cache.UpdateIfNewer(prefix + streamName, options, Token.supersedes, mkCacheEntry value)

    let applyCacheUpdatesWithFixedTimeSpan (cache : ICache, prefix : string) (period : TimeSpan) =
        let mkCacheEntry struct (initialToken : StreamToken, initialState : 'state) = CacheEntry<'state>(initialToken, initialState)
        fun streamName value ->
            let expirationPoint = let creationDate = DateTimeOffset.UtcNow in creationDate.Add period
            let options = CacheItemOptions.AbsoluteExpiration expirationPoint
            cache.UpdateIfNewer(prefix + streamName, options, Token.supersedes, mkCacheEntry value)

    type CachingCategory<'event, 'state, 'context>
        (   category : Category<'event, 'state, 'context>,
            fold : 'state -> 'event seq -> 'state, initial : 'state, isOrigin : 'event -> bool,
            tryReadCache, updateCache : _ -> struct (_*_) -> Task<unit>,
            checkUnfolds, compressUnfolds,
            mapUnfolds : Choice<unit, 'event list -> 'state -> 'event seq, 'event list -> 'state -> 'event list * 'event list>) =
        let cache streamName (inner : unit -> Task<_>) = task {
            let! struct (token, state) = inner ()
            do! updateCache streamName (token, state)
            return struct (token, state) }
        interface ICategory<'event, 'state, 'context> with
            member _.Load(log, _categoryName, _streamId, streamName, allowStale, ct) : Task<struct (StreamToken * 'state)> = task {
                match! tryReadCache streamName : Task<struct (_*_) voption> with
                | ValueNone -> return! (fun () -> category.Load(log, streamName, initial, checkUnfolds, fold, isOrigin, ct)) |> cache streamName
                | ValueSome struct (token, state) when allowStale -> return struct (token, state) // read already updated TTL, no need to write
                | ValueSome (token, state) -> return! (fun () -> category.Reload(log, streamName, token, state, fold, isOrigin, ct)) |> cache streamName }
            member _.TrySync(log : ILogger, _categoryName, _streamId, streamName, context, maybeInit, streamToken, state, events, ct) : Task<SyncResult<'state>> = task {
                match maybeInit with ValueNone -> () | ValueSome i -> do! i ct
                match! category.Sync(log, streamName, streamToken, state, events, mapUnfolds, fold, isOrigin, context, compressUnfolds) |> Async.startAsTask ct with
                | SyncResult.Conflict resync ->
                    return SyncResult.Conflict (fun ct -> cache streamName (fun () -> resync ct))
                | SyncResult.Written (token', state') ->
                    do! updateCache streamName struct (token', state')
                    return SyncResult.Written (token', state') }

module ConnectionString =

    let (|AccountEndpoint|) connectionString =
        match System.Data.Common.DbConnectionStringBuilder(ConnectionString = connectionString).TryGetValue "AccountEndpoint" with
        | true, (:? string as s) when not (String.IsNullOrEmpty s) -> s
        | _ -> invalidOp "Connection string does not contain an \"AccountEndpoint\""

namespace Equinox.CosmosStore

open Equinox.Core
open Equinox.CosmosStore.Core
open Microsoft.Azure.Cosmos
open System
open System.Threading.Tasks

[<RequireQualifiedAccess; NoComparison>]
type Discovery =
    /// Separated Account Uri and Key (for interop with previous versions)
    | AccountUriAndKey of accountUri : Uri * key : string
    /// Cosmos SDK Connection String
    | ConnectionString of connectionString : string
    member x.Endpoint : Uri = x |> function
        | Discovery.AccountUriAndKey (u, _k) -> u
        | Discovery.ConnectionString (ConnectionString.AccountEndpoint e) -> Uri e

/// Manages establishing a CosmosClient, which is used by CosmosStoreClient to read from the underlying Cosmos DB Container.
[<System.ComponentModel.EditorBrowsable(System.ComponentModel.EditorBrowsableState.Never)>]
type CosmosClientFactory
    (   // Timeout to apply to individual reads/write round-trips going to CosmosDB. CosmosDB Default: 1m.
        requestTimeout : TimeSpan,
        // Maximum number of times to attempt when failure reason is a 429 from CosmosDB, signifying RU limits have been breached. CosmosDB default : 9
        maxRetryAttemptsOnRateLimitedRequests : int,
        // Maximum number of seconds to wait (especially if a higher wait delay is suggested by CosmosDB in the 429 response). CosmosDB default : 30s
        maxRetryWaitTimeOnRateLimitedRequests : TimeSpan,
        // Connection mode (default: ConnectionMode.Direct (best performance, same as Microsoft.Azure.Cosmos SDK default)
        // NOTE: default for Equinox.Cosmos.Connector (i.e. V2) was Gateway (worst performance, least trouble, Microsoft.Azure.DocumentDb SDK default)
        [<O; D null>] ?mode : ConnectionMode,
        // Connection limit for Gateway Mode. CosmosDB default: 50
        [<O; D null>] ?gatewayModeMaxConnectionLimit,
        // consistency mode (default: ConsistencyLevel.Session)
        [<O; D null>] ?defaultConsistencyLevel : ConsistencyLevel,
        // Inhibits certificate verification when set to <c>true</c>, i.e. for working with the CosmosDB Emulator (default <c>false</c>)
        [<O; D null>] ?bypassCertificateValidation : bool) =

    /// CosmosClientOptions for this CosmosClientFactory as configured
    member val Options =
        let co = CosmosClientOptions(
            MaxRetryAttemptsOnRateLimitedRequests = maxRetryAttemptsOnRateLimitedRequests,
            MaxRetryWaitTimeOnRateLimitedRequests = maxRetryWaitTimeOnRateLimitedRequests,
            RequestTimeout = requestTimeout,
            Serializer = CosmosJsonSerializer(System.Text.Json.JsonSerializerOptions()))
        match mode with
        | None | Some ConnectionMode.Direct -> co.ConnectionMode <- ConnectionMode.Direct
        | Some ConnectionMode.Gateway | Some _ (* enum total match :( *) -> co.ConnectionMode <- ConnectionMode.Gateway // only supports Https
        match gatewayModeMaxConnectionLimit with
        | Some _ when co.ConnectionMode = ConnectionMode.Direct -> invalidArg "gatewayModeMaxConnectionLimit" "Not admissible in Direct mode"
        | x -> if co.ConnectionMode = ConnectionMode.Gateway then co.GatewayModeMaxConnectionLimit <- defaultArg x 50
        match defaultConsistencyLevel with
        | Some x -> co.ConsistencyLevel <- x
        | None -> ()
        // https://github.com/Azure/azure-cosmos-dotnet-v3/blob/1ef6e399f114a0fd580272d4cdca86b9f8732cf3/Microsoft.Azure.Cosmos.Samples/Usage/HttpClientFactory/Program.cs#L96
        if bypassCertificateValidation = Some true && co.ConnectionMode = ConnectionMode.Gateway then
            let cb = System.Net.Http.HttpClientHandler.DangerousAcceptAnyServerCertificateValidator
            let ch = new System.Net.Http.HttpClientHandler(ServerCertificateCustomValidationCallback = cb)
            co.HttpClientFactory <- fun () -> new System.Net.Http.HttpClient(ch)
        co

    /// Creates an instance of CosmosClient without actually validating or establishing the connection
    /// It's recommended to use <c>Connect()</c> and/or <c>CosmosStoreClient.Connect()</c> in preference to this API
    ///   in order to avoid latency spikes, and/or deferring discovery of connectivity or permission issues.
    member x.CreateUninitialized(discovery : Discovery) = discovery |> function
        | Discovery.AccountUriAndKey (accountUri = uri; key = key) -> new CosmosClient(string uri, key, x.Options)
        | Discovery.ConnectionString cs -> new CosmosClient(cs, x.Options)

    /// Creates and validates a Client [including loading metadata](https://devblogs.microsoft.com/cosmosdb/improve-net-sdk-initialization) for the specified containers
    member x.CreateAndInitialize(discovery : Discovery, containers) = async {
        let! ct = Async.CancellationToken
       match discovery with //
        | Discovery.AccountUriAndKey (accountUri = uri; key = key) -> return! CosmosClient.CreateAndInitializeAsync(string uri, key, containers, x.Options, ct) |> Async.AwaitTaskCorrect
        | Discovery.ConnectionString cs -> return! CosmosClient.CreateAndInitializeAsync(cs, containers, x.Options) |> Async.AwaitTaskCorrect }

/// Manages establishing a CosmosClient, which is used by CosmosStoreClient to read from the underlying Cosmos DB Container.
type CosmosStoreConnector
    (   // CosmosDB endpoint/credentials specification.
        discovery : Discovery,
        // Timeout to apply to individual reads/write round-trips going to CosmosDB. CosmosDB Default: 1m.
        requestTimeout : TimeSpan,
        // Maximum number of times to attempt when failure reason is a 429 from CosmosDB, signifying RU limits have been breached. CosmosDB default: 9
        maxRetryAttemptsOnRateLimitedRequests : int,
        // Maximum number of seconds to wait (especially if a higher wait delay is suggested by CosmosDB in the 429 response). CosmosDB default: 30s
        maxRetryWaitTimeOnRateLimitedRequests : TimeSpan,
        // Connection mode (default: ConnectionMode.Direct (best performance, same as Microsoft.Azure.Cosmos SDK default)
        // NOTE: default for Equinox.Cosmos.Connector (i.e. V2) was Gateway (worst performance, least trouble, Microsoft.Azure.DocumentDb SDK default)
        [<O; D null>] ?mode : ConnectionMode,
        // Connection limit for Gateway Mode. CosmosDB default: 50
        [<O; D null>] ?gatewayModeMaxConnectionLimit,
        // consistency mode (default: ConsistencyLevel.Session)
        [<O; D null>] ?defaultConsistencyLevel : ConsistencyLevel,
        // Inhibits certificate verification when set to <c>true</c>, i.e. for working with the CosmosDB Emulator (default <c>false</c>)
        [<O; D null>] ?bypassCertificateValidation : bool) =

    let factory =
        CosmosClientFactory
          ( requestTimeout, maxRetryAttemptsOnRateLimitedRequests, maxRetryWaitTimeOnRateLimitedRequests, ?mode = mode,
            ?gatewayModeMaxConnectionLimit = gatewayModeMaxConnectionLimit, ?defaultConsistencyLevel = defaultConsistencyLevel,
            ?bypassCertificateValidation = bypassCertificateValidation)

    /// The <c>CosmosClientOptions</c> used when connecting to CosmosDB
    member _.Options = factory.Options

    /// The Endpoint Uri for the target CosmosDB
    member _.Endpoint = discovery.Endpoint

    /// Creates an instance of CosmosClient without actually validating or establishing the connection
    /// It's recommended to use <c>Connect()</c> and/or <c>CosmosStoreClient.Connect()</c> in preference to this API
    ///   in order to avoid latency spikes, and/or deferring discovery of connectivity or permission issues.
    member _.CreateUninitialized() = factory.CreateUninitialized(discovery)

    /// Creates and validates a Client [including loading metadata](https://devblogs.microsoft.com/cosmosdb/improve-net-sdk-initialization) for the specified containers
    member _.CreateAndInitialize(containers) = factory.CreateAndInitialize(discovery, containers)

/// Holds all relevant state for a Store within a given CosmosDB Database
/// - The CosmosDB CosmosClient (there should be a single one of these per process, plus an optional fallback one for pruning scenarios)
/// - The (singleton) per Container Stored Procedure initialization state
type CosmosStoreClient
    (   // Facilitates custom mapping of Stream Category Name to underlying Cosmos Database/Container names
        categoryAndStreamNameToDatabaseContainerStream : string * string -> string * string * string,
        createContainer : string * string -> Container,
        createFallbackContainer : string * string -> Container option,
        [<O; D null>] ?primaryDatabaseAndContainerToArchive : string * string -> string * string,
        // Admits a hook to enable customization of how <c>Equinox.CosmosStore</c> handles the low level interactions with the underlying <c>CosmosContainer</c>.
        [<O; D null>] ?createGateway,
        // Inhibit <c>CreateStoredProcedureIfNotExists</c> when a given Container is used for the first time
        [<O; D null>] ?disableInitialization) =
    let createGateway = match createGateway with Some creator -> creator | None -> id
    let primaryDatabaseAndContainerToArchive = defaultArg primaryDatabaseAndContainerToArchive id
    // Index of database*container -> Initialization Context
    let containerInitGuards = System.Collections.Concurrent.ConcurrentDictionary<string*string, Initialization.ContainerInitializerGuard>()
    new(client, databaseId : string, containerId : string,
        // Inhibit <c>CreateStoredProcedureIfNotExists</c> when a given Container is used for the first time
        [<O; D null>] ?disableInitialization,
        // Admits a hook to enable customization of how <c>Equinox.CosmosStore</c> handles the low level interactions with the underlying <c>CosmosContainer</c>.
        [<O; D null>] ?createGateway : Container -> Container,
        // Client to use for fallback Containers. Default: use <c>client</c>
        [<O; D null>] ?archiveClient : CosmosClient,
        // Database Name to use for locating missing events. Default: use <c>databaseId</c>
        [<O; D null>] ?archiveDatabaseId,
        // Container Name to use for locating missing events. Default: use <c>containerId</c>
        [<O; D null>] ?archiveContainerId) =
        let genStreamName (categoryName, streamId) =
            if categoryName = null then streamId
            else FsCodec.StreamName.Internal.ofCategoryAndStreamId (categoryName, streamId)
        let catAndStreamToDatabaseContainerStream (categoryName, streamId) = databaseId, containerId, genStreamName (categoryName, streamId)
        let primaryContainer (d, c) = (client : CosmosClient).GetDatabase(d).GetContainer(c)
        let fallbackContainer =
            if Option.isNone archiveClient && Option.isNone archiveDatabaseId && Option.isNone archiveContainerId then fun (_, _) -> None
            else fun (d, c) -> Some ((defaultArg archiveClient client).GetDatabase(defaultArg archiveDatabaseId d).GetContainer(defaultArg archiveContainerId c))
        CosmosStoreClient(catAndStreamToDatabaseContainerStream, primaryContainer, fallbackContainer,
            ?disableInitialization = disableInitialization, ?createGateway = createGateway)
    member internal _.ResolveContainerGuardAndStreamName(categoryName, streamId) : struct (Initialization.ContainerInitializerGuard * string) =
        let createContainerInitializerGuard (d, c) =
            let init =
                if Some true = disableInitialization then None
                else Some (fun cosmosContainer ct -> Initialization.createSyncStoredProcIfNotExists None cosmosContainer |> Async.Ignore |> Async.startAsTask ct)
            let archiveD, archiveC = primaryDatabaseAndContainerToArchive (d, c)
            let primaryContainer, fallbackContainer = createContainer (d, c), createFallbackContainer (archiveD, archiveC)
            Initialization.ContainerInitializerGuard(createGateway primaryContainer, Option.map createGateway fallbackContainer, ?initContainer = init)
        let databaseId, containerId, streamName = categoryAndStreamNameToDatabaseContainerStream (categoryName, streamId)
        let g = containerInitGuards.GetOrAdd((databaseId, containerId), createContainerInitializerGuard)
        struct (g, streamName)

    /// Connect to an Equinox.CosmosStore in the specified Container
    /// NOTE: The returned CosmosStoreClient instance should be held as a long-lived singleton within the application.
    /// <example><code>
    /// let createStoreClient (connectionString, database, container) =
    ///     let connector = CosmosStoreConnector(Discovery.ConnectionString connectionString, System.TimeSpan.FromSeconds 5., 2, System.TimeSpan.FromSeconds 5.)
    ///     CosmosStoreClient.Connect(connector.CreateAndInitialize, database, container)
    /// </code></example>
    static member Connect(connectContainers, databaseId : string, containerId : string) : Async<CosmosStoreClient> = async {
        let! client = connectContainers [| struct (databaseId, containerId) |]
        return CosmosStoreClient(client, databaseId, containerId) }

    /// Connect to a hot-warm CosmosStore pair within the same account
    /// Events that have been archived and purged (and hence are determined to be missing from the primary) are retrieved from the archive via a fallback request where necessary.
    /// NOTE: The returned CosmosStoreClient instance should be held as a long-lived singleton within the application.
    static member Connect(connectContainers, databaseId : string, containerId : string, archiveContainerId) : Async<CosmosStoreClient> = async {
        let! client = connectContainers [| struct (databaseId, containerId); struct (databaseId, archiveContainerId) |]
        return CosmosStoreClient(client, databaseId, containerId, archiveContainerId = archiveContainerId) }

/// Defines a set of related access policies for a given CosmosDB, together with a Containers map defining mappings from (category,id) to (databaseId,containerId,streamName)
type CosmosStoreContext(storeClient : CosmosStoreClient, tipOptions, queryOptions) =
    new(    storeClient : CosmosStoreClient,
            // Maximum number of events permitted in Tip. When this is exceeded, events are moved out to a standalone Batch.
            // NOTE <c>Equinox.Cosmos</c> versions <= 3.0.0 cannot read events in Tip, hence using a non-zero value will not be interoperable.
            tipMaxEvents,
            // Maximum serialized size (length of `JSON.stringify` representation) permitted in Tip before they get moved out to a standalone Batch. Default: 30_000.
            [<O; D null>] ?tipMaxJsonLength,
            // Inhibit throwing when events are missing, but no Archive Container has been supplied as a fallback.
            [<O; D null>] ?ignoreMissingEvents,
            // Max number of Batches to return per paged query response. Default: 10.
            [<O; D null>] ?queryMaxItems,
            // Maximum number of trips to permit when slicing the work into multiple responses limited by `queryMaxItems`. Default: unlimited.
            [<O; D null>] ?queryMaxRequests) =
        let tipOptions = TipOptions(maxEvents = tipMaxEvents, ?maxJsonLength = tipMaxJsonLength, ?ignoreMissingEvents = ignoreMissingEvents)
        let queryOptions = QueryOptions(?maxItems = queryMaxItems, ?maxRequests = queryMaxRequests)
        CosmosStoreContext(storeClient, tipOptions, queryOptions)
    member val StoreClient = storeClient
    member val QueryOptions = queryOptions
    member val TipOptions = tipOptions
    member internal x.ResolveContainerClientAndStreamIdAndInit(categoryName, streamId) =
        let struct (cg, streamName) = storeClient.ResolveContainerGuardAndStreamName(categoryName, streamId)
        let store = StoreClient(cg.Container, cg.Fallback, x.QueryOptions, x.TipOptions)
        struct (store, streamName, cg.InitializationGate)

/// For CosmosDB, caching is critical in order to reduce RU consumption.
/// As such, it can often be omitted, particularly if streams are short or there are snapshots being maintained
[<NoComparison; NoEquality; RequireQualifiedAccess>]
type CachingStrategy =
    /// Do not apply any caching strategy for this Stream.
    /// NB opting not to leverage caching when using CosmosDB can have significant implications for the scalability
    ///   of your application, both in terms of latency and running costs.
    /// While the cost of a cache miss can be ameliorated to varying degrees by employing an appropriate `AccessStrategy`
    ///   [that works well and has been validated for your scenario with real data], even a cache with a low Hit Rate provides
    ///   a direct benefit in terms of the number of Request Unit (RU)s that need to be provisioned to your CosmosDB instances.
    | NoCaching
    /// Retain a single 'state per streamName, together with the associated etag.
    /// Each cache hit for a stream renews the retention period for the defined <c>window</c>.
    /// Upon expiration of the defined <c>window</c> from the point at which the cache was entry was last used, a full reload is triggered.
    /// Unless <c>LoadOption.AllowStale</c> is used, each cache hit still incurs an etag-contingent Tip read (at a cost of a roundtrip with a 1RU charge if unmodified).
    // NB while a strategy like EventStore.Caching.SlidingWindowPrefixed is obviously easy to implement, the recommended approach is to
    // track all relevant data in the state, and/or have the `unfold` function ensure _all_ relevant events get held in the `u`nfolds in Tip
    | SlidingWindow of ICache * window : TimeSpan
    /// Retain a single 'state per streamName, together with the associated etag.
    /// Upon expiration of the defined <c>period</c>, a full reload is triggered.
    /// Typically combined with `Equinox.LoadOption.AllowStale` to minimize loads.
    /// Unless <c>LoadOption.AllowStale</c> is used, each cache hit still incurs an etag-contingent Tip read (at a cost of a roundtrip with a 1RU charge if unmodified).
    | FixedTimeSpan of ICache * period : TimeSpan

[<NoComparison; NoEquality; RequireQualifiedAccess>]
type AccessStrategy<'event, 'state> =
    /// Don't apply any optimized reading logic. Note this can be extremely RU cost prohibitive
    /// and can severely impact system scalability. Should hence only be used with careful consideration.
    | Unoptimized
    /// Load only the single most recent event defined in <c>'event</c> and trust that doing a <c>fold</c> from any such event
    /// will yield a correct and complete state
    /// In other words, the <c>fold</c> function should not need to consider either the preceding <c>'state</state> or <c>'event</c>s.
    /// <remarks>
    /// A copy of the event is also retained in the `Tip` document in order that the state of the stream can be
    /// retrieved using a single (cached, etag-checked) point read.
    /// </remarks
    | LatestKnownEvent
    /// Allow a 'snapshot' event (and/or other events that that pass the <c>isOrigin</c> test) to be used to build the state
    /// in lieu of folding all the events from the start of the stream, as a performance optimization.
    /// <c>toSnapshot</c> is used to generate the <c>unfold</c> that will be held in the Tip document in order to
    /// enable efficient reading without having to query the Event documents.
    | Snapshot of isOrigin : ('event -> bool) * toSnapshot : ('state -> 'event)
    /// Allow any events that pass the `isOrigin` test to be used in lieu of folding all the events from the start of the stream
    /// When writing, uses `toSnapshots` to 'unfold' the <c>'state</c>, representing it as one or more Event records to be stored in
    /// the Tip with efficient read cost.
    | MultiSnapshot of isOrigin : ('event -> bool) * toSnapshots : ('state -> 'event seq)
    /// Instead of actually storing the events representing the decisions, only ever update a snapshot stored in the Tip document
    /// <remarks>In this mode, Optimistic Concurrency Control is necessarily based on the _etag</remarks>
    | RollingState of toSnapshot : ('state -> 'event)
    /// Allow produced events to be filtered, transformed or removed completely and/or to be transmuted to unfolds.
    /// <remarks>
    /// In this mode, Optimistic Concurrency Control is based on the _etag (rather than the normal Expected Version strategy)
    /// in order that conflicting updates to the state not involving the writing of an event can trigger retries.
    /// </remarks>
    | Custom of isOrigin : ('event -> bool) * transmute : ('event list -> 'state -> 'event list*'event list)

type CosmosStoreCategory<'event, 'state, 'context>(resolveInner, empty) =
    inherit Equinox.Category<'event, 'state, 'context>(resolveInner, empty)
    new (   context : CosmosStoreContext, codec, fold, initial, caching, access,
            // Compress Unfolds in Tip. Default: <c>true</c>.
            // NOTE when set to <c>false</c>, requires Equinox.CosmosStore or Equinox.Cosmos Version >= 2.3.0 to be able to read
            [<O; D null>] ?compressUnfolds) =
        let compressUnfolds = defaultArg compressUnfolds true
        let categories = System.Collections.Concurrent.ConcurrentDictionary<string, ICategory<'event, 'state, 'context>>()
        let resolveCategory (categoryName, container) =
            let createCategory _name : ICategory<_, _, 'context> =
                let tryReadCache, updateCache =
                    match caching with
                    | CachingStrategy.NoCaching -> (fun _ -> Task.FromResult ValueNone), fun _ _ -> Task.FromResult ()
                    | CachingStrategy.SlidingWindow (cache, window) -> cache.TryGet, Caching.applyCacheUpdatesWithSlidingExpiration (cache, null) window
                    | CachingStrategy.FixedTimeSpan (cache, period) -> cache.TryGet, Caching.applyCacheUpdatesWithFixedTimeSpan (cache, null) period
                let isOrigin, checkUnfolds, mapUnfolds =
                    match access with
                    | AccessStrategy.Unoptimized ->                      (fun _ -> false), false, Choice1Of3 ()
                    | AccessStrategy.LatestKnownEvent ->                 (fun _ -> true),  true,  Choice2Of3 (fun events _ -> Seq.last events |> Seq.singleton)
                    | AccessStrategy.Snapshot (isOrigin, toSnapshot) ->  isOrigin,         true,  Choice2Of3 (fun _ state  -> toSnapshot state |> Seq.singleton)
                    | AccessStrategy.MultiSnapshot (isOrigin, unfold) -> isOrigin,         true,  Choice2Of3 (fun _ state  -> unfold state)
                    | AccessStrategy.RollingState toSnapshot ->          (fun _ -> true),  true,  Choice3Of3 (fun _ state  -> [], [toSnapshot state])
                    | AccessStrategy.Custom (isOrigin, transmute) ->     isOrigin,         true,  Choice3Of3 transmute
                let cosmosCat = Category<'event, 'state, 'context>(container, codec)
                Caching.CachingCategory<'event, 'state, 'context>(cosmosCat, fold, initial, isOrigin, tryReadCache, updateCache, checkUnfolds, compressUnfolds, mapUnfolds) :> _
            categories.GetOrAdd(categoryName, createCategory)
        let resolveInner struct (categoryName, streamId) =
            let struct (container, streamName, maybeContainerInitializationGate) = context.ResolveContainerClientAndStreamIdAndInit(categoryName, streamId)
            struct (resolveCategory (categoryName, container), streamName, maybeContainerInitializationGate)
        let empty = struct (Token.create Position.fromKnownEmpty, initial)
        CosmosStoreCategory(resolveInner, empty)

namespace Equinox.CosmosStore.Core

open Equinox.Core
open FsCodec
open FSharp.Control

/// Outcome of appending events, specifying the new and/or conflicting events, together with the updated Target write position
[<RequireQualifiedAccess; NoComparison>]
type AppendResult<'t> =
    | Ok of pos : 't
    | Conflict of index : 't * conflictingEvents : ITimelineEvent<EventBody>[]
    | ConflictUnknown of index : 't

/// Encapsulates the core facilities Equinox.CosmosStore offers for operating directly on Events in Streams.
type EventsContext internal
    (   context : Equinox.CosmosStore.CosmosStoreContext, store : StoreClient,
        // Logger to write to - see https://github.com/serilog/serilog/wiki/Provided-Sinks for how to wire to your logger
        log : Serilog.ILogger) =
    do if log = null then nullArg "log"
    let maxCountPredicate count =
        let acc = ref (max (count-1) 0)
        fun _ ->
            if acc.Value = 0 then true else
            acc.Value <- acc.Value - 1
            false

    let yieldPositionAndData res = async {
        let! Token.Unpack pos', data = res
        return pos', data }

    let getRange direction startPos =
        let startPos = startPos |> Option.map (fun x -> x.index)
        match direction with
        | Direction.Forward -> startPos, None
        | Direction.Backward -> None, startPos

    new (context : Equinox.CosmosStore.CosmosStoreContext, log) =
        let struct (store, _streamId, _init) = context.ResolveContainerClientAndStreamIdAndInit(null, null)
        EventsContext(context, store, log)

    member _.ResolveStream(streamName) =
        let struct (_cc, streamName, init) = context.ResolveContainerClientAndStreamIdAndInit(null, streamName)
        struct (streamName, init)
    member x.StreamId(streamName) : string = x.ResolveStream streamName |> ValueTuple.fst

    member internal _.GetLazy(stream, ?queryMaxItems, ?direction, ?minIndex, ?maxIndex) : AsyncSeq<ITimelineEvent<EventBody>[]> =
        let direction = defaultArg direction Direction.Forward
        let batching = match queryMaxItems with Some qmi -> QueryOptions(qmi) | _ -> context.QueryOptions
        store.ReadLazy(log, batching, stream, direction, (Some, fun _ -> false), ?minIndex = minIndex, ?maxIndex = maxIndex)

    member internal _.GetInternal((stream, startPos), ?maxCount, ?direction) = async {
        let direction = defaultArg direction Direction.Forward
        if maxCount = Some 0 then
            // Search semantics include the first hit so we need to special case this anyway
            return Token.create (defaultArg startPos Position.fromKnownEmpty), Array.empty
        else
            let isOrigin =
                match maxCount with
                | Some limit -> maxCountPredicate limit
                | None -> fun _ -> false
            let minIndex, maxIndex = getRange direction startPos
            let! token, events = store.Read(log, stream, direction, (ValueSome, isOrigin), ?minIndex = minIndex, ?maxIndex = maxIndex)
            if direction = Direction.Backward then System.Array.Reverse events
            return token, events }

    /// Establishes the current position of the stream in as efficient a manner as possible
    /// (The ideal situation is that the preceding token is supplied as input in order to avail of 1RU low latency state checks)
    member _.Sync(stream, [<O; D null>] ?position : Position) : Async<Position> = async {
        let! Token.Unpack pos' = store.GetPosition(log, stream, ?pos = position)
        return pos' }

    /// Query (with MaxItems set to `queryMaxItems`) from the specified `Position`, allowing the reader to efficiently walk away from a running query
    /// ... NB as long as they Dispose!
    member x.Walk(stream, queryMaxItems, [<O; D null>] ?minIndex, [<O; D null>] ?maxIndex, [<O; D null>] ?direction) : AsyncSeq<ITimelineEvent<EventBody>[]> =
        x.GetLazy(stream, queryMaxItems, ?direction = direction, ?minIndex = minIndex, ?maxIndex = maxIndex)

    /// Reads all Events from a `Position` in a given `direction`
    member x.Read(stream, [<O; D null>] ?position, [<O; D null>] ?maxCount, [<O; D null>] ?direction) : Async<Position*ITimelineEvent<EventBody>[]> =
        x.GetInternal((stream, position), ?maxCount = maxCount, ?direction = direction) |> yieldPositionAndData

    /// Appends the supplied batch of events, subject to a consistency check based on the `position`
    /// Callers should implement appropriate idempotent handling, or use Equinox.Decider for that purpose
    member x.Sync(stream, position, events : IEventData<_>[]) : Async<AppendResult<Position>> = async {
        // Writes go through the stored proc, which we need to provision per container
        // Having to do this here in this way is far from ideal, but work on caching, external snapshots and caching is likely
        //   to move this about before we reach a final destination in any case
        match x.ResolveStream stream |> ValueTuple.snd with
        | ValueNone -> ()
        | ValueSome init -> let! ct = Async.CancellationToken
                            do! init ct |> Async.AwaitTaskCorrect
        let batch = Sync.mkBatch stream events Seq.empty
        match! store.Sync(log, stream, SyncExp.Version position.index, batch) with
        | InternalSyncResult.Written (Token.Unpack pos) -> return AppendResult.Ok pos
        | InternalSyncResult.Conflict (pos, events) -> return AppendResult.Conflict (pos, events)
        | InternalSyncResult.ConflictUnknown (Token.Unpack pos) -> return AppendResult.ConflictUnknown pos }

    /// Low level, non-idempotent call appending events to a stream without a concurrency control mechanism in play
    /// NB Should be used sparingly; Equinox.Decider enables building equivalent equivalent idempotent handling with minimal code.
    member x.NonIdempotentAppend(stream, events : IEventData<_>[]) : Async<Position> = async {
        match! x.Sync(stream, Position.fromAppendAtEnd, events) with
        | AppendResult.Ok token -> return token
        | x -> return x |> sprintf "Conflict despite it being disabled %A" |> invalidOp }

    member _.Prune(stream, index) : Async<int * int * int64> =
        store.Prune(log, stream, index)

/// Provides mechanisms for building `EventData` records to be supplied to the `Events` API
type EventData() =
    /// Creates an Event record, suitable for supplying to Append et al
    static member FromUtf8Bytes(eventType, data, ?meta) : IEventData<_> = FsCodec.Core.EventData.Create(eventType, data, ?meta = meta) :> _

/// Api as defined in the Equinox Specification
/// Note the CosmosContext APIs can yield better performance due to the fact that a Position tracks the etag of the Stream's Tip
module Events =

    let private (|PositionIndex|) (x : Position) = x.index
    let private stripSyncResult (f : Async<AppendResult<Position>>) : Async<AppendResult<int64>> = async {
        match! f with
        | AppendResult.Ok (PositionIndex index)-> return AppendResult.Ok index
        | AppendResult.Conflict (PositionIndex index, events) -> return AppendResult.Conflict (index, events)
        | AppendResult.ConflictUnknown (PositionIndex index) -> return AppendResult.ConflictUnknown index }
    let private stripPosition (f : Async<Position>) : Async<int64> = async {
        let! (PositionIndex index) = f
        return index }
    let private dropPosition (f : Async<Position*ITimelineEvent<EventBody>[]>) : Async<ITimelineEvent<EventBody>[]> = async {
        let! _, xs = f
        return xs }
    let (|MinPosition|) = function
        | 0L -> None
        | i -> Some (Position.fromI i)
    let (|MaxPosition|) = function
        | int64.MaxValue -> None
        | i -> Some (Position.fromI (i + 1L))

    /// Returns an async sequence of events in the stream starting at the specified sequence number,
    /// reading in batches of the specified size.
    /// Returns an empty sequence if the stream is empty or if the sequence number is larger than the largest
    /// sequence number in the stream.
    let getAll (ctx : EventsContext) (streamName : string) (index : int64) (batchSize : int) : AsyncSeq<ITimelineEvent<EventBody>[]> =
        ctx.Walk(ctx.StreamId streamName, batchSize, minIndex = index)

    /// Returns an async array of events in the stream starting at the specified sequence number,
    /// number of events to read is specified by batchSize
    /// Returns an empty sequence if the stream is empty or if the sequence number is larger than the largest
    /// sequence number in the stream.
    let get (ctx : EventsContext) (streamName : string) (MinPosition index : int64) (maxCount : int) : Async<ITimelineEvent<EventBody>[]> =
        ctx.Read(ctx.StreamId streamName, ?position = index, maxCount = maxCount) |> dropPosition

    /// Appends a batch of events to a stream at the specified expected sequence number.
    /// If the specified expected sequence number does not match the stream, the events are not appended
    /// and a failure is returned.
    let append (ctx : EventsContext) (streamName : string) (index : int64) (events : IEventData<_>[]) : Async<AppendResult<int64>> =
        ctx.Sync(ctx.StreamId streamName, Position.fromI index, events) |> stripSyncResult

    /// Appends a batch of events to a stream at the the present Position without any conflict checks.
    /// NB typically, it is recommended to ensure idempotency of operations by using the `append` and related API as
    /// this facilitates ensuring consistency is maintained, and yields reduced latency and Request Charges impacts
    /// (See equivalent APIs on `Context` that yield `Position` values)
    let appendAtEnd (ctx : EventsContext) (streamName : string) (events : IEventData<_>[]) : Async<int64> =
        ctx.NonIdempotentAppend(ctx.StreamId streamName, events) |> stripPosition

    /// Requests deletion of events up and including the specified <c>index</c>.
    /// Due to the need to preserve ordering of data in the stream, only complete Batches will be removed.
    /// If the <c>index</c> is within the Tip, events are removed via an etag-checked update. Does not alter the unfolds held in the Tip, or remove the Tip itself.
    /// Returns count of events deleted this time, events that could not be deleted due to partial batches, and the stream's lowest remaining sequence number.
    let pruneUntil (ctx : EventsContext) (streamName : string) (index : int64) : Async<int * int * int64> =
        ctx.Prune(ctx.StreamId streamName, index)

    /// Returns an async sequence of events in the stream backwards starting from the specified sequence number,
    /// reading in batches of the specified size.
    /// Returns an empty sequence if the stream is empty or if the sequence number is smaller than the smallest
    /// sequence number in the stream.
    let getAllBackwards (ctx : EventsContext) (streamName : string) (index : int64) (batchSize : int) : AsyncSeq<ITimelineEvent<EventBody>[]> =
        ctx.Walk(ctx.StreamId streamName, batchSize, maxIndex = index, direction = Direction.Backward)

    /// Returns an async array of events in the stream backwards starting from the specified sequence number,
    /// number of events to read is specified by batchSize
    /// Returns an empty sequence if the stream is empty or if the sequence number is smaller than the smallest
    /// sequence number in the stream.
    let getBackwards (ctx : EventsContext) (streamName : string) (MaxPosition index : int64) (maxCount : int) : Async<ITimelineEvent<EventBody>[]> =
        ctx.Read(ctx.StreamId streamName, ?position = index, maxCount = maxCount, direction = Direction.Backward) |> dropPosition

    /// Obtains the `index` from the current write Position
    let getNextIndex (ctx : EventsContext) (streamName : string) : Async<int64> =
        ctx.Sync(ctx.StreamId streamName) |> stripPosition
