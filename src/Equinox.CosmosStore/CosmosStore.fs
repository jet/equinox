namespace Equinox.CosmosStore.Core

open Equinox.Core
open FSharp.Control
open FsCodec
open Microsoft.Azure.Cosmos
open Serilog
open System
open System.Collections.Generic
open System.Text.Json

type EncodedBody = (struct (int * JsonElement))

/// Interpretation of EncodedBody data is an external concern from the perspective of the Store
/// The idiomatic implementation of the encoding logic is FsCodec.SystemTextJson.Compression, in versions 3.1.0 or later
/// That implementation provides complete interop with encodings produced by Equinox.Cosmos/CosmosStore from V1 onwards, including integrated Deflate compression
module internal EncodedBody =
    let internal jsonRawText: EncodedBody -> string = ValueTuple.snd >> _.GetRawText()
    let internal jsonUtf8Bytes = jsonRawText >> System.Text.Encoding.UTF8.GetByteCount
    let [<Literal>] deflateEncoding = 1
    // prior to the addition of the `D` field in 4.1.0, the integrated compression support
    // was predicated entirely on a JSON String `d` value in the Unfold as implying it was UTF8->Deflate->Base64 encoded
    let parseUnfold = function struct (0, e: JsonElement) when e.ValueKind = JsonValueKind.String -> struct (deflateEncoding, e) | x -> x

/// A single Domain Event from the array held in a Batch
[<NoEquality; NoComparison>]
type Event =
    {   /// Creation datetime (as opposed to system-defined _lastUpdated which is touched by triggers, replication etc.)
        t: DateTimeOffset // Will be saved ISO 8601 formatted

        /// The Case (Event Type); used to drive deserialization (Required)
        c: string

        /// Event body
        [<Serialization.JsonIgnore(Condition = Serialization.JsonIgnoreCondition.WhenWritingDefault)>]
        d: JsonElement // Can be Json Null for Nullary cases
        /// The encoding scheme used for `d`
        [<Serialization.JsonIgnore(Condition = Serialization.JsonIgnoreCondition.WhenWritingDefault)>]
        D: int

        /// Optional metadata
        [<Serialization.JsonIgnore(Condition = Serialization.JsonIgnoreCondition.WhenWritingDefault)>]
        m: JsonElement
        /// The encoding scheme used for `m`
        [<Serialization.JsonIgnore(Condition = Serialization.JsonIgnoreCondition.WhenWritingDefault)>]
        M: int

        /// Optional correlationId
        correlationId: string

        /// Optional causationId
        causationId: string }

/// A 'normal' (frozen, not Tip) Batch of Events (without any Unfolds)
[<NoEquality; NoComparison>]
type Batch =
    {   /// CosmosDB-mandated Partition Key, must be maintained within the document
        /// Technically not required if running in single partition mode, but being over simplistic would be more confusing
        p: string // "{streamName}"

        /// CosmosDB-mandated unique row key; needs to be unique within any partition it is maintained; must be string
        /// There's no way to usefully ORDER BY on it; hence we have `i` shadowing it and use that instead
        /// NB Tip uses a well known value ("-1") for the `id`; that document lives for the life of the stream
        id: string // "{index}"

        /// When we read, we need to capture the value so we can retain it for caching purposes
        /// NB this is not relevant to fill in when we pass it to the writing stored procedure
        /// as it will do: 1. read 2. merge 3. write merged version contingent on the _etag not having changed
        _etag: string

        /// base 'i' value for the Events held herein
        i: int64 // {index}

        // `i` value for successor batch (to facilitate identifying which Batch a given startPos is within)
        n: int64 // {index}

        /// The Domain Events (as opposed to Unfolded Events, see Tip) at this offset in the stream
        e: Event[] }
    static member internal PartitionKeyField = "p"
    // As one cannot sort by the (mandatory) `id` field, we have an indexed `i` field for sort and range query use
    // NB it's critical to also index the nominated PartitionKey field as RU costs increase (things degrade to scans) if you don't
    // TOCONSIDER: indexing strategy was developed and tuned before composite key extensions in ~2021, which might potentially be more efficient
    //             a decent attempt at https://github.com/jet/equinox/issues/274 failed, but not 100% sure it's fundamentally impossible/wrong
    static member internal IndexedPaths = [| Batch.PartitionKeyField; "i"; "n" |] |> Array.map (fun k -> $"/%s{k}/?")

/// Compaction/Snapshot/Projection Event based on the state at a given point in time `i`
[<NoEquality; NoComparison>]
type Unfold =
    {   /// Base: Stream Position (Version) of State from which this Unfold Event was generated. An unfold from State Version 1 is i=1 and includes event i=1
        i: int64

        /// Generation datetime
        t: DateTimeOffset // ISO 8601 // Not written by versions <= 2.0.0-rc9

        /// The Case (Event Type) of this snapshot, used to drive deserialization
        c: string // required

        [<Serialization.JsonIgnore(Condition = Serialization.JsonIgnoreCondition.WhenWritingDefault)>]
        d: JsonElement // required
        /// The encoding scheme used for `d`
        [<Serialization.JsonIgnore(Condition = Serialization.JsonIgnoreCondition.WhenWritingDefault)>]
        D: int

        /// Optional metadata, same encoding as `d` (can be null; not written if missing)
        [<Serialization.JsonIgnore(Condition = Serialization.JsonIgnoreCondition.WhenWritingDefault)>]
        m: JsonElement
        /// The encoding scheme used for `m`
        [<Serialization.JsonIgnore(Condition = Serialization.JsonIgnoreCondition.WhenWritingDefault)>]
        M: int }
    // Arrays are not indexed by default. 1. enable filtering by `c`ase 2. index uncompressed fields within unfolds for filtering
    static member internal IndexedPaths = [| "/u/[]/c/?"; "/u/[]/d/*" |]

/// The special-case 'Pending' Batch Format used to read the currently active (and mutable) document
/// Stored representation has the following diffs vs a 'normal' (frozen/completed) Batch: a) `id` = `-1` b) contains unfolds (`u`)
/// NB the type does double duty as a) model for when we read it b) encoding a batch being sent to the stored proc
[<NoEquality; NoComparison>]
type Tip =
    {   /// Partition key, as per Batch
        p: string // "{streamName}"

        /// Document id within partition, as per Batch
        id: string // "{-1}" - Well known id Constant used for the tail document (the only one that get mutated)

        /// When we read, we need to capture the value so we can retain it for caching purposes
        /// NB this is not relevant to fill in when we pass it to the writing stored procedure
        /// as it will do: 1. read 2. merge 3. write merged version contingent on the _etag not having changed
        _etag: string

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
[<NoComparison; NoEquality>]
type Position = { index: int64; etag: string option }
module internal Position =
    let fromEtagOnly (value: string) = { index = -2; etag = Some value }
    let fromEtagAndIndex (etag, n) = { index = n; etag = Some etag }
    /// NB a token without an etag is inefficient compared to fromEtagAndIndex, so paths to this should be minimized
    let fromIndex (x: int64) = { index = x; etag = None }
    /// If we have strong reason to suspect a stream is empty, we won't have an etag (and Writer Stored Procedure special cases this)
    let fromKnownEmpty = fromIndex 0L
    /// Blind append mode
    let fromAppendAtEnd = fromIndex -1L // sic - needs to yield value -1 to trigger stored proc logic
    /// Sentinel value we assign so we can reject attempts to sync without having known the context
    let readOnly = { index = -2L; etag = None }
    let isReadOnly (x: Position) = x.index = -2L && Option.isNone x.etag
    /// If we encounter the tip (id=-1) item/document, we're interested in its etag so we can re-sync for 1 RU
    let tryFromBatch (x: Batch) =
        if x.id <> Tip.WellKnownDocumentId then None
        else Some { index = x.n; etag = Option.ofObj x._etag }

[<RequireQualifiedAccess>]
type Direction = Forward | Backward override this.ToString() = match this with Forward -> "Forward" | Backward -> "Backward"

[<AbstractClass; Sealed>]
type internal Enum private () =
    static member Events(i: int64, e: Event[], ?minIndex, ?maxIndex): ITimelineEvent<EncodedBody> seq = seq {
        let indexMin, indexMax = defaultArg minIndex 0L, defaultArg maxIndex Int64.MaxValue
        for offset in 0..e.Length-1 do
            let index = i + int64 offset
            // If we're loading from a nominated position, we need to discard items in the batch before/after the start on the start page
            if index >= indexMin && index < indexMax then
                let x = e[offset]
                yield FsCodec.Core.TimelineEvent.Create(index, x.c, (x.D, x.d), (x.M, x.m), Guid.Empty, x.correlationId, x.causationId, x.t) }
    static member internal Events(t: Tip, ?minIndex, ?maxIndex): ITimelineEvent<EncodedBody> seq =
        Enum.Events(t.i, t.e, ?minIndex = minIndex, ?maxIndex = maxIndex)
    static member internal Events(b: Batch, ?minIndex, ?maxIndex) =
        Enum.Events(b.i, b.e, ?minIndex = minIndex, ?maxIndex = maxIndex)
    static member Unfolds(xs: Unfold[]): ITimelineEvent<EncodedBody> seq = seq {
        for x in xs -> FsCodec.Core.TimelineEvent.Create(x.i, x.c, EncodedBody.parseUnfold (x.D, x.d), (x.M, x.m), Guid.Empty, null, null, x.t, isUnfold = true) }
    static member EventsAndUnfolds(x: Tip, ?minIndex, ?maxIndex): ITimelineEvent<EncodedBody> seq =
        Enum.Events(x, ?minIndex = minIndex, ?maxIndex = maxIndex)
        |> Seq.append (Enum.Unfolds x.u)
        // where Index is equal, unfolds get delivered after the events so the fold semantics can be 'idempotent'
        |> Seq.sortBy (fun x -> x.Index, x.IsUnfold)

type IRetryPolicy = abstract member Execute: (int -> CancellationToken -> Task<'T>) -> Task<'T>

module Log =

    /// <summary>Name of Property used for <c>Metric</c> in <c>LogEvent</c>s.</summary>
    let [<Literal>] PropertyTag = "cosmosEvt"

    [<NoEquality; NoComparison>]
    type Measurement =
        {   database: string; container: string; stream: string
            interval: StopwatchInterval; bytes: int; count: int; ru: float }
        member x.Category = x.stream |> StreamName.Internal.trust |> StreamName.Category.ofStreamName
    [<RequireQualifiedAccess; NoEquality; NoComparison>]
    type Metric =
        /// Individual read request for the Tip
        | Tip of Measurement
        /// Individual read request for the Tip, not found
        | TipNotFound of Measurement
        /// Tip read with Single RU Request Charge due to correct use of etag in cache
        | TipNotModified of Measurement

        /// Summarizes a set of Responses for a given Read request
        | Query of Direction * responses: int * Measurement
        /// Individual read request in a Batch
        /// Charges are rolled up into Query Metric (so do not double count)
        | QueryResponse of Direction * Measurement

        | SyncSuccess of Measurement
        | SyncResync of Measurement
        | SyncConflict of Measurement

        /// Summarizes outcome of request to trim batches from head of a stream and events in Tip
        /// Count in Measurement is number of batches (documents) deleted
        /// Bytes in Measurement is number of events deleted
        | Prune of responsesHandled: int * Measurement
        /// Handled response from listing of batches in a stream
        /// Charges are rolled up into the Prune Metric (so do not double count)
        | PruneResponse of Measurement
        /// Deleted an individual Batch
        | Delete of Measurement
        /// Trimmed the Tip
        | Trim of Measurement
        /// Queried via the Index
        | Index of Measurement
    let [<return: Struct>] (|MetricEvent|_|) (logEvent: Serilog.Events.LogEvent): Metric voption =
        let mutable p = Unchecked.defaultof<_>
        logEvent.Properties.TryGetValue(PropertyTag, &p) |> ignore
        match p with Log.ScalarValue (:? Metric as e) -> ValueSome e | _ -> ValueNone

    /// Attach a property to the captured event record to hold the metric information
    let internal event (value: Metric) = Internal.Log.withScalarProperty PropertyTag value
    let internal prop name value (log: ILogger) = log.ForContext(name, value)
    let internal propData name (events: #IEventData<EncodedBody> seq) (log: ILogger) =
        let items = seq { for e in events do yield sprintf "{\"%s\": %s}" e.EventType (EncodedBody.jsonRawText e.Data) }
        log.ForContext(name, sprintf "[%s]" (String.concat ",\n\r" items))
    let internal propEvents = propData "events"
    let internal propDataUnfolds = Enum.Unfolds >> propData "unfolds"
    let internal propStartPos (value: Position) log = prop "startPos" value.index log
    let internal propStartEtag (value: Position) log = prop "startEtag" value.etag log

    let withLoggedRetries<'t> (retryPolicy: IRetryPolicy option) (contextLabel: string) (f: ILogger -> CancellationToken -> Task<'t>) log ct: Task<'t> =
        match retryPolicy with
        | None -> f log ct
        | Some retryPolicy ->
            let withLoggingContextWrapping count =
                let log = if count = 1 then log else log |> prop contextLabel count
                f log
            retryPolicy.Execute withLoggingContextWrapping

    let internal eventLen (x: #IEventData<_>) = EncodedBody.jsonUtf8Bytes x.Data + EncodedBody.jsonUtf8Bytes x.Meta + 80
    let internal batchLen = Seq.sumBy eventLen
    [<RequireQualifiedAccess>]
    type Operation = Tip | Tip404 | Tip304 | Query | Index | Write | Resync | Conflict | Prune | Delete | Trim
    let (|Op|QueryRes|PruneRes|) = function
        | Metric.Tip s                        -> Op (Operation.Tip, s)
        | Metric.TipNotFound s                -> Op (Operation.Tip404, s)
        | Metric.TipNotModified s             -> Op (Operation.Tip304, s)

        | Metric.Query (_, _, s)              -> Op (Operation.Query, s)
        | Metric.QueryResponse (direction, s) -> QueryRes (direction, s)

        | Metric.Index s                      -> Op (Operation.Index, s)

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
                 { mutable minRu: float; mutable maxRu: float; mutable rux100: int64; mutable count: int64; mutable ms: int64 }
                 static member Create() = { minRu = Double.MaxValue; maxRu = 0; rux100 = 0L; count = 0L; ms = 0L }
                 member x.Ingest(ms, ru) =
                     Interlocked.Increment(&x.count) |> ignore
                     Interlocked.Add(&x.rux100, int64 (ru * 100.)) |> ignore
                     if ru < x.minRu then Interlocked.CompareExchange(&x.minRu, ru, x.minRu) |> ignore
                     if ru > x.maxRu then Interlocked.CompareExchange(&x.maxRu, ru, x.maxRu) |> ignore
                     Interlocked.Add(&x.ms, ms) |> ignore
            type internal Counters() =
                 let buckets = System.Collections.Concurrent.ConcurrentDictionary<string, Counter>()
                 let create (_name: string) = Counter.Create()
                 member _.Ingest(bucket, ms, ru) = buckets.GetOrAdd(bucket, create).Ingest(ms, ru)
                 member _.Buckets = buckets.Keys
                 member _.TryBucket bucket = match buckets.TryGetValue bucket with true, t -> Some t | false, _ -> None
            type Epoch() =
                let epoch = System.Diagnostics.Stopwatch.StartNew()
                member val internal Tip = Counters() with get, set
                member val internal Read = Counters() with get, set
                member val internal Index = Counters() with get, set
                member val internal Write = Counters() with get, set
                member val internal Resync = Counters() with get, set
                member val internal Conflict = Counters() with get, set
                member val internal Prune = Counters() with get, set
                member val internal Delete = Counters() with get, set
                member val internal Trim = Counters() with get, set
                member _.Stop() = epoch.Stop()
                member _.Elapsed = epoch.Elapsed
            type LogSink(categorize) =
                let bucket (x: Measurement) = if categorize then $"{x.container}/{x.Category}" else x.container
                let (|BucketMsRu|) ({ interval = i; ru = ru } as m) = bucket m, int64 i.ElapsedMilliseconds, ru
                static let mutable epoch = Epoch()
                new() = LogSink(false)
                static member Restart() =
                    let fresh = Epoch()
                    let outgoing = Interlocked.Exchange(&epoch, fresh)
                    outgoing.Stop()
                    outgoing
                interface Serilog.Core.ILogEventSink with
                    member _.Emit logEvent =
                        match logEvent with
                        | MetricEvent cm ->
                            match cm with
                            | Op ((Operation.Tip | Operation.Tip404 | Operation.Tip304), BucketMsRu m)  ->
                                                                                epoch.Tip.Ingest m
                            | Op (Operation.Query,            BucketMsRu m)  -> epoch.Read.Ingest m
                            | QueryRes (_direction,          _)              -> ()
                            | Op (Operation.Index,            BucketMsRu m)  -> epoch.Index.Ingest m
                            | Op (Operation.Write,            BucketMsRu m)  -> epoch.Write.Ingest m
                            | Op (Operation.Conflict,         BucketMsRu m)  -> epoch.Conflict.Ingest m
                            | Op (Operation.Resync,           BucketMsRu m)  -> epoch.Resync.Ingest m
                            | Op (Operation.Prune,            BucketMsRu m)  -> epoch.Prune.Ingest m
                            | PruneRes                        _              -> ()
                            | Op (Operation.Delete,           BucketMsRu m)  -> epoch.Delete.Ingest m
                            | Op (Operation.Trim,             BucketMsRu m)  -> epoch.Trim.Ingest m
                        | _ -> ()

        /// Relies on feeding of metrics from Log through to Stats.LogSink
        /// Use Stats.LogSink.Restart() to reset the start point (and stats) where relevant
        let dump (log: ILogger) =
            let res = Stats.LogSink.Restart()
            let stats =
              [|nameof res.Tip,         res.Tip
                nameof res.Read,        res.Read
                nameof res.Index,       res.Index
                nameof res.Write,       res.Write
                nameof res.Resync,      res.Resync
                nameof res.Conflict,    res.Conflict
                nameof res.Prune,       res.Prune
                nameof res.Delete,      res.Delete
                nameof res.Trim,        res.Trim |]
            let isRead = function nameof res.Tip | nameof res.Read | nameof res.Index | nameof res.Prune -> true | _ -> false
            let buckets = stats |> Seq.collect (fun (_n, stat) -> stat.Buckets) |> Seq.distinct |> Seq.sort |> Seq.toArray
            if Array.isEmpty buckets then () else

            let maxBucketLen = buckets |> Seq.map _.Length |> Seq.max
            let duration = res.Elapsed.TotalSeconds
            let mutable prevCat, catR, catW, catRRu, catWRu = null, 0L, 0L, 0., 0.
            let inline rps count = if duration = 0 then 0L else float count/duration |> int64
            let inline ups ru = if duration = 0 then 0. else ru/duration
            let logOnCatChange (cat: string) =
                if prevCat = null then prevCat <- cat
                elif prevCat = cat then ()
                else
                    let reqs = catR + catW
                    log.Information("{bucket}        {count,6}r                       @ {rps,5:f0} r/s {rups,5:f0} RU/s ({rrups:f0}{r:l}/{wrups:f0}{w:l})",
                                    prevCat.PadRight maxBucketLen, reqs, rps reqs, ups (catRRu + catWRu), ups catRRu, "R", ups catWRu, "W")
                    catR <- 0; catRRu <- 0; catW <- 0; catWRu <- 0; prevCat <- cat
            for bucket in buckets do
                let group = match bucket.IndexOf '/' with -1 -> bucket | i -> bucket.Substring(0, i)
                group |> logOnCatChange
                for act, counts in stats do
                    match counts.TryBucket bucket with
                    | Some stat when stat.count <> 0L ->
                        let ru = float stat.rux100 / 100.
                        let rut = if isRead act then catR <- catR + stat.count; catRRu <- catRRu + ru; "R"
                                                else catW <- catW + stat.count; catWRu <- catWRu + ru; "W"
                        let inline avg x = x / float stat.count
                        log.Information("{bucket} {act,-8}{count,6}r {minRu,5:f1}-{maxRu,3:f0} {rut:l}RU{lat,5:f0} ms @ {rps,5:f0} r/s {rups,5:f0} RU/s Σ {ru,7:f0} avg={avgRu,4:f1}",
                                        bucket.PadRight maxBucketLen, act, stat.count, stat.minRu, stat.maxRu, rut, avg (float stat.ms), rps stat.count, ups ru, ru, avg ru)
                    | _ -> ()
            null |> logOnCatChange

[<AutoOpen>]
module private MicrosoftAzureCosmosWrappers =

    type ReadResult<'T> = Found of 'T | NotFound | NotModified
    type Container with
        member private container.DeserializeResponseBody<'T>(rm: ResponseMessage): 'T =
            rm.EnsureSuccessStatusCode().Content
            |> container.Database.Client.ClientOptions.Serializer.FromStream<'T>
        member container.TryReadItem(partitionKey: PartitionKey, id: string, ct, ?options: ItemRequestOptions): Task<float * ReadResult<'T>> = task {
            use! rm = container.ReadItemStreamAsync(id, partitionKey, requestOptions = Option.toObj options, cancellationToken = ct)
            return rm.Headers.RequestCharge, rm.StatusCode |> function
                | System.Net.HttpStatusCode.NotFound -> NotFound
                | System.Net.HttpStatusCode.NotModified -> NotModified
                | _ -> container.DeserializeResponseBody<'T>(rm) |> Found }

// NB don't nest in a private module, or serialization will fail miserably ;)
[<CLIMutable; NoEquality; NoComparison>]
type SyncResponse = { etag: string; n: int64; conflicts: Unfold[]; e: Event[] }

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
type internal SyncExp private = Version of int64 | Etag of string | Any
module internal SyncExp =
    let fromVersion i =
        if i < 0L then raise <| ArgumentOutOfRangeException(nameof i, i, "must be >= 0")
        SyncExp.Version i
    let fromVersionOrAppendAtEnd = function -1L -> SyncExp.Any | i -> fromVersion i
    let fromEtag etag = SyncExp.Etag etag

module internal Sync =

    // NB don't nest in a private module, or serialization will fail miserably ;)
    [<RequireQualifiedAccess; NoEquality; NoComparison>]
    type Result =
        | Written of Position
        | Conflict of Position * events: ITimelineEvent<EncodedBody>[]
        | ConflictUnknown of Position

    let private run (container: Container, stream: string) (maxEventsInTip, maxStringifyLen) (exp, req: Tip, ct)
        : Task<float*Result> = task {
        let ep =
            match exp with
            | SyncExp.Version ev -> Position.fromIndex ev
            | SyncExp.Etag et -> Position.fromEtagOnly et
            | SyncExp.Any -> Position.fromAppendAtEnd
        let args = [| box req; box ep.index; box (Option.toObj ep.etag); box maxEventsInTip; box maxStringifyLen |]
        let! (res: Scripts.StoredProcedureExecuteResponse<SyncResponse>) =
            container.Scripts.ExecuteStoredProcedureAsync<SyncResponse>(SyncStoredProc.name, PartitionKey stream, args, cancellationToken = ct)
        let newPos = { index = res.Resource.n; etag = Option.ofObj res.Resource.etag }
        match res.Resource.conflicts with
        | null -> return res.RequestCharge, Result.Written newPos
        // ConflictUnknown is to be yielded if we believe querying is going to be necessary (as there are no unfolds, and no relevant events in the Tip)
        | [||] when res.Resource.e.Length = 0 && newPos.index > ep.index ->
            return res.RequestCharge, Result.ConflictUnknown newPos
        | unfolds -> // stored proc only returns events and unfolds with index >= req.i - no need to trim to a minIndex
            let events = (Enum.Events(ep.index, res.Resource.e), Enum.Unfolds unfolds) ||> Seq.append |> Array.ofSeq
            return res.RequestCharge, Result.Conflict (newPos, events) }

    let private logged (container, stream) (maxEventsInTip, maxStringifyLen) (exp: SyncExp, req: Tip) (log: ILogger) ct: Task<Result> = task {
        let! t, (ru, result) = (fun ct -> run (container, stream) (maxEventsInTip, maxStringifyLen) (exp, req, ct)) |> Stopwatch.time ct
        let verbose = log.IsEnabled Serilog.Events.LogEventLevel.Debug
        let count, bytes = req.e.Length, if verbose then Enum.Events req |> Log.batchLen else 0
        let log =
            let inline mkMetric ru: Log.Measurement =
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
            "Sync", stream, count, req.u.Length, t.ElapsedMilliseconds, ru, bytes, exp)
        return result }

    let batch (log: ILogger) (retryPolicy, maxEventsInTip, maxStringifyLen) containerStream expBatch ct: Task<Result> =
        let call = logged containerStream (maxEventsInTip, maxStringifyLen) expBatch
        Log.withLoggedRetries retryPolicy "writeAttempt" call log ct

    let private mkEvent (x: IEventData<EncodedBody>) =
        let struct (D, d), struct (M, m) = x.Data, x.Meta
        {   t = x.Timestamp; c = x.EventType; d = d; D = D; m = m; M = M; correlationId = x.CorrelationId; causationId = x.CausationId }
    let mkBatch (stream: string) (events: IEventData<_>[]) unfolds: Tip =
        {   p = stream; id = Tip.WellKnownDocumentId; n = -1L(*Server-managed*); i = -1L(*Server-managed*); _etag = null
            e = Array.map mkEvent events; u = unfolds }
    let mkUnfold baseIndex (x: IEventData<EncodedBody>): Unfold =
        let struct (D, d), struct (M, m) = x.Data, x.Meta
        {   i = baseIndex; t = x.Timestamp; c = x.EventType; d = d; D = D; m = m; M = M }

module Initialization =

    type [<RequireQualifiedAccess>] Throughput = Manual of rus: int | Autoscale of maxRus: int
    type [<RequireQualifiedAccess>] Provisioning = Container of Throughput | Database of Throughput | Serverless
    let private (|ThroughputProperties|) = function
        | Throughput.Manual rus -> ThroughputProperties.CreateManualThroughput(rus)
        | Throughput.Autoscale maxRus -> ThroughputProperties.CreateAutoscaleThroughput(maxRus)
    let private createDatabaseIfNotExists (client: CosmosClient) maybeTp dName = async {
        let! r = Async.call (fun ct -> client.CreateDatabaseIfNotExistsAsync(dName, throughputProperties = Option.toObj maybeTp, cancellationToken = ct))
        return r.Database }
    let private createOrProvisionDatabase (client: CosmosClient) dName = function
        | Provisioning.Container _ | Provisioning.Serverless -> createDatabaseIfNotExists client None dName
        | Provisioning.Database (ThroughputProperties tp) -> async {
            let! d = createDatabaseIfNotExists client (Some tp) dName
            do! Async.call (fun ct -> d.ReplaceThroughputAsync(tp, cancellationToken = ct)) |> Async.Ignore
            return d }
    let private createContainerIfNotExists (d: Database) cp maybeTp = async {
        let! r = Async.call (fun ct -> d.CreateContainerIfNotExistsAsync(cp, throughputProperties = Option.toObj maybeTp, cancellationToken = ct))
        let existed = r.StatusCode = Net.HttpStatusCode.OK
        if existed then
            do! Async.call (fun ct -> r.Container.ReplaceContainerAsync(cp, cancellationToken = ct)) |> Async.Ignore
        return r.Container, existed }
    let private createOrProvisionContainer (d: Database) (cName, pkPath, customizeContainer) mode =
        let cp = ContainerProperties(id = cName, partitionKeyPath = pkPath)
        customizeContainer cp
        match mode with
        | Provisioning.Database _ | Provisioning.Serverless -> async {
            let! c, _existed = createContainerIfNotExists d cp None
            return c }
        | Provisioning.Container (ThroughputProperties throughput) -> async {
            let! c, existed = createContainerIfNotExists d cp (Some throughput)
            if existed then do! Async.call (fun ct -> c.ReplaceThroughputAsync(throughput, cancellationToken = ct)) |> Async.Ignore
            return c }

    let private createStoredProcIfNotExists (c: Container) (name, body) ct: Task<float> = task {
        try let! r = c.Scripts.CreateStoredProcedureAsync(Scripts.StoredProcedureProperties(id = name, body = body), cancellationToken = ct)
            return r.RequestCharge
        with :? CosmosException as ce when ce.StatusCode = System.Net.HttpStatusCode.Conflict -> return ce.RequestCharge }
    let private applyBatchAndTipContainerProperties indexUnfolds (cp: ContainerProperties) =
        cp.IndexingPolicy.IndexingMode <- IndexingMode.Consistent
        cp.IndexingPolicy.Automatic <- true
        // We specify fields on whitelist basis; generic querying is inapplicable, and indexing is far from free (write RU and latency)
        // Given how long and variable the blacklist would be, we whitelist instead
        cp.IndexingPolicy.ExcludedPaths.Add(ExcludedPath(Path="/*"))
        for p in [| yield! Batch.IndexedPaths; if indexUnfolds then yield! Unfold.IndexedPaths |] do
            cp.IndexingPolicy.IncludedPaths.Add(IncludedPath(Path = p))
    let createSyncStoredProcIfNotExists (log: ILogger option) container ct = task {
        let! t, ru = createStoredProcIfNotExists container (SyncStoredProc.name, SyncStoredProc.body) |> Stopwatch.time ct
        match log with
        | None -> ()
        | Some log -> log.Information("Created stored procedure {procName} in {ms:f1}ms {ru}RU", SyncStoredProc.name, t.ElapsedMilliseconds, ru) }
    let init log (client: CosmosClient) (dName, cName) mode indexUnfolds skipStoredProc ct = task {
        let! d = createOrProvisionDatabase client dName mode
        let! c = createOrProvisionContainer d (cName, $"/%s{Batch.PartitionKeyField}", applyBatchAndTipContainerProperties indexUnfolds) mode
        if not skipStoredProc then
            do! createSyncStoredProcIfNotExists (Some log) c ct }

    let private applyAuxContainerProperties (cp: ContainerProperties) =
        // TL;DR no indexing of any kind; see https://github.com/Azure/azure-documentdb-changefeedprocessor-dotnet/issues/142
        cp.IndexingPolicy.Automatic <- false
        cp.IndexingPolicy.IndexingMode <- IndexingMode.None

    let initAux (client: CosmosClient) (dName, cName) mode = async {
        let! d = createOrProvisionDatabase client dName mode
        return! createOrProvisionContainer d (cName, "/id", applyAuxContainerProperties) mode } // per Cosmos team, Partition Key must be "/id"

    /// Per Container, we need to ensure the stored procedure has been created exactly once (per process lifetime)
    type internal ContainerInitializerGuard(container: Container, ?initContainer: Container -> CancellationToken -> Task<unit>) =
        let initGuard = initContainer |> Option.map (fun init -> TaskCell<unit>(init container))
        member val Container = container
        /// Coordinates max of one in flight call to the init logic, retrying on next request if it fails. Calls after it has succeeded noop
        member _.Initialize(ct): System.Threading.Tasks.ValueTask =
            match initGuard with
            | Some g when not (g.IsValid()) -> g.Await(ct) |> ValueTask.ofTask |> ValueTask.ignore
            | _ -> System.Threading.Tasks.ValueTask.CompletedTask

module internal Tip =

    let private get (container: Container, stream: string) (maybePos: Position option) ct =
        let ro = match maybePos with Some { etag = Some etag } -> ItemRequestOptions(IfNoneMatchEtag = etag) |> Some | _ -> None
        container.TryReadItem<Tip>(PartitionKey stream, Tip.WellKnownDocumentId, ct, ?options = ro)
    let private loggedGet (get: Container * string -> Position option -> CancellationToken -> Task<_>) (container, stream) (maybePos: Position option) (log: ILogger) ct = task {
        let log = log |> Log.prop "stream" stream
        let! t, (ru, res: ReadResult<Tip>) = get (container, stream) maybePos |> Stopwatch.time ct
        let verbose = log.IsEnabled Events.LogEventLevel.Debug
        let log bytes count (f: Log.Measurement -> _) = log |> Log.event (f { database = container.Database.Id; container = container.Id; stream = stream; interval = t; bytes = bytes; count = count; ru = ru })
        match res with
        | ReadResult.NotModified ->
            (log 0 0 Log.Metric.TipNotModified).Information("EqxCosmos {action:l} {stream} {res} {ms:f1}ms {ru}RU", "Tip", stream, 304, t.ElapsedMilliseconds, ru)
        | ReadResult.NotFound ->
            (log 0 0 Log.Metric.TipNotFound).Information("EqxCosmos {action:l} {stream} {res} {ms:f1}ms {ru}RU", "Tip", stream, 404, t.ElapsedMilliseconds, ru)
        | ReadResult.Found tip ->
            let log =
                let count, bytes = tip.u.Length, if verbose then Enum.Unfolds tip.u |> Log.batchLen else 0
                log bytes count Log.Metric.Tip
            let log = if verbose then log |> Log.propDataUnfolds tip.u else log
            let log = match maybePos with Some p -> log |> Log.propStartPos p |> Log.propStartEtag p | None -> log
            let log = log |> Log.prop "_etag" tip._etag |> Log.prop "n" tip.n
            log.Information("EqxCosmos {action:l} {stream} {res} {ms:f1}ms {ru}RU", "Tip", stream, 200, t.ElapsedMilliseconds, ru)
        return ru, res }
    type [<RequireQualifiedAccess; NoComparison; NoEquality>] Result = NotModified | NotFound | Found of Position * i: int64 * ITimelineEvent<EncodedBody>[]
    /// `pos` being Some implies that the caller holds a cached value and hence is ready to deal with Result.NotModified
    let tryLoad (log: ILogger) retryPolicy containerStream (maybePos: Position option, maxIndex) ct: Task<Result> = task {
        let! _rc, res = Log.withLoggedRetries retryPolicy "readAttempt" (loggedGet get containerStream maybePos) log ct
        match res with
        | ReadResult.NotModified -> return Result.NotModified
        | ReadResult.NotFound -> return Result.NotFound
        | ReadResult.Found tip ->
            let minIndex = maybePos |> Option.map _.index
            return Result.Found (Position.fromEtagAndIndex (tip._etag, tip.n), tip.i, Enum.EventsAndUnfolds(tip, ?maxIndex = maxIndex, ?minIndex = minIndex) |> Array.ofSeq) }
    let tryFindOrigin (tryDecode: ITimelineEvent<EncodedBody> -> 'event voption, isOrigin: 'event -> bool) xs =
        let stack = ResizeArray()
        let isOrigin' (u: ITimelineEvent<EncodedBody>) =
            match tryDecode u with
            | ValueNone -> false
            | ValueSome e ->
                stack.Insert(0, e) // WalkResult always renders events ordered correctly - here we're aiming to align with Enum.EventsAndUnfolds
                isOrigin e
        xs |> Seq.tryFindBack isOrigin', stack.ToArray()
    let tryHydrate (tryDecode: ITimelineEvent<EncodedBody> -> 'event voption, isOrigin: 'event -> bool) (unfolds: Unfold[], etag: string voption) =
        match Enum.Unfolds unfolds |> tryFindOrigin (tryDecode, isOrigin) with
        | Some u, events ->
            let pos = match etag with ValueSome etag -> Position.fromEtagAndIndex (etag, u.Index) | ValueNone -> Position.readOnly
            ValueSome (pos, events)
        | None, _ -> ValueNone

module internal Query =

    let feedIteratorMapTi (map: int -> StopwatchInterval -> FeedResponse<'t> -> 'u) (query: FeedIterator<'t>) ct: IAsyncEnumerable<'u> = taskSeq {
        // earlier versions, such as 3.9.0, do not implement IDisposable; see linked issue for detail on when SDK team added it
        use _ = query // see https://github.com/jet/equinox/issues/225 - in the Cosmos V4 SDK, all this is managed IAsyncEnumerable
        let mutable i = 0
        while query.HasMoreResults do
            let! t, (res: FeedResponse<'t>) = (fun ct -> query.ReadNextAsync(ct)) |> Stopwatch.time ct
            yield map i t res
            i <- i + 1 }
    let private mkQuery (log: ILogger) (container: Container, stream: string) includeTip (maxItems: int) (direction: Direction, minIndex, maxIndex): FeedIterator<Batch> =
        let order = if direction = Direction.Forward then "ASC" else "DESC"
        let query =
            let args = [
                 match minIndex with None -> () | Some x -> yield "c.n > @minPos", fun (q: QueryDefinition) -> q.WithParameter("@minPos", x)
                 match maxIndex with None -> () | Some x -> yield "c.i < @maxPos", fun (q: QueryDefinition) -> q.WithParameter("@maxPos", x) ]
            let whereClause =
                let notTip = sprintf "c.id!=\"%s\"" Tip.WellKnownDocumentId
                let conditions = Seq.map fst args
                if List.isEmpty args && includeTip then null
                else "WHERE " + String.Join(" AND ", if includeTip then conditions else Seq.append conditions (Seq.singleton notTip))
            let queryString = $"SELECT c.id, c.i, c._etag, c.n, c.e FROM c %s{whereClause} ORDER BY c.i %s{order}"
            let prams = Seq.map snd args
            (QueryDefinition queryString, prams) ||> Seq.fold (fun q wp -> q |> wp)
        log.Debug("EqxCosmos Query {stream} {query}; n>{minIndex} i<{maxIndex}", stream, query.QueryText, Option.toNullable minIndex, Option.toNullable maxIndex)
        container.GetItemQueryIterator<Batch>(query, requestOptions = QueryRequestOptions(PartitionKey = PartitionKey stream, MaxItemCount = maxItems))

    // Unrolls the Batches in a response
    // NOTE when reading backwards, the events are emitted in reverse Index order to suit the takeWhile consumption
    let private mapPage direction (container: Container, streamName: string) (minIndex, maxIndex) (maxRequests: int option)
            (log: ILogger) i t (res: FeedResponse<Batch>)
        : ITimelineEvent<EncodedBody>[] * Position option * float =
        let log = log |> Log.prop "batchIndex" i
        match maxRequests with
        | Some mr when i >= mr -> log.Information "batch Limit exceeded"; invalidOp "batch Limit exceeded"
        | _ -> ()
        let batches, ru = Array.ofSeq res, res.RequestCharge
        let unwrapBatch (b: Batch) =
            Enum.Events(b, ?minIndex = minIndex, ?maxIndex = maxIndex)
            |> if direction = Direction.Backward then Seq.rev else id
        let events = batches |> Seq.collect unwrapBatch |> Array.ofSeq
        let verbose = log.IsEnabled Events.LogEventLevel.Debug
        let count, bytes = events.Length, if verbose then events |> Log.batchLen else 0
        let reqMetric: Log.Measurement = { database = container.Database.Id; container = container.Id; stream = streamName; interval = t; bytes = bytes; count = count; ru = ru }
        let log = let evt = Log.Metric.QueryResponse (direction, reqMetric) in log |> Log.event evt
        let log = if verbose then log |> Log.propEvents events else log
        let index = if count = 0 then Nullable () else Nullable <| Seq.min (seq { for x in batches -> x.i })
        (log|> Log.prop "bytes" bytes
            |> match minIndex with None -> id | Some i -> Log.prop "minIndex" i
            |> match maxIndex with None -> id | Some i -> Log.prop "maxIndex" i)
            .Information("EqxCosmos {action:l} {count}/{batches} {direction} {ms:f1}ms i={index} {ru}RU",
                "Response", count, batches.Length, direction, t.ElapsedMilliseconds, index, ru)
        let maybePosition = batches |> Array.tryPick Position.tryFromBatch
        events, maybePosition, ru

    let private logQuery direction (container: Container, streamName) interval (responsesCount, events: ITimelineEvent<EncodedBody>[]) n (ru: float) (log: ILogger) =
        let verbose = log.IsEnabled Events.LogEventLevel.Debug
        let count, bytes = events.Length, if verbose then events |> Log.batchLen else 0
        let reqMetric: Log.Measurement = { database = container.Database.Id; container = container.Id; stream = streamName; interval = interval; bytes = bytes; count = count; ru = ru }
        let evt = Log.Metric.Query (direction, responsesCount, reqMetric)
        let action = match direction with Direction.Forward -> "QueryF" | Direction.Backward -> "QueryB"
        (log |> Log.prop "bytes" bytes |> Log.event evt).Information(
            "EqxCosmos {action:l} {stream} v{n} {count}/{responses} {ms:f1}ms {ru}RU",
            action, streamName, n, count, responsesCount, interval.ElapsedMilliseconds, ru)

    let private calculateUsedVersusDroppedPayload stopIndex (xs: ITimelineEvent<EncodedBody>[]): int * int =
        let mutable used, dropped = 0, 0
        let mutable found = false
        for x in xs do
            let bytes = Log.eventLen x
            if found then dropped <- dropped + bytes
            else used <- used + bytes
            if x.Index = stopIndex then found <- true
        used, dropped

    [<RequireQualifiedAccess; NoComparison; NoEquality>]
    type ScanResult<'event> = { found: bool; minIndex: int64; next: int64; maybeTipPos: Position option; events: 'event[] }

    let scanTip (tryDecode, isOrigin) (pos: Position, i: int64, xs: #ITimelineEvent<EncodedBody>[]): ScanResult<'event> =
        let origin, events = xs |> Tip.tryFindOrigin (tryDecode, isOrigin)
        { found = Option.isSome origin; maybeTipPos = Some pos; minIndex = i; next = pos.index + 1L; events = events }

    // Yields events in ascending Index order
    let scan<'event> (log: ILogger) (container, stream) includeTip (maxItems: int) maxRequests direction
        (tryDecode: ITimelineEvent<EncodedBody> -> 'event voption, isOrigin: 'event -> bool)
        (minIndex, maxIndex, ct)
        : Task<ScanResult<'event> option> = task {
        let mutable found = false
        let mutable responseCount = 0
        let mergeBatches (log: ILogger) (batchesBackward: IAsyncEnumerable<ITimelineEvent<EncodedBody>[] * Position option * float>) = task {
            let mutable lastResponse, maybeTipPos, ru = None, None, 0.
            let! events =
                batchesBackward
                |> TaskSeq.collectSeq (fun (events, maybePos, r) ->
                    if Option.isNone maybeTipPos then maybeTipPos <- maybePos
                    lastResponse <- Some events; ru <- ru + r
                    responseCount <- responseCount + 1
                    seq { for x in events -> struct (x, tryDecode x) })
                |> TaskSeq.takeWhileInclusive (function
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
                |> TaskSeq.toArrayAsync
            return events, maybeTipPos, ru }
        let log = log |> Log.prop "batchSize" maxItems |> Log.prop "stream" stream
        let readLog = log |> Log.prop "direction" direction
        let batches ct: IAsyncEnumerable<ITimelineEvent<EncodedBody>[] * Position option * float> =
            let query = mkQuery readLog (container, stream) includeTip maxItems (direction, minIndex, maxIndex)
            feedIteratorMapTi (mapPage direction (container, stream) (minIndex, maxIndex) maxRequests readLog) query ct
        let! t, (events, maybeTipPos, ru) = (fun ct -> batches ct |> mergeBatches log) |> Stopwatch.time ct
        let raws = Array.map ValueTuple.fst events
        let decoded = if direction = Direction.Forward then Array.chooseV ValueTuple.snd events else let xs = Array.chooseV ValueTuple.snd events in Array.Reverse xs; xs
        let minMax = (None, raws) ||> Array.fold (fun acc x -> let i = x.Index in Some (match acc with None -> i, i | Some (n, x) -> min n i, max x i))
        let version =
            match maybeTipPos, minMax with
            | Some { index = max }, _
            | _, Some (_, max) -> max + 1L
            | None, None -> 0L
        log |> logQuery direction (container, stream) t (responseCount, raws) version ru
        match minMax, maybeTipPos with
        | Some (i, m), _ -> return Some ({ found = found; minIndex = i; next = m + 1L; maybeTipPos = maybeTipPos; events = decoded }: ScanResult<_>)
        | None, Some { index = tipI } -> return Some { found = found; minIndex = tipI; next = tipI; maybeTipPos = maybeTipPos; events = [||] }
        | None, _ -> return None }

    let walkLazy<'event> (log: ILogger) (container, stream) maxItems maxRequests
        (tryDecode: ITimelineEvent<EncodedBody> -> 'event option, isOrigin: 'event -> bool)
        (direction, minIndex, maxIndex, ct: CancellationToken)
        : IAsyncEnumerable<'event[]> = taskSeq {
        let query = mkQuery log (container, stream) true maxItems (direction, minIndex, maxIndex)

        let readPage = mapPage direction (container, stream) (minIndex, maxIndex) maxRequests
        let log = log |> Log.prop "batchSize" maxItems |> Log.prop "stream" stream
        let readLog = log |> Log.prop "direction" direction
        let query = feedIteratorMapTi (readPage readLog) query ct
        let startTicks = System.Diagnostics.Stopwatch.GetTimestamp()
        let allEvents = ResizeArray()
        let mutable i, ru = 0, 0.
        try let mutable ok = true
            use e = query.GetAsyncEnumerator(ct)
            while ok do
                let batchLog = readLog |> Log.prop "batchIndex" i
                match maxRequests with
                | Some mr when i + 1 >= mr -> batchLog.Information "batch Limit exceeded"; invalidOp "batch Limit exceeded"
                | _ -> ()

                let! more = e.MoveNextAsync()
                if not more then ok <- false else // rest of block does not happen, while exits

                let events, _pos, rus = e.Current

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
    let load (log: ILogger) (minIndex, maxIndex) (tip: ScanResult<'event> option)
            (primary: int64 option * int64 option * CancellationToken -> Task<ScanResult<'event> option>)
            // Choice1Of2 -> indicates whether it's acceptable to ignore missing events; Choice2Of2 -> Fallback store
            (fallback: Choice<bool, int64 option * int64 option * CancellationToken -> Task<ScanResult<'event> option>>) ct
            : Task<Position * 'event[]> = task {
        let minI = defaultArg minIndex 0L
        match tip with
        | Some { found = true; maybeTipPos = Some p; events = e } -> return p, e
        | Some { minIndex = i; maybeTipPos = Some p; events = e } when i <= minI -> return p, e
        | _ ->

        let i, events, pos =
            match tip with
            | Some { minIndex = i; maybeTipPos = p; events = e } -> Some i, e, p
            | None -> maxIndex, Array.empty, None
        let! primary = primary (minIndex, i, ct)
        let events, pos =
            match primary with
            | None -> events, pos |> Option.defaultValue Position.fromKnownEmpty
            | Some p -> Array.append p.events events, pos |> Option.orElse p.maybeTipPos |> Option.defaultValue (Position.fromIndex p.next)
        let inline logMissing (minIndex, maxIndex) message =
            if log.IsEnabled Events.LogEventLevel.Debug then
                (log|> fun log -> match minIndex with None -> log | Some mi -> log |> Log.prop "minIndex" mi
                    |> fun log -> match maxIndex with None -> log | Some mi -> log |> Log.prop "maxIndex" mi)
                    .Debug(message)

        match primary, fallback with
        | Some { found = true }, _ -> return pos, events // origin found in primary, no need to look in fallback
        | Some { minIndex = i }, _ when i <= minI -> return pos, events // primary had requested min event Index, no need to look at fallback
        | None, _ when Option.isNone tip -> return pos, events // initial load where no documents present in stream
        | _, Choice1Of2 allowMissing ->
            logMissing (minIndex, i) "Origin event not found; no Archive Container supplied"
            if allowMissing then return pos, events
            else return failwith "Origin event not found; no Archive Container supplied"
        | _, Choice2Of2 fallback ->

        let maxIndex = match primary with Some p -> Some p.minIndex | None -> maxIndex // if no batches in primary, high-water mark from tip is max
        let! fallback = fallback (minIndex, maxIndex, ct)
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

    type BatchIndices = { id: string; i: int64; n: int64 }

    let until (log: ILogger) (container: Container, stream: string) (maxItems: int) indexInclusive ct: Task<int * int * int64> = task {
        let log = log |> Log.prop "stream" stream
        let deleteItem id count: Task<float> = task {
            let ro = ItemRequestOptions(EnableContentResponseOnWrite = false) // https://devblogs.microsoft.com/cosmosdb/enable-content-response-on-write/
            let! t, res = (fun ct -> container.DeleteItemAsync(id, PartitionKey stream, ro, ct)) |> Stopwatch.time ct
            let rc, ms = res.RequestCharge, t.ElapsedMilliseconds
            let reqMetric: Log.Measurement = { database = container.Database.Id; container = container.Id; stream = stream; interval = t; bytes = -1; count = count; ru = rc }
            let log = let evt = Log.Metric.Delete reqMetric in log |> Log.event evt
            log.Information("EqxCosmos {action:l} {id} {ms:f1}ms {ru}RU", "Delete", id, ms, rc)
            return rc }
        let trimTip expectedI count = task {
            match! container.TryReadItem<Tip>(PartitionKey stream, Tip.WellKnownDocumentId, ct) with
            | _, ReadResult.NotModified -> return failwith "unexpected NotModified; no etag supplied"
            | _, ReadResult.NotFound -> return failwith "unexpected NotFound"
            | _, ReadResult.Found tip when tip.i <> expectedI -> return failwith $"Concurrent write detected; Expected i=%d{expectedI} actual=%d{tip.i}"
            | tipRu, ReadResult.Found tip ->

            let tip = { tip with i = tip.i + int64 count; e = Array.skip count tip.e }
            let ro = ItemRequestOptions(EnableContentResponseOnWrite = false, IfMatchEtag = tip._etag)
            let! t, updateRes = (fun ct -> container.ReplaceItemAsync(tip, tip.id, PartitionKey stream, ro, ct)) |> Stopwatch.time ct
            let rc, ms = tipRu + updateRes.RequestCharge, t.ElapsedMilliseconds
            let reqMetric: Log.Measurement = { database = container.Database.Id; container = container.Id; stream = stream; interval = t; bytes = -1; count = count; ru = rc }
            let log = let evt = Log.Metric.Trim reqMetric in log |> Log.event evt
            log.Information("EqxCosmos {action:l} {count} {ms:f1}ms {ru}RU", "Trim", count, ms, rc)
            return rc }
        let log = log |> Log.prop "index" indexInclusive
        let query: FeedIterator<BatchIndices> =
             let qro = QueryRequestOptions(PartitionKey = PartitionKey stream, MaxItemCount = maxItems)
             // sort by i to guarantee we don't ever leave an observable gap in the sequence
             container.GetItemQueryIterator<_>("SELECT c.id, c.i, c.n FROM c ORDER by c.i", requestOptions = qro)
        let mapPage i (t: StopwatchInterval) (page: FeedResponse<BatchIndices>) =
            let batches, rc, ms = Array.ofSeq page, page.RequestCharge, t.ElapsedMilliseconds
            let next = Array.tryLast batches |> Option.map _.n
            let reqMetric: Log.Measurement = { database = container.Database.Id; container = container.Id; stream = stream; interval = t; bytes = -1; count = batches.Length; ru = rc }
            let log = let evt = Log.Metric.PruneResponse reqMetric in log |> Log.prop "batchIndex" i |> Log.event evt
            log.Information("EqxCosmos {action:l} {batches} {ms:f1}ms n={next} {ru}RU", "PruneResponse", batches.Length, ms, Option.toNullable next, rc)
            batches, rc
        let! pt, outcomes =
            let isTip (x: BatchIndices) = x.id = Tip.WellKnownDocumentId
            let isRelevant x = x.i <= indexInclusive || isTip x
            let handle (batches: BatchIndices[], rc) = task {
                let mutable delCharges, batchesDeleted, trimCharges, batchesTrimmed, eventsDeleted, eventsDeferred = 0., 0, 0., 0, 0, 0
                let mutable lwm = None
                for x in batches |> Seq.takeWhile (fun x -> isRelevant x || Option.isNone lwm) do
                    let batchSize = x.n - x.i |> int
                    let eligibleEvents = max 0 (min batchSize (int (indexInclusive + 1L - x.i)))
                    if isTip x then // Even if we remove the last event from the Tip, we need to retain a. unfolds b. position (n)
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
                return (rc, delCharges, trimCharges), lwm, (batchesDeleted + batchesTrimmed, eventsDeleted, eventsDeferred) }
            let hasRelevantItems (batches, _rc) = batches |> Array.exists isRelevant
            let loadOutcomes ct =
                Query.feedIteratorMapTi mapPage query ct
                |> TaskSeq.takeWhile hasRelevantItems
                |> TaskSeq.mapAsync handle
                |> TaskSeq.toArrayAsync
            loadOutcomes |> Stopwatch.time ct
        let mutable lwm, queryCharges, delCharges, trimCharges, responses, batches, eventsDeleted, eventsDeferred = None, 0., 0., 0., 0, 0, 0, 0
        let accumulate ((qc, dc, tc), bLwm, (bCount, eDel, eDef)) =
            lwm <- max lwm bLwm
            queryCharges <- queryCharges + qc
            delCharges <- delCharges + dc
            trimCharges <- trimCharges + tc
            responses <- responses + 1
            batches <- batches + bCount
            eventsDeleted <- eventsDeleted + eDel
            eventsDeferred <- eventsDeferred + eDef
        outcomes |> Array.iter accumulate
        let reqMetric: Log.Measurement = { database = container.Database.Id; container = container.Id; stream = stream; interval = pt; bytes = eventsDeleted; count = batches; ru = queryCharges }
        let log = let evt = Log.Metric.Prune (responses, reqMetric) in log |> Log.event evt
        let lwm = lwm |> Option.defaultValue 0L // If we've seen no batches at all, then the write position is 0L
        log.Information("EqxCosmos {action:l} {events}/{batches} lwm={lwm} {ms:f1}ms queryRu={queryRu} deleteRu={deleteRu} trimRu={trimRu}",
                        "Prune", eventsDeleted, batches, lwm, pt.ElapsedMilliseconds, queryCharges, delCharges, trimCharges)
        return eventsDeleted, eventsDeferred, lwm }

[<NoComparison; NoEquality>]
type Token = { pos: Position }
module Token =
    // TOCONSIDER Could implement accumulating the streamBytes as it's loaded (but should stop counting when it hits the origin event)
    //            Should probably be optional as computing the UTF8/16 size from the JsonElement is not cheap
    //            Alternately, can mirror DynamoStore scheme where the size is maintained in the Tip, and updated as batches are calved
    let create pos: StreamToken = { value = box { pos = pos }; version = pos.index; streamBytes = -1 }
    let (|Unpack|) (token: StreamToken): Position = let t = unbox<Token> token.value in t.pos

    // TOCONSIDER for RollingState, comparing etags is not meaningful - but the (server assigned) timestamp of the tip document can be used to filter out stale updates
    //            see also same function in DynamoStore - ideally the scheme would align - perhaps roundtripping a `revision` instead might make sense here too?
    let isStale current candidate = current.version > candidate.version

[<AutoOpen>]
module Internal =

    [<RequireQualifiedAccess; NoComparison; NoEquality>]
    type InternalSyncResult = Written of StreamToken | ConflictUnknown of StreamToken | Conflict of Position * ITimelineEvent<EncodedBody>[]

    [<RequireQualifiedAccess; NoComparison; NoEquality>]
    type LoadFromTokenResult<'event> = Unchanged | Found of StreamToken * 'event[]

/// Defines the policies in force regarding how to split up calls when loading Event Batches via queries
type QueryOptions
    (   // Max number of Batches to return per paged query response. Default: 10.
        [<O; D null>] ?maxItems: int,
        // Dynamic version of `maxItems`, allowing one to react to dynamic configuration changes. Default: use `maxItems` value.
        [<O; D null>] ?getMaxItems: unit -> int,
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
    member val MaxEvents: int = maxEvents
    /// Maximum serialized size (length of JSON.stringify representation) to permit to accumulate in Tip before they get moved out to a standalone Batch. Default: 30_000.
    member val MaxJsonLength = defaultArg maxJsonLength 30_000
    /// Whether to inhibit throwing when events are missing, but no Archive Container has been supplied
    member val IgnoreMissingEvents = defaultArg ignoreMissingEvents false

    member val ReadRetryPolicy = readRetryPolicy
    member val WriteRetryPolicy = writeRetryPolicy

type StoreClient(container: Container, fallback: Container option, query: QueryOptions, tip: TipOptions) =
    let loadTip log stream pos = Tip.tryLoad log tip.ReadRetryPolicy (container, stream) (pos, None)
    let ignoreMissing = tip.IgnoreMissingEvents

    // Always yields events forward, regardless of direction
    member internal _.Read(log, stream, direction, (tryDecode, isOrigin), ?ct, ?minIndex, ?maxIndex, ?tip): Task<StreamToken * 'event[]> = task {
        let tipContent = tip |> Option.map (Query.scanTip (tryDecode, isOrigin))
        let includeTip = Option.isNone tipContent
        let walk log container = Query.scan log (container, stream) includeTip query.MaxItems query.MaxRequests direction (tryDecode, isOrigin)
        let walkFallback =
            match fallback with
            | None -> Choice1Of2 ignoreMissing
            | Some f -> Choice2Of2 (walk (log |> Log.prop "fallback" true) f)

        let log = log |> Log.prop "stream" stream
        let! pos, events = Query.load log (minIndex, maxIndex) tipContent (walk log container) walkFallback (defaultArg ct CancellationToken.None)
        return Token.create pos, events }
    member _.ReadLazy(log, batching: QueryOptions, stream, direction, (tryDecode, isOrigin), ?ct, ?minIndex, ?maxIndex): IAsyncEnumerable<'event[]> =
        Query.walkLazy log (container, stream) batching.MaxItems batching.MaxRequests (tryDecode, isOrigin) (direction, minIndex, maxIndex, defaultArg ct CancellationToken.None)

    member store.Load(log, (stream, maybePos), (tryDecode, isOrigin), checkUnfolds: bool, ct): Task<StreamToken * 'event[]> =
        if not checkUnfolds then store.Read(log, stream, Direction.Backward, (tryDecode, isOrigin), ct)
        else task {
            match! loadTip log stream maybePos ct with
            | Tip.Result.NotFound -> return Token.create Position.fromKnownEmpty, Array.empty
            | Tip.Result.NotModified -> return invalidOp "Not applicable"
            | Tip.Result.Found (pos, i, xs) -> return! store.Read(log, stream, Direction.Backward, (tryDecode, isOrigin), ct, tip = (pos, i, xs)) }
    member _.GetPosition(log, stream, ct, ?pos): Task<StreamToken> = task {
        match! loadTip log stream pos ct with
        | Tip.Result.NotFound -> return Token.create Position.fromKnownEmpty
        | Tip.Result.NotModified -> return Token.create pos.Value
        | Tip.Result.Found (pos, _i, _unfoldsAndEvents) -> return Token.create pos }
    member store.Reload(log, (stream, pos), (tryDecode, isOrigin), ct, ?preview): Task<LoadFromTokenResult<'event>> =
        let read tipContent = task {
            let! res = store.Read(log, stream, Direction.Backward, (tryDecode, isOrigin), ct, minIndex = pos.index, tip = tipContent)
            return LoadFromTokenResult.Found res }
        match preview with
        | Some (pos, i, xs) -> read (pos, i, xs)
        | None -> task {
            match! loadTip log stream (Some pos) ct with
            | Tip.Result.NotFound -> return LoadFromTokenResult.Found (Token.create Position.fromKnownEmpty, Array.empty)
            | Tip.Result.NotModified -> return LoadFromTokenResult.Unchanged
            | Tip.Result.Found (pos, i, xs) -> return! read (pos, i, xs) }

    member internal _.Sync(log, stream, exp, batch: Tip, ct): Task<InternalSyncResult> = task {
        if Array.isEmpty batch.e && Array.isEmpty batch.u then invalidOp "Must write either events or unfolds."
        match! Sync.batch log (tip.WriteRetryPolicy, tip.MaxEvents, tip.MaxJsonLength) (container, stream) (exp, batch) ct with
        | Sync.Result.Written pos' -> return InternalSyncResult.Written (Token.create pos')
        | Sync.Result.Conflict (pos', events) -> return InternalSyncResult.Conflict (pos', events)
        | Sync.Result.ConflictUnknown pos' -> return InternalSyncResult.ConflictUnknown (Token.create pos') }

    member _.Prune(log, stream, index, ct) =
        Prune.until log (container, stream) query.MaxItems index ct

type internal StoreCategory<'event, 'state, 'req>
    (   store: StoreClient, createStoredProcIfNotExistsExactlyOnce: CancellationToken -> System.Threading.Tasks.ValueTask,
        codec: IEventCodec<'event, EncodedBody, 'req>, fold, initial: 'state, isOrigin: 'event -> bool,
        checkUnfolds, mapUnfolds: Choice<unit, 'event[] -> 'state -> 'event[], 'event[] -> 'state -> 'event[] * 'event[]>) =
    let fold s xs = (fold : Func<'state, 'event[], 'state>).Invoke(s, xs)
    let reload (log, streamName, (Token.Unpack pos as streamToken), state) preloaded ct: Task<struct (StreamToken * 'state)> = task {
        match! store.Reload(log, (streamName, pos), (codec.Decode, isOrigin), ct, ?preview = preloaded) with
        | LoadFromTokenResult.Unchanged -> return struct (streamToken, state)
        | LoadFromTokenResult.Found (token', events) -> return token', fold state events }
    interface ICategory<'event, 'state, 'req> with
        member _.Empty = Token.create Position.fromKnownEmpty, initial
        member _.Load(log, _categoryName, _streamId, stream, _maxAge, _requireLeader, ct): Task<struct (StreamToken * 'state)> = task {
            let! token, events = store.Load(log, (stream, None), (codec.Decode, isOrigin), checkUnfolds, ct)
            return struct (token, fold initial events) }
        member _.Sync(log, _categoryName, _streamId, streamName, req, (Token.Unpack pos as streamToken), state, events, ct) = task {
            let state' = fold state events
            let exp, events, eventsEncoded, unfoldsEncoded =
                let encode e = codec.Encode(req, e)
                match mapUnfolds with
                | Choice1Of3 () ->        SyncExp.fromVersion pos.index, events, Array.map encode events, Seq.empty
                | Choice2Of3 unfold ->    SyncExp.fromVersion pos.index, events, Array.map encode events, Seq.map encode (unfold events state')
                | Choice3Of3 transmute -> let events', unfolds = transmute events state'
                                          SyncExp.fromEtag (defaultArg pos.etag null), events', Array.map encode events', Seq.map encode unfolds
            let baseIndex = pos.index + int64 (Array.length events)
            let batch = Sync.mkBatch streamName eventsEncoded [| for x in unfoldsEncoded -> Sync.mkUnfold baseIndex x |]
            do! createStoredProcIfNotExistsExactlyOnce ct
            match! store.Sync(log, streamName, exp, batch, ct) with
            | InternalSyncResult.Written token' -> return SyncResult.Written (token', state')
            | InternalSyncResult.ConflictUnknown _token' -> return SyncResult.Conflict (reload (log, streamName, streamToken, state) None)
            | InternalSyncResult.Conflict (pos', tipEvents) ->
                return SyncResult.Conflict (reload (log, streamName, streamToken, state) (Some (pos', pos.index, tipEvents))) }
    interface Caching.IReloadable<'state> with member _.Reload(log, sn, _leader, token, state, ct) = reload (log, sn, token, state) None ct
    member _.TryHydrateTip(u, ?etag) =
        match Tip.tryHydrate (codec.Decode, isOrigin) (u, match etag with Some etag -> ValueSome etag | None -> ValueNone) with
        | ValueNone -> ValueNone
        | ValueSome (pos, events) -> ValueSome (Token.create pos, fold initial events)

module ConnectionString =

    let (|AccountEndpoint|) connectionString =
        match System.Data.Common.DbConnectionStringBuilder(ConnectionString = connectionString).TryGetValue "AccountEndpoint" with
        | true, (:? string as s) when not (String.IsNullOrEmpty s) -> s
        | _ -> invalidOp "Connection string does not contain an \"AccountEndpoint\""

[<RequireQualifiedAccess; NoComparison>]
type DiscoveryMode =
    | AccountUriAndKey of accountUri: string * key: string
    | ConnectionString of connectionString: string
    member x.Endpoint = x |> function
        | DiscoveryMode.AccountUriAndKey (u, _k) -> u
        | DiscoveryMode.ConnectionString (ConnectionString.AccountEndpoint e) -> e

/// Manages establishing a CosmosClient, which is used by CosmosStoreClient to read from the underlying Cosmos DB Container
type CosmosClientFactory(options) =
    [<Obsolete "Will be removed in V5; please use the overload that includes `serializerOptions`">]
    static member CreateDefaultOptions(requestTimeout: TimeSpan, maxRetryAttemptsOnRateLimitedRequests: int, maxRetryWaitTimeOnRateLimitedRequests: TimeSpan) =
        CosmosClientOptions(
            MaxRetryAttemptsOnRateLimitedRequests = maxRetryAttemptsOnRateLimitedRequests,
            MaxRetryWaitTimeOnRateLimitedRequests = maxRetryWaitTimeOnRateLimitedRequests,
            RequestTimeout = requestTimeout,
            UseSystemTextJsonSerializerWithOptions = JsonSerializerOptions())
    /// Default when rendering/parsing Batch/Tip/Event/Unfold - omitting null values
    static member val DefaultJsonSerializerOptions = JsonSerializerOptions(DefaultIgnoreCondition = Serialization.JsonIgnoreCondition.WhenWritingNull)
    /// CosmosClientOptions for this CosmosClientFactory as configured (NOTE while the Options object is not immutable, it should not have setters called on it)
    member val Options = options
    /// Creates an instance of CosmosClient without actually validating or establishing the connection
    /// It's recommended to use <c>CreateAndInitializeAsync</c> in preference to this API
    ///   in order to avoid latency spikes, and/or deferring discovery of connectivity or permission issues.
    member x.CreateUninitialized(discovery: DiscoveryMode) = discovery |> function
        | DiscoveryMode.AccountUriAndKey (accountUri = uri; key = key) -> new CosmosClient(uri, key, x.Options)
        | DiscoveryMode.ConnectionString cs -> new CosmosClient(cs, x.Options)
    /// Creates and validates a CosmosClient [including loading metadata](https://devblogs.microsoft.com/cosmosdb/improve-net-sdk-initialization) for the specified containers
    member x.CreateAndInitializeAsync(discovery: DiscoveryMode, containers, ct) = discovery |> function
        | DiscoveryMode.AccountUriAndKey (accountUri = uri; key = key) -> CosmosClient.CreateAndInitializeAsync(uri, key, containers, x.Options, ct)
        | DiscoveryMode.ConnectionString cs -> CosmosClient.CreateAndInitializeAsync(cs, containers, x.Options, ct)

namespace Equinox.CosmosStore

open Equinox.Core
open Equinox.CosmosStore.Core
open Microsoft.Azure.Cosmos
open System

[<RequireQualifiedAccess; NoComparison>]
type Discovery =
    /// Separated Account Uri and Key (for interop with previous versions)
    | AccountUriAndKey of accountUri: Uri * key: string
    /// Cosmos SDK Connection String
    | ConnectionString of connectionString: string
    member x.ToDiscoveryMode() = x |> function
        | Discovery.AccountUriAndKey (u, k) -> DiscoveryMode.AccountUriAndKey (string u, k)
        | Discovery.ConnectionString c -> DiscoveryMode.ConnectionString c

/// Manages establishing a CosmosClient, which is used by CosmosStoreClient to read from the underlying Cosmos DB Container.
type CosmosStoreConnector(discovery: Discovery, factory: CosmosClientFactory) =
    let discoveryMode = discovery.ToDiscoveryMode()
    new(// CosmosDB endpoint/credentials specification.
        discovery: Discovery,
        // Maximum number of times to attempt when failure reason is a 429 from CosmosDB, signifying RU limits have been breached. CosmosDB default: 9
        maxRetryAttemptsOnRateLimitedRequests: int,
        // Maximum number of seconds to wait (especially if a higher wait delay is suggested by CosmosDB in the 429 response). CosmosDB default: 30s
        maxRetryWaitTimeOnRateLimitedRequests: TimeSpan,
        // Connection mode (default: ConnectionMode.Direct (the best performance, same as Microsoft.Azure.Cosmos SDK default)
        // NOTE: default for Equinox.Cosmos.Connector (i.e. V2) was Gateway (worst performance, most tolerant, Microsoft.Azure.DocumentDb SDK default)
        [<O; D null>] ?mode: ConnectionMode,
        // Timeout to apply to individual reads/write round-trips going to CosmosDB. CosmosDB Default: 6s
        // NOTE Per CosmosDB Client guidance, it's recommended to leave this at its default
        [<O; D null>] ?timeout: TimeSpan,
        // System.Text.Json SerializerOptions used for rendering Batches, Events and Unfolds internally
        // NOTE as Events and/or Unfolds are serialized to `JsonElement`, there should rarely be a need to control the options at this level
        [<O; D null>] ?serializerOptions: System.Text.Json.JsonSerializerOptions,
        // consistency mode (default: use configuration specified for Database)
        [<O; D null>] ?defaultConsistencyLevel: ConsistencyLevel,
        [<O; D null>] ?customize: Action<CosmosClientOptions>) =
        let o =
            CosmosClientOptions(
                MaxRetryAttemptsOnRateLimitedRequests = maxRetryAttemptsOnRateLimitedRequests,
                MaxRetryWaitTimeOnRateLimitedRequests = maxRetryWaitTimeOnRateLimitedRequests,
                UseSystemTextJsonSerializerWithOptions = defaultArg serializerOptions CosmosClientFactory.DefaultJsonSerializerOptions)
        mode |> Option.iter (fun x -> o.ConnectionMode <- x)
        timeout |> Option.iter (fun x -> o.RequestTimeout <- x)
        defaultConsistencyLevel |> Option.iter (fun x -> o.ConsistencyLevel <- x)
        customize |> Option.iter (fun c -> c.Invoke o)
        CosmosStoreConnector(discovery, CosmosClientFactory o)

    [<Obsolete "For backcompat only; will be removed in V5">]
    new(discovery, requestTimeout: TimeSpan, maxRetryAttemptsOnRateLimitedRequests: int, maxRetryWaitTimeOnRateLimitedRequests,
        ?mode, ?defaultConsistencyLevel, ?customize) =
        CosmosStoreConnector(discovery, maxRetryAttemptsOnRateLimitedRequests, maxRetryWaitTimeOnRateLimitedRequests, ?mode = mode,
                             ?defaultConsistencyLevel = defaultConsistencyLevel, timeout = requestTimeout, ?customize = customize)

    /// The <c>CosmosClientOptions</c> used when connecting to CosmosDB
    member _.Options = factory.Options

    /// The Endpoint Uri for the target CosmosDB
    member val Endpoint = discoveryMode.Endpoint |> Uri

    /// Creates an instance of CosmosClient without actually validating or establishing the connection
    /// It's recommended to use <c>Connect</c> and/or <c>CreateAndInitialize</c> in preference to this API
    ///   in order to avoid latency spikes, and/or deferring discovery of connectivity or permission issues.
    member _.CreateUninitialized() = factory.CreateUninitialized(discoveryMode)

    /// Creates and validates a CosmosClient [including loading metadata](https://devblogs.microsoft.com/cosmosdb/improve-net-sdk-initialization) for the specified containers
    member _.CreateAndInitializeAsync(containers, ct): Task<CosmosClient> = factory.CreateAndInitializeAsync(discoveryMode, containers, ct)
    /// Creates and validates a CosmosClient [including loading metadata](https://devblogs.microsoft.com/cosmosdb/improve-net-sdk-initialization) for the specified containers
    member x.CreateAndInitialize(databaseAndContainerIds: struct (string * string)[]) =
        Async.call (fun ct -> x.CreateAndInitializeAsync(databaseAndContainerIds, ct))
    /// Creates and validates a CosmosClient [including loading metadata](https://devblogs.microsoft.com/cosmosdb/improve-net-sdk-initialization) for the specified containers within the (single) database
    member x.CreateAndInitialize(databaseId, containerIds: string[]) =
        x.CreateAndInitialize[| for containerId in containerIds -> databaseId, containerId |]

    /// Creates and validates a CosmosStoreClient [including loading metadata](https://devblogs.microsoft.com/cosmosdb/improve-net-sdk-initialization) for the specified containers
    member _.ConnectAsync(containers, ct): Task<CosmosStoreClient> = task {
        let! cosmosClient = factory.CreateAndInitializeAsync(discoveryMode, containers, ct)
        return CosmosStoreClient(cosmosClient) }
    /// Creates and validates a CosmosStoreClient [including loading metadata](https://devblogs.microsoft.com/cosmosdb/improve-net-sdk-initialization) for the specified containers
    member x.Connect(databaseAndContainerIds: struct (string * string)[]) =
        Async.call (fun ct -> x.ConnectAsync(databaseAndContainerIds, ct))
    /// Creates and validates a CosmosStoreClient [including loading metadata](https://devblogs.microsoft.com/cosmosdb/improve-net-sdk-initialization) for the specified containers
    member x.Connect(databaseId, containerIds: string[]) =
        x.Connect[| for containerId in containerIds -> databaseId, containerId |]

/// Holds all relevant state for a Store within a given CosmosDB Database
/// - The CosmosDB CosmosClient (there should be a single one of these per process, plus an optional fallback one for pruning scenarios)
/// - The (singleton) per Container Stored Procedure initialization state
and CosmosStoreClient
    (   client: CosmosClient,
        // Client to use for fallback Containers. Default: use <c>client</c>
        // Typically created via <c>CosmoStoreConnector.CreateAndInitialize</c>
        [<O; D null>] ?archiveClient: CosmosClient,
        // Admits a hook to enable customization of how <c>Equinox.CosmosStore</c> handles the low level interactions with the underlying Cosmos <c>Container</c>.
        [<O; D null>] ?customize: Func<Container, Container>,
        // Inhibit <c>CreateStoredProcedureIfNotExists</c> when a given Container is used for the first time
        [<O; D null>] ?disableInitialization) =
    let containerInitGuards = System.Collections.Concurrent.ConcurrentDictionary<struct (string * string), Initialization.ContainerInitializerGuard>()
    member private x.CreateContainer(client: CosmosClient, struct (databaseId, containerId)) =
        let container = client.GetDatabase(databaseId).GetContainer(containerId)
        match customize with Some f -> f.Invoke container | None -> container
    member internal x.GetOrAddPrimaryContainer(databaseId, containerId): Initialization.ContainerInitializerGuard =
        let createContainerInitializerGuard databaseIdAndContainerId =
            let init = match disableInitialization with Some true -> None | _ -> Some (Initialization.createSyncStoredProcIfNotExists None)
            Initialization.ContainerInitializerGuard(x.CreateContainer(client, databaseIdAndContainerId), ?initContainer = init)
        containerInitGuards.GetOrAdd(struct (databaseId, containerId), createContainerInitializerGuard)
    member internal x.CreateFallbackContainer(databaseId, containerId) =
        x.CreateContainer(defaultArg archiveClient client, (databaseId, containerId))

/// Defines the policies for accessing a given Container (And optional fallback Container for retrieval of archived data).
type CosmosStoreContext(client: CosmosStoreClient, databaseId, containerId, tipOptions, queryOptions, ?archive) =
    let containerGuard = client.GetOrAddPrimaryContainer(databaseId, containerId)
    member val Container = containerGuard.Container
    member val QueryOptions = queryOptions
    member val TipOptions = tipOptions
    new(client: CosmosStoreClient, databaseId, containerId,
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
        [<O; D null>] ?queryMaxRequests,
        // Database Name to use for locating missing events. Default: use <c>databaseId</c>, if <c>archiveContainerId</c> specified.
        [<O; D null>] ?archiveDatabaseId,
        // Container Name to use for locating missing events. Default: use <c>containerId</c>, if <c>archiveDatabaseId</c> specified.
        [<O; D null>] ?archiveContainerId) =
        let tipOptions = TipOptions(maxEvents = tipMaxEvents, ?maxJsonLength = tipMaxJsonLength, ?ignoreMissingEvents = ignoreMissingEvents)
        let queryOptions = QueryOptions(?maxItems = queryMaxItems, ?maxRequests = queryMaxRequests)
        let archive =
            match archiveDatabaseId, archiveContainerId with
            | None, None ->     None
            | None, Some c ->   Some (databaseId, c)
            | Some d, c ->      Some (d, defaultArg c containerId)
        CosmosStoreContext(client, databaseId, containerId, tipOptions, queryOptions, ?archive = archive)
    member val internal StoreClient =
        let fallback = archive |> Option.map client.CreateFallbackContainer
        StoreClient(containerGuard.Container, fallback, queryOptions, tipOptions)
    // Writes go through the stored proc, which we need to provision per container
    member internal _.EnsureStoredProcedureInitialized ct = containerGuard.Initialize ct

[<NoComparison; NoEquality; RequireQualifiedAccess>]
type AccessStrategy<'event, 'state> =
    /// Don't apply any optimized reading logic. Note this can be extremely RU cost prohibitive
    /// and can severely impact system scalability. Should hence only be used with careful consideration.
    | Unoptimized
    /// <summary>Load only the single most recent event defined in <c>&apos;event</c> and trust that doing a <c>fold</c> from any such event
    /// will yield a correct and complete state
    /// In other words, the <c>fold</c> function should not need to consider either the preceding <c>&apos;state</c> or <c>'event</c>s.</summary>
    /// <remarks>
    /// A copy of the event is also retained in the `Tip` document in order that the state of the stream can be
    /// retrieved using a single (cached, etag-checked) point read.
    /// </remarks>
    | LatestKnownEvent
    /// <summary>Allow a 'snapshot' event (and/or other events that that pass the <c>isOrigin</c> test) to be used to build the state
    /// in lieu of folding all the events from the start of the stream, as a performance optimization.
    /// <c>toSnapshot</c> is used to generate the <c>unfold</c> that will be held in the Tip document in order to
    /// enable efficient reading without having to query the Event documents.</summary>
    | Snapshot of isOrigin: ('event -> bool) * toSnapshot: ('state -> 'event)
    /// <summary>Allow any events that pass the <c>isOrigin</c> test to be used in lieu of folding all the events from the start of the stream
    /// When writing, uses `toSnapshots` to 'unfold' the <c>&apos;state</c>, representing it as one or more Event records to be stored in
    /// the Tip with efficient read cost.</summary>
    | MultiSnapshot of isOrigin: ('event -> bool) * toSnapshots: ('state -> 'event[])
    /// Instead of actually storing the events representing the decisions, only ever update a snapshot stored in the Tip document
    /// <remarks>In this mode, Optimistic Concurrency Control is necessarily based on the _etag</remarks>
    | RollingState of toSnapshot: ('state -> 'event)
    /// Allow produced events to be filtered, transformed or removed completely and/or to be transmuted to unfolds.
    /// <remarks>
    /// In this mode, Optimistic Concurrency Control is based on the _etag (rather than the normal Expected Version strategy)
    /// in order that conflicting updates to the state not involving the writing of an event can trigger retries.
    /// </remarks>
    | Custom of isOrigin: ('event -> bool) * transmute: ('event[] -> 'state -> 'event[] * 'event[])

type CosmosStoreCategory<'event, 'state, 'req> private (name, inner, tryHydrateTip) =
    inherit Equinox.Category<'event, 'state, 'req>(name, inner)
    new(context: CosmosStoreContext, name, codec, fold, initial, access,
        // For CosmosDB, caching is typically a central aspect of managing RU consumption to maintain performance and capacity.
        // The cache holds the Tip document's etag, which enables use of etag-contingent Reads (which cost only 1RU in the case where the document is unchanged)
        // Omitting can make sense in specific cases; if cache hit rates are low, or there's always a usable snapshot in a relatively small Tip document
        // NOTE Using NoCaching with CosmosDB can have significant implications for the scalability of your application, both in terms of latency and running costs.
        // While the cost of a cache miss can be ameliorated to varying degrees by employing an appropriate `AccessStrategy`
        //   [that works well and has been validated for your scenario with real data], even a cache with a low Hit Rate provides
        //   a direct benefit in terms of the number of Request Unit (RU)s that need to be provisioned to your CosmosDB instances.
        // NOTE Unless <c>LoadOption.AnyCachedValue</c> or <c>AllowStale</c> are used, cache hits still incurs an etag-contingent Tip read (at a cost of a roundtrip with a 1RU charge if unmodified).
        // NOTE re SlidingWindowPrefixed: the recommended approach is to track all relevant data in the state, and/or have the `unfold` function ensure _all_ relevant events get held in the `u`nfolds in Tip
        caching) =
        let isOrigin, checkUnfolds, mapUnfolds =
            match access with
            | AccessStrategy.Unoptimized ->                      (fun _ -> false), false, Choice1Of3 ()
            | AccessStrategy.LatestKnownEvent ->                 (fun _ -> true),  true,  Choice2Of3 (fun events _ -> events |> Array.last |> Array.singleton)
            | AccessStrategy.Snapshot (isOrigin, toSnapshot) ->  isOrigin,         true,  Choice2Of3 (fun _ -> toSnapshot >> Array.singleton)
            | AccessStrategy.MultiSnapshot (isOrigin, unfold) -> isOrigin,         true,  Choice2Of3 (fun _ -> unfold)
            | AccessStrategy.RollingState toSnapshot ->          (fun _ -> true),  true,  Choice3Of3 (fun _ state  -> Array.empty, toSnapshot state |> Array.singleton)
            | AccessStrategy.Custom (isOrigin, transmute) ->     isOrigin,         true,  Choice3Of3 transmute
        let sc = StoreCategory<'event, 'state, 'req>(context.StoreClient, context.EnsureStoredProcedureInitialized, codec, fold, initial, isOrigin, checkUnfolds, mapUnfolds)
        CosmosStoreCategory<'event, 'state, 'req>(name, sc |> Caching.apply Token.isStale caching, fun u e -> sc.TryHydrateTip(u, ?etag = e))
    /// Parses the Unfolds (the `u` field of the Item with `id = "-1"`) into a form that can be passed to `Decider.Query` or `Decider.Transact` via `, ?load = <result>>`
    member _.TryHydrateTip(u, [<O; D null>] ?etag: string) =
        match tryHydrateTip u etag with
        | ValueSome (t, s) -> Some (Equinox.LoadOption.FromMemento struct (t, s))
        | ValueNone -> None
    /// Derives from the supplied `u`nfolds. Yields `None` if there is no relevant `origin` event within the supplied array.
    member x.TryLoad u = x.TryHydrateTip u |> Option.bind (function Equinox.LoadOption.FromMemento (_, s) -> Some s | _ -> None)

module Exceptions =

    let [<return: Struct>] (|CosmosStatus|_|) (x: exn) = match x with :? CosmosException as ce -> ValueSome ce.StatusCode | _ -> ValueNone
    let (|RateLimited|RequestTimeout|ServiceUnavailable|CosmosStatusCode|Other|) = function
        | CosmosStatus System.Net.HttpStatusCode.TooManyRequests ->     RateLimited
        | CosmosStatus System.Net.HttpStatusCode.RequestTimeout ->      RequestTimeout
        | CosmosStatus System.Net.HttpStatusCode.ServiceUnavailable ->  ServiceUnavailable
        | CosmosStatus s -> CosmosStatusCode s
        | _ -> Other

namespace Equinox.CosmosStore.Core

open Equinox.Core
open FsCodec
open FSharp.Control
open System.Collections.Generic

/// Outcome of appending events, specifying the new and/or conflicting events, together with the updated Target write position
[<RequireQualifiedAccess; NoComparison>]
type AppendResult<'t> =
    | Ok of pos: 't
    | Conflict of index: 't * conflictingEvents: ITimelineEvent<EncodedBody>[]
    | ConflictUnknown of index: 't

/// Encapsulates the core facilities Equinox.CosmosStore offers for operating directly on Events in Streams.
type EventsContext
    (   context: Equinox.CosmosStore.CosmosStoreContext,
        // Logger to write to - see https://github.com/serilog/serilog/wiki/Provided-Sinks for how to wire to your logger
        log: Serilog.ILogger) =
    let resolve streamName = context.StoreClient, StreamName.toString streamName
    do if log = null then nullArg "log"
    let maxCountPredicate count =
        let acc = ref (max (count-1) 0)
        fun _ ->
            if acc.Value = 0 then true else
            acc.Value <- acc.Value - 1
            false

    let yieldPositionAndData res = task {
        let! Token.Unpack pos', data = res
        return pos', data }

    let getRange direction startPos =
        let startPos = startPos |> Option.map _.index
        match direction with
        | Direction.Forward -> startPos, None
        | Direction.Backward -> None, startPos

    member internal _.GetLazy(streamName, ?ct, ?queryMaxItems, ?direction, ?minIndex, ?maxIndex): IAsyncEnumerable<ITimelineEvent<EncodedBody>[]> =
        let direction = defaultArg direction Direction.Forward
        let batching = match queryMaxItems with Some qmi -> QueryOptions(qmi) | _ -> context.QueryOptions
        let store, stream = resolve streamName
        store.ReadLazy(log, batching, stream, direction, (Some, fun _ -> false), ?ct = ct, ?minIndex = minIndex, ?maxIndex = maxIndex)

    member internal _.GetInternal((streamName, startPos), ?ct, ?maxCount, ?direction) = task {
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
            let store, stream = resolve streamName
            let! token, events = store.Read(log, stream, direction, (ValueSome, isOrigin), ?ct = ct, ?minIndex = minIndex, ?maxIndex = maxIndex)
            if direction = Direction.Backward then System.Array.Reverse events
            return token, events }

    /// Establishes the current position of the stream in as efficient a manner as possible
    /// (The ideal situation is that the preceding token is supplied as input in order to avail of 1RU low latency validation in the case of an unchanged Tip)
    member _.Sync(streamName, ct, [<O; D null>] ?position: Position): Task<Position> = task {
        let store, stream = resolve streamName
        let! Token.Unpack pos' = store.GetPosition(log, stream, ct, ?pos = position)
        return pos' }

    /// Query (with MaxItems set to `queryMaxItems`) from the specified `Position`, allowing the reader to efficiently walk away from a running query
    /// ... NB as long as they Dispose!
    member x.Walk(streamName, queryMaxItems, [<O; D null>] ?ct, [<O; D null>] ?minIndex, [<O; D null>] ?maxIndex, [<O; D null>] ?direction): IAsyncEnumerable<ITimelineEvent<EncodedBody>[]> =
        x.GetLazy(streamName, ?ct = ct, queryMaxItems = queryMaxItems, ?direction = direction, ?minIndex = minIndex, ?maxIndex = maxIndex)

    /// Reads all Events from a `Position` in a given `direction`
    member x.Read(streamName, [<O; D null>] ?ct, [<O; D null>] ?position, [<O; D null>] ?maxCount, [<O; D null>] ?direction): Task<Position*ITimelineEvent<EncodedBody>[]> =
        x.GetInternal((streamName, position), ?ct = ct, ?maxCount = maxCount, ?direction = direction) |> yieldPositionAndData

    [<System.Obsolete "Will be removed in V5 in favor of the overload that requires explicit passing of the Unfolds">]
    member x.Sync(streamName, position, events: IEventData<_>[], ct): Task<AppendResult<Position>> =
        x.Sync(streamName, position, events, Array.empty, ct)

    /// Appends the supplied batch of events (and, optionally, unfolds), subject to a consistency check based on the `position`
    /// Callers should implement appropriate idempotent handling, or use Equinox.Decider for that purpose
    member _.Sync(streamName, position, events: IEventData<_>[], unfolds: #IEventData<EncodedBody>[], ct): Task<AppendResult<Position>> = task {
        do! context.EnsureStoredProcedureInitialized ct
        let store, stream = resolve streamName

        let baseIndex = position.index + int64 events.Length
        let batch = Sync.mkBatch stream events [| for x in unfolds -> Sync.mkUnfold baseIndex x |]
        match! store.Sync(log, stream, SyncExp.fromVersionOrAppendAtEnd position.index, batch, ct) with
        | InternalSyncResult.Written (Token.Unpack pos) -> return AppendResult.Ok pos
        | InternalSyncResult.Conflict (pos, events) -> return AppendResult.Conflict (pos, events)
        | InternalSyncResult.ConflictUnknown (Token.Unpack pos) -> return AppendResult.ConflictUnknown pos }

    /// Low level, non-idempotent call appending events to a stream without a concurrency control mechanism in play
    /// NB Should be used sparingly; Equinox.Decider enables building equivalent idempotent handling with minimal code.
    member x.NonIdempotentAppend(streamName, events: IEventData<_>[], ct): Task<Position> = task {
        match! x.Sync(streamName, Position.fromAppendAtEnd, events, Array.empty, ct) with
        | AppendResult.Ok token -> return token
        | x -> return invalidOp $"Conflict despite it being disabled %A{x}" }

    member _.Prune(streamName, index, ct): Task<int * int * int64> =
        let store, stream = resolve streamName
        store.Prune(log, stream, index, ct)

/// Api as defined in the Equinox Specification
/// Note the CosmosContext APIs can yield better performance due to the fact that a Position tracks the etag of the Stream's Tip
module Events =

    let private (|PositionIndex|) (x: Position) = x.index
    let private stripSyncResult (f: Task<AppendResult<Position>>): Task<AppendResult<int64>> = task {
        match! f with
        | AppendResult.Ok (PositionIndex index) -> return AppendResult.Ok index
        | AppendResult.Conflict (PositionIndex index, events) -> return AppendResult.Conflict (index, events)
        | AppendResult.ConflictUnknown (PositionIndex index) -> return AppendResult.ConflictUnknown index }
    let private stripPosition (f: Task<Position>): Task<int64> = task {
        let! (PositionIndex index) = f
        return index }
    let private dropPosition (f: Task<Position * ITimelineEvent<EncodedBody>[]>): Task<ITimelineEvent<EncodedBody>[]> = task {
        let! _, xs = f
        return xs }
    let (|MinPosition|) = function
        | 0L -> None
        | i -> Some (Position.fromIndex i)
    let (|MaxPosition|) = function
        | int64.MaxValue -> None
        | i -> Some (Position.fromIndex (i + 1L))

    /// Returns an async sequence of events in the stream starting at the specified sequence number,
    /// reading in batches of the specified size.
    /// Returns an empty sequence if the stream is empty or if the sequence number is larger than the largest
    /// sequence number in the stream.
    let getAll (ctx: EventsContext) (streamName: StreamName) (index: int64) (batchSize: int): Async<IAsyncEnumerable<ITimelineEvent<EncodedBody>[]>> = async {
        let! ct = Async.CancellationToken
        return ctx.Walk(streamName, batchSize, ct, minIndex = index) }

    /// Returns an async array of events in the stream starting at the specified sequence number,
    /// number of events to read is specified by batchSize
    /// Returns an empty sequence if the stream is empty or if the sequence number is larger than the largest
    /// sequence number in the stream.
    let get (ctx: EventsContext) (streamName: StreamName) (MinPosition index: int64) (maxCount: int): Async<ITimelineEvent<EncodedBody>[]> =
        Async.call (fun ct -> ctx.Read(streamName, ct, ?position = index, maxCount = maxCount) |> dropPosition)

    /// Appends a batch of events to a stream at the specified expected sequence number.
    /// If the specified expected sequence number does not match the stream, the events are not appended
    /// and a failure is returned.
    let append (ctx: EventsContext) (streamName: StreamName) (index: int64) (events: IEventData<_>[]): Async<AppendResult<int64>> =
        Async.call (fun ct -> ctx.Sync(streamName, Position.fromIndex index, events, Array.empty, ct) |> stripSyncResult)

    /// Appends a batch of events to a stream at the present Position without any conflict checks.
    /// NB typically, it is recommended to ensure idempotency of operations by using the `append` and related API as
    /// this facilitates ensuring consistency is maintained, and yields reduced latency and Request Charges impacts
    /// (See equivalent APIs on `Context` that yield `Position` values)
    let appendAtEnd (ctx: EventsContext) (streamName: StreamName) (events: IEventData<_>[]): Async<int64> =
        Async.call (fun ct -> ctx.NonIdempotentAppend(streamName, events, ct) |> stripPosition)

    /// Requests deletion of events up and including the specified <c>index</c>.
    /// Due to the need to preserve ordering of data in the stream, only complete Batches will be removed.
    /// If the <c>index</c> is within the Tip, events are removed via an etag-checked update. Does not alter the unfolds held in the Tip, or remove the Tip itself.
    /// Returns count of events deleted this time, events that could not be deleted due to partial batches, and the stream's lowest remaining sequence number.
    let pruneUntil (ctx: EventsContext) (streamName: StreamName) (index: int64): Async<int * int * int64> =
        Async.call (fun ct -> ctx.Prune(streamName, index, ct))

    /// Returns an async sequence of events in the stream backwards starting from the specified sequence number,
    /// reading in batches of the specified size.
    /// Returns an empty sequence if the stream is empty or if the sequence number is smaller than the smallest
    /// sequence number in the stream.
    let getAllBackwards (ctx: EventsContext) (streamName: StreamName) (index: int64) (batchSize: int): Async<IAsyncEnumerable<ITimelineEvent<EncodedBody>[]>> = async {
        let! ct = Async.CancellationToken
        return ctx.Walk(streamName, batchSize, ct, maxIndex = index, direction = Direction.Backward) }

    /// Returns an async array of events in the stream backwards starting from the specified sequence number,
    /// number of events to read is specified by batchSize
    /// Returns an empty sequence if the stream is empty or if the sequence number is smaller than the smallest
    /// sequence number in the stream.
    let getBackwards (ctx: EventsContext) (streamName: StreamName) (MaxPosition index: int64) (maxCount: int): Async<ITimelineEvent<EncodedBody>[]> =
        Async.call (fun ct -> ctx.Read(streamName, ct, ?position = index, maxCount = maxCount, direction = Direction.Backward) |> dropPosition)

    /// Obtains the `index` from the current write Position
    let getNextIndex (ctx: EventsContext) (streamName: StreamName): Async<int64> =
        Async.call (fun ct -> ctx.Sync(streamName, ct = ct) |> stripPosition)
