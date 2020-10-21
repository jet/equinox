namespace Equinox.Cosmos.Store

open Equinox.Core
open FsCodec
open Microsoft.Azure.Documents
open Newtonsoft.Json
open Serilog
open System
open System.IO

/// A single Domain Event from the array held in a Batch
type [<NoEquality; NoComparison; JsonObject(ItemRequired=Required.Always)>]
    Event =
    {   /// Creation datetime (as opposed to system-defined _lastUpdated which is touched by triggers, replication etc.)
        t: DateTimeOffset // ISO 8601

        /// The Case (Event Type); used to drive deserialization
        c: string // required

        /// Event body, as UTF-8 encoded json ready to be injected into the Json being rendered for CosmosDB
        [<JsonConverter(typeof<FsCodec.NewtonsoftJson.VerbatimUtf8JsonConverter>)>]
        [<JsonProperty(Required=Required.AllowNull)>]
        d: byte[] // Required, but can be null so Nullary cases can work

        /// Optional metadata, as UTF-8 encoded json, ready to emit directly (null, not written if missing)
        [<JsonConverter(typeof<FsCodec.NewtonsoftJson.VerbatimUtf8JsonConverter>)>]
        [<JsonProperty(Required=Required.Default, NullValueHandling=NullValueHandling.Ignore)>]
        m: byte[]

        /// Optional correlationId (can be null, not written if missing)
        [<JsonProperty(Required=Required.Default, NullValueHandling=NullValueHandling.Ignore)>]
        correlationId : string

        /// Optional causationId (can be null, not written if missing)
        [<JsonProperty(Required=Required.Default, NullValueHandling=NullValueHandling.Ignore)>]
        causationId : string }

    interface IEventData<byte[]> with
        member __.EventType = __.c
        member __.Data = __.d
        member __.Meta = __.m
        member __.EventId = Guid.Empty
        member __.CorrelationId = __.correlationId
        member __.CausationId = __.causationId
        member __.Timestamp = __.t

/// A 'normal' (frozen, not Tip) Batch of Events (without any Unfolds)
type [<NoEquality; NoComparison; JsonObject(ItemRequired=Required.Always)>]
    Batch =
    {   /// CosmosDB-mandated Partition Key, must be maintained within the document
        /// Not actually required if running in single partition mode, but for simplicity, we always write it
        [<JsonProperty(Required=Required.Default)>] // Not requested in queries
        p: string // "{streamName}"

        /// CosmosDB-mandated unique row key; needs to be unique within any partition it is maintained; must be string
        /// At the present time, one can't perform an ORDER BY on this field, hence we also have i shadowing it
        /// NB Tip uses a well known value here while it's actively 'open'
        id: string // "{index}"

        /// When we read, we need to capture the value so we can retain it for caching purposes
        /// NB this is not relevant to fill in when we pass it to the writing stored procedure
        /// as it will do: 1. read 2. merge 3. write merged version contingent on the _etag not having changed
        [<JsonProperty(DefaultValueHandling=DefaultValueHandling.Ignore, Required=Required.Default)>]
        _etag: string

        /// base 'i' value for the Events held herein
        i: int64 // {index}

        // `i` value for successor batch (to facilitate identifying which Batch a given startPos is within)
        n: int64 // {index}

        /// The Domain Events (as opposed to Unfolded Events, see Tip) at this offset in the stream
        e: Event[] }
    /// Unless running in single partition mode (which would restrict us to 10GB per collection)
    /// we need to nominate a partition key that will be in every document
    static member internal PartitionKeyField = "p"
    /// As one cannot sort by the implicit `id` field, we have an indexed `i` field for sort and range query use
    static member internal IndexedFields = [Batch.PartitionKeyField; "i"; "n"]

/// Compaction/Snapshot/Projection Event based on the state at a given point in time `i`
type Unfold =
    {   /// Base: Stream Position (Version) of State from which this Unfold Event was generated
        i: int64

        /// Generation datetime
        t: DateTimeOffset // ISO 8601 // Not written by versions <= 2.0.0-rc9

        /// The Case (Event Type) of this compaction/snapshot, used to drive deserialization
        c: string // required

        /// UTF-8 JSON OR Event body - Json -> UTF-8 -> Deflate -> Base64
        [<JsonConverter(typeof<Base64MaybeDeflateUtf8JsonConverter>)>]
        d: byte[] // required

        /// Optional metadata, same encoding as `d` (can be null; not written if missing)
        [<JsonConverter(typeof<Base64MaybeDeflateUtf8JsonConverter>)>]
        [<JsonProperty(Required=Required.Default, NullValueHandling=NullValueHandling.Ignore)>]
        m: byte[] } // optional

/// Transparently encodes/decodes fields that can optionally by compressed by
/// 1. Writing outgoing values (which may be JSON string, JSON object, or null) from a UTF-8 JSON array representation as per VerbatimUtf8Converter
/// 2a. Decoding incoming JSON String values by Decompressing it to a UTF-8 JSON array representation
/// 2b. Decoding incoming JSON non-string values by reading the raw value directly into a UTF-8 JSON array as per VerbatimUtf8Converter
and Base64MaybeDeflateUtf8JsonConverter() =
    inherit JsonConverter()
    let inflate str : byte[] =
        let compressedBytes = System.Convert.FromBase64String str
        use input = new MemoryStream(compressedBytes)
        use decompressor = new System.IO.Compression.DeflateStream(input, System.IO.Compression.CompressionMode.Decompress)
        use output = new MemoryStream()
        decompressor.CopyTo(output)
        output.ToArray()
    static member Compress(input : byte[]) : byte[] =
        if input = null || input.Length = 0 then null else

        use output = new System.IO.MemoryStream()
        use compressor = new System.IO.Compression.DeflateStream(output, System.IO.Compression.CompressionLevel.Optimal)
        compressor.Write(input,0,input.Length)
        compressor.Close()
        String.Concat("\"", System.Convert.ToBase64String(output.ToArray()), "\"")
        |> System.Text.Encoding.UTF8.GetBytes
    override __.CanConvert(objectType) =
        typeof<byte[]>.Equals(objectType)
    override __.ReadJson(reader, _, _, serializer) =
        match reader.TokenType with
        | JsonToken.Null -> null
        | JsonToken.String -> serializer.Deserialize(reader, typedefof<string>) :?> string |> inflate |> box
        | _ -> Newtonsoft.Json.Linq.JToken.Load reader |> string |> System.Text.Encoding.UTF8.GetBytes |> box
    override __.WriteJson(writer, value, serializer) =
        let array = value :?> byte[]
        if array = null || array.Length = 0 then serializer.Serialize(writer, null)
        else System.Text.Encoding.UTF8.GetString array |> writer.WriteRawValue

/// The special-case 'Pending' Batch Format used to read the currently active (and mutable) document
/// Stored representation has the following diffs vs a 'normal' (frozen/completed) Batch: a) `id` = `-1` b) contains unfolds (`u`)
/// NB the type does double duty as a) model for when we read it b) encoding a batch being sent to the stored proc
type [<NoEquality; NoComparison; JsonObject(ItemRequired=Required.Always)>]
    Tip =
    {   [<JsonProperty(Required=Required.Default)>] // Not requested in queries
        /// Partition key, as per Batch
        p: string // "{streamName}"
        /// Document Id within partition, as per Batch
        id: string // "{-1}" - Well known IdConstant used while this remains the pending batch

        /// When we read, we need to capture the value so we can retain it for caching purposes
        /// NB this is not relevant to fill in when we pass it to the writing stored procedure
        /// as it will do: 1. read 2. merge 3. write merged version contingent on the _etag not having changed
        [<JsonProperty(DefaultValueHandling=DefaultValueHandling.Ignore, Required=Required.Default)>]
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
type [<NoComparison>]
    Position = { index: int64; etag: string option }

module internal Position =
    /// NB very inefficient compared to FromDocument or using one already returned to you
    let fromI (i: int64) = { index = i; etag = None }
    /// If we have strong reason to suspect a stream is empty, we won't have an etag (and Writer Stored Procedure special cases this)
    let fromKnownEmpty = fromI 0L
    /// Just Do It mode
    let fromAppendAtEnd = fromI -1L // sic - needs to yield -1
    let fromEtag (value : string) = { fromI -2L with etag = Some value }
    /// NB very inefficient compared to FromDocument or using one already returned to you
    let fromMaxIndex (xs: ITimelineEvent<byte[]>[]) =
        if Array.isEmpty xs then fromKnownEmpty
        else fromI (1L + Seq.max (seq { for x in xs -> x.Index }))
    /// Create Position from Tip record context (facilitating 1 RU reads)
    let fromTip (x: Tip) = { index = x.n; etag = match x._etag with null -> None | x -> Some x }
    /// If we encounter the tip (id=-1) item/document, we're interested in its etag so we can re-sync for 1 RU
    let tryFromBatch (x: Batch) =
        if x.id <> Tip.WellKnownDocumentId then None
        else Some { index = x.n; etag = match x._etag with null -> None | x -> Some x }

[<RequireQualifiedAccess>]
type Direction = Forward | Backward override this.ToString() = match this with Forward -> "Forward" | Backward -> "Backward"

type internal Enum() =
    static member internal Events(b: Tip) : ITimelineEvent<byte[]> seq =
        b.e |> Seq.mapi (fun offset x -> FsCodec.Core.TimelineEvent.Create(b.i + int64 offset, x.c, x.d, x.m, Guid.Empty, x.correlationId, x.causationId, x.t))
    static member Events(i: int64, e: Event[], startPos : Position option, direction) : ITimelineEvent<byte[]> seq = seq {
        // If we're loading from a nominated position, we need to discard items in the batch before/after the start on the start page
        let isValidGivenStartPos i =
            match startPos with
            | Some sp when direction = Direction.Backward -> i < sp.index
            | Some sp -> i >= sp.index
            | _ -> true
        for offset in 0..e.Length-1 do
            let index = i + int64 offset
            if isValidGivenStartPos index then
                let x = e.[offset]
                yield FsCodec.Core.TimelineEvent.Create(index, x.c, x.d, x.m, Guid.Empty, x.correlationId, x.causationId, x.t) }
    static member internal Events(b: Batch, startPos, direction) =
        Enum.Events(b.i, b.e, startPos, direction)
        |> if direction = Direction.Backward then System.Linq.Enumerable.Reverse else id
    static member Unfolds(xs: Unfold[]) : ITimelineEvent<byte[]> seq = seq {
        for x in xs -> FsCodec.Core.TimelineEvent.Create(x.i, x.c, x.d, x.m, Guid.Empty, null, null, x.t, isUnfold=true) }
    static member EventsAndUnfolds(x: Tip): ITimelineEvent<byte[]> seq =
        Enum.Events x
        |> Seq.append (Enum.Unfolds x.u)
        // where Index is equal, unfolds get delivered after the events so the fold semantics can be 'idempotent'
        |> Seq.sortBy (fun x -> x.Index, x.IsUnfold)

type IRetryPolicy = abstract member Execute: (int -> Async<'T>) -> Async<'T>

module Log =
    [<NoEquality; NoComparison>]
    type Measurement = { stream: string; interval: StopwatchInterval; bytes: int; count: int; ru: float }
    [<NoEquality; NoComparison>]
    type Event =
        /// Individual read request for the Tip
        | Tip of Measurement
        /// Individual read request for the Tip, not found
        | TipNotFound of Measurement
        /// Tip read with Single RU Request Charge due to correct use of etag in cache
        | TipNotModified of Measurement
        /// Summarizes a set of Responses for a given Read request
        | Query of Direction * responses: int * Measurement
        /// Individual read request in a Batch
        /// Charges are rolled up into Query (so do not double count)
        | Response of Direction * Measurement
        | SyncSuccess of Measurement
        | SyncResync of Measurement
        | SyncConflict of Measurement
        /// Handled response from listing of batches in a stream
        /// Charges are rolled up into Prune (so do not double count)
        | PruneResponse of Measurement
        /// Deleted an individual Batch
        | Delete of Measurement
        /// Pruned batches from head of a stream
        /// Count in Measurement is number of batches (documents) deleted
        /// Bytes in Measurement is number of events deleted
        | Prune of responsesHandled : int * Measurement
    let prop name value (log : ILogger) = log.ForContext(name, value)
    let propData name (events: #IEventData<byte[]> seq) (log : ILogger) =
        let render = function null -> "null" | bytes -> System.Text.Encoding.UTF8.GetString bytes
        let items = seq { for e in events do yield sprintf "{\"%s\": %s}" e.EventType (render e.Data) }
        log.ForContext(name, sprintf "[%s]" (String.concat ",\n\r" items))
    let propEvents = propData "events"
    let propDataUnfolds = Enum.Unfolds >> propData "unfolds"
    let propStartPos (value : Position) log = prop "startPos" value.index log
    let propStartEtag (value : Position) log = prop "startEtag" value.etag log

    let withLoggedRetries<'t> (retryPolicy: IRetryPolicy option) (contextLabel : string) (f : ILogger -> Async<'t>) log: Async<'t> =
        match retryPolicy with
        | None -> f log
        | Some retryPolicy ->
            let withLoggingContextWrapping count =
                let log = if count = 1 then log else log |> prop contextLabel count
                f log
            retryPolicy.Execute withLoggingContextWrapping
    /// Attach a property to the log context to hold the metrics
    // Sidestep Log.ForContext converting to a string; see https://github.com/serilog/serilog/issues/1124
    open Serilog.Events
    let event (value : Event) (log : ILogger) =
        let enrich (e : LogEvent) = e.AddPropertyIfAbsent(LogEventProperty("cosmosEvt", ScalarValue(value)))
        log.ForContext({ new Serilog.Core.ILogEventEnricher with member __.Enrich(evt,_) = enrich evt })
    let (|BlobLen|) = function null -> 0 | (x : byte[]) -> x.Length
    let (|EventLen|) (x: #IEventData<_>) = let (BlobLen bytes), (BlobLen metaBytes) = x.Data, x.Meta in bytes + metaBytes + 80
    let (|BatchLen|) = Seq.sumBy (|EventLen|)

    /// NB Caveat emptor; this is subject to unlimited change without the major version changing - while the `dotnet-templates` repo will be kept in step, and
    /// the ChangeLog will mention changes, it's critical to not assume that the presence or nature of these helpers be considered stable
    module InternalMetrics =

        module Stats =
            let inline (|Stats|) ({ interval = i; ru = ru }: Measurement) = ru, let e = i.Elapsed in int64 e.TotalMilliseconds

            let (|CosmosReadRc|CosmosWriteRc|CosmosResyncRc|CosmosResponseRc|CosmosDeleteRc|CosmosPruneRc|) = function
                | Tip (Stats s)
                | TipNotFound (Stats s)
                | TipNotModified (Stats s)
                | Query (_,_, (Stats s)) -> CosmosReadRc s
                // slices are rolled up into batches so be sure not to double-count
                | Response (_,(Stats s))
                // costs roll up into Prune operation so be sure not to double-count
                | PruneResponse (Stats s) -> CosmosResponseRc s
                | SyncSuccess (Stats s)
                | SyncConflict (Stats s) -> CosmosWriteRc s
                | SyncResync (Stats s) -> CosmosResyncRc s
                | Delete (Stats s) -> CosmosDeleteRc s
                | Prune (_, (Stats s)) -> CosmosPruneRc s
            let (|SerilogScalar|_|) : LogEventPropertyValue -> obj option = function
                | (:? ScalarValue as x) -> Some x.Value
                | _ -> None
            let (|CosmosMetric|_|) (logEvent : LogEvent) : Event option =
                match logEvent.Properties.TryGetValue("cosmosEvt") with
                | true, SerilogScalar (:? Event as e) -> Some e
                | _ -> None
            type Counter =
                { mutable rux100: int64; mutable count: int64; mutable ms: int64 }
                static member Create() = { rux100 = 0L; count = 0L; ms = 0L }
                member __.Ingest (ru, ms) =
                    System.Threading.Interlocked.Increment(&__.count) |> ignore
                    System.Threading.Interlocked.Add(&__.rux100, int64 (ru*100.)) |> ignore
                    System.Threading.Interlocked.Add(&__.ms, ms) |> ignore
            type LogSink() =
                static let epoch = System.Diagnostics.Stopwatch.StartNew()
                static member val Read = Counter.Create() with get, set
                static member val Write = Counter.Create() with get, set
                static member val Resync = Counter.Create() with get, set
                static member val Delete = Counter.Create() with get, set
                static member val Prune = Counter.Create() with get, set
                static member Restart() =
                    LogSink.Read <- Counter.Create()
                    LogSink.Write <- Counter.Create()
                    LogSink.Resync <- Counter.Create()
                    LogSink.Delete <- Counter.Create()
                    LogSink.Prune <- Counter.Create()
                    let span = epoch.Elapsed
                    epoch.Restart()
                    span
                interface Serilog.Core.ILogEventSink with
                    member __.Emit logEvent = logEvent |> function
                        | CosmosMetric (CosmosReadRc stats) -> LogSink.Read.Ingest stats
                        | CosmosMetric (CosmosWriteRc stats) -> LogSink.Write.Ingest stats
                        | CosmosMetric (CosmosResyncRc stats) -> LogSink.Resync.Ingest stats
                        | CosmosMetric (CosmosDeleteRc stats) -> LogSink.Delete.Ingest stats
                        | CosmosMetric (CosmosPruneRc stats) -> LogSink.Prune.Ingest stats
                        | CosmosMetric (CosmosResponseRc _) -> () // Costs are already included in others
                        | _ -> ()

        /// Relies on feeding of metrics from Log through to Stats.LogSink
        /// Use Stats.LogSink.Restart() to reset the start point (and stats) where relevant
        let dump (log: Serilog.ILogger) =
            let stats =
              [ "Read", Stats.LogSink.Read
                "Write", Stats.LogSink.Write
                "Resync", Stats.LogSink.Resync
                "Delete", Stats.LogSink.Delete
                "Prune", Stats.LogSink.Prune ]
            let mutable rows, totalCount, totalRc, totalMs = 0, 0L, 0., 0L
            let logActivity name count rc lat =
                log.Information("{name}: {count:n0} requests costing {ru:n0} RU (average: {avg:n2}); Average latency: {lat:n0}ms",
                    name, count, rc, (if count = 0L then Double.NaN else rc/float count), (if count = 0L then Double.NaN else float lat/float count))
            for name, stat in stats do
                if stat.count <> 0L then
                    let ru = float stat.rux100 / 100.
                    totalCount <- totalCount + stat.count
                    totalRc <- totalRc + ru
                    totalMs <- totalMs + stat.ms
                    logActivity name stat.count ru stat.ms
                    rows <- rows + 1
            // Yes, there's a minor race here between the use of the values and the reset
            let duration = Stats.LogSink.Restart()
            if rows > 1 then logActivity "TOTAL" totalCount totalRc totalMs
            let measures : (string * (TimeSpan -> float)) list = [ "s", fun x -> x.TotalSeconds(*; "m", fun x -> x.TotalMinutes; "h", fun x -> x.TotalHours*) ]
            let logPeriodicRate name count ru = log.Information("rp{name} {count:n0} = ~{ru:n0} RU", name, count, ru)
            for uom, f in measures do let d = f duration in if d <> 0. then logPeriodicRate uom (float totalCount/d |> int64) (totalRc/d)

type Container(client : Client.DocumentClient, databaseId, containerId) =
    let collectionUri = Microsoft.Azure.Documents.Client.UriFactory.CreateDocumentCollectionUri(databaseId, containerId)
    member __.Client = client
    member __.CollectionUri = collectionUri

[<AutoOpen>]
module private DocDb =
    /// Extracts the innermost exception from a nested hierarchy of Aggregate Exceptions
    let (|AggregateException|) (exn : exn) =
        let rec aux (e : exn) =
            match e with
            | :? AggregateException as agg when agg.InnerExceptions.Count = 1 ->
                aux agg.InnerExceptions.[0]
            | _ -> e
        aux exn
    /// CosmosDB Error HttpStatusCode extractor
    let (|CosmosException|_|) (e : exn) =
        match e with
        | AggregateException (:? DocumentClientException as ce) -> Some ce
        | _ -> None
    /// Map Nullable to Option
    let (|HasValue|Null|) (x:Nullable<_>) =
        if x.HasValue then HasValue x.Value
        else Null
    /// CosmosDB Error HttpStatusCode extractor
    let (|CosmosStatusCode|_|) (e : DocumentClientException) =
        match e.StatusCode with
        | HasValue x -> Some x
        | Null -> None
    type ReadResult<'T> = Found of 'T | NotFound | NotModified
    type Container with
        member container.TryReadItem(documentId : string, ?options : Client.RequestOptions): Async<float * ReadResult<'T>> = async {
            let options = defaultArg options null
            let docLink = sprintf "%O/docs/%s" container.CollectionUri documentId
            let! ct = Async.CancellationToken
            try let! item = async { return! container.Client.ReadDocumentAsync<'T>(docLink, options = options, cancellationToken = ct) |> Async.AwaitTaskCorrect }
                if item.StatusCode = System.Net.HttpStatusCode.NotModified then return item.RequestCharge, NotModified
                // NB `.Document` will NRE if a IfNoneModified precondition triggers a NotModified result
                else return item.RequestCharge, Found item.Document
            with CosmosException (CosmosStatusCode System.Net.HttpStatusCode.NotFound as e) -> return e.RequestCharge, NotFound
                // NB while the docs suggest you may see a 412, the NotModified in the body of the try/with is actually what happens
                | CosmosException (CosmosStatusCode System.Net.HttpStatusCode.PreconditionFailed as e) -> return e.RequestCharge, NotModified }

module Sync =
    // NB don't nest in a private module, or serialization will fail miserably ;)
    [<CLIMutable; NoEquality; NoComparison; Newtonsoft.Json.JsonObject(ItemRequired=Newtonsoft.Json.Required.AllowNull)>]
    type SyncResponse = { etag: string; n: int64; conflicts: Unfold[] }
    let [<Literal>] private sprocName = "EquinoxRollingUnfolds5"  // NB need to rename/number for any breaking change
    let [<Literal>] private sprocBody = """
// Manages the merging of the supplied Request Batch into the stream

// 0 perform concurrency check (index=-1 -> always append; index=-2 -> check based on .etag; _ -> check .n=.index)
// High level end-states:
// 1a if there is a Tip, but are only changes to the `u`nfolds (and no `e`vents) -> update Tip only
// 1b if there is a Tip, but incoming request includes an event -> generate a batch document + create empty Tip
// 2a if stream empty, but incoming request includes an event -> generate a batch document + create empty Tip
// 2b if no current Tip, and no events being written -> the incoming `req` becomes the Tip batch
function sync(req, expIndex, expEtag) {
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
            response.setBody({ etag: null, n: 0, conflicts: [] });
        } else if (current && ((expIndex === -2 && expEtag !== current._etag) || (expIndex !== -2 && expIndex !== current.n))) {
            // Where possible, we extract conflicting events from e and/or u in order to avoid another read cycle;
            // yielding [] triggers the client to go loading the events itself
            // if we're working based on etags, the `u`nfolds likely bear relevant info as state-bearing unfolds
            // if there are no `u`nfolds, we need to be careful not to yield `conflicts: null`, as that signals a successful write (see below)
            response.setBody({ etag: current._etag, n: current.n, conflicts: current.u || [] });
        } else {
            executeUpsert(current);
        }
    });
    if (!isAccepted) throw new Error("readDocument not Accepted");
    function executeUpsert(tip) {
        function callback(err, doc) {
            if (err) throw err;
            response.setBody({ etag: doc._etag, n: doc.n, conflicts: null });
        }
        function shouldCalveBatch(events) {
            return events.length > 0;
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
            // Replace all the unfolds // TODO: should remove only unfolds being superseded
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

    [<RequireQualifiedAccess; NoEquality; NoComparison>]
    type Result =
        | Written of Position
        | Conflict of Position * events: ITimelineEvent<byte[]>[]
        | ConflictUnknown of Position

    type [<RequireQualifiedAccess>] Exp = Version of int64 | Etag of string | Any
    let private run (container : Container, stream : string) (exp, req: Tip)
        : Async<float*Result> = async {
        let sprocLink = sprintf "%O/sprocs/%s" container.CollectionUri sprocName
        let opts = Client.RequestOptions(PartitionKey=PartitionKey stream)
        let ep = match exp with Exp.Version ev -> Position.fromI ev | Exp.Etag et -> Position.fromEtag et | Exp.Any -> Position.fromAppendAtEnd
        let! ct = Async.CancellationToken
        let! (res : Client.StoredProcedureResponse<SyncResponse>) =
            container.Client.ExecuteStoredProcedureAsync(sprocLink, opts, ct, box req, box ep.index, box (Option.toObj ep.etag)) |> Async.AwaitTaskCorrect
        let newPos = { index = res.Response.n; etag = Option.ofObj res.Response.etag }
        return res.RequestCharge, res.Response.conflicts |> function
            | null -> Result.Written newPos
            | [||] when newPos.index = 0L -> Result.Conflict (newPos, Array.empty)
            | [||] -> Result.ConflictUnknown newPos
            | xs -> Result.Conflict (newPos, Enum.Unfolds xs |> Array.ofSeq) }

    let private logged (container,stream) (exp : Exp, req: Tip) (log : ILogger)
        : Async<Result> = async {
        let! t, (ru, result) = run (container,stream) (exp, req) |> Stopwatch.Time
        let (Log.BatchLen bytes), count = Enum.Events req, req.e.Length
        let log =
            let inline mkMetric ru : Log.Measurement = { stream = stream; interval = t; bytes = bytes; count = count; ru = ru }
            let inline propConflict log = log |> Log.prop "conflict" true |> Log.prop "eventTypes" (Seq.truncate 5 (seq { for x in req.e -> x.c }))
            let verbose = log.IsEnabled Serilog.Events.LogEventLevel.Debug
            (if verbose then log |> Log.propEvents (Enum.Events req) |> Log.propDataUnfolds req.u else log)
            |> match exp with
                | Exp.Etag et ->     Log.prop "expectedEtag" et
                | Exp.Version ev ->  Log.prop "expectedVersion" ev
                | Exp.Any ->         Log.prop "expectedVersion" -1
            |> match result with
                | Result.Written pos ->
                    Log.prop "nextExpectedVersion" pos >> Log.event (Log.SyncSuccess (mkMetric ru))
                | Result.ConflictUnknown pos ->
                    Log.prop "nextExpectedVersion" pos >> propConflict >> Log.event (Log.SyncConflict (mkMetric ru))
                | Result.Conflict (pos, xs) ->
                    (if verbose then Log.propData "conflicts" xs else id)
                    >> Log.prop "nextExpectedVersion" pos >> propConflict >> Log.event (Log.SyncResync(mkMetric ru))
        log.Information("EqxCosmos {action:l} {stream} {count}+{ucount} {ms:f1}ms {ru}RU {bytes:n0}b {exp}",
            "Sync", stream, count, req.u.Length, (let e = t.Elapsed in e.TotalMilliseconds), ru, bytes, exp)
        return result }

    let batch (log : ILogger) retryPolicy containerStream batch: Async<Result> =
        let call = logged containerStream batch
        Log.withLoggedRetries retryPolicy "writeAttempt" call log

    let private mkEvent (e : IEventData<_>) =
        {   t = e.Timestamp; c = e.EventType; d = e.Data; m = e.Meta; correlationId = e.CorrelationId; causationId = e.CausationId }
    let mkBatch (stream: string) (events: IEventData<_>[]) unfolds: Tip =
        {   p = stream; id = Tip.WellKnownDocumentId; n = -1L(*Server-managed*); i = -1L(*Server-managed*); _etag = null
            e = Array.map mkEvent events; u = Array.ofSeq unfolds }
    let mkUnfold compressor baseIndex (unfolds: IEventData<_> seq) : Unfold seq =
        unfolds
        |> Seq.mapi (fun offset x ->
                {   i = baseIndex + int64 offset
                    c = x.EventType
                    d = compressor x.Data
                    m = compressor x.Meta
                    t = DateTimeOffset.UtcNow
                } : Unfold)

    module Initialization =
        open System.Linq
        type [<RequireQualifiedAccess>] Provisioning = Container of rus: int | Database of rus: int | Serverless
        let adjustOffer (c:Client.DocumentClient) resourceLink rus = async {
            let offer = c.CreateOfferQuery().Where(fun r -> r.ResourceLink = resourceLink).AsEnumerable().Single()
            let! _ = c.ReplaceOfferAsync(OfferV2(offer,rus)) |> Async.AwaitTaskCorrect in () }
        let private createDatabaseIfNotExists (client:Client.DocumentClient) dName maybeRus =
            let opts = Client.RequestOptions(ConsistencyLevel = Nullable ConsistencyLevel.Session)
            maybeRus |> Option.iter (fun rus -> opts.OfferThroughput <- Nullable rus)
            client.CreateDatabaseIfNotExistsAsync(Database(Id=dName), options = opts) |> Async.AwaitTaskCorrect
        let private createOrProvisionDatabase (client:Client.DocumentClient) dName mode = async {
            match mode with
            | Provisioning.Database rus ->
                let! db = createDatabaseIfNotExists client dName (Some rus)
                return! adjustOffer client db.Resource.SelfLink rus
            | Provisioning.Container _ | Provisioning.Serverless ->
                let! _ = createDatabaseIfNotExists client dName None in () }
        let private createContainerIfNotExists (client:Client.DocumentClient) dName (def: DocumentCollection) maybeRus =
            let dbUri = Client.UriFactory.CreateDatabaseUri dName
            let opts = match maybeRus with None -> Client.RequestOptions() | Some rus -> Client.RequestOptions(OfferThroughput=Nullable rus)
            client.CreateDocumentCollectionIfNotExistsAsync(dbUri, def, opts) |> Async.AwaitTaskCorrect
        let private createOrProvisionContainer (client: Client.DocumentClient) (dName, def: DocumentCollection) mode = async {
            match mode with
            | Provisioning.Container rus ->
                let! container = createContainerIfNotExists client dName def (Some rus) in ()
                return! adjustOffer client container.Resource.SelfLink rus
            | Provisioning.Database _  | Provisioning.Serverless ->
                let! _ = createContainerIfNotExists client dName def None in () }
        let private createStoredProcIfNotExists (c:Container) (name, body): Async<float> = async {
            try let! r = c.Client.CreateStoredProcedureAsync(c.CollectionUri, StoredProcedure(Id = name, Body = body)) |> Async.AwaitTaskCorrect
                return r.RequestCharge
            with CosmosException ((CosmosStatusCode sc) as e) when sc = System.Net.HttpStatusCode.Conflict -> return e.RequestCharge }
        let private mkContainerProperties idFieldName partitionKeyFieldName =
            // While the v2 SDK and earlier portal versions admitted 'fixed' collections where no Partition Key is defined, we follow the recent policy
            // simplification of having a convention of always defining a partition key
            let pkd = PartitionKeyDefinition()
            pkd.Paths.Add(sprintf "/%s" partitionKeyFieldName)
            DocumentCollection(Id = idFieldName, PartitionKey = pkd)
        let private createBatchAndTipContainerIfNotExists (client: Client.DocumentClient) (dName,cName) mode : Async<unit> =
            let def = mkContainerProperties cName Batch.PartitionKeyField
            def.IndexingPolicy.IndexingMode <- IndexingMode.Consistent
            def.IndexingPolicy.Automatic <- true
            // Can either do a blacklist or a whitelist
            // Given how long and variable the blacklist would be, we whitelist instead
            def.IndexingPolicy.ExcludedPaths.Add(ExcludedPath(Path="/*"))
            // NB its critical to index the nominated PartitionKey field defined above or there will be runtime errors
            for k in Batch.IndexedFields do def.IndexingPolicy.IncludedPaths.Add(IncludedPath(Path = sprintf "/%s/?" k))
            createOrProvisionContainer client (dName, def) mode
        let createSyncStoredProcIfNotExists (log: ILogger option) container = async {
            let! t, ru = createStoredProcIfNotExists container (sprocName,sprocBody) |> Stopwatch.Time
            match log with
            | None -> ()
            | Some log -> log.Information("Created stored procedure {sprocId} in {ms}ms rc={ru}", sprocName, (let e = t.Elapsed in e.TotalMilliseconds), ru) }
        let private createAuxContainerIfNotExists client (dName,cName) mode : Async<unit> =
            let def = mkContainerProperties cName "id" // as per Cosmos team, Partition Key must be "/id"
            // TL;DR no indexing of any kind; see https://github.com/Azure/azure-documentdb-changefeedprocessor-dotnet/issues/142
            def.IndexingPolicy.Automatic <- false
            def.IndexingPolicy.IndexingMode <- IndexingMode.None
            createOrProvisionContainer client (dName,def) mode
        let init log (client : Client.DocumentClient) (dName,cName) mode skipStoredProc = async {
            do! createOrProvisionDatabase client dName mode
            do! createBatchAndTipContainerIfNotExists client (dName,cName) mode
            let container = Container(client,dName,cName)
            if not skipStoredProc then
                do! createSyncStoredProcIfNotExists (Some log) container }
        let initAux (client: Client.DocumentClient) (dName,cName) rus = async {
            // Hardwired for now (not sure if CFP can store in a Database-allocated as it would need to be supplying partition keys)
            let mode = Provisioning.Container rus
            do! createOrProvisionDatabase client dName mode
            return! createAuxContainerIfNotExists client (dName,cName) mode }

module internal Tip =
    let private get (container : Container, stream : string) (maybePos: Position option) =
        let ac = match maybePos with Some { etag=Some etag } -> Client.AccessCondition(Type=Client.AccessConditionType.IfNoneMatch, Condition=etag) | _ -> null
        let ro = Client.RequestOptions(PartitionKey=PartitionKey stream, AccessCondition = ac)
        container.TryReadItem(Tip.WellKnownDocumentId, ro)
    let private loggedGet (get : Container * string -> Position option -> Async<_>) (container,stream) (maybePos: Position option) (log: ILogger) = async {
        let log = log |> Log.prop "stream" stream
        let! t, (ru, res : ReadResult<Tip>) = get (container,stream) maybePos |> Stopwatch.Time
        let log bytes count (f : Log.Measurement -> _) = log |> Log.event (f { stream = stream; interval = t; bytes = bytes; count = count; ru = ru })
        match res with
        | ReadResult.NotModified ->
            (log 0 0 Log.TipNotModified).Information("EqxCosmos {action:l} {res} {ms}ms rc={ru}", "Tip", 302, (let e = t.Elapsed in e.TotalMilliseconds), ru)
        | ReadResult.NotFound ->
            (log 0 0 Log.TipNotFound).Information("EqxCosmos {action:l} {res} {ms}ms rc={ru}", "Tip", 404, (let e = t.Elapsed in e.TotalMilliseconds), ru)
        | ReadResult.Found tip ->
            let log =
                let (Log.BatchLen bytes), count = Enum.Unfolds tip.u, tip.u.Length
                log bytes count Log.Tip
            let log = if (not << log.IsEnabled) Events.LogEventLevel.Debug then log else log |> Log.propDataUnfolds tip.u
            let log = match maybePos with Some p -> log |> Log.propStartPos p |> Log.propStartEtag p | None -> log
            let log = log |> Log.prop "_etag" tip._etag |> Log.prop "n" tip.n
            log.Information("EqxCosmos {action:l} {res} {ms}ms rc={ru}", "Tip", 200, (let e = t.Elapsed in e.TotalMilliseconds), ru)
        return ru, res }
    type [<RequireQualifiedAccess; NoComparison; NoEquality>] Result = NotModified | NotFound | Found of Position * ITimelineEvent<byte[]>[]
    /// `pos` being Some implies that the caller holds a cached value and hence is ready to deal with IndexResult.NotModified
    let tryLoad (log : ILogger) retryPolicy containerStream (maybePos: Position option): Async<Result> = async {
        let! _rc, res = Log.withLoggedRetries retryPolicy "readAttempt" (loggedGet get containerStream maybePos) log
        match res with
        | ReadResult.NotModified -> return Result.NotModified
        | ReadResult.NotFound -> return Result.NotFound
        | ReadResult.Found tip -> return Result.Found (Position.fromTip tip, Enum.EventsAndUnfolds tip |> Array.ofSeq) }

 module internal Query =
    open Microsoft.Azure.Documents.Linq
    open FSharp.Control
    let private mkQuery (container : Container, stream : string) maxItems (direction: Direction) startPos =
        let query =
            let root = sprintf "SELECT c.id, c.i, c._etag, c.n, c.e FROM c WHERE c.id!=\"%s\"" Tip.WellKnownDocumentId
            let tail = sprintf "ORDER BY c.i %s" (if direction = Direction.Forward then "ASC" else "DESC")
            match startPos with
            | None -> SqlQuerySpec(sprintf "%s %s" root tail)
            | Some { index = positionSoExclusiveWhenBackward } ->
                let cond = if direction = Direction.Forward then "c.n > @startPos" else "c.i < @startPos"
                SqlQuerySpec(sprintf "%s AND %s %s" root cond tail, SqlParameterCollection [SqlParameter("@startPos", positionSoExclusiveWhenBackward)])
        let qro = new Client.FeedOptions(PartitionKey = PartitionKey stream, MaxItemCount=Nullable maxItems)
        container.Client.CreateDocumentQuery<Batch>(container.CollectionUri, query, qro).AsDocumentQuery()

    // Unrolls the Batches in a response - note when reading backwards, the events are emitted in reverse order of index
    let private handleResponse direction (streamName: string) startPos (query: IDocumentQuery<Batch>) (log: ILogger)
        : Async<ITimelineEvent<byte[]>[] * Position option * float> = async {
        let! ct = Async.CancellationToken
        let! t, (res : Client.FeedResponse<Batch>) = query.ExecuteNextAsync<Batch>(ct) |> Async.AwaitTaskCorrect |> Stopwatch.Time
        let batches, ru = Array.ofSeq res, res.RequestCharge
        let events = batches |> Seq.collect (fun b -> Enum.Events(b, startPos, direction)) |> Array.ofSeq
        let (Log.BatchLen bytes), count = events, events.Length
        let reqMetric : Log.Measurement = { stream = streamName; interval = t; bytes = bytes; count = count; ru = ru }
        let log = let evt = Log.Response (direction, reqMetric) in log |> Log.event evt
        let log = if (not << log.IsEnabled) Events.LogEventLevel.Debug then log else log |> Log.propEvents events
        let index = if count = 0 then Nullable () else Nullable <| Seq.min (seq { for x in batches -> x.i })
        (log |> (match startPos with Some pos -> Log.propStartPos pos | None -> id) |> Log.prop "bytes" bytes)
            .Information("EqxCosmos {action:l} {count}/{batches} {direction} {ms}ms i={index} rc={ru}",
                "Response", count, batches.Length, direction, (let e = t.Elapsed in e.TotalMilliseconds), index, ru)
        let maybePosition = batches |> Array.tryPick Position.tryFromBatch
        return events, maybePosition, ru }

    let private run (log : ILogger) (readSlice: IDocumentQuery<Batch> -> ILogger -> Async<ITimelineEvent<byte[]>[] * Position option * float>)
            (maxPermittedBatchReads: int option)
            (query: IDocumentQuery<Batch>)
        : AsyncSeq<ITimelineEvent<byte[]>[] * Position option * float> =
        let rec loop batchCount : AsyncSeq<ITimelineEvent<byte[]>[] * Position option * float> = asyncSeq {
            match maxPermittedBatchReads with
            | Some mpbr when batchCount >= mpbr -> log.Information "batch Limit exceeded"; invalidOp "batch Limit exceeded"
            | _ -> ()

            let batchLog = log |> Log.prop "batchIndex" batchCount
            let! (slice : ITimelineEvent<byte[]>[] * Position option * float) = readSlice query batchLog
            yield slice
            if query.HasMoreResults then
                yield! loop (batchCount + 1) }
        loop 0

    let private logQuery direction batchSize streamName interval (responsesCount, events : ITimelineEvent<byte[]>[]) n (ru: float) (log : ILogger) =
        let (Log.BatchLen bytes), count = events, events.Length
        let reqMetric : Log.Measurement = { stream = streamName; interval = interval; bytes = bytes; count = count; ru = ru }
        let evt = Log.Event.Query (direction, responsesCount, reqMetric)
        let action = match direction with Direction.Forward -> "QueryF" | Direction.Backward -> "QueryB"
        (log |> Log.prop "bytes" bytes |> Log.prop "batchSize" batchSize |> Log.event evt).Information(
            "EqxCosmos {action:l} {stream} v{n} {count}/{responses} {ms}ms rc={ru}",
            action, streamName, n, count, responsesCount, (let e = interval.Elapsed in e.TotalMilliseconds), ru)

    let private calculateUsedVersusDroppedPayload stopIndex (xs: ITimelineEvent<byte[]>[]) : int * int =
        let mutable used, dropped = 0, 0
        let mutable found = false
        for x in xs do
            let (Log.EventLen bytes) = x
            if found then dropped <- dropped + bytes
            else used <- used + bytes
            if x.Index = stopIndex then found <- true
        used, dropped

    let walk<'event> (log : ILogger) (container,stream) retryPolicy maxItems maxRequests direction startPos
        (tryDecode : ITimelineEvent<byte[]> -> 'event option, isOrigin: 'event -> bool)
        : Async<Position * 'event[]> = async {
        let responseCount = ref 0
        let mergeBatches (log : ILogger) (batchesBackward: AsyncSeq<ITimelineEvent<byte[]>[] * Position option * float>) = async {
            let mutable lastResponse, maybeTipPos, ru = None, None, 0.
            let! events =
                batchesBackward
                |> AsyncSeq.map (fun (events, maybePos, r) ->
                    if maybeTipPos = None then maybeTipPos <- maybePos
                    lastResponse <- Some events; ru <- ru + r
                    incr responseCount
                    events |> Array.map (fun x -> x, tryDecode x))
                |> AsyncSeq.concatSeq
                |> AsyncSeq.takeWhileInclusive (function
                    | x, Some e when isOrigin e ->
                        match lastResponse with
                        | None -> log.Information("EqxCosmos Stop stream={stream} at={index} {case}", stream, x.Index, x.EventType)
                        | Some batch ->
                            let used, residual = batch |> calculateUsedVersusDroppedPayload x.Index
                            log.Information("EqxCosmos Stop stream={stream} at={index} {case} used={used} residual={residual}",
                                stream, x.Index, x.EventType, used, residual)
                        false
                    | _ -> true) (*continue the search*)
                |> AsyncSeq.toArrayAsync
            return events, maybeTipPos, ru }
        let query = mkQuery (container,stream) maxItems direction startPos
        let pullSlice = handleResponse direction stream startPos
        let retryingLoggingReadSlice query = Log.withLoggedRetries retryPolicy "readAttempt" (pullSlice query)
        let log = log |> Log.prop "batchSize" maxItems |> Log.prop "stream" stream
        let readlog = log |> Log.prop "direction" direction
        let batches : AsyncSeq<ITimelineEvent<byte[]>[] * Position option * float> = run readlog retryingLoggingReadSlice maxRequests query
        let! t, (events, maybeTipPos, ru) = mergeBatches log batches |> Stopwatch.Time
        let raws, decoded = (Array.map fst events), (events |> Seq.choose snd |> Array.ofSeq)
        let pos = match maybeTipPos with Some p -> p | None -> Position.fromMaxIndex raws

        log |> logQuery direction maxItems stream t (!responseCount,raws) pos.index ru
        return pos, decoded }

    let walkLazy<'event> (log : ILogger) (container,stream) retryPolicy maxItems maxRequests direction startPos
        (tryDecode : ITimelineEvent<byte[]> -> 'event option, isOrigin: 'event -> bool)
        : AsyncSeq<'event[]> = asyncSeq {
        let responseCount = ref 0
        let query = mkQuery (container,stream) maxItems direction startPos
        let pullSlice = handleResponse direction stream startPos
        let retryingLoggingReadSlice query = Log.withLoggedRetries retryPolicy "readAttempt" (pullSlice query)
        let log = log |> Log.prop "batchSize" maxItems |> Log.prop "stream" stream
        let mutable ru = 0.
        let allSlices = ResizeArray()
        let startTicks = System.Diagnostics.Stopwatch.GetTimestamp()
        try let readlog = log |> Log.prop "direction" direction
            let mutable ok = true
            while ok do
                incr responseCount

                match maxRequests with
                | Some mpbr when !responseCount >= mpbr -> readlog.Information "batch Limit exceeded"; invalidOp "batch Limit exceeded"
                | _ -> ()

                let batchLog = readlog |> Log.prop "batchIndex" !responseCount
                let! (slice,_pos,rus) = retryingLoggingReadSlice query batchLog
                ru <- ru + rus
                allSlices.AddRange(slice)

                let acc = ResizeArray()
                for x in slice do
                    match tryDecode x with
                    | Some e when isOrigin e ->
                        let used, residual = slice |> calculateUsedVersusDroppedPayload x.Index
                        log.Information("EqxCosmos Stop stream={stream} at={index} {case} used={used} residual={residual}",
                            stream, x.Index, x.EventType, used, residual)
                        ok <- false
                        acc.Add e
                    | Some e -> acc.Add e
                    | None -> ()
                yield acc.ToArray()
                ok <- ok && query.HasMoreResults
        finally
            let endTicks = System.Diagnostics.Stopwatch.GetTimestamp()
            let t = StopwatchInterval(startTicks, endTicks)
            log |> logQuery direction maxItems stream t (!responseCount,allSlices.ToArray()) -1L ru }

// Manages deletion of batches
// Note: it's critical that we delete individually, in the correct order so as not to leave gaps
// Note: public so BatchIndices can be deserialized into
module Delete =

    open FSharp.Control
    open Microsoft.Azure.Documents.Linq

    type BatchIndices = { id : string; i : int64; n : int64 }

    let pruneBefore (log: ILogger) (container: Container, stream: string) maxItems beforePos : Async<int * int * int64> = async {
        let! ct = Async.CancellationToken
        let log = log |> Log.prop "stream" stream
        let deleteItem id count : Async<float> = async {
            let docLink = sprintf "%O/docs/%s" container.CollectionUri id
            let qo = Client.RequestOptions(PartitionKey = PartitionKey stream)
            let! t, res = container.Client.DeleteDocumentAsync(docLink, qo, cancellationToken=ct) |> Async.AwaitTaskCorrect |> Stopwatch.Time
            let rc, ms = res.RequestCharge, (let e = t.Elapsed in e.TotalMilliseconds)
            let reqMetric : Log.Measurement = { stream = stream; interval = t; bytes = -1; count = count; ru = rc }
            let log = let evt = Log.Delete reqMetric in log |> Log.event evt
            log.Information("EqxCosmos {action:l} {id} {ms}ms rc={ru}", "Delete", id, ms, rc)
            return res.RequestCharge
        }
        let log = log |> Log.prop "beforePos" beforePos
        let query : IDocumentQuery<BatchIndices> =
            let q = SqlQuerySpec("SELECT c.id, c.i, c.n FROM c")
            let qro = Client.FeedOptions(PartitionKey = PartitionKey stream, MaxItemCount=Nullable maxItems)
            container.Client.CreateDocumentQuery<_>(container.CollectionUri, q, qro).AsDocumentQuery()
        let tryReadNextPage (x : IDocumentQuery<_>) = async {
            if not x.HasMoreResults then return None else

            let! t, (res : Client.FeedResponse<_>) = query.ExecuteNextAsync<BatchIndices>(ct) |> Async.AwaitTaskCorrect |> Stopwatch.Time
            let batches, rc, ms = Array.ofSeq res, res.RequestCharge, (let e = t.Elapsed in e.TotalMilliseconds)
            let next = (match batches with [||] -> None | xs -> Some (xs.[ xs.Length - 1])) |> Option.map (fun x -> x.n) |> Option.toNullable
            let reqMetric : Log.Measurement = { stream = stream; interval = t; bytes = -1; count = batches.Length; ru = rc }
            let log = let evt = Log.PruneResponse reqMetric in log |> Log.event evt
            log.Information("EqxCosmos {action:l} {batches} {ms}ms n={next} rc={ru}", "PruneResponse", batches.Length, ms, next, rc)
            return Some ((rc, batches), x)
        }
        // If we have results: []
        // - deleteBefore  9 would: return 0,0,0

        // If we have results: ["-1",10,10; "0",0,1; "2",1,3; "3",3,8; "8",8,10]
        // - deleteBefore  3 would: inspect first 3, delete 2, return 3,0,3
        // - deleteBefore  4 would: inspect first 4, delete 2, return 3,1,3
        // - deleteBefore  8 would: inspect first 4, delete 2, return 8,0,8

        // If we have results: ["-1",10,10; "8",8,10]
        // - deleteBefore  3 would: inspect first 2, delete 0, return 0,0,8
        // - deleteBefore  8 would: inspect first 2, delete 0, return 0,0,8
        // - deleteBefore  9 would: inspect first 2, delete 0, return 0,1,8
        // - deleteBefore 10 would: inspect first 2, delete 1, return 2,0,10
        // - deleteBefore 11 would: inspect first 2, delete 1, return 2,0,10

        // If we have results: ["-1",10,10]
        // - deleteBefore  9 would: inspect first 1, delete 0, return 0,0,10
        // - deleteBefore 10 would: inspect first 1, delete 0, return 0,0,10
        // - deleteBefore 11 would: inspect first 1, delete 0, return 0,0,10
        let! pt, outcomes =
            let isTip (x : BatchIndices) = x.id = Tip.WellKnownDocumentId
            let isRelevant x = isTip x || x.i < beforePos
            let hasRelevantItems (_, batches) = batches |> Array.exists isRelevant
            let handle (rc, batches : BatchIndices[]) = async {
                let mutable delCharges, batchesDeleted, eventsDeleted, eventsDeferred = 0., 0, 0, 0
                let mutable tipI, lwm = None, None
                for x in batches |> Seq.takeWhile isRelevant do
                    let count = x.n - x.i |> int
                    if isTip x then
                        tipI <- Some x.i
                    elif x.n > beforePos then
                        eventsDeferred <- eventsDeferred + min count (int (beforePos - x.i))
                        lwm <- Some x.i
                    else
                        let! charge = deleteItem x.id count
                        delCharges <- delCharges + charge
                        batchesDeleted <- batchesDeleted + 1
                        eventsDeleted <- eventsDeleted + count
                        lwm <- Some x.n
                return rc, (tipI, lwm), (delCharges, batchesDeleted, eventsDeleted, eventsDeferred)
            }
            AsyncSeq.unfoldAsync tryReadNextPage query
            |> AsyncSeq.takeWhile hasRelevantItems
            |> AsyncSeq.mapAsync handle
            |> AsyncSeq.toArrayAsync
            |> Stopwatch.Time
        let mutable queryCharges, delCharges, responses, batchesDeleted, eventsDeleted, eventsDeferred = 0., 0., 0, 0, 0, 0
        let mutable lwm, tipI = None, None
        for qc, (bTipI, bLwm), (dc, bDel, eDel, eDef) in outcomes do
            lwm <- max lwm bLwm
            tipI <- max tipI bTipI
            queryCharges <- queryCharges + qc
            delCharges <- delCharges + dc
            responses <- responses + 1
            batchesDeleted <- batchesDeleted + bDel
            eventsDeleted <- eventsDeleted + eDel
            eventsDeferred <- eventsDeferred + eDef
        let reqMetric : Log.Measurement = { stream = stream; interval = pt; bytes = eventsDeleted; count = batchesDeleted; ru = queryCharges }
        let log = let evt = Log.Prune (responses, reqMetric) in log |> Log.event evt
        let lwm =
            match lwm, tipI with
            | Some lwm, _     -> lwm  // we saw a batch and identified a Low Water mark based on it
            | None, Some tipI -> tipI // we saw the Tip, but no batches along the way, therefore it's i is the low water mark
            | None, None      -> 0L   // If we've seen no batches at all, then the write position is 0L
        log.Information("EqxCosmos {action:l} {events}/{batches} lwm={lwm} {ms}ms queryRu={ru} deleteRu={delRu}",
                "Prune", eventsDeleted, batchesDeleted, lwm, (let e = pt.Elapsed in e.TotalMilliseconds), queryCharges, delCharges)
        return eventsDeleted, eventsDeferred, lwm
    }

type [<NoComparison>] Token = { container: Container; stream: string; pos: Position }
module Token =
    let create (container,stream) pos : StreamToken =
        {  value = box { container = container; stream = stream; pos = pos }
           version = pos.index }
    let (|Unpack|) (token: StreamToken) : Container*string*Position = let t = unbox<Token> token.value in t.container,t.stream,t.pos
    let supersedes (Unpack (_,_,currentPos)) (Unpack (_,_,xPos)) =
        let currentVersion, newVersion = currentPos.index, xPos.index
        let currentETag, newETag = currentPos.etag, xPos.etag
        newVersion > currentVersion || currentETag <> newETag

[<AutoOpen>]
module Internal =
    [<RequireQualifiedAccess; NoComparison; NoEquality>]
    type InternalSyncResult = Written of StreamToken | ConflictUnknown of StreamToken | Conflict of StreamToken * ITimelineEvent<byte[]>[]

    [<RequireQualifiedAccess; NoComparison; NoEquality>]
    type LoadFromTokenResult<'event> = Unchanged | Found of StreamToken * 'event[]

namespace Equinox.Cosmos

open Equinox
open Equinox.Core
open Equinox.Cosmos.Store
open FsCodec
open FSharp.Control
open Microsoft.Azure.Documents
open Serilog
open System
open System.Collections.Concurrent

/// Defines policies for retrying with respect to transient failures calling CosmosDb (as opposed to application level concurrency conflicts)
type Connection(client: Client.DocumentClient, [<O; D(null)>]?readRetryPolicy: IRetryPolicy, [<O; D(null)>]?writeRetryPolicy) =
    member __.Client = client
    member __.TipRetryPolicy = readRetryPolicy
    member __.QueryRetryPolicy = readRetryPolicy
    member __.WriteRetryPolicy = writeRetryPolicy

/// Defines the policies in force regarding how to a) split up calls b) limit the number of events per slice
type BatchingPolicy
    (   // Max items to request in query response. Defaults to 10.
        [<O; D(null)>]?defaultMaxItems : int,
        // Dynamic version of `defaultMaxItems`, allowing one to react to dynamic configuration changes. Default to using `defaultMaxItems`
        [<O; D(null)>]?getDefaultMaxItems : unit -> int,
        /// Maximum number of trips to permit when slicing the work into multiple responses based on `MaxSlices`. Default: unlimited.
        [<O; D(null)>]?maxRequests) =
    let getDefaultMaxItems = defaultArg getDefaultMaxItems (fun () -> defaultArg defaultMaxItems 10)
    /// Limit for Maximum number of `Batch` records in a single query batch response
    member __.MaxItems = getDefaultMaxItems ()
    /// Maximum number of trips to permit when slicing the work into multiple responses based on `MaxItems`
    member __.MaxRequests = maxRequests

type Gateway(conn : Connection, batching : BatchingPolicy) =
    let (|FromUnfold|_|) (tryDecode: #IEventData<_> -> 'event option) (isOrigin: 'event -> bool) (xs:#IEventData<_>[]) : Option<'event[]> =
        let items = ResizeArray()
        let isOrigin' e =
            match tryDecode e with
            | None -> false
            | Some e ->
                items.Insert(0,e)
                isOrigin e
        match Array.tryFindIndexBack isOrigin' xs with
        | None -> None
        | Some _ -> items.ToArray() |> Some
    member __.Client = conn.Client
    member __.LoadBackwardsStopping log (container, stream) (tryDecode,isOrigin): Async<StreamToken * 'event[]> = async {
        let! pos, events = Query.walk log (container,stream) conn.QueryRetryPolicy batching.MaxItems batching.MaxRequests Direction.Backward None (tryDecode,isOrigin)
        Array.Reverse events
        return Token.create (container,stream) pos, events }
    member __.Read log (container,stream) direction startPos (tryDecode,isOrigin) : Async<StreamToken * 'event[]> = async {
        let! pos, events = Query.walk log (container,stream) conn.QueryRetryPolicy batching.MaxItems batching.MaxRequests direction startPos (tryDecode,isOrigin)
        return Token.create (container,stream) pos, events }
    member __.ReadLazy (batching: BatchingPolicy) log (container,stream) direction startPos (tryDecode,isOrigin) : AsyncSeq<'event[]> =
        Query.walkLazy log (container,stream) conn.QueryRetryPolicy batching.MaxItems batching.MaxRequests direction startPos (tryDecode,isOrigin)
    member __.LoadFromUnfoldsOrRollingSnapshots log (containerStream,maybePos) (tryDecode,isOrigin): Async<StreamToken * 'event[]> = async {
        let! res = Tip.tryLoad log conn.TipRetryPolicy containerStream maybePos
        match res with
        | Tip.Result.NotFound -> return Token.create containerStream Position.fromKnownEmpty, Array.empty
        | Tip.Result.NotModified -> return invalidOp "Not handled"
        | Tip.Result.Found (pos, FromUnfold tryDecode isOrigin span) -> return Token.create containerStream pos, span
        | _ -> return! __.LoadBackwardsStopping log containerStream (tryDecode,isOrigin) }
    member __.GetPosition(log, containerStream, ?pos): Async<StreamToken> = async {
        let! res = Tip.tryLoad log conn.TipRetryPolicy containerStream pos
        match res with
        | Tip.Result.NotFound -> return Token.create containerStream Position.fromKnownEmpty
        | Tip.Result.NotModified -> return Token.create containerStream pos.Value
        | Tip.Result.Found (pos, _unfoldsAndEvents) -> return Token.create containerStream pos }
    member __.LoadFromToken(log, (container,stream,pos), (tryDecode, isOrigin)): Async<LoadFromTokenResult<'event>> = async {
        let! res = Tip.tryLoad log conn.TipRetryPolicy (container,stream) (Some pos)
        match res with
        | Tip.Result.NotFound -> return LoadFromTokenResult.Found (Token.create (container,stream) Position.fromKnownEmpty,Array.empty)
        | Tip.Result.NotModified -> return LoadFromTokenResult.Unchanged
        | Tip.Result.Found (pos, FromUnfold tryDecode isOrigin span) -> return LoadFromTokenResult.Found (Token.create (container,stream) pos, span)
        | _ ->  let! res = __.Read log (container,stream) Direction.Forward (Some pos) (tryDecode,isOrigin)
                return LoadFromTokenResult.Found res }
    member __.CreateSyncStoredProcIfNotExists log container =
        Sync.Initialization.createSyncStoredProcIfNotExists log container
    member __.Sync log containerStream (exp, batch: Tip): Async<InternalSyncResult> = async {
        if Array.isEmpty batch.e && Array.isEmpty batch.u then invalidOp "Must write either events or unfolds."
        let! wr = Sync.batch log conn.WriteRetryPolicy containerStream (exp,batch)
        match wr with
        | Sync.Result.Conflict (pos',events) -> return InternalSyncResult.Conflict (Token.create containerStream pos',events)
        | Sync.Result.ConflictUnknown pos' -> return InternalSyncResult.ConflictUnknown (Token.create containerStream pos')
        | Sync.Result.Written pos' -> return InternalSyncResult.Written (Token.create containerStream pos') }
    member __.Prune(log, (container, stream), beforeIndex) =
        Delete.pruneBefore log (container, stream) batching.MaxItems beforeIndex

type private Category<'event, 'state, 'context>(gateway : Gateway, codec : IEventCodec<'event,byte[],'context>) =
    let (|TryDecodeFold|) (fold: 'state -> 'event seq -> 'state) initial (events: ITimelineEvent<byte[]> seq) : 'state = Seq.choose codec.TryDecode events |> fold initial
    member __.Load includeUnfolds containerStream fold initial isOrigin (log : ILogger): Async<StreamToken * 'state> = async {
        let! token, events =
            if not includeUnfolds then gateway.LoadBackwardsStopping log containerStream (codec.TryDecode,isOrigin)
            else gateway.LoadFromUnfoldsOrRollingSnapshots log (containerStream,None) (codec.TryDecode,isOrigin)
        return token, fold initial events }
    member __.LoadFromToken (Token.Unpack streamPos, state: 'state as current) fold isOrigin (log : ILogger): Async<StreamToken * 'state> = async {
        let! res = gateway.LoadFromToken(log, streamPos, (codec.TryDecode,isOrigin))
        match res with
        | LoadFromTokenResult.Unchanged -> return current
        | LoadFromTokenResult.Found (token', events') -> return token', fold state events' }
    member __.Sync(Token.Unpack (container,stream,pos), state as current, events, mapUnfolds, fold, isOrigin, log, context, compressUnfolds): Async<SyncResult<'state>> = async {
        let state' = fold state (Seq.ofList events)
        let encode e = codec.Encode(context,e)
        let exp,events,eventsEncoded,projectionsEncoded =
            match mapUnfolds with
            | Choice1Of3 () ->     Sync.Exp.Version pos.index, events, Seq.map encode events |> Array.ofSeq, Seq.empty
            | Choice2Of3 unfold -> Sync.Exp.Version pos.index, events, Seq.map encode events |> Array.ofSeq, Seq.map encode (unfold events state')
            | Choice3Of3 transmute ->
                let events', unfolds = transmute events state'
                Sync.Exp.Etag (defaultArg pos.etag null), events', Seq.map encode events' |> Array.ofSeq, Seq.map encode unfolds
        let baseIndex = pos.index + int64 (List.length events)
        let compressor = if compressUnfolds then Base64MaybeDeflateUtf8JsonConverter.Compress else id
        let projections = Sync.mkUnfold compressor baseIndex projectionsEncoded
        let batch = Sync.mkBatch stream eventsEncoded projections
        let! res = gateway.Sync log (container,stream) (exp,batch)
        match res with
        | InternalSyncResult.Conflict (token',TryDecodeFold fold state events') -> return SyncResult.Conflict (async { return token', events' })
        | InternalSyncResult.ConflictUnknown _token' -> return SyncResult.Conflict (__.LoadFromToken current fold isOrigin log)
        | InternalSyncResult.Written token' -> return SyncResult.Written (token', state') }

module Caching =
    /// Forwards all state changes in all streams of an ICategory to a `tee` function
    type CategoryTee<'event, 'state, 'context>(inner: ICategory<'event, 'state, Container*string,'context>, tee : string -> StreamToken * 'state -> Async<unit>) =
        let intercept streamName tokenAndState = async {
            let! _ = tee streamName tokenAndState
            return tokenAndState }
        let loadAndIntercept load streamName = async {
            let! tokenAndState = load
            return! intercept streamName tokenAndState }
        interface ICategory<'event, 'state, Container*string, 'context> with
            member __.Load(log, (container,streamName), opt) : Async<StreamToken * 'state> =
                loadAndIntercept (inner.Load(log, (container,streamName), opt)) streamName
            member __.TrySync(log : ILogger, (Token.Unpack (_container,stream,_) as streamToken), state, events : 'event list, context)
                : Async<SyncResult<'state>> = async {
                let! syncRes = inner.TrySync(log, streamToken, state, events, context)
                match syncRes with
                | SyncResult.Conflict resync -> return SyncResult.Conflict(loadAndIntercept resync stream)
                | SyncResult.Written(token', state') ->
                    let! intercepted = intercept stream (token', state')
                    return SyncResult.Written intercepted }

    let applyCacheUpdatesWithSlidingExpiration
            (cache : ICache)
            (prefix : string)
            (slidingExpiration : TimeSpan)
            (category : ICategory<'event, 'state, Container*string, 'context>)
            : ICategory<'event, 'state, Container*string, 'context> =
        let mkCacheEntry (initialToken : StreamToken, initialState : 'state) = new CacheEntry<'state>(initialToken, initialState, Token.supersedes)
        let options = CacheItemOptions.RelativeExpiration slidingExpiration
        let addOrUpdateSlidingExpirationCacheEntry streamName value = cache.UpdateIfNewer(prefix + streamName, options, mkCacheEntry value)
        CategoryTee<'event, 'state, 'context>(category, addOrUpdateSlidingExpirationCacheEntry) :> _

type private Folder<'event, 'state, 'context>
    (   category: Category<'event, 'state, 'context>, fold: 'state -> 'event seq -> 'state, initial: 'state,
        isOrigin: 'event -> bool,
        mapUnfolds: Choice<unit,('event list -> 'state -> 'event seq),('event list -> 'state -> 'event list * 'event list)>,
        compressUnfolds,
        ?readCache) =
    let inspectUnfolds = match mapUnfolds with Choice1Of3 () -> false | _ -> true
    let batched log containerStream = category.Load inspectUnfolds containerStream fold initial isOrigin log
    interface ICategory<'event, 'state, Container*string, 'context> with
        member __.Load(log, (container,streamName), opt): Async<StreamToken * 'state> =
            match readCache with
            | None -> batched log (container,streamName)
            | Some (cache : ICache, prefix : string) -> async {
                match! cache.TryGet(prefix + streamName) with
                | None -> return! batched log (container,streamName)
                | Some tokenAndState when opt = Some AllowStale -> return tokenAndState
                | Some tokenAndState -> return! category.LoadFromToken tokenAndState fold isOrigin log }
        member __.TrySync(log : ILogger, streamToken, state, events : 'event list, context)
            : Async<SyncResult<'state>> = async {
            match! category.Sync((streamToken, state), events, mapUnfolds, fold, isOrigin, log, context, compressUnfolds) with
            | SyncResult.Conflict resync ->         return SyncResult.Conflict resync
            | SyncResult.Written (token',state') -> return SyncResult.Written (token',state') }

/// Holds Container state, coordinating initialization activities
type private ContainerWrapper(container : Container, ?initContainer : Container -> Async<unit>) =
    let initGuard = initContainer |> Option.map (fun init -> AsyncCacheCell<unit>(init container))

    member __.Container = container
    member internal __.InitializationGate = match initGuard with Some g when g.PeekIsValid() |> not -> Some g.AwaitValue | _ -> None

/// Defines a process for mapping from a Stream Name to the appropriate storage area, allowing control over segregation / co-locating of data
type Containers(categoryAndIdToDatabaseContainerStream : string -> string -> string*string*string, [<O; D(null)>]?disableInitialization) =
    // Index of database*collection -> Initialization Context
    let wrappers = ConcurrentDictionary<string*string, ContainerWrapper>()
    new (databaseId, containerId) =
        // TOCONSIDER - this works to support the Core.Events APIs
        let genStreamName categoryName streamId = if categoryName = null then streamId else sprintf "%s-%s" categoryName streamId
        Containers(fun categoryName streamId -> databaseId, containerId, genStreamName categoryName streamId)

    member internal __.Resolve(client, categoryName, id, init) : (Container*string) * (unit -> Async<unit>) option =
        let databaseId, containerName, streamName = categoryAndIdToDatabaseContainerStream categoryName id
        let init = match disableInitialization with Some true -> None | _ -> Some init
        let mkWrapped (db,containerName) = ContainerWrapper(Container(client,db,containerName), ?initContainer = init)
        let wrapped = wrappers.GetOrAdd((databaseId,containerName), mkWrapped)
        (wrapped.Container,streamName),wrapped.InitializationGate

/// Pairs a Gateway, defining the retry policies for CosmosDb with a Containers map defining mappings from (category,id) to (databaseId,containerId,streamName)
type Context(gateway: Gateway, containers: Containers, [<O; D(null)>] ?log) =
    let init = gateway.CreateSyncStoredProcIfNotExists log
    new(gateway: Gateway, databaseId: string, containerId: string, [<O; D(null)>]?log) =
        Context(gateway, Containers(databaseId, containerId), ?log = log)
    new(connection: Connection, databaseId: string, containerId: string, [<O; D(null)>]?log) =
        Context(Gateway(connection, BatchingPolicy()), databaseId, containerId, ?log = log)

    member __.Gateway = gateway
    member __.Containers = containers
    member internal __.ResolveContainerStream(categoryName, id) : (Container*string) * (unit -> Async<unit>) option =
        containers.Resolve(gateway.Client, categoryName, id, init)

[<NoComparison; NoEquality; RequireQualifiedAccess>]
type CachingStrategy =
    /// Do not apply any caching strategy for this Stream.
    /// NB opting not to leverage caching when using CosmosDb can have significant implications for the scalability
    ///   of your application, both in terms of latency and running costs.
    /// While the cost of a cache miss can be ameliorated to varying degrees by employing an appropriate `AccessStrategy`
    ///   [that works well and has been validated for your scenario with real data], even a cache with a low Hit Rate provides
    ///   a direct benefit in terms of the number of Request Unit (RU)s that need to be provisioned to your CosmosDb instances.
    | NoCaching
    /// Retain a single 'state per streamName, together with the associated etag
    /// NB while a strategy like EventStore.Caching.SlidingWindowPrefixed is obviously easy to implement, the recommended approach is to
    /// track all relevant data in the state, and/or have the `unfold` function ensure _all_ relevant events get held in the `u`nfolds in tip
    | SlidingWindow of ICache * window: TimeSpan

[<NoComparison; NoEquality; RequireQualifiedAccess>]
type AccessStrategy<'event,'state> =
    /// Don't apply any optimized reading logic. Note this can be extremely RU cost prohibitive
    /// and can severely impact system scalability. Should hence only be used with careful consideration.
    | Unoptimized
    /// Load only the single most recent event defined in <c>'event`</c> and trust that doing a <c>fold</c> from any such event
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
    | Snapshot of isOrigin: ('event -> bool) * toSnapshot: ('state -> 'event)
    /// Allow any events that pass the `isOrigin` test to be used in lieu of folding all the events from the start of the stream
    /// When writing, uses `toSnapshots` to 'unfold' the <c>'state</c>, representing it as one or more Event records to be stored in
    /// the Tip with efficient read cost.
    | MultiSnapshot of isOrigin: ('event -> bool) * toSnapshots: ('state -> 'event seq)
    /// Instead of actually storing the events representing the decisions, only ever update a snapshot stored in the Tip document
    /// <remarks>In this mode, Optimistic Concurrency Control is necessarily based on the _etag</remarks>
    | RollingState of toSnapshot: ('state -> 'event)
    /// Allow produced events to be filtered, transformed or removed completely and/or to be transmuted to unfolds.
    /// <remarks>
    /// In this mode, Optimistic Concurrency Control is based on the _etag (rather than the normal Expected Version strategy)
    /// in order that conflicting updates to the state not involving the writing of an event can trigger retries.
    /// </remarks>
    | Custom of isOrigin: ('event -> bool) * transmute: ('event list -> 'state -> 'event list*'event list)

type Resolver<'event, 'state, 'context>
    (   context : Context, codec, fold, initial, caching, access,
        /// Compress Unfolds in Tip. Default: <c>true<c>.
        /// NOTE when set to <c>false</c>, requires Equinox.Cosmos Version >= 2.3.0 to be able to read
        ?compressUnfolds) =
    let compressUnfolds = defaultArg compressUnfolds true
    let readCacheOption =
        match caching with
        | CachingStrategy.NoCaching -> None
        | CachingStrategy.SlidingWindow(cache, _) -> Some(cache, null)
    let isOrigin, mapUnfolds =
        match access with
        | AccessStrategy.Unoptimized ->                      (fun _ -> false), Choice1Of3 ()
        | AccessStrategy.LatestKnownEvent ->                 (fun _ -> true),  Choice2Of3 (fun events _ -> Seq.last events |> Seq.singleton)
        | AccessStrategy.Snapshot (isOrigin,toSnapshot) ->   isOrigin,         Choice2Of3 (fun _ state  -> toSnapshot state |> Seq.singleton)
        | AccessStrategy.MultiSnapshot (isOrigin, unfold) -> isOrigin,         Choice2Of3 (fun _ state  -> unfold state)
        | AccessStrategy.RollingState toSnapshot ->          (fun _ -> true),  Choice3Of3 (fun _ state  -> [],[toSnapshot state])
        | AccessStrategy.Custom (isOrigin,transmute) ->      isOrigin,         Choice3Of3 transmute
    let cosmosCat = Category<'event, 'state, 'context>(context.Gateway, codec)
    let folder = Folder<'event, 'state, 'context>(cosmosCat, fold, initial, isOrigin, mapUnfolds, compressUnfolds, ?readCache = readCacheOption)
    let category : ICategory<_, _, Container*string, 'context> =
        match caching with
        | CachingStrategy.NoCaching -> folder :> _
        | CachingStrategy.SlidingWindow(cache, window) ->
            Caching.applyCacheUpdatesWithSlidingExpiration cache null window folder

    let resolveStream (streamId, maybeContainerInitializationGate) opt context =
        { new IStream<'event, 'state> with
            member __.Load log = category.Load(log, streamId, opt)
            member __.TrySync(log: ILogger, token: StreamToken, originState: 'state, events: 'event list) =
                match maybeContainerInitializationGate with
                | None -> category.TrySync(log, token, originState, events, context)
                | Some init -> async {
                    do! init ()
                    return! category.TrySync(log, token, originState, events, context) } }

    let resolveTarget = function
        | StreamName.CategoryAndId (categoryName, streamId) -> context.ResolveContainerStream(categoryName, streamId)

    member __.Resolve(streamName : StreamName, [<O; D null>]?option, [<O; D null>]?context) =
        match resolveTarget streamName, option with
        | streamArgs,(None|Some AllowStale) -> resolveStream streamArgs option context
        | (containerStream,maybeInit),Some AssumeEmpty ->
            Stream.ofMemento (Token.create containerStream Position.fromKnownEmpty,initial) (resolveStream (containerStream,maybeInit) option context)

    member __.FromMemento(Token.Unpack (container,stream,_pos) as streamToken,state) =
        let skipInitialization = None
        Stream.ofMemento (streamToken,state) (resolveStream ((container,stream),skipInitialization) None None)

[<RequireQualifiedAccess; NoComparison>]
type Discovery =
    | UriAndKey of databaseUri:Uri * key:string
    /// Implements connection string parsing logic curiously missing from the CosmosDB SDK
    static member FromConnectionString (connectionString: string) =
        match connectionString with
        | _ when String.IsNullOrWhiteSpace connectionString -> nullArg "connectionString"
        | Regex.Match "^\s*AccountEndpoint\s*=\s*([^;\s]+)\s*;\s*AccountKey\s*=\s*([^;\s]+)\s*;?\s*$" m ->
            let uri = m.Groups.[1].Value
            let key = m.Groups.[2].Value
            UriAndKey (Uri uri, key)
        | _ -> invalidArg "connectionString" "unrecognized connection string format; must be `AccountEndpoint=https://...;AccountKey=...=;`"

[<RequireQualifiedAccess>]
type ConnectionMode =
    /// Default mode, uses Https - inefficient as uses a double hop
    | Gateway
    /// Most efficient, but requires direct connectivity
    | Direct
    // More efficient than Gateway, but suboptimal
    | DirectHttps

type Connector
    (   /// Timeout to apply to individual reads/write round-trips going to CosmosDb
        requestTimeout: TimeSpan,
        /// Maximum number of times attempt when failure reason is a 429 from CosmosDb, signifying RU limits have been breached
        maxRetryAttemptsOnRateLimitedRequests: int,
        /// Maximum number of seconds to wait (especially if a higher wait delay is suggested by CosmosDb in the 429 response)
        // naming matches SDK ver >=3
        maxRetryWaitTimeOnRateLimitedRequests: TimeSpan,
        /// Log to emit connection messages to
        log : ILogger,
        /// Connection limit for Gateway Mode (default 1000)
        [<O; D(null)>]?gatewayModeMaxConnectionLimit,
        /// Connection mode (default: ConnectionMode.Gateway (lowest perf, least trouble))
        [<O; D(null)>]?mode : ConnectionMode,
        /// consistency mode  (default: ConsistencyLevel.Session)
        [<O; D(null)>]?defaultConsistencyLevel : ConsistencyLevel,

        /// Retries for read requests, over and above those defined by the mandatory policies
        [<O; D(null)>]?readRetryPolicy,
        /// Retries for write requests, over and above those defined by the mandatory policies
        [<O; D(null)>]?writeRetryPolicy,
        /// Additional strings identifying the context of this connection; should provide enough context to disambiguate all potential connections to a cluster
        /// NB as this will enter server and client logs, it should not contain sensitive information
        [<O; D(null)>]?tags : (string*string) seq,
        /// Inhibits certificate verification when set to <c>true</c>, i.e. for working with the CosmosDB Emulator (default <c>false</c>)
        [<O; D(null)>]?bypassCertificateValidation : bool) =
    do if log = null then nullArg "log"

    let logName (uri : Uri) name =
        let name = String.concat ";" <| seq {
            yield name
            match tags with None -> () | Some tags -> for key, value in tags do yield sprintf "%s=%s" key value }
        let sanitizedName = name.Replace('\'','_').Replace(':','_') // sic; Align with logging for ES Adapter
        log.ForContext("uri", uri).Information("CosmosDb Connecting {connectionName}", sanitizedName)

    /// ClientOptions (ConnectionPolicy with v2 SDK) for this Connector as configured
    member val ClientOptions =
        let co = Client.ConnectionPolicy.Default
        match mode with
        | None | Some ConnectionMode.Gateway -> co.ConnectionMode <- Client.ConnectionMode.Gateway // default; only supports Https
        | Some ConnectionMode.DirectHttps -> co.ConnectionMode <- Client.ConnectionMode.Direct; co.ConnectionProtocol <- Client.Protocol.Https // Https is default when using Direct
        | Some ConnectionMode.Direct -> co.ConnectionMode <- Client.ConnectionMode.Direct; co.ConnectionProtocol <- Client.Protocol.Tcp
        co.RetryOptions <-
            Client.RetryOptions(
                MaxRetryAttemptsOnThrottledRequests = maxRetryAttemptsOnRateLimitedRequests,
                MaxRetryWaitTimeInSeconds = (Math.Ceiling(maxRetryWaitTimeOnRateLimitedRequests.TotalSeconds) |> int))
        co.RequestTimeout <- requestTimeout
        co.MaxConnectionLimit <- defaultArg gatewayModeMaxConnectionLimit 1000
        co

    /// Yields a DocumentClient configured per the specified strategy
    member __.CreateClient
        (   /// Name should be sufficient to uniquely identify this connection within a single app instance's logs
            name, discovery : Discovery,
            /// <c>true</c> to inhibit logging of client name
            [<O; D null>]?skipLog) : Client.DocumentClient =
        let (Discovery.UriAndKey (databaseUri=uri; key=key)) = discovery
        if skipLog <> Some true then logName uri name
        let consistencyLevel = Nullable(defaultArg defaultConsistencyLevel ConsistencyLevel.Session)
        if defaultArg bypassCertificateValidation false then
            let inhibitCertCheck = new System.Net.Http.HttpClientHandler(ServerCertificateCustomValidationCallback = fun _ _ _ _ -> true)
            new Client.DocumentClient(uri, key, inhibitCertCheck, __.ClientOptions, consistencyLevel) // overload introduced in 2.2.0 SDK
        else new Client.DocumentClient(uri, key, __.ClientOptions, consistencyLevel)

    /// Yields a Connection configured per the specified strategy
    member __.Connect
        (   /// Name should be sufficient to uniquely identify this connection within a single app instance's logs
            name, discovery : Discovery,
            /// <c>true</c> to inhibit OpenAsync call
            [<O; D null>]?skipOpen,
            /// <c>true</c> to inhibit logging of client name
            [<O; D null>]?skipLog) : Async<Connection> = async {
        let client = __.CreateClient(name, discovery, ?skipLog=skipLog)
        if skipOpen <> Some true then do! client.OpenAsync() |> Async.AwaitTaskCorrect
        return Connection(client, ?readRetryPolicy=readRetryPolicy, ?writeRetryPolicy=writeRetryPolicy) }

namespace Equinox.Cosmos.Core

open Equinox.Cosmos
open Equinox.Cosmos.Store
open FsCodec
open FSharp.Control
open System.Runtime.InteropServices

/// Outcome of appending events, specifying the new and/or conflicting events, together with the updated Target write position
[<RequireQualifiedAccess; NoComparison>]
type AppendResult<'t> =
    | Ok of pos: 't
    | Conflict of index: 't * conflictingEvents: ITimelineEvent<byte[]>[]
    | ConflictUnknown of index: 't

/// Encapsulates the core facilities Equinox.Cosmos offers for operating directly on Events in Streams.
type Context
    (   /// Connection to CosmosDb, includes defined Transient Read and Write Retry policies
        conn : Connection,
        /// Container selector, mapping Stream Categories to Containers
        containers : Containers,
        /// Logger to write to - see https://github.com/serilog/serilog/wiki/Provided-Sinks for how to wire to your logger
        log : Serilog.ILogger,
        /// Optional maximum number of Store.Batch records to retrieve as a set (how many Events are placed therein is controlled by average batch size when appending events
        /// Defaults to 10
        [<Optional; DefaultParameterValue(null)>]?defaultMaxItems,
        /// Alternate way of specifying defaultMaxItems which facilitates reading it from a cached dynamic configuration
        [<Optional; DefaultParameterValue(null)>]?getDefaultMaxItems) =
    do if log = null then nullArg "log"
    let getDefaultMaxItems = match getDefaultMaxItems with Some f -> f | None -> fun () -> defaultArg defaultMaxItems 10
    let batching = BatchingPolicy(getDefaultMaxItems=getDefaultMaxItems)
    let gateway = Gateway(conn, batching)

    let maxCountPredicate count =
        let acc = ref (max (count-1) 0)
        fun _ ->
            if !acc = 0 then true else
            decr acc
            false

    let yieldPositionAndData res = async {
        let! (Token.Unpack (_,_,pos')), data = res
        return pos', data }

    member __.ResolveStream(streamName) = containers.Resolve(conn.Client, null, streamName, gateway.CreateSyncStoredProcIfNotExists (Some log))
    member __.CreateStream(streamName) = __.ResolveStream streamName |> fst

    member internal __.GetLazy((stream, startPos), ?batchSize, ?direction) : AsyncSeq<ITimelineEvent<byte[]>[]> =
        let direction = defaultArg direction Direction.Forward
        let batching = BatchingPolicy(defaultArg batchSize batching.MaxItems)
        gateway.ReadLazy batching log stream direction startPos (Some,fun _ -> false)

    member internal __.GetInternal((stream, startPos), ?maxCount, ?direction) = async {
        let direction = defaultArg direction Direction.Forward
        if maxCount = Some 0 then
            // Search semantics include the first hit so we need to special case this anyway
            return Token.create stream (defaultArg startPos Position.fromKnownEmpty), Array.empty
        else
            let isOrigin =
                match maxCount with
                | Some limit -> maxCountPredicate limit
                | None -> fun _ -> false
            return! gateway.Read log stream direction startPos (Some,isOrigin) }

    /// Establishes the current position of the stream in as efficient a manner as possible
    /// (The ideal situation is that the preceding token is supplied as input in order to avail of 1RU low latency state checks)
    member __.Sync(stream, ?position: Position) : Async<Position> = async {
        let! (Token.Unpack (_,_,pos')) = gateway.GetPosition(log, stream, ?pos=position)
        return pos' }

    /// Reads in batches of `batchSize` from the specified `Position`, allowing the reader to efficiently walk away from a running query
    /// ... NB as long as they Dispose!
    member __.Walk(stream, batchSize, ?position, ?direction) : AsyncSeq<ITimelineEvent<byte[]>[]> =
        __.GetLazy((stream, position), batchSize, ?direction=direction)

    /// Reads all Events from a `Position` in a given `direction`
    member __.Read(stream, ?position, ?maxCount, ?direction) : Async<Position*ITimelineEvent<byte[]>[]> =
        __.GetInternal((stream, position), ?maxCount=maxCount, ?direction=direction) |> yieldPositionAndData

    /// Appends the supplied batch of events, subject to a consistency check based on the `position`
    /// Callers should implement appropriate idempotent handling, or use Equinox.Stream for that purpose
    member __.Sync((container,stream), position, events: IEventData<_>[]) : Async<AppendResult<Position>> = async {
        // Writes go through the stored proc, which we need to provision per-collection
        // Having to do this here in this way is far from ideal, but work on caching, external snapshots and caching is likely
        //   to move this about before we reach a final destination in any case
        match __.ResolveStream stream |> snd with
        | None -> ()
        | Some init -> do! init ()
        let batch = Sync.mkBatch stream events Seq.empty
        let! res = gateway.Sync log (container,stream) (Sync.Exp.Version position.index,batch)
        match res with
        | InternalSyncResult.Written (Token.Unpack (_,_,pos)) -> return AppendResult.Ok pos
        | InternalSyncResult.Conflict (Token.Unpack (_,_,pos),events) -> return AppendResult.Conflict (pos, events)
        | InternalSyncResult.ConflictUnknown (Token.Unpack (_,_,pos)) -> return AppendResult.ConflictUnknown pos }

    /// Low level, non-idempotent call appending events to a stream without a concurrency control mechanism in play
    /// NB Should be used sparingly; Equinox.Stream enables building equivalent equivalent idempotent handling with minimal code.
    member __.NonIdempotentAppend(stream, events: IEventData<_>[]) : Async<Position> = async {
        let! res = __.Sync(stream, Position.fromAppendAtEnd, events)
        match res with
        | AppendResult.Ok token -> return token
        | x -> return x |> sprintf "Conflict despite it being disabled %A" |> invalidOp }

    member __.Prune((container,stream), beforeIndex) : Async<int * int * int64> =
        gateway.Prune(log, (container,stream), beforeIndex)

/// Provides mechanisms for building `EventData` records to be supplied to the `Events` API
type EventData() =
    /// Creates an Event record, suitable for supplying to Append et al
    static member FromUtf8Bytes(eventType, data, ?meta) : IEventData<_> = FsCodec.Core.EventData.Create(eventType, data, ?meta=meta) :> _

/// Api as defined in the Equinox Specification
/// Note the CosmosContext APIs can yield better performance due to the fact that a Position tracks the etag of the Stream's Tip
module Events =
    let private (|PositionIndex|) (x: Position) = x.index
    let private stripSyncResult (f: Async<AppendResult<Position>>): Async<AppendResult<int64>> = async {
        let! res = f
        match res with
        | AppendResult.Ok (PositionIndex index)-> return AppendResult.Ok index
        | AppendResult.Conflict (PositionIndex index,events) -> return AppendResult.Conflict (index, events)
        | AppendResult.ConflictUnknown (PositionIndex index) -> return AppendResult.ConflictUnknown index }
    let private stripPosition (f: Async<Position>): Async<int64> = async {
        let! (PositionIndex index) = f
        return index }
    let private dropPosition (f: Async<Position*ITimelineEvent<byte[]>[]>): Async<ITimelineEvent<byte[]>[]> = async {
        let! _,xs = f
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
    let getAll (ctx: Context) (streamName: string) (MinPosition index: int64) (batchSize: int): FSharp.Control.AsyncSeq<ITimelineEvent<byte[]>[]> =
        ctx.Walk(ctx.CreateStream streamName, batchSize, ?position=index)

    /// Returns an async array of events in the stream starting at the specified sequence number,
    /// number of events to read is specified by batchSize
    /// Returns an empty sequence if the stream is empty or if the sequence number is larger than the largest
    /// sequence number in the stream.
    let get (ctx: Context) (streamName: string) (MinPosition index: int64) (maxCount: int): Async<ITimelineEvent<byte[]>[]> =
        ctx.Read(ctx.CreateStream streamName, ?position=index, maxCount=maxCount) |> dropPosition

    /// Appends a batch of events to a stream at the specified expected sequence number.
    /// If the specified expected sequence number does not match the stream, the events are not appended
    /// and a failure is returned.
    let append (ctx: Context) (streamName: string) (index: int64) (events: IEventData<_>[]): Async<AppendResult<int64>> =
        ctx.Sync(ctx.CreateStream streamName, Position.fromI index, events) |> stripSyncResult

    /// Appends a batch of events to a stream at the the present Position without any conflict checks.
    /// NB typically, it is recommended to ensure idempotency of operations by using the `append` and related API as
    /// this facilitates ensuring consistency is maintained, and yields reduced latency and Request Charges impacts
    /// (See equivalent APIs on `Context` that yield `Position` values)
    let appendAtEnd (ctx: Context) (streamName: string) (events: IEventData<_>[]): Async<int64> =
        ctx.NonIdempotentAppend(ctx.CreateStream streamName, events) |> stripPosition

    /// Requests deletion of events prior to the specified Index
    /// Due to the need to preserve ordering of data in the stream, only full batches will be removed
    /// Returns count of events deleted this time, events that could not be deleted due to partial batches, and the stream's lowest remaining sequence number
    let prune (ctx: Context) (streamName: string) (beforeIndex: int64): Async<int * int * int64> =
        ctx.Prune(ctx.CreateStream streamName, beforeIndex)

    /// Returns an async sequence of events in the stream backwards starting from the specified sequence number,
    /// reading in batches of the specified size.
    /// Returns an empty sequence if the stream is empty or if the sequence number is smaller than the smallest
    /// sequence number in the stream.
    let getAllBackwards (ctx: Context) (streamName: string) (MaxPosition index: int64) (batchSize: int): AsyncSeq<ITimelineEvent<byte[]>[]> =
        ctx.Walk(ctx.CreateStream streamName, batchSize, ?position=index, direction=Direction.Backward)

    /// Returns an async array of events in the stream backwards starting from the specified sequence number,
    /// number of events to read is specified by batchSize
    /// Returns an empty sequence if the stream is empty or if the sequence number is smaller than the smallest
    /// sequence number in the stream.
    let getBackwards (ctx: Context) (streamName: string) (MaxPosition index: int64) (maxCount: int): Async<ITimelineEvent<byte[]>[]> =
        ctx.Read(ctx.CreateStream streamName, ?position=index, maxCount=maxCount, direction=Direction.Backward) |> dropPosition

    /// Obtains the `index` from the current write Position
    let getNextIndex (ctx: Context) (streamName: string) : Async<int64> =
        ctx.Sync(ctx.CreateStream streamName) |> stripPosition
