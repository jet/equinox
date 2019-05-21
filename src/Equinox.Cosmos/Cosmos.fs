namespace Equinox.Cosmos.Store

open Newtonsoft.Json
open System

type IIndexedEvent = Equinox.Codec.Core.IIndexedEvent<byte[]>

/// A single Domain Event from the array held in a Batch
type [<NoEquality; NoComparison; JsonObject(ItemRequired=Required.Always)>]
    Event =
    {   /// Creation datetime (as opposed to system-defined _lastUpdated which is touched by triggers, replication etc.)
        t: DateTimeOffset // ISO 8601

        /// The Case (Event Type); used to drive deserialization
        c: string // required

        /// Event body, as UTF-8 encoded json ready to be injected into the Json being rendered for DocDb
        [<JsonConverter(typeof<Equinox.Cosmos.Internal.Json.VerbatimUtf8JsonConverter>)>]
        [<JsonProperty(Required=Required.AllowNull)>]
        d: byte[] // Required, but can be null so Nullary cases can work

        /// Optional metadata, as UTF-8 encoded json, ready to emit directly (null, not written if missing)
        [<JsonConverter(typeof<Equinox.Cosmos.Internal.Json.VerbatimUtf8JsonConverter>)>]
        [<JsonProperty(Required=Required.Default, NullValueHandling=NullValueHandling.Ignore)>]
        m: byte[] } // optional

/// A 'normal' (frozen, not Tip) Batch of Events (without any Unfolds)
type [<NoEquality; NoComparison; JsonObject(ItemRequired=Required.Always)>]
    Batch =
    {   /// DocDb-mandated Partition Key, must be maintained within the document
        /// Not actually required if running in single partition mode, but for simplicity, we always write it
        [<JsonProperty(Required=Required.Default)>] // Not requested in queries
        p: string // "{streamName}"

        /// DocDb-mandated unique row key; needs to be unique within any partition it is maintained; must be string
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
    /// Unless running in single partion mode (which would restrict us to 10GB per collection)
    /// we need to nominate a partition key that will be in every document
    static member internal PartitionKeyField = "p"
    /// As one cannot sort by the implicit `id` field, we have an indexed `i` field for sort and range query use
    static member internal IndexedFields = [Batch.PartitionKeyField; "i"; "n"]

/// Compaction/Snapshot/Projection Event based on the state at a given point in time `i`
type Unfold =
    {   /// Base: Stream Position (Version) of State from which this Unfold Event was generated
        i: int64

        /// The Case (Event Type) of this compaction/snapshot, used to drive deserialization
        c: string // required

        /// Event body - Json -> UTF-8 -> Deflate -> Base64
        [<JsonConverter(typeof<Equinox.Cosmos.Internal.Json.Base64ZipUtf8JsonConverter>)>]
        d: byte[] // required

        /// Optional metadata, same encoding as `d` (can be null; not written if missing)
        [<JsonConverter(typeof<Equinox.Cosmos.Internal.Json.Base64ZipUtf8JsonConverter>)>]
        [<JsonProperty(Required=Required.Default, NullValueHandling=NullValueHandling.Ignore)>]
        m: byte[] } // optional

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
    /// arguably this should be a high nember to reflect fact it is the freshest ?
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
    /// NB very inefficient compared to FromDocument or using one already returned to you
    let fromMaxIndex (xs: IIndexedEvent[]) =
        if Array.isEmpty xs then fromKnownEmpty
        else fromI (1L + Seq.max (seq { for x in xs -> x.Index }))
    /// Create Position from Tip record context (facilitating 1 RU reads)
    let fromTip (x: Tip) = { index = x.n; etag = match x._etag with null -> None | x -> Some x }
    /// If we encounter the tip (id=-1) doc, we're interested in its etag so we can re-sync for 1 RU
    let tryFromBatch (x: Batch) =
        if x.id <> Tip.WellKnownDocumentId then None
        else Some { index = x.n; etag = match x._etag with null -> None | x -> Some x }

[<RequireQualifiedAccess>]
type Direction = Forward | Backward override this.ToString() = match this with Forward -> "Forward" | Backward -> "Backward"

/// Reference to Collection and name that will be used as the location for the stream
type [<NoComparison>] CollectionStream = { collectionUri: Uri; name: string }

type internal Enum() =
    static member internal Events(b: Tip) =
        b.e |> Seq.mapi (fun offset x ->
            { new IIndexedEvent with
                member __.Index = b.i + int64 offset
                member __.IsUnfold = false
                member __.EventType = x.c
                member __.Data = x.d
                member __.Meta = x.m
                member __.Timestamp = x.t })
    static member Events(i: int64, e: Event[], startPos : Position option, direction) = seq {
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
                yield {
                    new IIndexedEvent with
                        member __.Index = index
                        member __.IsUnfold = false
                        member __.EventType = x.c
                        member __.Data = x.d
                        member __.Meta = x.m
                        member __.Timestamp = x.t } }
    static member internal Events(b: Batch, startPos, direction) =
        Enum.Events(b.i, b.e, startPos, direction)
        |> if direction = Direction.Backward then System.Linq.Enumerable.Reverse else id
    static member Unfolds (xs: Unfold[]) = seq {
        for x in xs -> { new IIndexedEvent with
            member __.Index = x.i
            member __.IsUnfold = true
            member __.EventType = x.c
            member __.Data = x.d
            member __.Meta = x.m
            member __.Timestamp = DateTimeOffset.MinValue } }
    static member EventsAndUnfolds(x: Tip): IIndexedEvent seq =
        Enum.Events x
        |> Seq.append (Enum.Unfolds x.u)
        // where Index is equal, unfolds get delivered after the events so the fold semantics can be 'idempotent'
        |> Seq.sortBy (fun x -> x.Index, x.IsUnfold)

type IRetryPolicy = abstract member Execute: (int -> Async<'T>) -> Async<'T>

open Equinox.Store
open Serilog

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
        | Response of Direction * Measurement
        | SyncSuccess of Measurement
        | SyncResync of Measurement
        | SyncConflict of Measurement
    let prop name value (log : ILogger) = log.ForContext(name, value)
    let propData name (events: #Equinox.Codec.IEvent<byte[]> seq) (log : ILogger) =
        let items = seq { for e in events do yield sprintf "{\"%s\": %s}" e.EventType (System.Text.Encoding.UTF8.GetString e.Data) }
        log.ForContext(name, sprintf "[%s]" (String.concat ",\n\r" items))
    let propEvents = propData "events"
    let propDataUnfolds = Enum.Unfolds >> propData "unfolds"
    let propStartPos (value : Position) log = prop "startPos" value.index log
    let propMaybeStartPos (value : Position option) log = match value with None -> log | Some value -> propStartPos value log

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
    let (|EventLen|) (x: #Equinox.Codec.IEvent<_>) = let (BlobLen bytes), (BlobLen metaBytes) = x.Data, x.Meta in bytes+metaBytes
    let (|BatchLen|) = Seq.sumBy (|EventLen|)

    /// NB Caveat emptor; this is subject to unlimited change without the major version changing - while the `dotnet-templates` repo will be kept in step, and
    /// the ChangeLog will mention changes, it's critical to not assume that the presence or nature of these helpers be considered stable
    module InternalMetrics =

        module RuCounters =
            let inline (|Stats|) ({ interval = i; ru = ru }: Measurement) = ru, let e = i.Elapsed in int64 e.TotalMilliseconds

            let (|CosmosReadRc|CosmosWriteRc|CosmosResyncRc|CosmosResponseRc|) = function
                | Tip (Stats s)
                | TipNotFound (Stats s)
                | TipNotModified (Stats s)
                | Query (_,_, (Stats s)) -> CosmosReadRc s
                // slices are rolled up into batches so be sure not to double-count
                | Response (_,(Stats s)) -> CosmosResponseRc s
                | SyncSuccess (Stats s)
                | SyncConflict (Stats s) -> CosmosWriteRc s
                | SyncResync (Stats s) -> CosmosResyncRc s
            let (|SerilogScalar|_|) : LogEventPropertyValue -> obj option = function
                | (:? ScalarValue as x) -> Some x.Value
                | _ -> None
            let (|CosmosMetric|_|) (logEvent : LogEvent) : Event option =
                match logEvent.Properties.TryGetValue("cosmosEvt") with
                | true, SerilogScalar (:? Event as e) -> Some e
                | _ -> None
            type RuCounter =
                { mutable rux100: int64; mutable count: int64; mutable ms: int64 }
                static member Create() = { rux100 = 0L; count = 0L; ms = 0L }
                member __.Ingest (ru, ms) =
                    System.Threading.Interlocked.Increment(&__.count) |> ignore
                    System.Threading.Interlocked.Add(&__.rux100, int64 (ru*100.)) |> ignore
                    System.Threading.Interlocked.Add(&__.ms, ms) |> ignore
            type RuCounterSink() =
                static let epoch = System.Diagnostics.Stopwatch.StartNew()
                static member val Read = RuCounter.Create() with get, set
                static member val Write = RuCounter.Create() with get, set
                static member val Resync = RuCounter.Create() with get, set
                static member Restart() =
                    RuCounterSink.Read <- RuCounter.Create()
                    RuCounterSink.Write <- RuCounter.Create()
                    RuCounterSink.Resync <- RuCounter.Create()
                    let span = epoch.Elapsed
                    epoch.Restart()
                    span
                interface Serilog.Core.ILogEventSink with
                    member __.Emit logEvent = logEvent |> function
                        | CosmosMetric (CosmosReadRc stats) -> RuCounterSink.Read.Ingest stats
                        | CosmosMetric (CosmosWriteRc stats) -> RuCounterSink.Write.Ingest stats
                        | CosmosMetric (CosmosResyncRc stats) -> RuCounterSink.Resync.Ingest stats
                        | _ -> ()

        /// Relies on feeding of metrics from Log through to RuCounterSink
        /// Use RuCounters.RuCounterSink.Reset() to reset the start point (and stats) where relevant
        let dump (log: Serilog.ILogger) =
            let stats =
              [ "Read", RuCounters.RuCounterSink.Read
                "Write", RuCounters.RuCounterSink.Write
                "Resync", RuCounters.RuCounterSink.Resync ]
            let mutable totalCount, totalRc, totalMs = 0L, 0., 0L
            let logActivity name count rc lat =
                if count <> 0L then
                    log.Information("{name}: {count:n0} requests costing {ru:n0} RU (average: {avg:n2}); Average latency: {lat:n0}ms",
                        name, count, rc, (if count = 0L then Double.NaN else rc/float count), (if count = 0L then Double.NaN else float lat/float count))
            for name, stat in stats do
                let ru = float stat.rux100 / 100.
                totalCount <- totalCount + stat.count
                totalRc <- totalRc + ru
                totalMs <- totalMs + stat.ms
                logActivity name stat.count ru stat.ms
            // Yes, there's a minor race here between the use of the values and the reset
            let duration = RuCounters.RuCounterSink.Restart()
            logActivity "TOTAL" totalCount totalRc totalMs
            let measures : (string * (TimeSpan -> float)) list = [ "s", fun x -> x.TotalSeconds(*; "m", fun x -> x.TotalMinutes; "h", fun x -> x.TotalHours*) ]
            let logPeriodicRate name count ru = log.Information("rp{name} {count:n0} = ~{ru:n0} RU", name, count, ru)
            for uom, f in measures do let d = f duration in if d <> 0. then logPeriodicRate uom (float totalCount/d |> int64) (totalRc/d) 

open Microsoft.Azure.Documents

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
    /// DocumentDB Error HttpStatusCode extractor
    let (|DocDbException|_|) (e : exn) =
        match e with
        | AggregateException (:? DocumentClientException as dce) -> Some dce
        | _ -> None
    /// Map Nullable to Option
    let (|HasValue|Null|) (x:Nullable<_>) =
        if x.HasValue then HasValue x.Value
        else Null
    /// DocumentDB Error HttpStatusCode extractor
    let (|DocDbStatusCode|_|) (e : DocumentClientException) =
        match e.StatusCode with
        | HasValue x -> Some x
        | Null -> None

    type ReadResult<'T> = Found of 'T | NotFound | NotModified
    type DocDbCollection(client : Client.DocumentClient, collectionUri) =
        member __.TryReadDocument(documentId : string, ?options : Client.RequestOptions): Async<float * ReadResult<'T>> = async {
            let options = defaultArg options null
            let docLink = sprintf "%O/docs/%s" collectionUri documentId
            let! ct = Async.CancellationToken
            try let! document = async { return! client.ReadDocumentAsync<'T>(docLink, options = options, cancellationToken = ct) |> Async.AwaitTaskCorrect }
                if document.StatusCode = System.Net.HttpStatusCode.NotModified then return document.RequestCharge, NotModified
                // NB `.Document` will NRE if a IfNoneModified precondition triggers a NotModified result
                else return document.RequestCharge, Found document.Document
            with DocDbException (DocDbStatusCode System.Net.HttpStatusCode.NotFound as e) -> return e.RequestCharge, NotFound
                // NB while the docs suggest you may see a 412, the NotModified in the body of the try/with is actually what happens
                | DocDbException (DocDbStatusCode System.Net.HttpStatusCode.PreconditionFailed as e) -> return e.RequestCharge, NotModified }

module Sync =
    // NB don't nest in a private module, or serialization will fail miserably ;)
    [<CLIMutable; NoEquality; NoComparison; Newtonsoft.Json.JsonObject(ItemRequired=Newtonsoft.Json.Required.AllowNull)>]
    type SyncResponse = { etag: string; n: int64; conflicts: Event[] }
    let [<Literal>] private sprocName = "EquinoxNoTipEvents"  // NB need to renumber for any breaking change
    let [<Literal>] private sprocBody = """

// Manages the merging of the supplied Request Batch, fulfilling one of the following end-states
// 1 perform expectedVersion verification (can request inhibiting of check by supplying -1)
// 2a Verify no current Tip; if so - incoming req.e and defines the 'next' position / unfolds
// 2b If we already have a tip, move position forward, replace unfolds
// 3 insert a new document containing the events as part of the same batch of work
function sync(req, expectedVersion, maxEvents) {
    if (!req) throw new Error("Missing req argument");
    const collection = getContext().getCollection();
    const collectionLink = collection.getSelfLink();
    const response = getContext().getResponse();

    // Locate the Tip (-1) batch (which may not exist)
    const tipDocId = collection.getAltLink() + "/docs/" + req.id;
    const isAccepted = collection.readDocument(tipDocId, {}, function (err, current) {
        // Verify we dont have a conflicting write
        if (expectedVersion === -1) {
            executeUpsert(current);
        } else if (!current && expectedVersion !== 0) {
            // If there is no Tip page, the writer has no possible reason for writing at an index other than zero
            response.setBody({ etag: null, n: 0, conflicts: [] });
        } else if (current && expectedVersion !== current.n) {
            response.setBody({ etag: current._etag, n: current.n, conflicts: [] });
        } else {
            executeUpsert(current);
        }
    });
    if (!isAccepted) throw new Error("readDocument not Accepted");

    function executeUpsert(current) {
        function callback(err, doc) {
            if (err) throw err;
            response.setBody({ etag: doc._etag, n: doc.n, conflicts: null });
        }
        var tip;
        if (!current) {
            tip = { p: req.p, id: req.id, i: req.e.length, n: req.e.length, e: [], u: req.u };
            const tipAccepted = collection.createDocument(collectionLink, tip, { disableAutomaticIdGeneration: true }, callback);
            if (!tipAccepted) throw new Error("Unable to create Tip.");
        } else {
            // TODO Carry forward `u` items not in `req`, together with supporting catchup events from preceding batches
            const n = current.n + req.e.length;
            tip = { p: current.p, id: current.id, i: n, n: n, e: [], u: req.u };

            // as we've mutated the document in a manner that can conflict with other writers, out write needs to be contingent on no competing updates having taken place
            const tipAccepted = collection.replaceDocument(current._self, tip, { etag: current._etag }, callback);
            if (!tipAccepted) throw new Error("Unable to replace Tip.");
        }
        // For now, always do an Insert, as Change Feed mechanism does not yet afford us a way to
        // a) guarantee an item per write (can be squashed)
        // b) with metadata sufficient for us to determine the items added (only etags, no way to convey i/n in feed item)
        const i = tip.n - req.e.length;
        const batch = { p: tip.p, id: i.toString(), i: i, n: tip.n, e: req.e };
        const batchAccepted = collection.createDocument(collectionLink, batch, { disableAutomaticIdGeneration: true });
        if (!batchAccepted) throw new Error("Unable to insert Batch.");
    }
}"""

    [<RequireQualifiedAccess; NoEquality; NoComparison>]
    type Result =
        | Written of Position
        | Conflict of Position * events: IIndexedEvent[]
        | ConflictUnknown of Position

    let private run (client: Client.DocumentClient) (stream: CollectionStream) (expectedVersion: int64 option, req: Tip, maxEvents: int)
        : Async<float*Result> = async {
        let sprocLink = sprintf "%O/sprocs/%s" stream.collectionUri sprocName
        let opts = Client.RequestOptions(PartitionKey=PartitionKey(stream.name))
        let ev = match expectedVersion with Some ev -> Position.fromI ev | None -> Position.fromAppendAtEnd
        let! ct = Async.CancellationToken
        let! (res : Client.StoredProcedureResponse<SyncResponse>) =
            client.ExecuteStoredProcedureAsync(sprocLink, opts, ct, box req, box ev.index, box maxEvents) |> Async.AwaitTaskCorrect
        let newPos = { index = res.Response.n; etag = Option.ofObj res.Response.etag }
        return res.RequestCharge, res.Response.conflicts |> function
            | null -> Result.Written newPos
            | [||] when newPos.index = 0L -> Result.Conflict (newPos, Array.empty)
            | [||] -> Result.ConflictUnknown newPos
            | xs  -> Result.Conflict (newPos, Enum.Events(ev.index, xs, None, Direction.Forward) |> Array.ofSeq) }

    let private logged client (stream: CollectionStream) (expectedVersion, req: Tip, maxEvents) (log : ILogger)
        : Async<Result> = async {
        let verbose = log.IsEnabled Serilog.Events.LogEventLevel.Debug
        let log = if verbose then log |> Log.propEvents (Enum.Events req) |> Log.propDataUnfolds req.u else log
        let (Log.BatchLen bytes), count = Enum.Events req, req.e.Length
        let log = log |> Log.prop "bytes" bytes
        let writeLog =
            log |> Log.prop "stream" stream.name |> Log.prop "expectedVersion" expectedVersion
                |> Log.prop "count" req.e.Length |> Log.prop "ucount" req.u.Length
        let! t, (ru,result) = run client stream (expectedVersion, req, maxEvents) |> Stopwatch.Time
        let resultLog =
            let mkMetric ru : Log.Measurement = { stream = stream.name; interval = t; bytes = bytes; count = count; ru = ru }
            let logConflict () = writeLog.Information("EqxCosmos Sync: Conflict writing {eventTypes}", [| for x in req.e -> x.c |])
            match result with
            | Result.Written pos ->
                log |> Log.event (Log.SyncSuccess (mkMetric ru)) |> Log.prop "nextExpectedVersion" pos
            | Result.ConflictUnknown pos ->
                logConflict ()
                log |> Log.event (Log.SyncConflict (mkMetric ru)) |> Log.prop "nextExpectedVersion" pos |> Log.prop "conflict" true
            | Result.Conflict (pos, xs) ->
                logConflict ()
                let log = if verbose then log |> Log.prop "nextExpectedVersion" pos |> Log.propData "conflicts" xs else log
                log |> Log.event (Log.SyncResync(mkMetric ru)) |> Log.prop "conflict" true
        resultLog.Information("EqxCosmos {action:l} {count}+{ucount} {ms}ms rc={ru}", "Sync", req.e.Length, req.u.Length, (let e = t.Elapsed in e.TotalMilliseconds), ru)
        return result }

    let batch (log : ILogger) retryPolicy client pk batch: Async<Result> =
        let call = logged client pk batch
        Log.withLoggedRetries retryPolicy "writeAttempt" call log
    let mkBatch (stream: CollectionStream) (events: Equinox.Codec.IEvent<_>[]) unfolds: Tip =
        {   p = stream.name; id = Tip.WellKnownDocumentId; n = -1L(*Server-managed*); i = -1L(*Server-managed*); _etag = null
            e = [| for e in events -> { t = e.Timestamp; c = e.EventType; d = e.Data; m = e.Meta } |]
            u = Array.ofSeq unfolds }
    let mkUnfold baseIndex (unfolds: Equinox.Codec.IEvent<_> seq) : Unfold seq =
        unfolds |> Seq.mapi (fun offset x -> { i = baseIndex + int64 offset; c = x.EventType; d = x.Data; m = x.Meta } : Unfold)

    module Initialization =
        open System.Collections.ObjectModel
        open System.Linq
        type [<RequireQualifiedAccess>] Provisioning = Container of rus: int | Database of rus: int
        let adjustOffer (client:Client.DocumentClient) resourceLink rus = async {
            let offer = client.CreateOfferQuery().Where(fun r -> r.ResourceLink = resourceLink).AsEnumerable().Single()
            let! _ = client.ReplaceOfferAsync(OfferV2(offer,rus)) |> Async.AwaitTaskCorrect in () }
        let private createDatabaseIfNotExists (client:Client.DocumentClient) dbName maybeRus =
            let opts = Client.RequestOptions(ConsistencyLevel = Nullable ConsistencyLevel.Session)
            maybeRus |> Option.iter (fun rus -> opts.OfferThroughput <- Nullable rus)
            client.CreateDatabaseIfNotExistsAsync(Database(Id=dbName), options = opts) |> Async.AwaitTaskCorrect
        let private createOrProvisionDatabase (client:Client.DocumentClient) dbName mode = async {
            match mode with
            | Provisioning.Database rus ->
                let! db = createDatabaseIfNotExists client dbName (Some rus)
                do! adjustOffer client db.Resource.SelfLink rus
            | Provisioning.Container _ ->
                let! _ = createDatabaseIfNotExists client dbName None in () }
        let private createCollIfNotExists (client:Client.DocumentClient) dbName (def: DocumentCollection) maybeRus =
            let dbUri = Client.UriFactory.CreateDatabaseUri dbName
            let opts = match maybeRus with None -> Client.RequestOptions() | Some rus -> Client.RequestOptions(OfferThroughput=Nullable rus)
            client.CreateDocumentCollectionIfNotExistsAsync(dbUri, def, opts) |> Async.AwaitTaskCorrect
        let private createOrProvisionCollection (client: Client.DocumentClient) (dbName, def: DocumentCollection) mode = async {
            match mode with
            | Provisioning.Database _ ->
                let! _ = createCollIfNotExists client dbName def None in ()
            | Provisioning.Container rus ->
                let! coll = createCollIfNotExists client dbName def (Some rus) in ()
                do! adjustOffer client coll.Resource.SelfLink rus }
        let private createStoredProcIfNotExists (client: IDocumentClient) (collectionUri: Uri) (name, body): Async<float> = async {
            try let! r = client.CreateStoredProcedureAsync(collectionUri, StoredProcedure(Id = name, Body = body)) |> Async.AwaitTaskCorrect
                return r.RequestCharge
            with DocDbException ((DocDbStatusCode sc) as e) when sc = System.Net.HttpStatusCode.Conflict -> return e.RequestCharge }
        let private createBatchAndTipCollectionIfNotExists (client: Client.DocumentClient) (dbName,collName) mode : Async<unit> =
            let pkd = PartitionKeyDefinition()
            pkd.Paths.Add(sprintf "/%s" Batch.PartitionKeyField)
            let def = DocumentCollection(Id = collName, PartitionKey = pkd)

            def.IndexingPolicy.IndexingMode <- IndexingMode.Consistent
            def.IndexingPolicy.Automatic <- true
            // Can either do a blacklist or a whitelist
            // Given how long and variable the blacklist would be, we whitelist instead
            def.IndexingPolicy.ExcludedPaths <- Collection [|ExcludedPath(Path="/*")|]
            // NB its critical to index the nominated PartitionKey field defined above or there will be runtime errors
            def.IndexingPolicy.IncludedPaths <- Collection [| for k in Batch.IndexedFields -> IncludedPath(Path=sprintf "/%s/?" k) |]
            createOrProvisionCollection client (dbName, def) mode
        let createSyncStoredProcIfNotExists (log: ILogger option) client collUri = async {
            let! t, ru = createStoredProcIfNotExists client collUri (sprocName,sprocBody) |> Stopwatch.Time
            match log with
            | None -> ()
            | Some log -> log.Information("Created stored procedure {sprocId} in {ms}ms rc={ru}", sprocName, (let e = t.Elapsed in e.TotalMilliseconds), ru) }
        let private createAuxCollectionIfNotExists (client: Client.DocumentClient) (dbName,collName) mode : Async<unit> =
            let def = DocumentCollection(Id = collName)
            // TL;DR no indexing of any kind; see https://github.com/Azure/azure-documentdb-changefeedprocessor-dotnet/issues/142
            def.IndexingPolicy.Automatic <- false
            def.IndexingPolicy.IndexingMode <- IndexingMode.None
            createOrProvisionCollection client (dbName,def) mode
        let init log (client: Client.DocumentClient) (dbName,collName) mode skipStoredProc = async {
            do! createOrProvisionDatabase client dbName mode
            do! createBatchAndTipCollectionIfNotExists client (dbName,collName) mode
            let collectionUri = Microsoft.Azure.Documents.Client.UriFactory.CreateDocumentCollectionUri(dbName,collName)
            if not skipStoredProc then
                do! createSyncStoredProcIfNotExists (Some log) client collectionUri }
        let initAux (client: Client.DocumentClient) (dbName,collName) rus = async {
            // Hardwired for now (not sure if CFP can store in a Database-allocated as it would need to be supplying partion keys)
            let mode = Provisioning.Container rus
            do! createOrProvisionDatabase client dbName mode 
            do! createAuxCollectionIfNotExists client (dbName,collName) mode }

module internal Tip =
    let private get (client: Client.DocumentClient) (stream: CollectionStream, maybePos: Position option) =
        let coll = DocDbCollection(client, stream.collectionUri)
        let ac = match maybePos with Some { etag=Some etag } -> Client.AccessCondition(Type=Client.AccessConditionType.IfNoneMatch, Condition=etag) | _ -> null
        let ro = Client.RequestOptions(PartitionKey=PartitionKey(stream.name), AccessCondition = ac)
        coll.TryReadDocument(Tip.WellKnownDocumentId, ro)
    let private loggedGet (get : CollectionStream * Position option -> Async<_>) (stream: CollectionStream, maybePos: Position option) (log: ILogger) = async {
        let log = log |> Log.prop "stream" stream.name
        let! t, (ru, res : ReadResult<Tip>) = get (stream,maybePos) |> Stopwatch.Time
        let log count bytes (f : Log.Measurement -> _) = log |> Log.event (f { stream = stream.name; interval = t; bytes = bytes; count = count; ru = ru })
        match res with
        | ReadResult.NotModified ->
            (log 0 0 Log.TipNotModified).Information("EqxCosmos {action:l} {res} {ms}ms rc={ru}", "Tip", 302, (let e = t.Elapsed in e.TotalMilliseconds), ru)
        | ReadResult.NotFound ->
            (log 0 0 Log.TipNotFound).Information("EqxCosmos {action:l} {res} {ms}ms rc={ru}", "Tip", 404, (let e = t.Elapsed in e.TotalMilliseconds), ru)
        | ReadResult.Found doc ->
            let log =
                let (Log.BatchLen bytes), count = Enum.Unfolds doc.u, doc.u.Length
                log bytes count Log.Tip
            let log = if (not << log.IsEnabled) Events.LogEventLevel.Debug then log else log |> Log.propDataUnfolds doc.u |> Log.prop "etag" doc._etag
            log.Information("EqxCosmos {action:l} {res} {ms}ms rc={ru}", "Tip", 200, (let e = t.Elapsed in e.TotalMilliseconds), ru)
        return ru, res }
    type [<RequireQualifiedAccess; NoComparison; NoEquality>] Result = NotModified | NotFound | Found of Position * IIndexedEvent[]
    /// `pos` being Some implies that the caller holds a cached value and hence is ready to deal with IndexResult.UnChanged
    let tryLoad (log : ILogger) retryPolicy client (stream: CollectionStream) (maybePos: Position option): Async<Result> = async {
        let get = get client
        let! _rc, res = Log.withLoggedRetries retryPolicy "readAttempt" (loggedGet get (stream,maybePos)) log
        match res with
        | ReadResult.NotModified -> return Result.NotModified
        | ReadResult.NotFound -> return Result.NotFound
        | ReadResult.Found doc -> return Result.Found (Position.fromTip doc, Enum.EventsAndUnfolds doc |> Array.ofSeq) }

 module internal Query =
    open Microsoft.Azure.Documents.Linq
    open FSharp.Control
    let private mkQuery (client : IDocumentClient) maxItems (stream: CollectionStream) (direction: Direction) startPos =
        let querySpec =
            let root = sprintf "SELECT c.id, c.i, c._etag, c.n, c.e FROM c WHERE c.id!=\"%s\"" Tip.WellKnownDocumentId
            let tail = sprintf "ORDER BY c.i %s" (if direction = Direction.Forward then "ASC" else "DESC")
            match startPos with
            | None -> SqlQuerySpec(sprintf "%s %s" root tail)
            | Some { index = positionSoExclusiveWhenBackward } ->
                let cond = if direction = Direction.Forward then "c.n > @startPos" else "c.i < @startPos"
                SqlQuerySpec(sprintf "%s AND %s %s" root cond tail, SqlParameterCollection [SqlParameter("@startPos", positionSoExclusiveWhenBackward)])
        let feedOptions = new Client.FeedOptions(PartitionKey=PartitionKey(stream.name), MaxItemCount=Nullable maxItems)
        client.CreateDocumentQuery<Batch>(stream.collectionUri, querySpec, feedOptions).AsDocumentQuery()

    // Unrolls the Batches in a response - note when reading backwards, the events are emitted in reverse order of index
    let private handleResponse direction (stream: CollectionStream) startPos (query: IDocumentQuery<Batch>) (log: ILogger)
        : Async<IIndexedEvent[] * Position option * float> = async {
        let! ct = Async.CancellationToken
        let! t, (res : Client.FeedResponse<Batch>) = query.ExecuteNextAsync<Batch>(ct) |> Async.AwaitTaskCorrect |> Stopwatch.Time
        let batches, ru = Array.ofSeq res, res.RequestCharge
        let events = batches |> Seq.collect (fun b -> Enum.Events(b, startPos, direction)) |> Array.ofSeq
        let (Log.BatchLen bytes), count = events, events.Length
        let reqMetric : Log.Measurement = { stream = stream.name; interval = t; bytes = bytes; count = count; ru = ru }
        let log = let evt = Log.Response (direction, reqMetric) in log |> Log.event evt
        let log = if (not << log.IsEnabled) Events.LogEventLevel.Debug then log else log |> Log.propEvents events
        let index = if count = 0 then Nullable () else Nullable <| Seq.min (seq { for x in batches -> x.i })
        (log |> (match startPos with Some pos -> Log.propStartPos pos | None -> id) |> Log.prop "bytes" bytes)
            .Information("EqxCosmos {action:l} {count}/{batches} {direction} {ms}ms i={index} rc={ru}",
                "Response", count, batches.Length, direction, (let e = t.Elapsed in e.TotalMilliseconds), index, ru)
        let maybePosition = batches |> Array.tryPick Position.tryFromBatch
        return events, maybePosition, ru }

    let private run (log : ILogger) (readSlice: IDocumentQuery<Batch> -> ILogger -> Async<IIndexedEvent[] * Position option * float>)
            (maxPermittedBatchReads: int option)
            (query: IDocumentQuery<Batch>)
        : AsyncSeq<IIndexedEvent[] * Position option * float> =
        let rec loop batchCount : AsyncSeq<IIndexedEvent[] * Position option * float> = asyncSeq {
            match maxPermittedBatchReads with
            | Some mpbr when batchCount >= mpbr -> log.Information "batch Limit exceeded"; invalidOp "batch Limit exceeded"
            | _ -> ()

            let batchLog = log |> Log.prop "batchIndex" batchCount
            let! (slice : IIndexedEvent[] * Position option * float) = readSlice query batchLog
            yield slice
            if query.HasMoreResults then
                yield! loop (batchCount + 1) }
        loop 0

    let private logQuery direction batchSize streamName interval (responsesCount, events : IIndexedEvent[]) n (ru: float) (log : ILogger) =
        let (Log.BatchLen bytes), count = events, events.Length
        let reqMetric : Log.Measurement = { stream = streamName; interval = interval; bytes = bytes; count = count; ru = ru }
        let evt = Log.Event.Query (direction, responsesCount, reqMetric)
        let action = match direction with Direction.Forward -> "QueryF" | Direction.Backward -> "QueryB"
        (log |> Log.prop "bytes" bytes |> Log.prop "batchSize" batchSize |> Log.event evt).Information(
            "EqxCosmos {action:l} {stream} v{n} {count}/{responses} {ms}ms rc={ru}",
            action, streamName, n, count, responsesCount, (let e = interval.Elapsed in e.TotalMilliseconds), ru)

    let private calculateUsedVersusDroppedPayload stopIndex (xs: IIndexedEvent[]) : int * int =
        let mutable used, dropped = 0, 0
        let mutable found = false
        for x in xs do
            let (Log.EventLen bytes) = x
            if found then dropped <- dropped + bytes
            else used <- used + bytes
            if x.Index = stopIndex then found <- true
        used, dropped

    let walk<'event> (log : ILogger) client retryPolicy maxItems maxRequests direction (stream: CollectionStream) startPos
        (tryDecode : IIndexedEvent -> 'event option, isOrigin: 'event -> bool)
        : Async<Position * 'event[]> = async {
        let responseCount = ref 0
        let mergeBatches (log : ILogger) (batchesBackward: AsyncSeq<IIndexedEvent[] * Position option * float>) = async {
            let mutable lastResponse, mapbeTipPos, ru = None, None, 0.
            let! events =
                batchesBackward
                |> AsyncSeq.map (fun (events, maybePos, r) ->
                    if mapbeTipPos = None then mapbeTipPos <- maybePos
                    lastResponse <- Some events; ru <- ru + r
                    incr responseCount
                    events |> Array.map (fun x -> x, tryDecode x))
                |> AsyncSeq.concatSeq
                |> AsyncSeq.takeWhileInclusive (function
                    | x, Some e when isOrigin e ->
                        match lastResponse with
                        | None -> log.Information("EqxCosmos Stop stream={stream} at={index} {case}", stream.name, x.Index, x.EventType)
                        | Some batch ->
                            let used, residual = batch |> calculateUsedVersusDroppedPayload x.Index
                            log.Information("EqxCosmos Stop stream={stream} at={index} {case} used={used} residual={residual}",
                                stream.name, x.Index, x.EventType, used, residual)
                        false
                    | _ -> true) (*continue the search*)
                |> AsyncSeq.toArrayAsync
            return events, mapbeTipPos, ru }
        use query = mkQuery client maxItems stream direction startPos
        let pullSlice = handleResponse direction stream startPos
        let retryingLoggingReadSlice query = Log.withLoggedRetries retryPolicy "readAttempt" (pullSlice query)
        let log = log |> Log.prop "batchSize" maxItems |> Log.prop "stream" stream.name
        let readlog = log |> Log.prop "direction" direction
        let batches : AsyncSeq<IIndexedEvent[] * Position option * float> = run readlog retryingLoggingReadSlice maxRequests query
        let! t, (events, maybeTipPos, ru) = mergeBatches log batches |> Stopwatch.Time
        query.Dispose()
        let raws, decoded = (Array.map fst events), (events |> Seq.choose snd |> Array.ofSeq)
        let pos = match maybeTipPos with Some p -> p | None -> Position.fromMaxIndex raws

        log |> logQuery direction maxItems stream.name t (!responseCount,raws) pos.index ru
        return pos, decoded }

    let walkLazy<'event> (log : ILogger) client retryPolicy maxItems maxRequests direction (stream: CollectionStream) startPos
        (tryDecode : IIndexedEvent -> 'event option, isOrigin: 'event -> bool)
        : AsyncSeq<'event[]> = asyncSeq {
        let responseCount = ref 0
        use query = mkQuery client maxItems stream direction startPos
        let pullSlice = handleResponse direction stream startPos
        let retryingLoggingReadSlice query = Log.withLoggedRetries retryPolicy "readAttempt" (pullSlice query)
        let log = log |> Log.prop "batchSize" maxItems |> Log.prop "stream" stream.name
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
                            stream.name, x.Index, x.EventType, used, residual)
                        ok <- false
                        acc.Add e
                    | Some e -> acc.Add e
                    | None -> ()
                yield acc.ToArray()
                ok <- ok && query.HasMoreResults
        finally
            let endTicks = System.Diagnostics.Stopwatch.GetTimestamp()
            let t = StopwatchInterval(startTicks, endTicks)

            query.Dispose()
            log |> logQuery direction maxItems stream.name t (!responseCount,allSlices.ToArray()) -1L ru }

type [<NoComparison>] Token = { stream: CollectionStream; pos: Position }
module Token =
    let create stream pos : Equinox.Store.StreamToken = { value = box { stream = stream; pos = pos } }
    let (|Unpack|) (token: Equinox.Store.StreamToken) : CollectionStream*Position = let t = unbox<Token> token.value in t.stream,t.pos
    let supersedes (Unpack (_,currentPos)) (Unpack (_,xPos)) =
        let currentVersion, newVersion = currentPos.index, xPos.index
        let currentETag, newETag = currentPos.etag, xPos.etag
        newVersion > currentVersion || currentETag <> newETag

[<AutoOpen>]
module Internal =
    [<RequireQualifiedAccess; NoComparison; NoEquality>]
    type InternalSyncResult = Written of StreamToken | ConflictUnknown of StreamToken | Conflict of StreamToken * IIndexedEvent[]

    [<RequireQualifiedAccess; NoComparison; NoEquality>]
    type LoadFromTokenResult<'event> = Unchanged | Found of Equinox.Store.StreamToken * 'event[]

namespace Equinox.Cosmos

open Equinox
open Equinox.Cosmos.Store
open Equinox.Store.Infrastructure
open FSharp.Control
open Serilog
open System
open System.Collections.Concurrent

/// Defines policies for retrying with respect to transient failures calling CosmosDb (as opposed to application level concurrency conflicts)
type CosmosConnection(client: Microsoft.Azure.Documents.Client.DocumentClient, [<O; D(null)>]?readRetryPolicy: IRetryPolicy, [<O; D(null)>]?writeRetryPolicy) =
    member __.Client = client
    member __.TipRetryPolicy = readRetryPolicy
    member __.QueryRetryPolicy = readRetryPolicy
    member __.WriteRetryPolicy = writeRetryPolicy

/// Defines the policies in force regarding how to a) split up calls b) limit the number of events per slice
type CosmosBatchingPolicy
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

type CosmosGateway(conn : CosmosConnection, batching : CosmosBatchingPolicy) =
    let (|FromUnfold|_|) (tryDecode: #Equinox.Codec.IEvent<_> -> 'event option) (isOrigin: 'event -> bool) (xs:#Equinox.Codec.IEvent<_>[]) : Option<'event[]> =
        match Array.tryFindIndexBack (tryDecode >> Option.exists isOrigin) xs with
        | None -> None
        | Some index -> xs |> Seq.skip index |> Seq.choose tryDecode |> Array.ofSeq |> Some
    member __.LoadBackwardsStopping log stream (tryDecode,isOrigin): Async<Store.StreamToken * 'event[]> = async {
        let! pos, events = Query.walk log conn.Client conn.QueryRetryPolicy batching.MaxItems batching.MaxRequests Direction.Backward stream None (tryDecode,isOrigin)
        Array.Reverse events
        return Token.create stream pos, events }
    member __.Read log stream direction startPos (tryDecode,isOrigin) : Async<Store.StreamToken * 'event[]> = async {
        let! pos, events = Query.walk log conn.Client conn.QueryRetryPolicy batching.MaxItems batching.MaxRequests direction stream startPos (tryDecode,isOrigin)
        return Token.create stream pos, events }
    member __.ReadLazy (batching: CosmosBatchingPolicy) log stream direction startPos (tryDecode,isOrigin) : AsyncSeq<'event[]> =
        Query.walkLazy log conn.Client conn.QueryRetryPolicy batching.MaxItems batching.MaxRequests direction stream startPos (tryDecode,isOrigin)
    member __.LoadFromUnfoldsOrRollingSnapshots log (stream,maybePos) (tryDecode,isOrigin): Async<Store.StreamToken * 'event[]> = async {
        let! res = Tip.tryLoad log conn.TipRetryPolicy conn.Client stream maybePos
        match res with
        | Tip.Result.NotFound -> return Token.create stream Position.fromKnownEmpty, Array.empty
        | Tip.Result.NotModified -> return invalidOp "Not handled"
        | Tip.Result.Found (pos, FromUnfold tryDecode isOrigin span) -> return Token.create stream pos, span
        | _ -> return! __.LoadBackwardsStopping log stream (tryDecode,isOrigin) }
    member __.GetPosition(log, stream, ?pos): Async<Store.StreamToken> = async {
        let! res = Tip.tryLoad log conn.TipRetryPolicy conn.Client stream pos
        match res with
        | Tip.Result.NotFound -> return Token.create stream Position.fromKnownEmpty
        | Tip.Result.NotModified -> return Token.create stream pos.Value
        | Tip.Result.Found (pos, _unfoldsAndEvents) -> return Token.create stream pos }
    member __.LoadFromToken(log, (stream,pos), (tryDecode, isOrigin)): Async<LoadFromTokenResult<'event>> = async {
        let! res = Tip.tryLoad log conn.TipRetryPolicy conn.Client stream (Some pos)
        match res with
        | Tip.Result.NotFound -> return LoadFromTokenResult.Found (Token.create stream Position.fromKnownEmpty,Array.empty)
        | Tip.Result.NotModified -> return LoadFromTokenResult.Unchanged
        | Tip.Result.Found (pos, FromUnfold tryDecode isOrigin span) -> return LoadFromTokenResult.Found (Token.create stream pos, span)
        | _ ->  let! res = __.Read log stream Direction.Forward (Some pos) (tryDecode,isOrigin)
                return LoadFromTokenResult.Found res }
    member __.CreateSyncStoredProcIfNotExists log = 
        Sync.Initialization.createSyncStoredProcIfNotExists log conn.Client
    member __.Sync log stream (expectedVersion, batch: Tip): Async<InternalSyncResult> = async {
        if Array.isEmpty batch.e then invalidArg "batch" "Cannot write empty events batch."
        let! wr = Sync.batch log conn.WriteRetryPolicy conn.Client stream (expectedVersion,batch,batching.MaxItems)
        match wr with
        | Sync.Result.Conflict (pos',events) -> return InternalSyncResult.Conflict (Token.create stream pos',events)
        | Sync.Result.ConflictUnknown pos' -> return InternalSyncResult.ConflictUnknown (Token.create stream pos')
        | Sync.Result.Written pos' -> return InternalSyncResult.Written (Token.create stream pos') }

type private Category<'event, 'state>(gateway : CosmosGateway, codec : Codec.IUnionEncoder<'event, byte[]>) =
    let (|TryDecodeFold|) (fold: 'state -> 'event seq -> 'state) initial (events: IIndexedEvent seq) : 'state = Seq.choose codec.TryDecode events |> fold initial
    member __.Load includeUnfolds collectionStream fold initial isOrigin (log : ILogger): Async<Store.StreamToken * 'state> = async {
        let! token, events =
            if not includeUnfolds then gateway.LoadBackwardsStopping log collectionStream (codec.TryDecode,isOrigin)
            else gateway.LoadFromUnfoldsOrRollingSnapshots log (collectionStream,None) (codec.TryDecode,isOrigin)
        return token, fold initial events }
    member __.LoadFromToken (Token.Unpack streamPos, state: 'state as current) fold isOrigin (log : ILogger): Async<Store.StreamToken * 'state> = async {
        let! res = gateway.LoadFromToken(log, streamPos, (codec.TryDecode,isOrigin))
        match res with
        | LoadFromTokenResult.Unchanged -> return current
        | LoadFromTokenResult.Found (token', events') -> return token', fold state events' }
    member __.Sync(Token.Unpack (stream,pos), state as current, expectedVersion, events, unfold, fold, isOrigin, log): Async<Store.SyncResult<'state>> = async {
        let state' = fold state (Seq.ofList events)
        let eventsEncoded, projectionsEncoded = Seq.map codec.Encode events |> Array.ofSeq, Seq.map codec.Encode (unfold state' events)
        let baseIndex = pos.index + int64 (List.length events)
        let projections = Sync.mkUnfold baseIndex projectionsEncoded
        let batch = Sync.mkBatch stream eventsEncoded projections
        let! res = gateway.Sync log stream (expectedVersion,batch)
        match res with
        | InternalSyncResult.Conflict (token',TryDecodeFold fold state events') -> return Store.SyncResult.Conflict (async { return token', events' })
        | InternalSyncResult.ConflictUnknown _token' -> return Store.SyncResult.Conflict (__.LoadFromToken current fold isOrigin log)
        | InternalSyncResult.Written token' -> return Store.SyncResult.Written (token', state') }

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
    type CategoryTee<'event, 'state>(inner: Store.ICategory<'event, 'state, CollectionStream>, tee : string -> Store.StreamToken * 'state -> unit) =
        let intercept streamName tokenAndState =
            tee streamName tokenAndState
            tokenAndState
        let interceptAsync load streamName = async {
            let! tokenAndState = load
            return intercept streamName tokenAndState }
        interface Store.ICategory<'event, 'state, CollectionStream> with
            member __.Load stream (log : ILogger) : Async<Store.StreamToken * 'state> =
                interceptAsync (inner.Load stream log) stream.name
            member __.TrySync (log : ILogger) (Token.Unpack (stream,_) as streamToken,state) (events : 'event list)
                : Async<Store.SyncResult<'state>> = async {
                let! syncRes = inner.TrySync log (streamToken, state) events
                match syncRes with
                | Store.SyncResult.Conflict resync ->         return Store.SyncResult.Conflict (interceptAsync resync stream.name)
                | Store.SyncResult.Written (token', state') ->return Store.SyncResult.Written (intercept stream.name (token', state')) }

    let applyCacheUpdatesWithSlidingExpiration
            (cache: Cache)
            (prefix: string)
            (slidingExpiration : TimeSpan)
            (category: Store.ICategory<'event, 'state, CollectionStream>)
            : Store.ICategory<'event, 'state, CollectionStream> =
        let policy = new CacheItemPolicy(SlidingExpiration = slidingExpiration)
        let addOrUpdateSlidingExpirationCacheEntry streamName = CacheEntry >> cache.UpdateIfNewer policy (prefix + streamName)
        CategoryTee<'event,'state>(category, addOrUpdateSlidingExpirationCacheEntry) :> _

type private Folder<'event, 'state>
    (   category: Category<'event, 'state>, fold: 'state -> 'event seq -> 'state, initial: 'state,
        isOrigin: 'event -> bool,
        // Whether or not an `unfold` function is supplied controls whether reads do a point read before querying
        ?unfold: ('state -> 'event list -> 'event seq),
        ?readCache) =
    interface Store.ICategory<'event, 'state, CollectionStream> with
        member __.Load collStream (log : ILogger): Async<Store.StreamToken * 'state> =
            let batched = category.Load (Option.isSome unfold) collStream fold initial isOrigin log
            let cached tokenAndState = category.LoadFromToken tokenAndState fold isOrigin log
            match readCache with
            | None -> batched
            | Some (cache : Caching.Cache, prefix : string) ->
                match cache.TryGet(prefix + collStream.name) with
                | None -> batched
                | Some tokenAndState -> cached tokenAndState
        member __.TrySync (log : ILogger) (Token.Unpack (_stream,pos) as streamToken,state) (events : 'event list)
            : Async<Store.SyncResult<'state>> = async {
            let! res = category.Sync((streamToken,state), Some pos.index, events, (defaultArg unfold (fun _ _ -> Seq.empty)), fold, isOrigin, log)
            match res with
            | Store.SyncResult.Conflict resync ->             return Store.SyncResult.Conflict resync
            | Store.SyncResult.Written (token',state') ->     return Store.SyncResult.Written (token',state') }

/// Holds Database/Collection pair, coordinating initialization activities
type private CosmosCollection(databaseId, collectionId, ?initCollection : Uri -> Async<unit>) =
    let collectionUri = Microsoft.Azure.Documents.Client.UriFactory.CreateDocumentCollectionUri(databaseId, collectionId)
    let initGuard = initCollection |> Option.map (fun init -> AsyncCacheCell<unit>(init collectionUri))

    member __.CollectionUri = collectionUri
    member internal __.InitializationGate = match initGuard with Some g when g.PeekIsValid() |> not -> Some g.AwaitValue | _ -> None

/// Defines a process for mapping from a Stream Name to the appropriate storage area, allowing control over segregation / co-locating of data
type CosmosCollections(categoryAndIdToDatabaseCollectionAndStream : string -> string -> string*string*string, [<O; D(null)>]?disableInitialization) =
    // Index of database*collection -> Initialization Context
    let collections = ConcurrentDictionary<string*string, CosmosCollection>()
    new (databaseId, collectionId) =
        // TOCONSIDER - this works to support the Core.Events APIs
        let genStreamName categoryName streamId = if categoryName = null then streamId else sprintf "%s-%s" categoryName streamId
        CosmosCollections(fun categoryName streamId -> databaseId, collectionId, genStreamName categoryName streamId)

    member internal __.Resolve(categoryName, id, init) : CollectionStream * (unit -> Async<unit>) option =
        let databaseId, collectionId, streamName = categoryAndIdToDatabaseCollectionAndStream categoryName id
        let init = match disableInitialization with Some true -> None | _ -> Some init

        let coll = collections.GetOrAdd((databaseId,collectionId), fun (db,coll) -> CosmosCollection(db, coll, ?initCollection = init))
        { collectionUri = coll.CollectionUri; name = streamName },coll.InitializationGate

/// Pairs a Gateway, defining the retry policies for CosmosDb with a Collections map defining mappings from (category,id) to (database,collection,streamName)
type CosmosStore(gateway: CosmosGateway, collections: CosmosCollections, [<O; D(null)>] ?log) =
    let init = gateway.CreateSyncStoredProcIfNotExists log
    new(gateway: CosmosGateway, databaseId: string, collectionId: string, [<O; D(null)>]?log) =
        CosmosStore(gateway, CosmosCollections(databaseId, collectionId), ?log = log)

    member __.Gateway = gateway
    member __.Collections = collections
    member internal __.ResolveCollStream(categoryName, id) : CollectionStream * (unit -> Async<unit>) option =
        collections.Resolve(categoryName, id, init)

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
    | SlidingWindow of Caching.Cache * window: TimeSpan

[<NoComparison; NoEquality; RequireQualifiedAccess>]
type AccessStrategy<'event,'state> =
    /// Allow events that pass the `isOrigin` test to be used in lieu of folding all the events from the start of the stream
    /// When saving, `unfold` the 'state, saving in the Tip
    | Unfolded of isOrigin: ('event -> bool) * unfold: ('state -> 'event seq)
    /// Simplified version of projection that only has a single Projection Event Type
    /// Provides equivalent performance to Projections, just simplified function signatures
    | Snapshot of isValid: ('event -> bool) * generate: ('state -> 'event)
    /// Trust every event type as being an origin
    | AnyKnownEventType

type CosmosResolver<'event, 'state>(store : CosmosStore, codec, fold, initial, caching, [<O; D(null)>]?access) =
    let readCacheOption =
        match caching with
        | CachingStrategy.NoCaching -> None
        | CachingStrategy.SlidingWindow(cache, _) -> Some(cache, null)
    let isOrigin, projectOption =
        match access with
        | None -> (fun _ -> false), None
        | Some (AccessStrategy.Unfolded (isOrigin, unfold)) -> isOrigin, Some (fun state _events -> unfold state)
        | Some (AccessStrategy.Snapshot (isValid,generate)) -> isValid, Some (fun state _events -> seq [generate state])
        | Some (AccessStrategy.AnyKnownEventType) ->           (fun _ -> true), Some (fun _ events -> Seq.last events |> Seq.singleton)
    let cosmosCat = Category<'event, 'state>(store.Gateway, codec)
    let folder = Folder<'event, 'state>(cosmosCat, fold, initial, isOrigin, ?unfold=projectOption, ?readCache = readCacheOption)
    let category : Store.ICategory<_,_,CollectionStream> =
        match caching with
        | CachingStrategy.NoCaching -> folder :> _
        | CachingStrategy.SlidingWindow(cache, window) ->
            Caching.applyCacheUpdatesWithSlidingExpiration cache null window folder

    let resolveStream (streamId, maybeCollectionInitializationGate) =
        { new Store.IStream<'event, 'state> with
            member __.Load log = category.Load streamId log
            member __.TrySync (log: ILogger) (token: Store.StreamToken, originState: 'state) (events: 'event list) =
                match maybeCollectionInitializationGate with
                | None -> category.TrySync log (token, originState) events
                | Some init -> async {
                    do! init ()
                    return! category.TrySync log (token, originState) events } }

    member __.Resolve = function
        | Target.AggregateId (categoryName,streamId) ->
            store.ResolveCollStream(categoryName, streamId) |> resolveStream
        | Target.AggregateIdEmpty (categoryName,streamId) ->
            let collStream, maybeInit = store.ResolveCollStream(categoryName, streamId)
            Store.Stream.ofMemento (Token.create collStream Position.fromKnownEmpty,initial) (resolveStream (collStream, maybeInit))
        | Target.DeprecatedRawName _ as x -> failwithf "Stream name not supported: %A" x

    member __.FromMemento(Token.Unpack (stream,_pos) as streamToken,state) =
        let skipInitialization = None
        Store.Stream.ofMemento (streamToken,state) (resolveStream (stream,skipInitialization))

[<RequireQualifiedAccess; NoComparison>]
type Discovery =
    | UriAndKey of databaseUri:Uri * key:string
    /// Implements connection string parsing logic curiously missing from the DocDb SDK
    static member FromConnectionString (connectionString: string) =
        match connectionString with
        | _ when String.IsNullOrWhiteSpace connectionString -> nullArg "connectionString"
        | Regex.Match "^\s*AccountEndpoint\s*=\s*([^;\s]+)\s*;\s*AccountKey\s*=\s*([^;\s]+)\s*;?\s*$" m ->
            let uri = m.Groups.[1].Value
            let key = m.Groups.[2].Value
            UriAndKey (Uri uri, key)
        | _ -> invalidArg "connectionString" "unrecognized connection string format; must be `AccountEndpoint=https://...;AccountKey=...=;`"

type ConnectionMode =
    /// Default mode, uses Https - inefficient as uses a double hop
    | Gateway
    /// Most efficient, but requires direct connectivity
    | DirectTcp
    // More efficient than Gateway, but suboptimal
    | DirectHttps

open Microsoft.Azure.Documents
type CosmosConnector
    (   /// Timeout to apply to individual reads/write roundtrips going to CosmosDb
        requestTimeout: TimeSpan,
        /// Maximum number of times attempt when failure reason is a 429 from CosmosDb, signifying RU limits have been breached
        maxRetryAttemptsOnThrottledRequests: int,
        /// Maximum number of seconds to wait (especially if a higher wait delay is suggested by CosmosDb in the 429 response)
        maxRetryWaitTimeInSeconds: int,
        /// Log to emit connection messages to
        log : ILogger,
        /// Connection limit (default 1000)
        [<O; D(null)>]?maxConnectionLimit,
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
        [<O; D(null)>]?tags : (string*string) seq) =
    do if log = null then nullArg "log"

    let connPolicy =
        let cp = Client.ConnectionPolicy.Default
        match mode with
        | None | Some Gateway -> cp.ConnectionMode <- Client.ConnectionMode.Gateway // default; only supports Https
        | Some DirectHttps -> cp.ConnectionMode <- Client.ConnectionMode.Direct; cp.ConnectionProtocol <- Client.Protocol.Https // Https is default when using Direct
        | Some DirectTcp -> cp.ConnectionMode <- Client.ConnectionMode.Direct; cp.ConnectionProtocol <- Client.Protocol.Tcp
        cp.RetryOptions <-
            Client.RetryOptions(
                MaxRetryAttemptsOnThrottledRequests = maxRetryAttemptsOnThrottledRequests,
                MaxRetryWaitTimeInSeconds = maxRetryWaitTimeInSeconds)
        cp.RequestTimeout <- requestTimeout
        cp.MaxConnectionLimit <- defaultArg maxConnectionLimit 1000
        cp

    /// Yields an IDocumentClient configured and Connect()ed to a given DocDB collection per the requested `discovery` strategy
    let connect
        (   /// Name should be sufficient to uniquely identify this connection within a single app instance's logs
            name,
            discovery : Discovery) : Async<Client.DocumentClient> =
        let connect (uri: Uri, key: string) = async {
            let name = String.concat ";" <| seq {
                yield name
                match tags with None -> () | Some tags -> for key, value in tags do yield sprintf "%s=%s" key value }
            let sanitizedName = name.Replace('\'','_').Replace(':','_') // sic; Align with logging for ES Adapter
            let client = new Client.DocumentClient(uri, key, connPolicy, Nullable(defaultArg defaultConsistencyLevel ConsistencyLevel.Session))
            log.ForContext("Uri", uri).Information("CosmosDb Connection Name {connectionName}", sanitizedName)
            do! client.OpenAsync() |> Async.AwaitTaskCorrect
            return client }

        match discovery with Discovery.UriAndKey(databaseUri=uri; key=key) -> connect (uri,key)

    /// Connection policy (for ChangeFeed)
    member __.ConnectionPolicy : Client.ConnectionPolicy = connPolicy

    /// Yields a DocDbConnection configured per the specified strategy
    member __.Connect(name, discovery : Discovery) : Async<CosmosConnection> = async {
        let! conn = connect(name, discovery)
        return CosmosConnection(conn, ?readRetryPolicy=readRetryPolicy, ?writeRetryPolicy=writeRetryPolicy) }

namespace Equinox.Cosmos.Core

open Equinox.Cosmos
open Equinox.Cosmos.Store
open FSharp.Control
open Equinox.Codec // must shadow Control.IEvent
open System.Runtime.InteropServices

/// Outcome of appending events, specifying the new and/or conflicting events, together with the updated Target write position
[<RequireQualifiedAccess; NoComparison>]
type AppendResult<'t> =
    | Ok of pos: 't
    | Conflict of index: 't * conflictingEvents: IIndexedEvent[]
    | ConflictUnknown of index: 't

/// Encapsulates the core facilites Equinox.Cosmos offers for operating directly on Events in Streams.
type CosmosContext
    (   /// Connection to CosmosDb with DocumentDb Transient Read and Write Retry policies
        conn : CosmosConnection,
        /// Database + Collection selector
        collections: CosmosCollections,
        /// Logger to write to - see https://github.com/serilog/serilog/wiki/Provided-Sinks for how to wire to your logger
        log : Serilog.ILogger,
        /// Optional maximum number of Store.Batch records to retrieve as a set (how many Events are placed therein is controlled by average batch size when appending events
        /// Defaults to 10
        [<Optional; DefaultParameterValue(null)>]?defaultMaxItems,
        /// Alternate way of specifying defaultMaxItems which facilitates reading it from a cached dynamic configuration
        [<Optional; DefaultParameterValue(null)>]?getDefaultMaxItems) =
    do if log = null then nullArg "log"
    let getDefaultMaxItems = match getDefaultMaxItems with Some f -> f | None -> fun () -> defaultArg defaultMaxItems 10
    let batching = CosmosBatchingPolicy(getDefaultMaxItems=getDefaultMaxItems)
    let gateway = CosmosGateway(conn, batching)

    let maxCountPredicate count =
        let acc = ref (max (count-1) 0)
        fun _ ->
            if !acc = 0 then true else
            decr acc
            false

    let yieldPositionAndData res = async {
        let! (Token.Unpack (_,pos')), data = res
        return pos', data }

    member __.ResolveStream(streamName) = collections.Resolve(null, streamName, gateway.CreateSyncStoredProcIfNotExists (Some log))
    member __.CreateStream(streamName) = __.ResolveStream streamName |> fst

    member internal __.GetLazy((stream, startPos), ?batchSize, ?direction) : AsyncSeq<IIndexedEvent[]> =
        let direction = defaultArg direction Direction.Forward
        let batching = CosmosBatchingPolicy(defaultArg batchSize batching.MaxItems)
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

    /// Establishes the current position of the stream in as effficient a manner as possible
    /// (The ideal situation is that the preceding token is supplied as input in order to avail of 1RU low latency state checks)
    member __.Sync(stream, ?position: Position) : Async<Position> = async {
        //let indexed predicate = load fold initial (coll.Gateway.IndexedOrBatched log predicate (stream,None))
        let! (Token.Unpack (_,pos')) = gateway.GetPosition(log, stream, ?pos=position)
        return pos' }

    /// Reads in batches of `batchSize` from the specified `Position`, allowing the reader to efficiently walk away from a running query
    /// ... NB as long as they Dispose!
    member __.Walk(stream, batchSize, ?position, ?direction) : AsyncSeq<IIndexedEvent[]> =
        __.GetLazy((stream, position), batchSize, ?direction=direction)

    /// Reads all Events from a `Position` in a given `direction`
    member __.Read(stream, ?position, ?maxCount, ?direction) : Async<Position*IIndexedEvent[]> =
        __.GetInternal((stream, position), ?maxCount=maxCount, ?direction=direction) |> yieldPositionAndData

    /// Appends the supplied batch of events, subject to a consistency check based on the `position`
    /// Callers should implement appropriate idempotent handling, or use Equinox.Stream for that purpose
    member __.Sync(stream : CollectionStream, position, events: IEvent<_>[]) : Async<AppendResult<Position>> = async {
        // Writes go through the stored proc, which we need to provision per-collection
        // Having to do this here in this way is far from ideal, but work on caching, external snapshots and caching is likely
        //   to move this about before we reach a final destination in any case
        match __.ResolveStream stream.name |> snd with
        | None -> ()
        | Some init -> do! init ()
        let batch = Sync.mkBatch stream events Seq.empty
        let! res = gateway.Sync log stream (Some position.index,batch)
        match res with
        | InternalSyncResult.Written (Token.Unpack (_,pos)) -> return AppendResult.Ok pos
        | InternalSyncResult.Conflict (Token.Unpack (_,pos),events) -> return AppendResult.Conflict (pos, events)
        | InternalSyncResult.ConflictUnknown (Token.Unpack (_,pos)) -> return AppendResult.ConflictUnknown pos }

    /// Low level, non-idempotent call appending events to a stream without a concurrency control mechanism in play
    /// NB Should be used sparingly; Equinox.Stream enables building equivalent equivalent idempotent handling with minimal code.
    member __.NonIdempotentAppend(stream, events: IEvent<_>[]) : Async<Position> = async {
        let! res = __.Sync(stream, Position.fromAppendAtEnd, events)
        match res with
        | AppendResult.Ok token -> return token
        | x -> return x |> sprintf "Conflict despite it being disabled %A" |> invalidOp }

/// Provides mechanisms for building `EventData` records to be supplied to the `Events` API
type EventData() =
    /// Creates an Event record, suitable for supplying to Append et al
    static member FromUtf8Bytes(eventType, data, ?meta) : IEvent<_> = Core.EventData.Create(eventType, data, ?meta=meta) :> _

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
    let private dropPosition (f: Async<Position*IIndexedEvent[]>): Async<IIndexedEvent[]> = async {
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
    let getAll (ctx: CosmosContext) (streamName: string) (MinPosition index: int64) (batchSize: int): AsyncSeq<IIndexedEvent[]> =
        ctx.Walk(ctx.CreateStream streamName, batchSize, ?position=index)

    /// Returns an async array of events in the stream starting at the specified sequence number,
    /// number of events to read is specified by batchSize
    /// Returns an empty sequence if the stream is empty or if the sequence number is larger than the largest
    /// sequence number in the stream.
    let get (ctx: CosmosContext) (streamName: string) (MinPosition index: int64) (maxCount: int): Async<IIndexedEvent[]> =
        ctx.Read(ctx.CreateStream streamName, ?position=index, maxCount=maxCount) |> dropPosition

    /// Appends a batch of events to a stream at the specified expected sequence number.
    /// If the specified expected sequence number does not match the stream, the events are not appended
    /// and a failure is returned.
    let append (ctx: CosmosContext) (streamName: string) (index: int64) (events: IEvent<_>[]): Async<AppendResult<int64>> =
        ctx.Sync(ctx.CreateStream streamName, Position.fromI index, events) |> stripSyncResult

    /// Appends a batch of events to a stream at the the present Position without any conflict checks.
    /// NB typically, it is recommended to ensure idempotency of operations by using the `append` and related API as
    /// this facilitates ensuring consistency is maintained, and yields reduced latency and Request Charges impacts
    /// (See equivalent APIs on `Context` that yield `Position` values)
    let appendAtEnd (ctx: CosmosContext) (streamName: string) (events: IEvent<_>[]): Async<int64> =
        ctx.NonIdempotentAppend(ctx.CreateStream streamName, events) |> stripPosition

    /// Returns an async sequence of events in the stream backwards starting from the specified sequence number,
    /// reading in batches of the specified size.
    /// Returns an empty sequence if the stream is empty or if the sequence number is smaller than the smallest
    /// sequence number in the stream.
    let getAllBackwards (ctx: CosmosContext) (streamName: string) (MaxPosition index: int64) (batchSize: int): AsyncSeq<IIndexedEvent[]> =
        ctx.Walk(ctx.CreateStream streamName, batchSize, ?position=index, direction=Direction.Backward)

    /// Returns an async array of events in the stream backwards starting from the specified sequence number,
    /// number of events to read is specified by batchSize
    /// Returns an empty sequence if the stream is empty or if the sequence number is smaller than the smallest
    /// sequence number in the stream.
    let getBackwards (ctx: CosmosContext) (streamName: string) (MaxPosition index: int64) (maxCount: int): Async<IIndexedEvent[]> =
        ctx.Read(ctx.CreateStream streamName, ?position=index, maxCount=maxCount, direction=Direction.Backward) |> dropPosition

    /// Obtains the `index` from the current write Position
    let getNextIndex (ctx: CosmosContext) (streamName: string) : Async<int64> =
        ctx.Sync(ctx.CreateStream streamName) |> stripPosition