namespace Equinox.Cosmos.Events

/// Common form for either a Domain Event or an Unfolded Event
type IEvent =
    /// The Event Type, used to drive deserialization
    abstract member EventType : string
    /// Event body, as UTF-8 encoded json ready to be injected into the Json being rendered for DocDb
    abstract member Data : byte[]
    /// Optional metadata (null, or same as d, not written if missing)
    abstract member Meta : byte[]

/// Represents a Domain Event or Unfold, together with it's Index in the event sequence
type IIndexedEvent =
    inherit IEvent
    /// The index into the event sequence of this event
    abstract member Index : int64
    /// Indicates whether this is a Domain Event or an Unfolded Event based on the state inferred from the events up to `Index`
    abstract member IsUnfold: bool

/// Position and Etag to which an operation is relative
type [<NoComparison>] Position = { index: int64; etag: string option } with
    /// If we have strong reason to suspect a stream is empty, we won't have an etag (and Writer Stored Procedure special cases this)
    static member internal FromKnownEmpty = Position.FromI 0L
    /// NB very inefficient compared to FromDocument or using one already returned to you
    static member internal FromI(i: int64) = { index = i; etag = None }
    /// Just Do It mode
    static member internal FromAppendAtEnd = Position.FromI -1L // sic - needs to yield -1
    /// NB very inefficient compared to FromDocument or using one already returned to you
    static member internal FromMaxIndex(xs: IIndexedEvent[]) =
        if Array.isEmpty xs then Position.FromKnownEmpty
        else Position.FromI (1L + Seq.max (seq { for x in xs -> x.Index }))

namespace Equinox.Cosmos.Store

open Equinox.Cosmos.Events
open Newtonsoft.Json

/// A 'normal' (frozen, not Tip) Batch of Events (without any Unfolds)
type [<NoEquality; NoComparison; JsonObject(ItemRequired=Required.Always)>]
    Event =
    {   /// DocDb-mandated Partition Key, must be maintained within the document
        /// Not actually required if running in single partition mode, but for simplicity, we always write it
        p: string // "{streamName}"

        /// DocDb-mandated unique row key; needs to be unique within any partition it is maintained; must be string
        /// At the present time, one can't perform an ORDER BY on this field, hence we also have i shadowing it
        /// NB WipBatch uses a well known value here while it's actively 'open'
        id: string // "{index}"

        /// When we read, we need to capture the value so we can retain it for caching purposes
        /// NB this is not relevant to fill in when we pass it to the writing stored procedure
        /// as it will do: 1. read 2. merge 3. write merged version contingent on the _etag not having changed
        [<JsonProperty(DefaultValueHandling=DefaultValueHandling.Ignore, Required=Required.Default)>]
        _etag: string

        /// Same as `id`; necessitated by fact that it's not presently possible to do an ORDER BY on the row key
        i: int64 // {index}

        /// Creation datetime (as opposed to system-defined _lastUpdated which is touched by triggers, replication etc.)
        c: System.DateTimeOffset // ISO 8601

        /// The Case (Event Type), used to drive deserialization
        t: string // required

        /// Event body, as UTF-8 encoded json ready to be injected into the Json being rendered for DocDb
        [<JsonConverter(typeof<Equinox.Cosmos.Internal.Json.VerbatimUtf8JsonConverter>)>]
        d: byte[] // required

        /// Optional metadata, as UTF-8 encoded json, ready to emit directly (null, not written if missing)
        [<JsonConverter(typeof<Equinox.Cosmos.Internal.Json.VerbatimUtf8JsonConverter>)>]
        [<JsonProperty(Required=Required.Default, NullValueHandling=NullValueHandling.Ignore)>]
        m: byte[] } // optional
    /// Unless running in single partion mode (which would restrict us to 10GB per collection)
    /// we need to nominate a partition key that will be in every document
    static member PartitionKeyField = "p"
    /// As one cannot sort by the implicit `id` field, we have an indexed `i` field for sort and range query use
    static member IndexedFields = [Event.PartitionKeyField; "i"]
    /// If we encounter a -1 doc, we're interested in its etag so we can re-read for one RU
    member x.TryToPosition() =
        if x.id <> Tip.WellKnownDocumentId then None
        else Some { index = (let ``x.e.LongLength`` = 1L in x.i+``x.e.LongLength``); etag = match x._etag with null -> None | x -> Some x }
/// The Special 'Pending' Batch Format
/// NB this Type does double duty as
/// a) transport for when we read it
/// b) a way of encoding a batch that the stored procedure will write in to the actual document (`i` is -1 until Stored Proc computes it)
/// The stored representation has the following differences vs a 'normal' (frozen/completed) Batch
/// a) `id` and `i` = `-1` as WIP document currently always is
/// b) events are retained as in an `e` array, not top level fields
/// c) contains unfolds (`c`)
and [<NoEquality; NoComparison; JsonObject(ItemRequired=Required.Always)>]
    Tip =
    {   /// Partition key, as per Batch
        p: string // "{streamName}"
        /// Document Id within partition, as per Batch
        id: string // "{-1}" - Well known IdConstant used while this remains the pending batch

        /// When we read, we need to capture the value so we can retain it for caching purposes
        /// NB this is not relevant to fill in when we pass it to the writing stored procedure
        /// as it will do: 1. read 2. merge 3. write merged version contingent on the _etag not having changed
        [<JsonProperty(DefaultValueHandling=DefaultValueHandling.Ignore, Required=Required.Default)>]
        _etag: string

        /// base 'i' value for the Events held herein
        _i: int64

        /// Events
        e: BatchEvent[]

        /// Compaction/Snapshot/Projection events
        c: Unfold[] }
    /// arguably this should be a high nember to reflect fact it is the freshest ?
    static member WellKnownDocumentId = "-1"
    /// Create Position from Tip record context (facilitating 1 RU reads)
    member x.ToPosition() = { index = x._i+x.e.LongLength; etag = match x._etag with null -> None | x -> Some x }
/// A single event from the array held in a batch
and [<NoEquality; NoComparison; JsonObject(ItemRequired=Required.Always)>]
    BatchEvent =
    {   /// Creation date (as opposed to system-defined _lastUpdated which is touched by triggers, replication etc.)
        c: System.DateTimeOffset // ISO 8601

        /// The Event Type, used to drive deserialization
        t: string // required

        /// Event body, as UTF-8 encoded json ready to be injected into the Json being rendered for DocDb
        [<JsonConverter(typeof<Equinox.Cosmos.Internal.Json.VerbatimUtf8JsonConverter>)>]
        d: byte[] // required

        /// Optional metadata, as UTF-8 encoded json, ready to emit directly (null, not written if missing)
        [<JsonConverter(typeof<Equinox.Cosmos.Internal.Json.VerbatimUtf8JsonConverter>)>]
        [<JsonProperty(Required=Required.Default, NullValueHandling=NullValueHandling.Ignore)>]
        m: byte[] } // optional
/// Compaction/Snapshot/Projection Event based on the state at a given point in time `i`
and Unfold =
    {   /// Base: Stream Position (Version) of State from which this Unfold Event was generated
        i: int64

        /// The Case (Event Type) of this compaction/snapshot, used to drive deserialization
        t: string // required

        /// Event body - Json -> UTF-8 -> Deflate -> Base64
        [<JsonConverter(typeof<Equinox.Cosmos.Internal.Json.Base64ZipUtf8JsonConverter>)>]
        d: byte[] // required

        /// Optional metadata, same encoding as `d` (can be null; not written if missing)
        [<JsonConverter(typeof<Equinox.Cosmos.Internal.Json.Base64ZipUtf8JsonConverter>)>]
        [<JsonProperty(Required=Required.Default, NullValueHandling=NullValueHandling.Ignore)>]
        m: byte[] } // optional

type Enum() =
    static member Events(b: Tip) =
        b.e |> Seq.mapi (fun offset x ->
            { new IIndexedEvent with
                member __.Index = b._i + int64 offset
                member __.IsUnfold = false
                member __.EventType = x.t
                member __.Data = x.d
                member __.Meta = x.m })
    static member Events(i: int64, e: BatchEvent[]) =
        e |> Seq.mapi (fun offset x ->
            { new IIndexedEvent with
                member __.Index = i + int64 offset
                member __.IsUnfold = false
                member __.EventType = x.t
                member __.Data = x.d
                member __.Meta = x.m })
    static member Event(x: Event) =
        Seq.singleton
            { new IIndexedEvent with
                member __.Index = x.i
                member __.IsUnfold = false
                member __.EventType = x.t
                member __.Data = x.d
                member __.Meta = x.m }
    static member Unfolds(xs: Unfold[]) = seq {
        for x in xs -> { new IIndexedEvent with
            member __.Index = x.i
            member __.IsUnfold = true
            member __.EventType = x.t
            member __.Data = x.d
            member __.Meta = x.m } }
    static member EventsAndUnfolds(x:Tip): IIndexedEvent seq =
        Enum.Unfolds x.c

/// Reference to Collection and name that will be used as the location for the stream
type [<NoComparison>] CollectionStream = { collectionUri: System.Uri; name: string } with
    static member Create(collectionUri, name) = { collectionUri = collectionUri; name = name }

namespace Equinox.Cosmos

open Equinox
open Equinox.Cosmos.Events
open Equinox.Cosmos.Store
open Equinox.Store
open FSharp.Control
open Microsoft.Azure.Documents
open Serilog
open System

[<RequireQualifiedAccess>]
type Direction = Forward | Backward with
    override this.ToString() = match this with Forward -> "Forward" | Backward -> "Backward"

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
        | Response of Direction * Measurement
        | SyncSuccess of Measurement
        | SyncResync of Measurement
        | SyncConflict of Measurement
    let prop name value (log : ILogger) = log.ForContext(name, value)
    let propData name (events: #IEvent seq) (log : ILogger) =
        let items = seq { for e in events do yield sprintf "{\"%s\": %s}" e.EventType (System.Text.Encoding.UTF8.GetString e.Data) }
        log.ForContext(name, sprintf "[%s]" (String.concat ",\n\r" items))
    let propEvents = propData "events"
    let propDataUnfolds = Enum.Unfolds >> propData "unfolds"

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
    let (|EventLen|) (x: #IEvent) = let (BlobLen bytes), (BlobLen metaBytes) = x.Data, x.Meta in bytes+metaBytes
    let (|BatchLen|) = Seq.sumBy (|EventLen|)

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
    type DocDbCollection(client : IDocumentClient, collectionUri) =
        member __.TryReadDocument(documentId : string, ?options : Client.RequestOptions): Async<float * ReadResult<'T>> = async {
            let! ct = Async.CancellationToken
            let options = defaultArg options null
            let docLink = sprintf "%O/docs/%s" collectionUri documentId
            try let! document = async { return! client.ReadDocumentAsync<'T>(docLink, options = options, cancellationToken = ct) |> Async.AwaitTaskCorrect }
                if document.StatusCode = System.Net.HttpStatusCode.NotModified then return document.RequestCharge, NotModified
                // NB `.Document` will NRE if a IfNoneModified precondition triggers a NotModified result
                else return document.RequestCharge, Found document.Document
            with
            | DocDbException (DocDbStatusCode System.Net.HttpStatusCode.NotFound as e) -> return e.RequestCharge, NotFound
            // NB while the docs suggest you may see a 412, the NotModified in the body of the try/with is actually what happens
            | DocDbException (DocDbStatusCode System.Net.HttpStatusCode.PreconditionFailed as e) -> return e.RequestCharge, NotModified }

module Sync =
    // NB don't nest in a private module, or serialization will fail miserably ;)
    [<CLIMutable; NoEquality; NoComparison; Newtonsoft.Json.JsonObject(ItemRequired=Newtonsoft.Json.Required.AllowNull)>]
    type SyncResponse = { etag: string; nextI: int64; conflicts: BatchEvent[] }
    let [<Literal>] sprocName = "EquinoxSync-SingleEvents-021"  // NB need to renumber for any breaking change
    let [<Literal>] sprocBody = """

// Manages the merging of the supplied Request Batch, fulfilling one of the following end-states
// 1 Verify no current WIP batch, the incoming `req` becomes the WIP batch (the caller is entrusted to provide a valid and complete set of inputs, or it's GIGO)
// 2 Current WIP batch has space to accommodate the incoming projections (req.c) and events (req.e) - merge them in, replacing any superseded projections
// 3. Current WIP batch would become too large - remove WIP state from active document by replacing the well known id with a correct one; proceed as per 1
function sync(req, expectedVersion) {
    if (!req) throw new Error("Missing req argument");
    const collection = getContext().getCollection();
    const collectionLink = collection.getSelfLink();
    const response = getContext().getResponse();

    // Locate the WIP (-1) batch (which may not exist)
    const wipDocId = collection.getAltLink() + "/docs/" + req.id;
    const isAccepted = collection.readDocument(wipDocId, {}, function (err, current) {
        // Verify we dont have a conflicting write
        if (expectedVersion === -1) {
            executeUpsert(current);
        } else if (!current && expectedVersion !== 0) {
            // If there is no WIP page, the writer has no possible reason for writing at an index other than zero
            response.setBody({ etag: null, nextI: 0, conflicts: [] });
        } else if (current && expectedVersion !== current._i + current.e.length) {
            // Where possible, we extract conflicting events from e and/or c in order to avoid another read cycle
            // yielding [] triggers the client to go loading the events itself
            const conflicts = expectedVersion < current._i ? [] : current.e.slice(expectedVersion - current._i);
            const nextI = current._i + current.e.length;
            response.setBody({ etag: current._etag, nextI: nextI, conflicts: conflicts });
        } else {
            executeUpsert(current);
        }
    });
    if (!isAccepted) throw new Error("readDocument not Accepted");

    function executeUpsert(current) {
        function callback(err, doc) {
            if (err) throw err;
            response.setBody({ etag: doc._etag, nextI: doc._i + doc.e.length, conflicts: null });
        }
        // If we have hit a sensible limit for a slice in the WIP document, trim the events
        if (current && current.e.length + req.e.length > 10) {
            current._i = current._i + current.e.length;
            current.e = req.e;
            current.c = req.c;

            // as we've mutated the document in a manner that can conflict with other writers, out write needs to be contingent on no competing updates having taken place
            finalize(current);
            const isAccepted = collection.replaceDocument(current._self, current, { etag: current._etag }, callback);
            if (!isAccepted) throw new Error("Unable to restart WIP batch.");
        } else if (current) {
            // Append the new events into the current batch
            Array.prototype.push.apply(current.e, req.e);
            // Replace all the projections
            current.c = req.c;
            // TODO: should remove only projections being superseded

            // as we've mutated the document in a manner that can conflict with other writers, out write needs to be contingent on no competing updates having taken place
            finalize(current);
            const isAccepted = collection.replaceDocument(current._self, current, { etag: current._etag }, callback);
            if (!isAccepted) throw new Error("Unable to replace WIP batch.");
        } else {
            current = req;
            current._i = 0;
            // concurrency control is by virtue of fact that any conflicting writer will encounter a primary key violation (which will result in a retry)
            finalize(current);
            const isAccepted = collection.createDocument(collectionLink, current, { disableAutomaticIdGeneration: true }, callback);
            if (!isAccepted) throw new Error("Unable to create WIP batch.");
        }
        for (i = 0; i < req.e.length; i++) {
            const e = req.e[i];
            const eventI = current._i + current.e.length - req.e.length + i;
            const doc = {
                p: req.p,
                id: eventI.toString(),
                i: eventI,
                c: e.c,
                t: e.t,
                d: e.d,
                m: e.m
            };
            const isAccepted = collection.createDocument(collectionLink, doc, function (err) {
                if (err) throw err;
            });
            if (!isAccepted) throw new Error("Unable to add event " + doc.i);
        }
    }

    function finalize(current) {
        current.i = -1;
        current.id = current.i.toString();
    }
}"""

    [<RequireQualifiedAccess; NoEquality; NoComparison>]
    type Result =
        | Written of Position
        | Conflict of Position * events: IIndexedEvent[]
        | ConflictUnknown of Position

    let private run (client: IDocumentClient) (stream: CollectionStream) (expectedVersion: int64 option, req: Tip)
        : Async<float*Result> = async {
        let sprocLink = sprintf "%O/sprocs/%s" stream.collectionUri sprocName
        let opts = Client.RequestOptions(PartitionKey=PartitionKey(stream.name))
        let! ct = Async.CancellationToken
        let ev = match expectedVersion with Some ev -> Position.FromI ev | None -> Position.FromAppendAtEnd
        let! (res : Client.StoredProcedureResponse<SyncResponse>) =
            client.ExecuteStoredProcedureAsync(sprocLink, opts, ct, box req, box ev.index) |> Async.AwaitTaskCorrect

        let newPos = { index = res.Response.nextI; etag = Option.ofObj res.Response.etag }
        return res.RequestCharge, res.Response.conflicts |> function
            | null -> Result.Written newPos
            | [||] when newPos.index = 0L -> Result.Conflict (newPos, Array.empty)
            | [||] -> Result.ConflictUnknown newPos
            | xs  -> Result.Conflict (newPos, Enum.Events (ev.index, xs) |> Array.ofSeq) }

    let private logged client (stream: CollectionStream) (expectedVersion, req: Tip) (log : ILogger)
        : Async<Result> = async {
        let verbose = log.IsEnabled Events.LogEventLevel.Debug
        let log = if verbose then log |> Log.propEvents (Enum.Events req) |> Log.propDataUnfolds req.c else log
        let (Log.BatchLen bytes), count = Enum.Events req, req.e.Length
        let log = log |> Log.prop "bytes" bytes
        let writeLog =
            log |> Log.prop "stream" stream.name |> Log.prop "expectedVersion" expectedVersion
                |> Log.prop "count" req.e.Length |> Log.prop "ucount" req.c.Length
        let! t, (ru,result) = run client stream (expectedVersion,req) |> Stopwatch.Time
        let resultLog =
            let mkMetric ru : Log.Measurement = { stream = stream.name; interval = t; bytes = bytes; count = count; ru = ru }
            let logConflict () = writeLog.Information("EqxCosmos Sync: Conflict writing {eventTypes}", [| for x in req.e -> x.t |])
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
        resultLog.Information("EqxCosmos {action:l} {count}+{ucount} {ms}ms rc={ru}", "Sync", req.e.Length, req.c.Length, (let e = t.Elapsed in e.TotalMilliseconds), ru)
        return result }

    let batch (log : ILogger) retryPolicy client pk batch: Async<Result> =
        let call = logged client pk batch
        Log.withLoggedRetries retryPolicy "writeAttempt" call log
    let mkBatch (stream: Store.CollectionStream) (events: IEvent[]) unfolds: Tip =
        {   p = stream.name; id = Store.Tip.WellKnownDocumentId; _i = -1L(*Server-managed*); _etag = null
            e = [| for e in events -> { c = DateTimeOffset.UtcNow; t = e.EventType; d = e.Data; m = e.Meta } |]
            c = Array.ofSeq unfolds }
    let mkUnfold baseIndex (unfolds: IEvent seq) : Store.Unfold seq =
        unfolds |> Seq.mapi (fun offset x -> { i = baseIndex + int64 offset; t = x.EventType; d = x.Data; m = x.Meta } : Store.Unfold)

    module Initialization =
        open System.Collections.ObjectModel
        let createDatabase (client:IDocumentClient) dbName = async {
            let opts = Client.RequestOptions(ConsistencyLevel = Nullable ConsistencyLevel.Session)
            let! db = client.CreateDatabaseIfNotExistsAsync(Database(Id=dbName), options = opts) |> Async.AwaitTaskCorrect
            return db.Resource.Id }

        let createCollection (client: IDocumentClient) (dbUri: Uri) collName ru = async {
            let pkd = PartitionKeyDefinition()
            pkd.Paths.Add(sprintf "/%s" Store.Event.PartitionKeyField)
            let colld = DocumentCollection(Id = collName, PartitionKey = pkd)

            colld.IndexingPolicy.IndexingMode <- IndexingMode.Consistent
            colld.IndexingPolicy.Automatic <- true
            // Can either do a blacklist or a whitelist
            // Given how long and variable the blacklist would be, we whitelist instead
            colld.IndexingPolicy.ExcludedPaths <- Collection [|ExcludedPath(Path="/*")|]
            // NB its critical to index the nominated PartitionKey field defined above or there will be runtime errors
            colld.IndexingPolicy.IncludedPaths <- Collection [| for k in Store.Event.IndexedFields -> IncludedPath(Path=sprintf "/%s/?" k) |]
            let! coll = client.CreateDocumentCollectionIfNotExistsAsync(dbUri, colld, Client.RequestOptions(OfferThroughput=Nullable ru)) |> Async.AwaitTaskCorrect
            return coll.Resource.Id }

        let createProc (log: ILogger) (client: IDocumentClient) (collectionUri: Uri) = async {
            let def = new StoredProcedure(Id = sprocName, Body = sprocBody)
            log.Information("Creating stored procedure {sprocId}", def.Id)
            // TODO ifnotexist semantics
            return! client.CreateStoredProcedureAsync(collectionUri, def) |> Async.AwaitTaskCorrect |> Async.Ignore }

        let initialize log (client : IDocumentClient) dbName collName ru = async {
            let! dbId = createDatabase client dbName
            let dbUri = Client.UriFactory.CreateDatabaseUri dbId
            let! collId = createCollection client dbUri collName ru
            let collUri = Client.UriFactory.CreateDocumentCollectionUri (dbName, collId)
            //let! _aux = createAux client dbUri collName auxRu
            return! createProc log client collUri }

module private Tip =
    let private get (client: IDocumentClient) (stream: CollectionStream, maybePos: Position option) =
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
                let (Log.BatchLen bytes), count = Enum.Unfolds doc.c, doc.c.Length
                log bytes count Log.Tip
            let log = if (not << log.IsEnabled) Events.LogEventLevel.Debug then log else log |> Log.propDataUnfolds doc.c |> Log.prop "etag" doc._etag
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
        | ReadResult.Found doc -> return Result.Found (doc.ToPosition(), Enum.EventsAndUnfolds doc |> Array.ofSeq) }

 module private Query =
    open Microsoft.Azure.Documents.Linq
    let private mkQuery (client : IDocumentClient) maxItems (stream: CollectionStream) (direction: Direction) (startPos: Position option) =
        let querySpec =
            match startPos with
            | None -> SqlQuerySpec("SELECT * FROM c WHERE c.i!=-1 ORDER BY c.i " + if direction = Direction.Forward then "ASC" else "DESC")
            | Some p ->
                let f = if direction = Direction.Forward then "c.i >= @id ORDER BY c.i ASC" else "c.i < @id ORDER BY c.i DESC"
                SqlQuerySpec("SELECT * FROM c WHERE c.i != -1 AND " + f, SqlParameterCollection [SqlParameter("@id", p.index)])
        let feedOptions = new Client.FeedOptions(PartitionKey=PartitionKey(stream.name), MaxItemCount=Nullable maxItems)
        client.CreateDocumentQuery<Event>(stream.collectionUri, querySpec, feedOptions).AsDocumentQuery()

    // Unrolls the Batches in a response - note when reading backwards, the events are emitted in reverse order of index
    let private handleResponse direction (stream: CollectionStream) (startPos: Position option) (query: IDocumentQuery<Event>) (log: ILogger)
        : Async<IIndexedEvent[] * Position option * float> = async {
        let! ct = Async.CancellationToken
        let! t, (res : Client.FeedResponse<Event>) = query.ExecuteNextAsync<Event>(ct) |> Async.AwaitTaskCorrect |> Stopwatch.Time
        let batches, ru = Array.ofSeq res, res.RequestCharge
        let events = batches |> Seq.collect Enum.Event |> Array.ofSeq
        let (Log.BatchLen bytes), count = events, events.Length
        let reqMetric : Log.Measurement = { stream = stream.name; interval = t; bytes = bytes; count = count; ru = ru }
        // TODO investigate whether there is a way to avoid the potential cost (or whether there is significance to it) of these null responses
        let log = if batches.Length = 0 && count = 0 && ru = 0. then log else let evt = Log.Response (direction, reqMetric) in log |> Log.event evt
        let log = if (not << log.IsEnabled) Events.LogEventLevel.Debug then log else log |> Log.propEvents events
        let index = if count = 0 then Nullable () else Nullable <| Seq.min (seq { for x in batches -> x.i })
        (log |> Log.prop "startIndex" (match startPos with Some { index = i } -> Nullable i | _ -> Nullable()) |> Log.prop "bytes" bytes)
            .Information("EqxCosmos {action:l} {count}/{batches} {direction} {ms}ms i={index} rc={ru}",
                "Response", count, batches.Length, direction, (let e = t.Elapsed in e.TotalMilliseconds), index, ru)
        let maybePosition = batches |> Array.tryPick (fun x -> x.TryToPosition())
        return events, maybePosition, ru }

    let private run (log : ILogger) (readSlice: IDocumentQuery<Event> -> ILogger -> Async<IIndexedEvent[] * Position option * float>)
            (maxPermittedBatchReads: int option)
            (query: IDocumentQuery<Event>)
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

    let private logQuery direction batchSize streamName interval (responsesCount, events : IIndexedEvent []) nextI (ru: float) (log : ILogger) =
        let (Log.BatchLen bytes), count = events, events.Length
        let reqMetric : Log.Measurement = { stream = streamName; interval = interval; bytes = bytes; count = count; ru = ru }
        let action = match direction with Direction.Forward -> "QueryF" | Direction.Backward -> "QueryB"
        // TODO investigate whether there is a way to avoid the potential cost (or whether there is significance to it) of these null responses
        let log = if count = 0 && ru = 0. then log else let evt = Log.Event.Query (direction, responsesCount, reqMetric) in log |> Log.event evt
        (log |> Log.prop "bytes" bytes |> Log.prop "batchSize" batchSize).Information(
            "EqxCosmos {action:l} {stream} v{nextI} {count}/{responses} {ms}ms rc={ru}",
            action, streamName, nextI, count, responsesCount, (let e = interval.Elapsed in e.TotalMilliseconds), ru)

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
        let pos = match maybeTipPos with Some p -> p | None -> Position.FromMaxIndex raws

        log |> logQuery direction maxItems stream.name t (!responseCount,raws) pos.index ru
        return pos, decoded }

type [<NoComparison>] Token = { stream: CollectionStream; pos: Position }
module Token =
    let create stream pos : Storage.StreamToken = { value = box { stream = stream; pos = pos } }
    let (|Unpack|) (token: Storage.StreamToken) : CollectionStream*Position = let t = unbox<Token> token.value in t.stream,t.pos
    let supersedes (Unpack (_,currentPos)) (Unpack (_,xPos)) =
        let currentVersion, newVersion = currentPos.index, xPos.index
        let currentETag, newETag = currentPos.etag, xPos.etag
        newVersion > currentVersion || currentETag <> newETag

namespace Equinox.Cosmos.Builder

open Equinox
open Equinox.Cosmos.Events // NB needs to be shadow by Equinox.Cosmos
open Equinox.Cosmos
open Equinox.Store.Infrastructure
open FSharp.Control
open Microsoft.Azure.Documents
open Serilog
open System

[<AutoOpen>]
module Internal =
    [<RequireQualifiedAccess; NoComparison; NoEquality>]
    type InternalSyncResult = Written of Storage.StreamToken | ConflictUnknown of Storage.StreamToken | Conflict of Storage.StreamToken * IIndexedEvent[]

    [<RequireQualifiedAccess; NoComparison; NoEquality>]
    type LoadFromTokenResult<'event> = Unchanged | Found of Storage.StreamToken * 'event[]

/// Defines policies for retrying with respect to transient failures calling CosmosDb (as opposed to application level concurrency conflicts)
type EqxConnection(client: IDocumentClient, ?readRetryPolicy: IRetryPolicy, ?writeRetryPolicy: IRetryPolicy) =
    member __.Client = client
    member __.TipRetryPolicy = readRetryPolicy
    member __.QueryRetryPolicy = readRetryPolicy
    member __.WriteRetryPolicy = writeRetryPolicy

/// Defines the policies in force regarding how to constrain query responses
type EqxBatchingPolicy
    (   // Max items to request in query response. Defaults to 10.
        ?defaultMaxItems : int,
        // Dynamic version of `defaultMaxItems`, allowing one to react to dynamic configuration changes. Default to using `defaultMaxItems`
        ?getDefaultMaxItems : unit -> int,
        /// Maximum number of trips to permit when slicing the work into multiple responses based on `MaxSlices`. Default: unlimited.
        ?maxRequests) =
    let getdefaultMaxItems = defaultArg getDefaultMaxItems (fun () -> defaultArg defaultMaxItems 10)
    /// Limit for Maximum number of `Batch` records in a single query batch response
    member __.MaxItems = getdefaultMaxItems ()
    /// Maximum number of trips to permit when slicing the work into multiple responses based on `MaxSlices`
    member __.MaxRequests = maxRequests

type EqxGateway(conn : EqxConnection, batching : EqxBatchingPolicy) =
    let (|FromUnfold|_|) (tryDecode: #IEvent -> 'event option) (isOrigin: 'event -> bool) (xs:#IEvent[]) : Option<'event[]> =
        match Array.tryFindIndexBack (tryDecode >> Option.exists isOrigin) xs with
        | None -> None
        | Some index -> xs |> Seq.skip index |> Seq.choose tryDecode |> Array.ofSeq |> Some
    member __.LoadBackwardsStopping log stream (tryDecode,isOrigin): Async<Storage.StreamToken * 'event[]> = async {
        let! pos, events = Query.walk log conn.Client conn.QueryRetryPolicy batching.MaxItems batching.MaxRequests Direction.Backward stream None (tryDecode,isOrigin)
        Array.Reverse events
        return Token.create stream pos, events }
    member __.Read log stream direction startPos (tryDecode,isOrigin) : Async<Storage.StreamToken * 'event[]> = async {
        let! pos, events = Query.walk log conn.Client conn.QueryRetryPolicy batching.MaxItems batching.MaxRequests direction stream startPos (tryDecode,isOrigin)
        return Token.create stream pos, events }
    member __.LoadFromUnfoldsOrRollingSnapshots log (stream,maybePos) (tryDecode,isOrigin): Async<Storage.StreamToken * 'event[]> = async {
        let! res = Tip.tryLoad log conn.TipRetryPolicy conn.Client stream maybePos
        match res with
        | Tip.Result.NotFound -> return Token.create stream Position.FromKnownEmpty, Array.empty
        | Tip.Result.NotModified -> return invalidOp "Not handled"
        | Tip.Result.Found (pos, FromUnfold tryDecode isOrigin span) -> return Token.create stream pos, span
        | _ -> return! __.LoadBackwardsStopping log stream (tryDecode,isOrigin) }
    member __.GetPosition(log, stream, ?pos): Async<Storage.StreamToken> = async {
        let! res = Tip.tryLoad log conn.TipRetryPolicy conn.Client stream pos
        match res with
        | Tip.Result.NotFound -> return Token.create stream Position.FromKnownEmpty
        | Tip.Result.NotModified -> return Token.create stream pos.Value
        | Tip.Result.Found (pos, _unfoldsAndEvents) -> return Token.create stream pos }
    member __.LoadFromToken(log, (stream,pos), (tryDecode, isOrigin)): Async<LoadFromTokenResult<'event>> = async {
        let! res = Tip.tryLoad log conn.TipRetryPolicy conn.Client stream (Some pos)
        match res with
        | Tip.Result.NotFound -> return LoadFromTokenResult.Found (Token.create stream Position.FromKnownEmpty,Array.empty)
        | Tip.Result.NotModified -> return LoadFromTokenResult.Unchanged
        | Tip.Result.Found (pos, FromUnfold tryDecode isOrigin span) -> return LoadFromTokenResult.Found (Token.create stream pos, span)
        | _ ->  let! res = __.Read log stream Direction.Forward (Some pos) (tryDecode,isOrigin)
                return LoadFromTokenResult.Found res }
    member __.Sync log stream (expectedVersion, batch: Store.Tip): Async<InternalSyncResult> = async {
        let! wr = Sync.batch log conn.WriteRetryPolicy conn.Client stream (expectedVersion,batch)
        match wr with
        | Sync.Result.Conflict (pos',events) -> return InternalSyncResult.Conflict (Token.create stream pos',events)
        | Sync.Result.ConflictUnknown pos' -> return InternalSyncResult.ConflictUnknown (Token.create stream pos')
        | Sync.Result.Written pos' -> return InternalSyncResult.Written (Token.create stream pos') }

type private Category<'event, 'state>(gateway : EqxGateway, codec : UnionCodec.IUnionEncoder<'event, byte[]>) =
    let tryDecode (x: #IEvent) = codec.TryDecode { caseName = x.EventType; payload = x.Data }
    let (|TryDecodeFold|) (fold: 'state -> 'event seq -> 'state) initial (events: IIndexedEvent seq) : 'state = Seq.choose tryDecode events |> fold initial
    member __.Load includeUnfolds collectionStream fold initial isOrigin (log : ILogger): Async<Storage.StreamToken * 'state> = async {
        let! token, events =
            if not includeUnfolds then gateway.LoadBackwardsStopping log collectionStream (tryDecode,isOrigin)
            else gateway.LoadFromUnfoldsOrRollingSnapshots log (collectionStream,None) (tryDecode,isOrigin)
        return token, fold initial events }
    member __.LoadFromToken (Token.Unpack streamPos, state: 'state as current) fold isOrigin (log : ILogger): Async<Storage.StreamToken * 'state> = async {
        let! res = gateway.LoadFromToken(log, streamPos, (tryDecode,isOrigin))
        match res with
        | LoadFromTokenResult.Unchanged -> return current
        | LoadFromTokenResult.Found (token', events') -> return token', fold state events' }
    member __.Sync(Token.Unpack (stream,pos), state as current, expectedVersion, events, unfold, fold, isOrigin, log): Async<Storage.SyncResult<'state>> = async {
        let encodeEvent (x : 'event) : IEvent =
            let e = codec.Encode x
            { new IEvent with
                member __.EventType = e.caseName
                member __.Data = e.payload
                member __.Meta = null }
        let state' = fold state (Seq.ofList events)
        let eventsEncoded, projectionsEncoded = Seq.map encodeEvent events |> Array.ofSeq, Seq.map encodeEvent (unfold state' events)
        let baseIndex = pos.index + int64 (List.length events)
        let projections = Sync.mkUnfold baseIndex projectionsEncoded
        let batch = Sync.mkBatch stream eventsEncoded projections
        let! res = gateway.Sync log stream (expectedVersion,batch)
        match res with
        | InternalSyncResult.Conflict (token',TryDecodeFold fold state events') -> return Storage.SyncResult.Conflict (async { return token', events' })
        | InternalSyncResult.ConflictUnknown _token' -> return Storage.SyncResult.Conflict (__.LoadFromToken current fold isOrigin log)
        | InternalSyncResult.Written token' -> return Storage.SyncResult.Written (token', state') }

module Caching =
    open System.Runtime.Caching
    [<AllowNullLiteral>]
    type CacheEntry<'state>(initialToken : Storage.StreamToken, initialState :'state) =
        let mutable currentToken, currentState = initialToken, initialState
        member __.UpdateIfNewer (other : CacheEntry<'state>) =
            lock __ <| fun () ->
                let otherToken, otherState = other.Value
                if otherToken |> Token.supersedes currentToken then
                    currentToken <- otherToken
                    currentState <- otherState
        member __.Value : Storage.StreamToken  * 'state =
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
    type CategoryTee<'event, 'state>(inner: ICategory<'event, 'state>, tee : string -> Storage.StreamToken * 'state -> unit) =
        let intercept streamName tokenAndState =
            tee streamName tokenAndState
            tokenAndState
        let interceptAsync load streamName = async {
            let! tokenAndState = load
            return intercept streamName tokenAndState }
        interface ICategory<'event, 'state> with
            member __.Load (streamName : string) (log : ILogger) : Async<Storage.StreamToken * 'state> =
                interceptAsync (inner.Load streamName log) streamName
            member __.TrySync (log : ILogger) (Token.Unpack (stream,_) as streamToken,state) (events : 'event list)
                : Async<Storage.SyncResult<'state>> = async {
                let! syncRes = inner.TrySync log (streamToken, state) events
                match syncRes with
                | Storage.SyncResult.Conflict resync ->         return Storage.SyncResult.Conflict (interceptAsync resync stream.name)
                | Storage.SyncResult.Written (token', state') ->return Storage.SyncResult.Written (intercept stream.name (token', state')) }

    let applyCacheUpdatesWithSlidingExpiration
            (cache: Cache)
            (prefix: string)
            (slidingExpiration : TimeSpan)
            (category: ICategory<'event, 'state>)
            : ICategory<'event, 'state> =
        let policy = new CacheItemPolicy(SlidingExpiration = slidingExpiration)
        let addOrUpdateSlidingExpirationCacheEntry streamName = CacheEntry >> cache.UpdateIfNewer policy (prefix + streamName)
        CategoryTee<'event,'state>(category, addOrUpdateSlidingExpirationCacheEntry) :> _

type private Folder<'event, 'state>
    (   category: Category<'event, 'state>, fold: 'state -> 'event seq -> 'state, initial: 'state,
        isOrigin: 'event -> bool,
        mkCollectionStream: string -> Store.CollectionStream,
        // Whether or not an `unfold` function is supplied controls whether reads do a point read before querying
        ?unfold: ('state -> 'event list -> 'event seq),
        ?readCache) =
    interface ICategory<'event, 'state> with
        member __.Load streamName (log : ILogger): Async<Storage.StreamToken * 'state> =
            let collStream = mkCollectionStream streamName
            let batched = category.Load (Option.isSome unfold) collStream fold initial isOrigin log
            let cached tokenAndState = category.LoadFromToken tokenAndState fold isOrigin log
            match readCache with
            | None -> batched
            | Some (cache : Caching.Cache, prefix : string) ->
                match cache.TryGet(prefix + streamName) with
                | None -> batched
                | Some tokenAndState -> cached tokenAndState
        member __.TrySync (log : ILogger) (Token.Unpack (_stream,pos) as streamToken,state) (events : 'event list)
            : Async<Storage.SyncResult<'state>> = async {
            let! res = category.Sync((streamToken,state), Some pos.index, events, (defaultArg unfold (fun _ _ -> Seq.empty)), fold, isOrigin, log)
            match res with
            | Storage.SyncResult.Conflict resync ->             return Storage.SyncResult.Conflict resync
            | Storage.SyncResult.Written (token',state') ->     return Storage.SyncResult.Written (token',state') }

/// Defines a process for mapping from a Stream Name to the appropriate storage area, allowing control over segregation / co-locating of data
type EqxCollections(selectDatabaseAndCollection : string -> string*string) =
    new (databaseId, collectionId) = EqxCollections(fun _streamName -> databaseId, collectionId)
    member __.CollectionForStream streamName =
        let databaseId, collectionId = selectDatabaseAndCollection streamName
        Store.CollectionStream.Create(Client.UriFactory.CreateDocumentCollectionUri(databaseId, collectionId), streamName)

/// Pairs a Gateway, defining the retry policies for CosmosDb with an EqxCollections to
type EqxStore(gateway: EqxGateway, collections: EqxCollections) =
    member __.Gateway = gateway
    member __.Collections = collections

[<NoComparison; NoEquality; RequireQualifiedAccess>]
type CachingStrategy =
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

type EqxStreamBuilder<'event, 'state>(store : EqxStore, codec, fold, initial, ?access, ?caching) =
    member __.Create streamName : Equinox.IStream<'event, 'state> =
        let readCacheOption =
            match caching with
            | None -> None
            | Some (CachingStrategy.SlidingWindow(cache, _)) -> Some(cache, null)
        let isOrigin, projectOption =
            match access with
            | None -> (fun _ -> false), None
            | Some (AccessStrategy.Unfolded (isOrigin, unfold)) -> isOrigin, Some (fun state _events -> unfold state)
            | Some (AccessStrategy.Snapshot (isValid,generate)) -> isValid, Some (fun state _events -> seq [generate state])
            | Some (AccessStrategy.AnyKnownEventType) ->           (fun _ -> true), Some (fun _ events -> Seq.last events |> Seq.singleton)
        let category = Category<'event, 'state>(store.Gateway, codec)
        let folder = Folder<'event, 'state>(category, fold, initial, isOrigin, store.Collections.CollectionForStream, ?unfold=projectOption, ?readCache = readCacheOption)
        let category : ICategory<_,_> =
            match caching with
            | None -> folder :> _
            | Some (CachingStrategy.SlidingWindow(cache, window)) ->
                Caching.applyCacheUpdatesWithSlidingExpiration cache null window folder

        Equinox.Stream.create category streamName

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
        | _ -> invalidArg "connectionString" "unrecognized connection string format"

type ConnectionMode =
    /// Default mode, uses Https - inefficient as uses a double hop
    | Gateway
    /// Most efficient, but requires direct connectivity
    | DirectTcp
    // More efficient than Gateway, but suboptimal
    | DirectHttps

type EqxConnector
    (   requestTimeout: TimeSpan, maxRetryAttemptsOnThrottledRequests: int, maxRetryWaitTimeInSeconds: int,
        log : ILogger,
        /// Connection limit (default 1000)
        ?maxConnectionLimit,
        /// Connection mode (default: ConnectionMode.Gateway (lowest perf, least trouble))
        ?mode : ConnectionMode,
        /// consistency mode  (default: ConsistencyLevel.Session)
        ?defaultConsistencyLevel : ConsistencyLevel,

        /// Retries for read requests, over and above those defined by the mandatory policies
        ?readRetryPolicy,
        /// Retries for write requests, over and above those defined by the mandatory policies
        ?writeRetryPolicy,
        /// Additional strings identifying the context of this connection; should provide enough context to disambiguate all potential connections to a cluster
        /// NB as this will enter server and client logs, it should not contain sensitive information
        ?tags : (string*string) seq) =

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
            discovery  : Discovery) : Async<IDocumentClient> =
        let connect (uri: Uri, key: string) = async {
            let name = String.concat ";" <| seq {
                yield name
                match tags with None -> () | Some tags -> for key, value in tags do yield sprintf "%s=%s" key value }
            let sanitizedName = name.Replace('\'','_').Replace(':','_') // sic; Align with logging for ES Adapter
            let client = new Client.DocumentClient(uri, key, connPolicy, Nullable(defaultArg defaultConsistencyLevel ConsistencyLevel.Session))
            log.ForContext("Uri", uri).Information("EqxCosmos connecting to Cosmos with Connection Name {connectionName}", sanitizedName)
            do! client.OpenAsync() |> Async.AwaitTaskCorrect
            return client :> IDocumentClient }

        match discovery with Discovery.UriAndKey(databaseUri=uri; key=key) -> connect (uri,key)

    /// Yields a DocDbConnection configured per the specified strategy
    member __.Connect(name, discovery : Discovery) : Async<EqxConnection> = async {
        let! conn = connect(name, discovery)
        return EqxConnection(conn, ?readRetryPolicy=readRetryPolicy, ?writeRetryPolicy=writeRetryPolicy) }

namespace Equinox.Cosmos.Core

open Equinox.Cosmos
open Equinox.Cosmos.Builder
open Equinox.Cosmos.Events
open FSharp.Control

/// Outcome of appending events, specifying the new and/or conflicting events, together with the updated Target write position
[<RequireQualifiedAccess; NoComparison>]
type AppendResult<'t> =
    | Ok of pos: 't
    | Conflict of index: 't * conflictingEvents: IIndexedEvent[]
    | ConflictUnknown of index: 't

/// Encapsulates the core facilites Equinox.Cosmos offers for operating directly on Events in Streams.
type EqxContext
    (   /// Connection to CosmosDb with DocumentDb Transient Read and Write Retry policies
        conn : EqxConnection,
        /// Database + Collection selector
        collections: EqxCollections,
        /// Logger to write to - see https://github.com/serilog/serilog/wiki/Provided-Sinks for how to wire to your logger
        logger : Serilog.ILogger,
        /// Optional maximum number of Store.Batch records to retrieve as a set (how many Events are placed therein is controlled by maxEventsPerSlice).
        /// Defaults to 10
        ?defaultMaxItems,
        /// Alternate way of specifying defaultMaxItems which facilitates reading it from a cached dynamic configuration
        ?getDefaultMaxItems) =
    let getDefaultMaxItems = match getDefaultMaxItems with Some f -> f | None -> fun () -> defaultArg defaultMaxItems 10
    let batching = EqxBatchingPolicy(getDefaultMaxItems=getDefaultMaxItems)
    let gateway = EqxGateway(conn, batching)

    let maxCountPredicate count =
        let acc = ref (max (count-1) 0)
        fun _ ->
            if !acc = 0 then true else
            decr acc
            false

    let yieldPositionAndData res = async {
        let! (Token.Unpack (_,pos')), data = res
        return pos', data }

    member __.CreateStream(streamName) = collections.CollectionForStream streamName

    member internal __.GetInternal((stream, startPos), ?maxCount, ?direction) = async {
        let direction = defaultArg direction Direction.Forward
        if maxCount = Some 0 then
            // Search semantics include the first hit so we need to special case this anyway
            return Token.create stream (defaultArg startPos Position.FromKnownEmpty), Array.empty
        else
            let isOrigin =
                match maxCount with
                | Some limit -> maxCountPredicate limit
                | None -> fun _ -> false
            return! gateway.Read logger stream direction startPos (Some,isOrigin) }

    /// Establishes the current position of the stream in as effficient a manner as possible
    /// (The ideal situation is that the preceding token is supplied as input in order to avail of 1RU low latency state checks)
    member __.Sync(stream, ?position: Position) : Async<Position> = async {
        //let indexed predicate = load fold initial (coll.Gateway.IndexedOrBatched log predicate (stream,None))
        let! (Token.Unpack (_,pos')) = gateway.GetPosition(logger, stream, ?pos=position)
        return pos' }

    /// Reads in batches of `batchSize` from the specified `Position`, allowing the reader to efficiently walk away from a running query
    /// ... NB as long as they Dispose!
    member __.Walk(stream, batchSize, ?position, ?direction) : AsyncSeq<IIndexedEvent[]> = asyncSeq {
        let! _pos,data = __.GetInternal((stream, position), batchSize, ?direction=direction)
        // TODO add laziness
        return AsyncSeq.ofSeq data }

    /// Reads all Events from a `Position` in a given `direction`
    member __.Read(stream, ?position, ?maxCount, ?direction) : Async<Position*IIndexedEvent[]> =
        __.GetInternal((stream, position), ?maxCount=maxCount, ?direction=direction) |> yieldPositionAndData

    /// Appends the supplied batch of events, subject to a consistency check based on the `position`
    /// Callers should implement appropriate idempotent handling, or use Equinox.Handler for that purpose
    member __.Sync(stream, position, events: IEvent[]) : Async<AppendResult<Position>> = async {
        let batch = Sync.mkBatch stream events Seq.empty
        let! res = gateway.Sync logger stream (Some position.index,batch)
        match res with
        | Builder.Internal.InternalSyncResult.Written (Token.Unpack (_,pos)) -> return AppendResult.Ok pos
        | Builder.Internal.InternalSyncResult.Conflict (Token.Unpack (_,pos),events) -> return AppendResult.Conflict (pos, events)
        | Builder.Internal.InternalSyncResult.ConflictUnknown (Token.Unpack (_,pos)) -> return AppendResult.ConflictUnknown pos }

    /// Low level, non-idempotent call appending events to a stream without a concurrency control mechanism in play
    /// NB Should be used sparingly; Equinox.Handler enables building equivalent equivalent idempotent handling with minimal code.
    member __.NonIdempotentAppend(stream, events: IEvent[]) : Async<Position> = async {
        let! res = __.Sync(stream, Position.FromAppendAtEnd, events)
        match res with
        | AppendResult.Ok token -> return token
        | x -> return x |> sprintf "Conflict despite it being disabled %A" |> invalidOp }

/// Api as defined in the Equinox Specification
/// Note the EqxContext APIs can yield better performance due to the fact that a Position tracks the etag of the Stream's WipBatch
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
        | i -> Some (Position.FromI i)
    let (|MaxPosition|) = function
        | int64.MaxValue -> None
        | i -> Some (Position.FromI (i + 1L))

    /// Returns an async sequence of events in the stream starting at the specified sequence number,
    /// reading in batches of the specified size.
    /// Returns an empty sequence if the stream is empty or if the sequence number is larger than the largest
    /// sequence number in the stream.
    let getAll (ctx: EqxContext) (streamName: string) (MinPosition index: int64) (batchSize: int): AsyncSeq<IIndexedEvent[]> =
        ctx.Walk(ctx.CreateStream streamName, batchSize, ?position=index)

    /// Returns an async array of events in the stream starting at the specified sequence number,
    /// number of events to read is specified by batchSize
    /// Returns an empty sequence if the stream is empty or if the sequence number is larger than the largest
    /// sequence number in the stream.
    let get (ctx: EqxContext) (streamName: string) (MinPosition index: int64) (maxCount: int): Async<IIndexedEvent[]> =
        ctx.Read(ctx.CreateStream streamName, ?position=index, maxCount=maxCount) |> dropPosition

    /// Appends a batch of events to a stream at the specified expected sequence number.
    /// If the specified expected sequence number does not match the stream, the events are not appended
    /// and a failure is returned.
    let append (ctx: EqxContext) (streamName: string) (index: int64) (events: IEvent[]): Async<AppendResult<int64>> =
        ctx.Sync(ctx.CreateStream streamName, Position.FromI index, events) |> stripSyncResult

    /// Appends a batch of events to a stream at the the present Position without any conflict checks.
    /// NB typically, it is recommended to ensure idempotency of operations by using the `append` and related API as
    /// this facilitates ensuring consistency is maintained, and yields reduced latency and Request Charges impacts
    /// (See equivalent APIs on `Context` that yield `Position` values)
    let appendAtEnd (ctx: EqxContext) (streamName: string) (events: IEvent[]): Async<int64> =
        ctx.NonIdempotentAppend(ctx.CreateStream streamName, events) |> stripPosition

    /// Returns an async sequence of events in the stream backwards starting from the specified sequence number,
    /// reading in batches of the specified size.
    /// Returns an empty sequence if the stream is empty or if the sequence number is smaller than the smallest
    /// sequence number in the stream.
    let getAllBackwards (ctx: EqxContext) (streamName: string) (MaxPosition index: int64) (maxCount: int): AsyncSeq<IIndexedEvent[]> =
        ctx.Walk(ctx.CreateStream streamName, maxCount, ?position=index, direction=Direction.Backward)

    /// Returns an async array of events in the stream backwards starting from the specified sequence number,
    /// number of events to read is specified by batchSize
    /// Returns an empty sequence if the stream is empty or if the sequence number is smaller than the smallest
    /// sequence number in the stream.
    let getBackwards (ctx: EqxContext) (streamName: string) (MaxPosition index: int64) (maxCount: int): Async<IIndexedEvent[]> =
        ctx.Read(ctx.CreateStream streamName, ?position=index, maxCount=maxCount, direction=Direction.Backward) |> dropPosition

    /// Obtains the `index` from the current write Position
    let getNextIndex (ctx: EqxContext) (streamName: string) : Async<int64> =
        ctx.Sync(ctx.CreateStream streamName) |> stripPosition