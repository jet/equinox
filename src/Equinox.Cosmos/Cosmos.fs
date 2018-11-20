namespace Equinox.Cosmos.Internal.Json

open Newtonsoft.Json.Linq
open Newtonsoft.Json

/// Manages injecting prepared json into the data being submitted to DocDb as-is, on the basis we can trust it to be valid json as DocDb will need it to be
type VerbatimUtf8JsonConverter() =
    inherit JsonConverter()

    override __.ReadJson(reader, _, _, _) =
        let token = JToken.Load(reader)
        if token.Type = JTokenType.Object then token.ToString() |> System.Text.Encoding.UTF8.GetBytes |> box
        else Array.empty<byte> |> box

    override __.CanConvert(objectType) =
        typeof<byte[]>.Equals(objectType)

    override __.WriteJson(writer, value, serializer) =
        let array = value :?> byte[]
        if array = null || Array.length array = 0 then serializer.Serialize(writer, null)
        else writer.WriteRawValue(System.Text.Encoding.UTF8.GetString(array))

open System.IO
open System.IO.Compression

/// Manages zipping of the UTF-8 json bytes to make the index record minimal from the perspective of the writer stored proc
/// Only applied to snapshots in the Index
type Base64ZipUtf8JsonConverter() =
    inherit JsonConverter()
    let pickle (input : byte[]) : string =
        if input = null then null else

        use output = new MemoryStream()
        use compressor = new DeflateStream(output, CompressionLevel.Optimal)
        compressor.Write(input,0,input.Length)
        compressor.Close()
        System.Convert.ToBase64String(output.ToArray())
    let unpickle str : byte[] =
        if str = null then null else

        let compressedBytes = System.Convert.FromBase64String str
        use input = new MemoryStream(compressedBytes)
        use decompressor = new DeflateStream(input, CompressionMode.Decompress)
        use output = new MemoryStream()
        decompressor.CopyTo(output)
        output.ToArray()

    override __.CanConvert(objectType) =
        typeof<byte[]>.Equals(objectType)
    override __.ReadJson(reader, _, _, serializer) =
        //(   if reader.TokenType = JsonToken.Null then null else
        serializer.Deserialize(reader, typedefof<string>) :?> string |> unpickle |> box
    override __.WriteJson(writer, value, serializer) =
        let pickled = value |> unbox |> pickle
        serializer.Serialize(writer, pickled)

namespace Equinox.Cosmos.Events

open Equinox.Store.Infrastructure // Option shims for downlevel frameworks

/// Common form for either a raw Event or a Projection
type IEvent =
    /// The Event Type, used to drive deserialization
    abstract member EventType : string
    /// Event body, as UTF-8 encoded json ready to be injected into the Json being rendered for DocDb
    abstract member Data : byte[]
    /// Optional metadata (null, or same as d, not written if missing)
    abstract member Meta : byte[]

/// Represents an Event or Projection and its relative position in the event sequence
type IOrderedEvent =
    inherit IEvent
    /// The index into the event sequence of this event
    abstract member Index : int64
    /// Indicates whether this is a primary event or a projection based on the events <= up to `Index`
    abstract member IsProjection: bool

/// Position and Etag to which an operation is relative
type [<NoComparison>] Position = { index: int64; etag: string option } with
    /// Just Do It mode
    static member internal FromAppendAtEnd = Position.FromIndexOnly -1L
    /// Create Position from [Wip]Batch record context facilitating 1 RU reads
    static member internal FromDocument(i: int64, etag: string) = { index = i; etag = Option.ofObj etag }
    /// NB very inefficient compared to FromDocument or using one already returned to you
    static member internal FromIndexOnly(i: int64) = { index = i; etag = None }
    /// If we have strong reason to suspect a stream is empty empty, we won't have an etag (and Writer Stored Procedure special cases this)
    static member FromEmptyStream = Position.FromIndexOnly 0L

/// Reference to Storage Partition
type [<NoComparison>] Stream = { collectionUri: System.Uri; name: string } with
    static member Create(collectionUri, name) = { collectionUri = collectionUri; name = name }

namespace Equinox.Cosmos.Store

open Equinox.Cosmos.Events
open Newtonsoft.Json

/// A 'normal' (frozen, not Pending) Batch of Events, without any Projections
type [<NoEquality; NoComparison; JsonObject(ItemRequired=Required.Always)>]
    Batch =
    {   /// DocDb-mandated Partition Key, must be maintained within the document
        /// Not actually required if running in single partition mode, but for simplicity, we always write it
        p: string // "{streamName}"

        /// DocDb-mandated unique row key; needs to be unique within any partition it is maintained; must be string
        /// At the present time, one can't perform an ORDER BY on this field, hence we also have i shadowing it
        /// NB WipBatch uses a well known value here while it's actively 'open'
        id: string // "{index}"

        /// Same as `id`; necessitated by fact that it's not presently possible to do an ORDER BY on the row key
        i: int64 // {index}

        /// The events at this offset in the stream
        e: BatchEvent[] }
    /// Unless running in single partion mode (which would restrict us to 10GB per collection)
    /// we need to nominate a partition key that will be in every document
    static member PartitionKeyField = "p"
    /// As one cannot sort by the implicit `id` field, we have an indexed `i` field for sort and range query use
    static member IndexedFields = [Batch.PartitionKeyField; "i"]
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

/// The Special 'Pending' Batch Format
/// NB this Type does double duty as
/// a) transport for when we read it
/// b) a way of encoding a batch that the stored procedure will write in to the actual document
/// The stored representation has the following differences vs a 'normal' (frozen/completed) Batch
/// a) `id` = `-1`
/// b) contains projections (`c`)
/// c) `i` is temporarily -1 and filled in by the server when this is used as the write batch request format
type [<NoEquality; NoComparison; JsonObject(ItemRequired=Required.Always)>]
    WipBatch =
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
        i: int64

        /// Events
        e: BatchEvent[]

        /// Projections
        c: Projection[] }
    /// arguably this should be a high nember to reflect fact it is the freshest ?
    static member WellKnownDocumentId = "-1"
/// Projection based on the state at a given point in time `i`
and [<CLIMutable>]
    Projection =
    {   /// Base: Max index rolled into this projection
        i: int64

        ///// Indicates whether this is actually an event being retained to support a lagging projection
        //x: bool

        /// The Event Type of this compaction/snapshot, used to drive deserialization
        t: string // required

        /// Event body - Json -> UTF-8 -> Deflate -> Base64
        [<JsonConverter(typeof<Equinox.Cosmos.Internal.Json.Base64ZipUtf8JsonConverter>)>]
        d: byte[] // required

        /// Optional metadata, same encoding as `d` (can be null; not written if missing)
        [<JsonConverter(typeof<Equinox.Cosmos.Internal.Json.Base64ZipUtf8JsonConverter>)>]
        [<JsonProperty(Required=Required.Default, NullValueHandling=NullValueHandling.Ignore)>]
        m: byte[] } // optional

type Enum() =
    static member Events (b:WipBatch) =
        b.e |> Seq.mapi (fun offset x ->
        { new IOrderedEvent with
            member __.Index = b.i + int64 offset
            member __.IsProjection = false
            member __.EventType = x.t
            member __.Data = x.d
            member __.Meta = x.m })
    static member Events (i: int64, e:BatchEvent[]) =
        e |> Seq.mapi (fun offset x ->
            { new IOrderedEvent with
                member __.Index = i + int64 offset
                member __.IsProjection = false
                member __.EventType = x.t
                member __.Data = x.d
                member __.Meta = x.m })
    static member Events (b:Batch) =
        Enum.Events (b.i, b.e)
    static member Projections (xs: Projection[]) = seq {
        for x in xs -> { new IOrderedEvent with
            member __.Index = x.i
            member __.IsProjection = true
            member __.EventType = x.t
            member __.Data = x.d
            member __.Meta = x.m } }
    static member EventsAndProjections (x:WipBatch): IOrderedEvent seq =
        Enum.Events x
        |> Seq.append (Enum.Projections x.c)
        // where Index is equal, projections get delivered after the events so the fold semantics can be 'idempotent'
        |> Seq.sortBy (fun x -> x.Index, x.IsProjection)

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

[<NoComparison; NoEquality; RequireQualifiedAccess>]
type AccessStrategy<'event,'state> =
    | EventsAreState
    | //[<Obsolete("Superseded by IndexedSearch")>]
      RollingSnapshots of eventType: string * compact: ('state -> 'event)
    | IndexedSearch of (string -> bool) * index: ('state -> 'event seq)

module Log =
    [<NoEquality; NoComparison>]
    type Measurement = { stream: string; interval: StopwatchInterval; bytes: int; count: int; ru: float }
    [<NoEquality; NoComparison>]
    type Event =
        | WriteSuccess of Measurement
        | WriteResync of Measurement
        | WriteConflict of Measurement
        /// Individual read request in a Batch
        | Slice of Direction * Measurement
        /// Individual read request for the Index
        | Index of Measurement
        /// Individual read request for the Index, not found
        | IndexNotFound of Measurement
        /// Index read with Single RU Request Charge due to correct use of etag in cache
        | IndexNotModified of Measurement
        /// Summarizes a set of Slices read together
        | Batch of Direction * slices: int * Measurement
    let prop name value (log : ILogger) = log.ForContext(name, value)
    let propData name (events: #IEvent seq) (log : ILogger) =
        let items = seq { for e in events do yield sprintf "{\"%s\": %s}" e.EventType (System.Text.Encoding.UTF8.GetString e.Data) }
        log.ForContext(name, sprintf "[%s]" (String.concat ",\n\r" items))
    let propEvents = propData "events"
    let propEventsBatch (x : Batch) log = log |> propData "events" (Enum.Events x)
    let propEventsWipBatch (x : WipBatch) log = log |> propData "events" (Enum.Events x)
    let propDataProjections = Enum.Projections >> propData "projections"

    let withLoggedRetries<'t> retryPolicy (contextLabel : string) (f : ILogger -> Async<'t>) log: Async<'t> =
        match retryPolicy with
        | None -> f log
        | Some retryPolicy ->
            let withLoggingContextWrapping count =
                let log = if count = 1 then log else log |> prop contextLabel count
                f log
            retryPolicy withLoggingContextWrapping
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
    let [<Literal>] sprocName = "EquinoxSync001"  // NB need to renumber for any breaking change
    let [<Literal>] sprocBody = """

// Manages the merging of the supplied Request Batch, fulfilling one of the following end-states
// 1 Verify no current WIP batch, the incoming `req` becomes the WIP batch (the caller is entrusted to provide a valid and complete set of inputs, or it's GIGO)
// 2 Current WIP batch has space to accommodate the incoming projections (req.c) and events (req.e) - merge them in, replacing any superseded projections
// 3. Current WIP batch would become too large - remove WIP state from active document by replacing the well known id with a correct one; proceed as per 1
function sync(req, expectedVersion, maxEvents) {
    if (!req) throw new Error("Missing req argument");
    const collection = getContext().getCollection();
    const collectionLink = collection.getSelfLink();
    const response = getContext().getResponse();

    // Locate the WIP (-1) batch (which may not exist)
    const wipDocId = collection.getAltLink() + "/docs/" + req.id;
    const isAccepted = collection.readDocument(wipDocId, {}, function (err, current, options) {
        // Verify we dont have a conflicting write
        if (expectedVersion === -1) {
            executeUpsert(current);
        } else if (current == null && expectedVersion !== 0) {
            // If there is no WIP page, the writer has no possible reason for writing at an index other than zero
            response.setBody({ etag: null, nextI: 0, conflicts: [] });
        } else if (current != null && expectedVersion !== current.i + current.e.length) {
            // Where possible, we extract conflicting events from e and/or c in order to avoid another read cycle
            // yielding [] triggers the client to go loading the events itself
            const conflicts = expectedVersion < current.i ? [] : current.e.slice(expectedVersion - current.i);
            const nextI = current.i + current.e.length;
            response.setBody({ etag: current._etag, nextI: nextI, conflicts: conflicts });
        } else {
            executeUpsert(current);
        }
    });
    if (!isAccepted) throw new Error("readDocument not Accepted");

    function executeUpsert(current) {
        function callback(err, doc, options) {
            if (err) throw err;
            response.setBody({ etag: doc._etag, nextI: doc.i + doc.e.length, conflicts: null });
        }
        // If we have hit a sensible limit for a slice, swap to a new one
        if (current != null && current.e.length + req.e.length > maxEvents) {
            // remove the well-known `id` value identifying the batch as being WIP
            current.id = current.i.toString();
            // ... As it's no longer a WIP batch, we definitely don't want projections taking up space
            delete current.c;

            // TODO Carry forward:
            // - `c` items not present in `batch`,
            // - their associated `c` items with `x:true`
            // - any required `e` items from the page being superseded (as `c` items with `x:true`])

            // as we've mutated the document in ways that can trigger loss, out write needs to be contingent on no competing updates having taken place
            const reqOptions = { etag: current._etag };
            const wipUpdateAccepted = collection.replaceDocument(current._self, current, reqOptions);
            if (!wipUpdateAccepted) throw new Error("Unable to remove WIP markings from WIP batch.");

            req.i = current.i + current.e.length
            const isAccepted = collection.createDocument(collectionLink, req, { disableAutomaticIdGeneration: true }, callback);
            if (!isAccepted) throw new Error("Unable to create WIP batch.");
        } else if (current) {
            // Append the new events into the current batch
            Array.prototype.push.apply(current.e, req.e);
            // Replace all the projections
            current.c = req.c;
            // TODO: should remove only projections being superseded

            // as we've mutated the document in ways that can trigger loss, out write needs to be contingent on no competing updates having taken place
            const reqOptions = { etag: current._etag };
            const isAccepted = collection.replaceDocument(current._self, current, reqOptions, callback);
            if (!isAccepted) throw new Error("Unable to replace WIP batch.");
        } else {
            req.i = 0
            const isAccepted = collection.createDocument(collectionLink, req, { disableAutomaticIdGeneration: true }, callback);
            if (!isAccepted) throw new Error("Unable to create WIP batch.");
        }
    }
}"""

    [<RequireQualifiedAccess; NoEquality; NoComparison>]
    type Result =
        | Written of Position
        | Conflict of Position * events: IOrderedEvent[]
        | ConflictUnknown of Position

    let private run (client: IDocumentClient) (stream: Stream) (expectedVersion: int64 option, req: WipBatch, maxEvents: int)
        : Async<float*Result> = async {
        let sprocLink = sprintf "%O/sprocs/%s" stream.collectionUri sprocName
        let opts = Client.RequestOptions(PartitionKey=PartitionKey(stream.name))
        let! ct = Async.CancellationToken
        let ev = match expectedVersion with Some ev -> Position.FromIndexOnly ev | None -> Position.FromAppendAtEnd
        let! (res : Client.StoredProcedureResponse<SyncResponse>) =
            client.ExecuteStoredProcedureAsync(sprocLink, opts, ct, box req, box ev.index, box maxEvents) |> Async.AwaitTaskCorrect

        let newPos = { index = res.Response.nextI; etag = Option.ofObj res.Response.etag }
        return res.RequestCharge, res.Response.conflicts |> function
            | null -> Result.Written newPos
            | [||] when newPos.index = 0L -> Result.Conflict (newPos, Array.empty)
            | [||] -> Result.ConflictUnknown newPos
            | xs  -> Result.Conflict (newPos, Enum.Events (ev.index, xs) |> Array.ofSeq) }

    let private logged client (stream: Stream) (expectedVersion, req: WipBatch, maxEvents) (log : ILogger)
        : Async<Result> = async {
        let verbose = log.IsEnabled Events.LogEventLevel.Debug
        let log = if verbose then log |> Log.propEvents (Enum.Events req) |> Log.propDataProjections req.c else log
        let (Log.BatchLen bytes), count = Enum.Events req, req.e.Length
        let log = log |> Log.prop "bytes" bytes
        let writeLog =
            log |> Log.prop "stream" stream.name |> Log.prop "expectedVersion" expectedVersion
                |> Log.prop "count" req.e.Length |> Log.prop "pcount" req.c.Length
        let! t, (ru,result) = run client stream (expectedVersion, req, maxEvents) |> Stopwatch.Time
        let resultLog =
            let mkMetric ru : Log.Measurement = { stream = stream.name; interval = t; bytes = bytes; count = count; ru = ru }
            let logConflict () = writeLog.Information("Eqx TrySync Conflict writing {eventTypes}", [| for x in req.e -> x.t |])
            match result with
            | Result.Written pos ->
                log |> Log.event (Log.WriteSuccess (mkMetric ru)) |> Log.prop "nextExpectedVersion" pos
            | Result.ConflictUnknown pos ->
                logConflict ()
                log |> Log.event (Log.WriteConflict (mkMetric ru)) |> Log.prop "nextExpectedVersion" pos |> Log.prop "conflict" true
            | Result.Conflict (pos, xs) ->
                logConflict ()
                let log = if verbose then log |> Log.prop "nextExpectedVersion" pos |> Log.propData "conflicts" xs else log
                log |> Log.event (Log.WriteResync(mkMetric ru)) |> Log.prop "conflict" true
        resultLog.Information("Eqx {action:l} {count}+{pcount} {ms}ms rc={ru}", "Write", req.e.Length, req.c.Length, (let e = t.Elapsed in e.TotalMilliseconds), ru)
        return result }

    let batch (log : ILogger) retryPolicy client pk batch: Async<Result> =
        let call = logged client pk batch
        Log.withLoggedRetries retryPolicy "writeAttempt" call log
    let mkBatch (stream: Events.Stream) (events: IEvent[]) projections : WipBatch =
        {   p = stream.name; id = Store.WipBatch.WellKnownDocumentId; i = -1L(*Server-managed*); _etag = null
            e = [| for e in events -> { c = DateTimeOffset.UtcNow; t = e.EventType; d = e.Data; m = e.Meta } |]
            c = Array.ofSeq projections }
    let mkProjections baseIndex (projectionEvents: IEvent seq) : Store.Projection seq =
        projectionEvents |> Seq.mapi (fun i x -> { i = baseIndex + int64 i; t = x.EventType; d = x.Data; m = x.Meta } : Store.Projection)

    module Initialization =
        open System.Collections.ObjectModel
        let createDatabase (client:IDocumentClient) dbName = async {
            let opts = Client.RequestOptions(ConsistencyLevel = Nullable ConsistencyLevel.Session)
            let! db = client.CreateDatabaseIfNotExistsAsync(Database(Id=dbName), options = opts) |> Async.AwaitTaskCorrect
            return db.Resource.Id }

        let createCollection (client: IDocumentClient) (dbUri: Uri) collName ru = async {
            let pkd = PartitionKeyDefinition()
            pkd.Paths.Add(sprintf "/%s" Store.Batch.PartitionKeyField)
            let colld = DocumentCollection(Id = collName, PartitionKey = pkd)

            colld.IndexingPolicy.IndexingMode <- IndexingMode.Consistent
            colld.IndexingPolicy.Automatic <- true
            // Can either do a blacklist or a whitelist
            // Given how long and variable the blacklist would be, we whitelist instead
            colld.IndexingPolicy.ExcludedPaths <- Collection [|ExcludedPath(Path="/*")|]
            // NB its critical to index the nominated PartitionKey field defined above or there will be runtime errors
            colld.IndexingPolicy.IncludedPaths <- Collection [| for k in Store.Batch.IndexedFields -> IncludedPath(Path=sprintf "/%s/?" k) |]
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
            return! createProc log client collUri
        }

module private Read =
    let private getIndex (client: IDocumentClient) (stream: Stream, maybePos: Position option) =
        let coll = DocDbCollection(client, stream.collectionUri)
        let ac = match maybePos with Some { etag=Some etag } -> Client.AccessCondition(Type=Client.AccessConditionType.IfNoneMatch, Condition=etag) | _ -> null
        let ro = Client.RequestOptions(PartitionKey=PartitionKey(stream.name), AccessCondition = ac)
        coll.TryReadDocument(WipBatch.WellKnownDocumentId, ro)
    let private loggedGetIndex (getIndex : Stream * Position option -> Async<_>) (stream: Stream, maybePos: Position option) (log: ILogger) = async {
        let log = log |> Log.prop "stream" stream.name
        let! t, (ru, res : ReadResult<WipBatch>) = getIndex (stream,maybePos) |> Stopwatch.Time
        let log count bytes (f : Log.Measurement -> _) = log |> Log.event (f { stream = stream.name; interval = t; bytes = bytes; count = count; ru = ru })
        match res with
        | ReadResult.NotModified ->
            (log 0 0 Log.IndexNotModified).Information("Eqx {action:l} {res} {ms}ms rc={ru}", "Index", 302, (let e = t.Elapsed in e.TotalMilliseconds), ru)
        | ReadResult.NotFound ->
            (log 0 0 Log.IndexNotFound).Information("Eqx {action:l} {res} {ms}ms rc={ru}", "Index", 404, (let e = t.Elapsed in e.TotalMilliseconds), ru)
        | ReadResult.Found doc ->
            let log =
                let (Log.BatchLen bytes), count = Enum.Projections doc.c, doc.c.Length
                log bytes count Log.Index
            let log = if (not << log.IsEnabled) Events.LogEventLevel.Debug then log else log |> Log.propDataProjections doc.c |> Log.prop "etag" doc._etag
            log.Information("Eqx {action:l} {res} {ms}ms rc={ru}", "Index", 200, (let e = t.Elapsed in e.TotalMilliseconds), ru)
        return ru, res }
    type [<RequireQualifiedAccess; NoComparison; NoEquality>] IndexResult = NotModified | NotFound | Found of Position * IOrderedEvent[]
    /// `pos` being Some implies that the caller holds a cached value and hence is ready to deal with IndexResult.UnChanged
    let tryLoadIndex (log : ILogger) retryPolicy client (stream: Stream, maybePos: Position option): Async<IndexResult> = async {
        let getIndex = getIndex client
        let! _rc, res = Log.withLoggedRetries retryPolicy "readAttempt" (loggedGetIndex getIndex (stream,maybePos)) log
        match res with
        | ReadResult.NotModified -> return IndexResult.NotModified
        | ReadResult.NotFound -> return IndexResult.NotFound
        | ReadResult.Found doc ->
            let pos' = Position.FromDocument(doc.i + int64 doc.e.Length, doc._etag)
            return IndexResult.Found (pos', Enum.EventsAndProjections doc |> Array.ofSeq) }

    open Microsoft.Azure.Documents.Linq
    let private genBatchesQuery (client : IDocumentClient) (stream: Stream, pos: Position option) (direction: Direction) batchSize =
        let querySpec =
            match pos with
            | None -> SqlQuerySpec("SELECT * FROM c ORDER BY c.i " + if direction = Direction.Forward then "ASC" else "DESC")
            | Some p ->
                let f = if direction = Direction.Forward then "c.i >= @id ORDER BY c.i ASC" else "c.i < @id ORDER BY c.i DESC"
                SqlQuerySpec( "SELECT * FROM c WHERE " + f, SqlParameterCollection [SqlParameter("@id", p.index)])
        let feedOptions = new Client.FeedOptions(PartitionKey=PartitionKey(stream.name), MaxItemCount=Nullable batchSize)
        client.CreateDocumentQuery<Batch>(stream.collectionUri, querySpec, feedOptions).AsDocumentQuery()

    // Unrolls the Batches in a response - note when reading backawards, the events are emitted in reverse order of index
    let private handleSlice (stream: Stream, startPos: Position option) direction (query: IDocumentQuery<Batch>) (log: ILogger)
        : Async<IOrderedEvent[] * Position option * float> = async {
        let! ct = Async.CancellationToken
        let! t, (res : Client.FeedResponse<Batch>) = query.ExecuteNextAsync<Batch>(ct) |> Async.AwaitTaskCorrect |> Stopwatch.Time
        let batches, ru = Array.ofSeq res, res.RequestCharge
        let events = batches |> Seq.collect Enum.Events |> Array.ofSeq
        if direction = Direction.Backward then Array.Reverse events // NB no Seq.rev in old FSharp.Core
        let (Log.BatchLen bytes), count = events, events.Length
        let reqMetric : Log.Measurement = { stream = stream.name; interval = t; bytes = bytes; count = count; ru = ru }
        let evt = Log.Slice (direction, reqMetric)
        let log = if (not << log.IsEnabled) Events.LogEventLevel.Debug then log else log |> Log.propEvents events
        let index = if count = 0 then Nullable () else Nullable <| Seq.min (seq { for x in batches -> x.i })
        (log |> Log.prop "startIndex" (match startPos with Some { index = i } -> Nullable i | _ -> Nullable()) |> Log.prop "bytes" bytes |> Log.event evt)
            .Information("Eqx {action:l} {count}/{batches} {direction} {ms}ms i={index} rc={ru}",
                "Query", count, batches.Length, direction, (let e = t.Elapsed in e.TotalMilliseconds), index, ru)
        // TODO we should be able to trap the etag from the -1 document with some tweaking
        let todoEtag = null
        let pos = match index with HasValue i -> Position.FromDocument(i,todoEtag) |> Some | Null -> None
        return events, pos, ru }

    let private runBatchesQuery (log : ILogger) (readSlice: IDocumentQuery<Batch> -> ILogger -> Async<IOrderedEvent[] * Position option * float>)
            (maxPermittedBatchReads: int option)
            (query: IDocumentQuery<Batch>)
        : AsyncSeq<IOrderedEvent[] * Position option * float> =
        let rec loop batchCount : AsyncSeq<IOrderedEvent[] * Position option * float> = asyncSeq {
            match maxPermittedBatchReads with
            | Some mpbr when batchCount >= mpbr -> log.Information "batch Limit exceeded"; invalidOp "batch Limit exceeded"
            | _ -> ()

            let batchLog = log |> Log.prop "batchIndex" batchCount
            let! slice = readSlice query batchLog
            yield slice
            if query.HasMoreResults then
                yield! loop (batchCount + 1) }
        loop 0

    let logBatchRead direction batchSize streamName interval (responsesCount, events : IOrderedEvent []) nextI (ru: float) (log : ILogger) =
        let (Log.BatchLen bytes), count = events, events.Length
        let reqMetric : Log.Measurement = { stream = streamName; interval = interval; bytes = bytes; count = count; ru = ru }
        let action = match direction with Direction.Forward -> "LoadF" | Direction.Backward -> "LoadB"
        let evt = Log.Event.Batch (direction, responsesCount, reqMetric)
        (log |> Log.prop "bytes" bytes |> Log.prop "batchSize" batchSize |> Log.event evt).Information(
            "Eqx {action:l} {stream} v{nextI} {count}/{responses} {ms}ms rc={ru}",
            action, streamName, Option.toNullable nextI, count, responsesCount, (let e = interval.Elapsed in e.TotalMilliseconds), ru)

    let loadFrom (log : ILogger) retryPolicy client direction batchSize maxPermittedBatchReads (stream: Stream, pos: Position)
        : Async<StopwatchInterval * int * IOrderedEvent[] * float> = async {
        let mutable ru = 0.0
        let mutable responses = 0
        let mergeBatches (batches: AsyncSeq<IOrderedEvent[] * Position option * float>) = async {
            let! (events : IOrderedEvent[]) =
                batches
                |> AsyncSeq.map (fun (events, _maybePos, r) -> ru <- ru + r; responses <- responses + 1; events)
                |> AsyncSeq.concatSeq
                |> AsyncSeq.toArrayAsync
            return events, ru }
        use query = genBatchesQuery client (stream,Some pos) direction batchSize
        let pullSlice = handleSlice (stream,Some pos) direction
        let retryingLoggingReadSlice query = Log.withLoggedRetries retryPolicy "readAttempt" (pullSlice query)
        let log = log |> Log.prop "batchSize" batchSize |> Log.prop "direction" direction |> Log.prop "stream" stream.name
        let slices : AsyncSeq<IOrderedEvent[] * Position option * float> = runBatchesQuery log retryingLoggingReadSlice maxPermittedBatchReads query
        let! t, (events, ru) = mergeBatches slices |> Stopwatch.Time
        query.Dispose()
        return t, responses, events, ru }

    let loadForwardsFrom (log : ILogger) retryPolicy client batchSize maxPermittedBatchReads (stream: Stream, pos: Position)
        : Async<Position * IOrderedEvent[]> = async {
        let direction = Direction.Forward
        let! t, responses, events, ru = loadFrom log retryPolicy client direction batchSize maxPermittedBatchReads (stream,pos)
        let nextI = if events.Length = 0 then 0L else events.[events.Length-1].Index+1L // No Array.tryLast in older FSharp.Core
        log |> logBatchRead direction batchSize stream.name t (responses,events) (Some nextI) ru
        return { pos with index = nextI }, events }

    let loadBackwardsFrom (log : ILogger) retryPolicy client batchSize maxPermittedBatchReads (stream: Stream, pos: Position)
        : Async<IOrderedEvent[]> = async {
        let direction = Direction.Backward
        let! t, responses, events, ru = loadFrom log retryPolicy client direction batchSize maxPermittedBatchReads (stream,pos)
        log |> logBatchRead direction batchSize stream.name t (responses,events) None ru
        return events }

    let calculateUsedVersusDroppedPayload firstUsedEventIndex (xs: IOrderedEvent[]) : int * int =
        let mutable used, dropped = 0, 0
        for x in xs do
            let (Log.EventLen bytes) = x
            if x.Index >= firstUsedEventIndex then used <- used + bytes
            else dropped <- dropped + bytes
        used, dropped
    let loadBackwardsUntilCompactionOrStart (log : ILogger) retryPolicy client batchSize maxPermittedBatchReads isCompactionEvent (stream: Stream)
        : Async<Position * IOrderedEvent[]> = async {
        let mutable responseCount = 0
        let mergeFromCompactionPointOrStartFromBackwardsStream (log : ILogger) (batchesBackward : AsyncSeq<IOrderedEvent[] * Position option * float>)
            : Async<IOrderedEvent[] * Position * float> = async {
            let mutable lastSlice = None
            let mutable maybeFirstPos = None
            let mutable ru = 0.0
            let! tempBackward =
                batchesBackward
                |> AsyncSeq.map (fun (events, maybePos, r) ->
                    if maybeFirstPos = None then maybeFirstPos <- maybePos
                    lastSlice <- Some events; ru <- ru + r
                    responseCount <- responseCount + 1
                    events)
                |> AsyncSeq.concatSeq
                |> AsyncSeq.takeWhileInclusive (fun x ->
                    if not (isCompactionEvent x) then true // continue the search
                    else
                        match lastSlice with
                        | None -> log.Information("Eqx Stop stream={stream} at={index}", stream.name, x.Index)
                        | Some batch ->
                            let used, residual = batch |> calculateUsedVersusDroppedPayload x.Index
                            log.Information("Eqx Stop stream={stream} at={index} used={used} residual={residual}", stream.name, x.Index, used, residual)
                        false)
                |> AsyncSeq.toArrayAsync
            let eventsForward = Array.Reverse(tempBackward); tempBackward // sic - relatively cheap, in-place reverse of something we own
            return eventsForward, (match maybeFirstPos with Some pos -> pos | None -> Position.FromEmptyStream), ru }
        let direction = Direction.Backward
        use query = genBatchesQuery client (stream,None) direction batchSize
        let pullSlice = handleSlice (stream,None) direction
        let retryingLoggingReadSlice query = Log.withLoggedRetries retryPolicy "readAttempt" (pullSlice query)
        let log = log |> Log.prop "batchSize" batchSize |> Log.prop "stream" stream.name
        let readlog = log |> Log.prop "direction" direction
        let batchesBackward : AsyncSeq<IOrderedEvent[] * Position option * float> = runBatchesQuery readlog retryingLoggingReadSlice maxPermittedBatchReads query
        let! t, (events, pos, ru) = mergeFromCompactionPointOrStartFromBackwardsStream log batchesBackward |> Stopwatch.Time
        query.Dispose()
        let version = if events.Length = 0 then 0L else events.[events.Length-1].Index+1L // No Array.tryLast in older FSharp.Core // the merge put them in order so this is correct
        log |> logBatchRead direction batchSize stream.name t (responseCount,events) (Some version) ru
        return pos, events }

module UnionEncoderAdapters =
    let private mkEvent (x : UnionCodec.EncodedUnion<byte[]>) : IEvent =
        { new IEvent with
            member __.EventType = x.caseName
            member __.Data = x.payload
            member __.Meta = null }
    let encodeEvent (codec : UnionCodec.IUnionEncoder<'event, byte[]>) (x : 'event) : IEvent =
        codec.Encode x |> mkEvent
    let decodeKnownEvents (codec : UnionCodec.IUnionEncoder<'event, byte[]>) (xs : IOrderedEvent seq) : 'event seq =
        xs |> Seq.choose (fun x -> codec.TryDecode { caseName = x.EventType; payload = x.Data })

type [<NoComparison>]Token = { stream: Stream; pos: Position; rollingSnapshotEventIndex: int64 option; batchCapacityLimit: int option }

module Token =
    let private create rollingSnapshotEventIndex batchCapacityLimit stream pos : Storage.StreamToken =
        { value = box { stream = stream; pos = pos; rollingSnapshotEventIndex = rollingSnapshotEventIndex; batchCapacityLimit = batchCapacityLimit } }
    /// No batching / compaction; we only need to retain the StreamVersion
    let ofNonCompacting (stream,pos) : Storage.StreamToken =
        create None None stream pos
    // headroom before compaction is necessary given the stated knowledge of the last (if known) `rollingSnapshotEventIndexption`
    let private batchCapacityLimit maybeSnapshotEventIndex unstoredEventsPending (windowSize: int) (pos: Position) : int =
        match maybeSnapshotEventIndex with
        | Some (rollingSnapshotEventIndex : int64) -> (windowSize - unstoredEventsPending) - int (pos.index - rollingSnapshotEventIndex + 1L) |> max 0
        | None -> (windowSize - unstoredEventsPending) - int pos.index |> max 0
    let (*private*) ofRollingSnapshotEventIndex maybeSnapshotEventIndex unstoredEventsPending batchSize (stream,pos) : Storage.StreamToken =
        let batchCapacityLimit = batchCapacityLimit maybeSnapshotEventIndex unstoredEventsPending batchSize pos
        create maybeSnapshotEventIndex (Some batchCapacityLimit) stream pos
    /// Assume we have not seen any compaction events; use the batchSize and version to infer headroom
    let ofUncompactedVersion batchSize pos : Storage.StreamToken =
        ofRollingSnapshotEventIndex None 0 batchSize pos
    let (|Unpack|) (token: Storage.StreamToken) : Token = unbox<Token> token.value
    /// Use previousToken plus the data we are adding and the position we are adding it to infer a headroom
    let ofPreviousTokenAndEventsLength (Unpack previousToken) eventsLength batchSize pos : Storage.StreamToken =
        let rollingSnapshotEventIndexOption = previousToken.rollingSnapshotEventIndex
        ofRollingSnapshotEventIndex rollingSnapshotEventIndexOption eventsLength batchSize pos
    let ofPreviousTokenWithUpdatedPosition (Unpack previousToken) batchSize pos : Storage.StreamToken =
        let rollingSnapshotEventIndexOption = previousToken.rollingSnapshotEventIndex
        ofRollingSnapshotEventIndex rollingSnapshotEventIndexOption 0 batchSize pos
    /// Use an event just read from the stream to infer headroom
    let ofCompactionResolvedEventAndVersion (compactionEvent: IOrderedEvent) batchSize pos : Storage.StreamToken =
        ofRollingSnapshotEventIndex (Some compactionEvent.Index) 0 batchSize pos
    /// Use an event we are about to write to the stream to infer headroom
    let ofPreviousStreamVersionAndCompactionEventDataIndex prevPos compactionEventDataIndex eventsLength batchSize streamVersion' : Storage.StreamToken =
        ofRollingSnapshotEventIndex (Some (prevPos.index + int64 compactionEventDataIndex)) eventsLength batchSize streamVersion'
    let supersedes (Unpack current) (Unpack x) =
        let currentVersion, newVersion = current.pos.index, x.pos.index
        let currentETag, newETag = current.pos.etag, x.pos.etag
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
    type InternalSyncResult = Written of Storage.StreamToken | ConflictUnknown of Storage.StreamToken | Conflict of Storage.StreamToken * IOrderedEvent[]

    [<RequireQualifiedAccess; NoComparison; NoEquality>]
    type LoadFromTokenResult = Unchanged | Found of Storage.StreamToken * IOrderedEvent[]

    [<NoComparison; NoEquality; RequireQualifiedAccess>]
    type SearchStrategy<'event> =
        | EventType of string
        | Predicate of ('event -> bool)

/// Defines the policies in force for retrying with regard to transient failures calling CosmosDb (as opposed to application level concurrency conflicts)
type EqxConnection(client: IDocumentClient, ?readRetryPolicy (*: (int -> Async<'T>) -> Async<'T>*), ?writeRetryPolicy) =
    member __.Client = client
    member __.ReadRetryPolicy = readRetryPolicy
    member __.WriteRetryPolicy = writeRetryPolicy
    member __.Close = (client :?> Client.DocumentClient).Dispose()

/// Defines the policies in force regarding how to a) split up calls b) limit the number of events per slice
type EqxBatchingPolicy
    (   // Max items to request in query response. Defaults to 10.
        ?defaultMaxSlices : int,
        // Dynamic version of `defaultMaxSlices`, allowing one to react to dynamic configuration changes. Default to using `defaultMaxSlices`
        ?getDefaultMaxSlices : unit -> int,
        /// Maximum number of trips to permit when slicing the work into multiple responses based on `MaxSlices`. Default: unlimited.
        ?maxCalls,
        /// Maximum number of events to accumualte within the `WipBatch` before switching to a new one when adding Events. Defaults to 10.
        ?maxEventsPerSlice) =
    let getDefaultMaxSlices = defaultArg getDefaultMaxSlices (fun () -> defaultArg defaultMaxSlices 10)
    /// Limit for Maximum number of `Batch` records in a single query batch response
    member __.MaxSlices = getDefaultMaxSlices ()
    /// Maximum number of trips to permit when slicing the work into multiple responses based on `MaxSlices`
    member __.MaxCalls = maxCalls
    /// Maximum number of events to accumualte within the `WipBatch` before switching to a new one when adding Events
    member __.MaxEventsPerSlice = defaultArg maxEventsPerSlice 10

type EqxGateway(conn : EqxConnection, batching : EqxBatchingPolicy) =
    let (|EventTypePredicate|) predicate (x:IOrderedEvent) = predicate x.EventType
    let (|IEventDataArray|) events = [| for e in events -> e :> IOrderedEvent |]
    member __.LoadForward log batchingOverride maybeRollingSnapshotPredicate (stream: Stream, pos: Position)
        : Async<Storage.StreamToken * IOrderedEvent[]> = async {
        let batching = defaultArg batchingOverride batching
        let! pos, events = Read.loadForwardsFrom log conn.ReadRetryPolicy conn.Client batching.MaxSlices batching.MaxCalls (stream,pos)
        match maybeRollingSnapshotPredicate with
        | None -> return Token.ofNonCompacting (stream,pos), events
        | Some (EventTypePredicate isCompactionEvent) ->
            match events |> Array.tryFindBack isCompactionEvent with
            | None -> return Token.ofUncompactedVersion batching.MaxSlices (stream,pos), events
            | Some resolvedEvent -> return Token.ofCompactionResolvedEventAndVersion resolvedEvent batching.MaxSlices (stream,pos), events }
    member __.LoadBackward log batchingOverride (stream: Stream, pos: Position)
        : Async<IOrderedEvent[]> = async {
        let batching = defaultArg batchingOverride batching
        return! Read.loadBackwardsFrom log conn.ReadRetryPolicy conn.Client batching.MaxSlices batching.MaxCalls (stream,pos) }
    member __.LoadBackwardsStoppingAtCompactionEvent log (EventTypePredicate isCompactionEvent) stream
        : Async<Storage.StreamToken * IOrderedEvent[]> = async {
        let! pos, events = Read.loadBackwardsUntilCompactionOrStart log conn.ReadRetryPolicy conn.Client batching.MaxSlices batching.MaxCalls isCompactionEvent stream
        match Array.tryHead events |> Option.filter isCompactionEvent with
        | None -> return Token.ofUncompactedVersion batching.MaxSlices (stream,pos), events
        | Some resolvedEvent -> return Token.ofCompactionResolvedEventAndVersion resolvedEvent batching.MaxSlices (stream,pos), events }
    member private __.InterpretIndexOrFallback log (EventTypePredicate isRelevantProjectionOrRollingSnapshot as etp) stream res
        : Async<Storage.StreamToken * IOrderedEvent[]> = async {
        match res with
        | Read.IndexResult.NotModified  -> return invalidOp "Not handled"
        | Read.IndexResult.Found (pos, projectionsAndEvents) when projectionsAndEvents |> Array.exists isRelevantProjectionOrRollingSnapshot ->
            return Token.ofNonCompacting (stream,pos), projectionsAndEvents
        | _ -> return! __.LoadBackwardsStoppingAtCompactionEvent log etp stream }
    member __.IndexedOrBatched log isCompactionEventType (stream,maybePos)
        : Async<Storage.StreamToken * IOrderedEvent[]> = async {
        let! res = Read.tryLoadIndex log None(* TODO conn.ReadRetryPolicy*) conn.Client (stream,maybePos)
        return! __.InterpretIndexOrFallback log isCompactionEventType stream res }
    member __.GetPosition(log, stream, ?pos)
        : Async<Storage.StreamToken> = async {
        let! res = Read.tryLoadIndex log None(* TODO conn.ReadRetryPolicy*) conn.Client (stream,pos)
        match res with
        | Read.IndexResult.NotFound -> return Token.ofNonCompacting(stream,Position.FromEmptyStream)
        | Read.IndexResult.NotModified -> return Token.ofNonCompacting (stream, pos.Value)
        | Read.IndexResult.Found (pos, _projectionsAndEvents) -> return Token.ofNonCompacting (stream,pos) }
    member __.LoadFromToken log (Token.Unpack token as streamToken) maybeRollingSnapshotOrProjectionPredicate tryIndex
        : Async<LoadFromTokenResult> = async {
        let ok r = LoadFromTokenResult.Found r
        if not tryIndex then
            let! pos, ((IEventDataArray xs) as events) =
                Read.loadForwardsFrom log conn.ReadRetryPolicy conn.Client batching.MaxSlices batching.MaxCalls (token.stream,token.pos)
            let ok t = ok (t,xs)
            match maybeRollingSnapshotOrProjectionPredicate with
            | None -> return ok (Token.ofNonCompacting (token.stream,token.pos))
            | Some (EventTypePredicate isCompactionEvent) ->
                match events |> Array.tryFindBack isCompactionEvent with
                | None -> return ok (Token.ofPreviousTokenAndEventsLength streamToken events.Length batching.MaxSlices (token.stream,token.pos))
                | Some resolvedEvent -> return ok (Token.ofCompactionResolvedEventAndVersion resolvedEvent batching.MaxSlices (token.stream,pos))
        else
            let! res = Read.tryLoadIndex log None(* TODO conn.ReadRetryPolicy*) conn.Client (token.stream,Some token.pos)
            match res with
            | Read.IndexResult.NotModified  ->
                return LoadFromTokenResult.Unchanged
            | _ ->
                let! loaded = __.InterpretIndexOrFallback log maybeRollingSnapshotOrProjectionPredicate.Value token.stream res
                return ok loaded }
    member __.TrySync log (Token.Unpack token as streamToken) maybeRollingSnapshotPredicate (expectedVersion, batch: Store.WipBatch)
        : Async<InternalSyncResult> = async {
        let! wr = Sync.batch log conn.WriteRetryPolicy conn.Client token.stream (expectedVersion,batch,batching.MaxEventsPerSlice)
        match wr with
        | Sync.Result.Conflict (pos',events) ->
            return InternalSyncResult.Conflict (Token.ofPreviousTokenAndEventsLength streamToken events.Length batching.MaxSlices (token.stream,pos'),events)
        | Sync.Result.ConflictUnknown pos' ->
            return InternalSyncResult.ConflictUnknown (Token.ofPreviousTokenWithUpdatedPosition streamToken batching.MaxSlices (token.stream,pos'))
        | Sync.Result.Written pos' ->

        let token =
            match maybeRollingSnapshotPredicate with
            | None -> Token.ofNonCompacting (token.stream,pos')
            | Some isCompactionEvent ->
                match batch.e |> Array.tryFindIndexBack (fun x -> isCompactionEvent x.t) with
                | None -> Token.ofPreviousTokenAndEventsLength streamToken batch.e.Length batching.MaxSlices (token.stream,pos')
                | Some compactionEventIndex ->
                    Token.ofPreviousStreamVersionAndCompactionEventDataIndex token.pos compactionEventIndex batch.e.Length batching.MaxSlices (token.stream,pos')
        return InternalSyncResult.Written token }

type EqxCollection(gateway : EqxGateway, databaseId, collectionId) =
    member __.Gateway = gateway
    member __.CollectionUri = Client.UriFactory.CreateDocumentCollectionUri(databaseId, collectionId)

type private CompactionContext(eventsLen : int, capacityBeforeCompaction : int) =
    /// Determines whether writing a Compaction event is warranted (based on the existing state and the current `Accumulated` changes)
    member __.IsCompactionDue = eventsLen > capacityBeforeCompaction

type private Category<'event, 'state>
    (   coll : EqxCollection,
        codec : UnionCodec.IUnionEncoder<'event, byte[]>,
        ?access : AccessStrategy<'event,'state>) =
    let compactionPredicate =
        match access with
        | None -> None
        | Some (AccessStrategy.IndexedSearch (predicate,_)) -> Some predicate
        | Some AccessStrategy.EventsAreState -> Some (fun _ -> true)
        | Some (AccessStrategy.RollingSnapshots (et,_)) -> Some ((=) et)
    let response (fold: 'state -> 'event seq -> 'state) initial token events =
        token, fold initial (UnionEncoderAdapters.decodeKnownEvents codec events)
    let load (fold: 'state -> 'event seq -> 'state) initial loadF = async {
        let! token, events = loadF
        return response fold initial token events }
    member __.Load (fold: 'state -> 'event seq -> 'state) (initial: 'state) streamName (log : ILogger)
        : Async<Storage.StreamToken * 'state> =
        let stream = Stream.Create(coll.CollectionUri, streamName)
        let forward = load fold initial (coll.Gateway.LoadForward log None None (stream,Position.FromIndexOnly 0L))
        let compacted predicate = load fold initial (coll.Gateway.LoadBackwardsStoppingAtCompactionEvent log predicate stream)
        let indexed predicate = load fold initial (coll.Gateway.IndexedOrBatched log predicate (stream,None))
        match access with
        | None -> forward
        | Some (AccessStrategy.IndexedSearch (predicate,_)) -> indexed predicate
        | Some AccessStrategy.EventsAreState -> compacted (fun _ -> true)
        | Some (AccessStrategy.RollingSnapshots (et,_)) -> compacted ((=) et)
    member __.LoadFromToken (fold: 'state -> 'event seq -> 'state) (initial: 'state) (state: 'state) token (log : ILogger)
        : Async<Storage.StreamToken * 'state> = async {
        let indexed = match access with Some (AccessStrategy.IndexedSearch _) -> true | _ -> false
        let! res = coll.Gateway.LoadFromToken log token compactionPredicate indexed
        match res with
        | LoadFromTokenResult.Unchanged -> return token, state
        | LoadFromTokenResult.Found (token,events ) -> return response fold initial token events }
    member __.TrySync (fold: 'state -> 'event seq -> 'state) initial (log : ILogger)
            (Token.Unpack token as streamToken, expectedVersion : int64 option, state : 'state)
            (events : 'event list, state' : 'state)
        : Async<Storage.SyncResult<'state>> = async {
        let eventsIncludingSnapshots, projections =
            match access with
            | None | Some AccessStrategy.EventsAreState ->
                Seq.ofList events, Seq.empty
            | Some (AccessStrategy.RollingSnapshots (_,f)) ->
                let cc = CompactionContext(List.length events, token.batchCapacityLimit.Value)
                (if cc.IsCompactionDue then Seq.append events [f state'] |> Seq.cache else Seq.ofList events), Seq.empty
            | Some (AccessStrategy.IndexedSearch (_,index)) ->
                Seq.ofList events, index state'
        let encode = UnionEncoderAdapters.encodeEvent codec
        let encodedEvents, projections = Seq.map encode events |> Array.ofSeq, Seq.map encode projections
        let baseIndex = token.pos.index + int64 events.Length
        let projections = Sync.mkProjections baseIndex projections
        let batch = Sync.mkBatch token.stream encodedEvents projections
        let! syncRes = coll.Gateway.TrySync log streamToken compactionPredicate (expectedVersion,batch)
        match syncRes with
        | InternalSyncResult.Conflict (token',events) -> return Storage.SyncResult.Conflict (async { return response fold initial token' events })
        | InternalSyncResult.ConflictUnknown token' -> return Storage.SyncResult.Conflict (__.LoadFromToken fold initial state token' log)
        | InternalSyncResult.Written token' -> return Storage.SyncResult.Written (token', fold state eventsIncludingSnapshots) }

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
            member __.TrySync (log : ILogger) (Token.Unpack token as streamToken, state) (events : 'event list, state' : 'state) : Async<Storage.SyncResult<'state>> = async {
                let! syncRes = inner.TrySync log (streamToken, state) (events,state')
                match syncRes with
                | Storage.SyncResult.Conflict resync ->             return Storage.SyncResult.Conflict (interceptAsync resync token.stream.name)
                | Storage.SyncResult.Written (token', state') ->    return Storage.SyncResult.Written (intercept token.stream.name (token', state')) }

    let applyCacheUpdatesWithSlidingExpiration
            (cache: Cache)
            (prefix: string)
            (slidingExpiration : TimeSpan)
            (category: ICategory<'event, 'state>)
            : ICategory<'event, 'state> =
        let policy = new CacheItemPolicy(SlidingExpiration = slidingExpiration)
        let addOrUpdateSlidingExpirationCacheEntry streamName = CacheEntry >> cache.UpdateIfNewer policy (prefix + streamName)
        CategoryTee<'event,'state>(category, addOrUpdateSlidingExpirationCacheEntry) :> _

type private Folder<'event, 'state>(category : Category<'event, 'state>, fold: 'state -> 'event seq -> 'state, initial: 'state, ?readCache) =
    let loadAlgorithm streamName initial log =
        let batched = category.Load fold initial streamName log
        let cached token state = category.LoadFromToken fold initial state token log
        match readCache with
        | None -> batched
        | Some (cache : Caching.Cache, prefix : string) ->
            match cache.TryGet(prefix + streamName) with
            | None -> batched
            | Some (token, state) -> cached token state
    interface ICategory<'event, 'state> with
        member __.Load streamName (log : ILogger) : Async<Storage.StreamToken * 'state> =
            loadAlgorithm streamName initial log
        member __.TrySync (log : ILogger) (Token.Unpack token as t, state) (events : 'event list, state': 'state) : Async<Storage.SyncResult<'state>> = async {
            let! syncRes = category.TrySync fold initial log (t, Some token.pos.index, state) (events,state')
            match syncRes with
            | Storage.SyncResult.Conflict resync ->         return Storage.SyncResult.Conflict resync
            | Storage.SyncResult.Written (token',state') -> return Storage.SyncResult.Written (token',state') }

[<NoComparison; NoEquality; RequireQualifiedAccess>]
type CachingStrategy =
    | SlidingWindow of Caching.Cache * window: TimeSpan
    /// Prefix is used to distinguish multiple folds per stream
    | SlidingWindowPrefixed of Caching.Cache * window: TimeSpan * prefix: string

type EqxStreamBuilder<'event, 'state>(gateway : EqxGateway, codec, fold, initial, ?access, ?caching) =
    member __.Create (databaseId, collectionId, streamName) : Equinox.IStream<'event, 'state> =
        let category = Category<'event, 'state>(EqxCollection(gateway, databaseId, collectionId), codec, ?access = access)

        let readCacheOption =
            match caching with
            | None -> None
            | Some (CachingStrategy.SlidingWindow(cache, _)) -> Some(cache, null)
            | Some (CachingStrategy.SlidingWindowPrefixed(cache, _, prefix)) -> Some(cache, prefix)
        let folder = Folder<'event, 'state>(category, fold, initial, ?readCache = readCacheOption)

        let category : ICategory<_,_> =
            match caching with
            | None -> folder :> _
            | Some (CachingStrategy.SlidingWindow(cache, window)) ->
                Caching.applyCacheUpdatesWithSlidingExpiration cache null window folder
            | Some (CachingStrategy.SlidingWindowPrefixed(cache, window, prefix)) ->
                Caching.applyCacheUpdatesWithSlidingExpiration cache prefix window folder

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
            log.ForContext("Uri", uri).Information("Eqx connecting to Cosmos with Connection Name {connectionName}", sanitizedName)
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
    | Conflict of index: 't * conflictingEvents: IOrderedEvent[]
    | ConflictUnknown of index: 't

/// Encapsulates the core facilites Equinox.Cosmos offers for operating directly on Events in Streams.
type EqxContext
    (   /// Connection to CosmosDb with DocumentDb Transient Read and Write Retry policies
        conn : EqxConnection,
        /// Default Cosmos Database name for this context
        databaseId,
        /// Default Cosmos Collection name for this context
        collectionId,
        /// Logger to write to - see https://github.com/serilog/serilog/wiki/Provided-Sinks for how to wire to your logger
        logger : Serilog.ILogger,
        /// Optional maximum number of Store.Batch records to retrieve as a set (how many Events are placed therein is controlled by maxEventsPerSlice).
        /// Defaults to 10
        ?defaultMaxSlices,
        /// Threshold defining the number of events a slice is allowed to hold before switching to a new Batch is triggered.
        /// Defaults to 100
        ?maxEventsPerSlice,
        /// Alternate way of specifying defaultMaxSlices which facilitates reading it from a cached dynamic configuration
        ?getDefaultMaxSlices) =
    let getDefaultMaxSlices = match getDefaultMaxSlices with Some f -> f | None -> fun () -> defaultArg defaultMaxSlices 10
    let batching = EqxBatchingPolicy(getDefaultMaxSlices=getDefaultMaxSlices, maxEventsPerSlice=defaultArg maxEventsPerSlice 100)
    let gateway = EqxGateway(conn, batching)

    member __.CreateStream(streamName,?dbId,?collId) =
        let collection = EqxCollection(gateway, defaultArg dbId databaseId, defaultArg collId collectionId)
        Stream.Create(collection.CollectionUri, streamName)

    member internal __.GetInternal((stream,pos) as streamPos, ?batchSize, ?direction) = async {
        let direction = defaultArg direction Direction.Forward
        let batching = batchSize |> Option.map (fun max -> EqxBatchingPolicy(defaultMaxSlices=max))
        let! data =
            match direction with
            | Direction.Backward -> gateway.LoadBackward logger batching streamPos
            | Direction.Forward -> async {
                // TODO fix query so we can honor start position
                let pos = Position.FromIndexOnly 0L
                // TOCONSIDER provide a way to send out the token
                let! (_token, data: IOrderedEvent[]) = gateway.LoadForward logger batching None (stream,pos)
                return data }
        // TODO fix algorithm so we don't need to do this
        match direction with
        | Direction.Backward -> return data |> Seq.skipWhile (fun e -> e.Index > (snd streamPos).index) |> Array.ofSeq
        | Direction.Forward -> return data |> Seq.skipWhile (fun e -> e.Index < (snd streamPos).index) |> Array.ofSeq }

    /// Establishes the current position of the stream in as effficient a manner as possible
    /// (The ideal situation is that the preceding token is supplied as input in order to avail of 1RU low latency state checks)
    member __.Sync(stream, ?position: Position) : Async<Position> = async {
        //let indexed predicate = load fold initial (coll.Gateway.IndexedOrBatched log predicate (stream,None))
        let! (Token.Unpack { pos = pos' }) = gateway.GetPosition(logger, stream, ?pos=position)
        return pos' }

    /// Reads in batches of `batchSize` from the specified `Position`, allowing the reader to efficiently walk way from a running query
    member __.Walk(stream, position, batchSize, ?direction) : AsyncSeq<IOrderedEvent[]> = asyncSeq {
        let! res = __.GetInternal((stream, position), batchSize, ?direction=direction)
        // TODO add laziness
        return AsyncSeq.ofSeq res }

    /// Reads all Events from a `Position` in a given `direction`, ideally
    member __.Read(stream, position, ?batchSize, ?direction) : Async<IOrderedEvent[]> =
        __.GetInternal((stream, position), ?batchSize = batchSize, ?direction=direction)

    /// Appends the supplied batch of events, subject to a consistency check based on the `position`
    /// Callers should implement appropriate idempotent handling, or use Equinox.Handler for that purpose
    member __.Sync(stream, position, events: IEvent[]) : Async<AppendResult<Position>> = async {
        let token = Token.ofNonCompacting (stream,position)
        let batch = Sync.mkBatch stream events Seq.empty
        let! res = gateway.TrySync logger token None (Some position.index,batch)
        match res with
        | InternalSyncResult.Written (Token.Unpack token) -> return AppendResult.Ok token.pos
        | InternalSyncResult.Conflict (Token.Unpack token,events) -> return AppendResult.Conflict (token.pos, events)
        | InternalSyncResult.ConflictUnknown (Token.Unpack token) -> return AppendResult.ConflictUnknown token.pos }

    /// Low level, non-idempotent call appending events to a stream without a concurrency control mechanism in play
    /// NB Should be used sparingly; Equinox.Handler enables building equivalent equivalent idempotent handling with minimal code.
    member __.NonIdempotentAppend(stream, events: IEvent[]) : Async<Position> = async {
        let! res = __.Sync(stream, Position.FromAppendAtEnd, events)
        match res with
        | AppendResult.Ok token -> return token
        | x -> return x |> sprintf "Conflict despite it being disabled %A" |> invalidOp }

[<AutoOpen>]
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

    /// Returns an async sequence of events in the stream starting at the specified sequence number,
    /// reading in batches of the specified size.
    /// Returns an empty sequence if the stream is empty or if the sequence number is larger than the largest
    /// sequence number in the stream.
    let getAll (ctx: EqxContext) (streamName: string) (index: int64) (batchSize: int): AsyncSeq<IOrderedEvent[]> =
        ctx.Walk(ctx.CreateStream streamName, Position.FromIndexOnly index, batchSize)

    /// Returns an async array of events in the stream starting at the specified sequence number,
    /// number of events to read is specified by batchSize
    /// Returns an empty sequence if the stream is empty or if the sequence number is larger than the largest
    /// sequence number in the stream.
    let get (ctx: EqxContext) (streamName: string) (index: int64) (batchSize: int): Async<IOrderedEvent[]> =
        ctx.Read(ctx.CreateStream streamName, Position.FromIndexOnly index, batchSize)

    /// Appends a batch of events to a stream at the specified expected sequence number.
    /// If the specified expected sequence number does not match the stream, the events are not appended
    /// and a failure is returned.
    let append (ctx: EqxContext) (streamName: string) (index: int64) (events: IEvent[]): Async<AppendResult<int64>> =
        ctx.Sync(ctx.CreateStream streamName, Position.FromIndexOnly index, events) |> stripSyncResult

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
    let getAllBackwards (ctx: EqxContext) (streamName: string) (index: int64) (batchSize: int): AsyncSeq<IOrderedEvent[]> =
        ctx.Walk(ctx.CreateStream streamName, Position.FromIndexOnly index, batchSize, Direction.Backward)

    /// Returns an async array of events in the stream backwards starting from the specified sequence number,
    /// number of events to read is specified by batchSize
    /// Returns an empty sequence if the stream is empty or if the sequence number is smaller than the smallest
    /// sequence number in the stream.
    let getBackwards (ctx: EqxContext) (streamName: string) (index: int64) (batchSize: int): Async<IOrderedEvent[]> =
        ctx.Read(ctx.CreateStream streamName, Position.FromIndexOnly index, batchSize, Direction.Backward)

    /// Obtains the `index` from the current write Position
    let getNextIndex (ctx: EqxContext) (streamName: string) : Async<int64> =
        ctx.Sync(ctx.CreateStream streamName) |> stripPosition