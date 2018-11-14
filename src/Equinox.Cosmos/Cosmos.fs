namespace Equinox.Cosmos

open Equinox
open Equinox.Store
open FSharp.Control
open Microsoft.Azure.Documents
open Newtonsoft.Json
open Serilog
open System

[<AutoOpen>]
module Json =
    open Newtonsoft.Json.Linq

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
            Convert.ToBase64String(output.ToArray())
        let unpickle str : byte[] =
            if str = null then null else

            let compressedBytes = Convert.FromBase64String str
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

module Store =
    /// Position and Etag to which a Batch is relative
    [<NoComparison>]
    type Position = { collectionUri: Uri; streamName: string; index: int64 option; etag: string option } with
        member __.Index : int64 = defaultArg __.index -1L

    [<RequireQualifiedAccess>]
    type Direction = Forward | Backward with
        override this.ToString() = match this with Forward -> "Forward" | Backward -> "Backward"

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
            c: DateTimeOffset // ISO 8601

            /// The Event Type, used to drive deserialization
            t: string // required

            /// Event body, as UTF-8 encoded json ready to be injected into the Json being rendered for DocDb
            [<JsonConverter(typeof<VerbatimUtf8JsonConverter>)>]
            d: byte[] // required

            /// Optional metadata, as UTF-8 encoded json, ready to emit directly (null, not written if missing)
            [<JsonConverter(typeof<VerbatimUtf8JsonConverter>)>]
            [<JsonProperty(Required=Required.Default, NullValueHandling=NullValueHandling.Ignore)>]
            m: byte[] } // optional

    /// The Special 'Pending' Batch Format
    /// a) `id` = `-1`
    /// b) contains projections (`c`)
    /// NB this Type does double duty as
    /// a) transport for when we read it
    /// b) a way of encoding a batch that the stored procedure will write in to the actual document
    type [<NoEquality; NoComparison; JsonObject(ItemRequired=Required.Always)>]
        WipBatch =
        {   /// Partition key, as per Batch
            p: string // "{streamName}"
            /// Document Id within partition, as per Batch
            id: string // "{-1}" - Well known IdConstant used while this remains the pending batch

            /// When we read, we need to capture the value so we can retain it for caching purposes
            /// NB this is not relevant to fill in when we pass it to the writing stored procedure
            /// as it will do it's own 1. read 2. merge 3. write merged version contingent on the _etag not having changed
            [<JsonProperty(DefaultValueHandling=DefaultValueHandling.Ignore,Required=Required.Default)>]
            _etag: string

            /// base 'i' value for the Events held herein
            i: int64 // {index}

            /// Events
            e: BatchEvent[]

            /// Projections
            c: Projection[] }
        /// TOCONSIDER maybe this should be a high nember to reflect fact it is the freshest ?
        static member IdConstant = "-1"
    /// Projection based on the state at a given point in time `i`
    and [<CLIMutable>]
        Projection =
        {   /// Base: Max index rolled into this projection
            i: int64

            /// Indicates whether this is actually an event being retained to support a lagging projection
            x: bool

            /// The Event Type of this compaction/snapshot, used to drive deserialization
            t: string // required

            /// Event body - Json -> UTF-8 -> Deflate -> Base64
            [<JsonConverter(typeof<Base64ZipUtf8JsonConverter>)>]
            d: byte[] // required

            /// Optional metadata, same encoding as `d` (can be null; not written if missing)
            [<JsonConverter(typeof<Base64ZipUtf8JsonConverter>)>]
            [<JsonProperty(Required=Required.Default, NullValueHandling=NullValueHandling.Ignore)>]
            m: byte[] } // optional

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

    type Enum() =
        static member Events (b:WipBatch) =
            b.e |> Seq.mapi (fun offset x ->
            { new IOrderedEvent with
                member __.Index = b.i + int64 offset
                member __.IsProjection = false
                member __.EventType = x.t
                member __.Data = x.d
                member __.Meta = x.m })
        static member Events (b:Batch) =
            b.e |> Seq.mapi (fun offset x ->
                { new IOrderedEvent with
                    member __.Index = b.i + int64 offset
                    member __.IsProjection = false
                    member __.EventType = x.t
                    member __.Data = x.d
                    member __.Meta = x.m })
        static member Projections (xs: Projection[]) = seq {
            for x in xs -> { new IOrderedEvent with
                member __.Index = x.i
                member __.IsProjection = not x.x
                member __.EventType = x.t
                member __.Data = x.d
                member __.Meta = x.m } }
        static member EventsAndProjections (x:WipBatch): IOrderedEvent seq =
            Enum.Events x
            |> Seq.append (Enum.Projections x.c)
            // where Index is equal, projections get delivered after the events so the fold semantics can be 'idempotent'
            |> Seq.sortBy (fun x -> x.Index, x.IsProjection)

    let mkBatch (pos: Position) (events: IEvent[]) (projections: Projection[]) : WipBatch =
        {   p = pos.streamName; id = WipBatch.IdConstant
            i = pos.Index; _etag = null
            e = [| for e in events -> { c = DateTimeOffset.UtcNow; t = e.EventType; d = e.Data; m = e.Meta } |]
            c = projections }
    let mkProjection baseIndex canDiscard (e: IEvent) = { i = baseIndex; x = canDiscard; t = e.EventType; d = e.Data; m = e.Meta }

open Store

module Log =
    [<NoEquality; NoComparison>]
    type Measurement = { stream: string; interval: StopwatchInterval; bytes: int; count: int; ru: float }
    [<NoEquality; NoComparison>]
    type Event =
        | WriteSuccess of Measurement
        | WriteConflict of Measurement
        /// Individual read request in a Batch
        | Slice of Store.Direction * Measurement
        /// Individual read request for the Index
        | Index of Measurement
        /// Individual read request for the Index, not found
        | IndexNotFound of Measurement
        /// Index read with Single RU Request Charge due to correct use of etag in cache
        | IndexNotModified of Measurement
        /// Summarizes a set of Slices read together
        | Batch of Store.Direction * slices: int * Measurement
    let prop name value (log : ILogger) = log.ForContext(name, value)
    let propData name (events: #Store.IEvent seq) (log : ILogger) =
        let items = seq { for e in events do yield sprintf "{\"%s\": %s}" e.EventType (System.Text.Encoding.UTF8.GetString e.Data) }
        log.ForContext(name, sprintf "[%s]" (String.concat ",\n\r" items))
    let propEvents = propData "events"
    let propEventsBatch (x : Batch) log = log |> propData "events" (Store.Enum.Events x)
    let propEventsWipBatch (x : WipBatch) log = log |> propData "events" (Store.Enum.Events x)
    let propDataProjections = Store.Enum.Projections >> propData "projections"

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
    let (|EventLen|) (x: #Store.IEvent) = let (BlobLen bytes), (BlobLen metaBytes) = x.Data, x.Meta in bytes+metaBytes
    let (|BatchLen|) = Seq.sumBy (|EventLen|)

[<RequireQualifiedAccess; NoEquality; NoComparison>]
type EqxSyncResult =
    | Written of Store.Position
    | Conflict of Store.Position * events: Store.IOrderedEvent[]
    | ConflictUnknown of Store.Position

// NB don't nest in a private module, or serialization will fail miserably ;)
[<CLIMutable; NoEquality; NoComparison; JsonObject(ItemRequired=Required.AllowNull)>]
type WriteResponse = { etag: string; i: int64; conflicts: Store.Projection[] }

module private Write =
    let [<Literal>] sprocName = "EquinoxIndexedWrite"
    let [<Literal>] sprocBody = """function indexedWrite(docs, expectedVersion, etag, index) {
    var response = getContext().getResponse();
    var collection = getContext().getCollection();
    var collectionLink = collection.getSelfLink();
    if (!docs) throw new Error("docs argument is missing.");
    if (index) {
        function callback(err, doc, options) {
            if (err) throw err;
            response.setBody({ etag: doc._etag, conflicts: null });
        }
        if (-1 == expectedVersion) {
            collection.createDocument(collectionLink, index, { disableAutomaticIdGeneration : true}, callback);
        } else {
            collection.replaceDocument(collection.getAltLink() + "/docs/" + index.id, index, callback);
        }
    } else {
        // call always expects a parseable json response with `etag` and `conflicts`
        // can also contain { conflicts: [{t, d}] } representing conflicting events since expectedVersion
        // null/missing signifies events have been written, with no conflict
        response.setBody({ etag: null, conflicts: null });
    }
    for (var i=0; i<docs.length; i++) {
        collection.createDocument(collectionLink, docs[i]);
    }
}"""

    let private run (client: IDocumentClient) (pos: Store.Position) (batch: Store.WipBatch): Async<float*EqxSyncResult> = async {
        let sprocLink = sprintf "%O/sprocs/%s" pos.collectionUri sprocName
        let opts = Client.RequestOptions(PartitionKey=PartitionKey(pos.streamName))
        let! ct = Async.CancellationToken
        let! (res : Client.StoredProcedureResponse<WriteResponse>) = client.ExecuteStoredProcedureAsync(sprocLink, opts, ct, box batch) |> Async.AwaitTaskCorrect
        match res.RequestCharge, (match res.Response.etag with null -> None | x -> Some x), res.Response.conflicts with
        | rc,e,null -> return rc, EqxSyncResult.Written { pos with index = Some res.Response.i; etag=e }
        | rc,e,[||] -> return rc, EqxSyncResult.ConflictUnknown { pos with index = Some res.Response.i; etag=e }
        | rc,e, xs  -> return rc, EqxSyncResult.Conflict ({ pos with index = Some res.Response.i; etag=e }, Store.Enum.Projections xs |> Array.ofSeq) }

    let private logged client (pos : Store.Position) (batch: Store.WipBatch) (log : ILogger): Async<EqxSyncResult> = async {
        let verbose = log.IsEnabled Events.LogEventLevel.Debug
        let log = if verbose then log |> Log.propEvents (Store.Enum.Events batch) |> Log.propDataProjections batch.c else log
        let (Log.BatchLen bytes), count = Store.Enum.Events batch, batch.e.Length
        let log = log |> Log.prop "bytes" bytes
        let writeLog = log |> Log.prop "stream" pos.streamName |> Log.prop "expectedVersion" pos.Index |> Log.prop "count" batch.e.Length |> Log.prop "pcount" batch.c.Length
        let! t, (ru,result) = run client pos batch |> Stopwatch.Time
        let resultLog =
            let mkMetric ru : Log.Measurement = { stream = pos.streamName; interval = t; bytes = bytes; count = count; ru = ru }
            let logConflict () = writeLog.Information("Eqx TrySync WrongExpectedVersion writing {eventTypes}", [| for x in batch.e -> x.t |])
            match result with
            | EqxSyncResult.Written pos ->
                log |> Log.event (Log.WriteSuccess (mkMetric ru)) |> Log.prop "nextExpectedVersion" pos
            | EqxSyncResult.ConflictUnknown pos ->
                logConflict ()
                log |> Log.event (Log.WriteConflict (mkMetric ru)) |> Log.prop "nextExpectedVersion" pos |> Log.prop "conflict" true
            | EqxSyncResult.Conflict (pos, xs) ->
                logConflict ()
                let log = if verbose then log |> Log.prop "nextExpectedVersion" pos |> Log.propData "conflicts" xs else log
                log |> Log.event (Log.WriteConflict (mkMetric ru)) |> Log.prop "conflict" true
        resultLog.Information("Eqx {action:l} {count}+{pcount} {ms}ms rc={ru}", "Write", batch.e.Length, batch.c.Length, (let e = t.Elapsed in e.TotalMilliseconds), ru)
        return result }

    let batch (log : ILogger) retryPolicy client pk (batch): Async<EqxSyncResult> =
        let call = logged client pk batch
        Log.withLoggedRetries retryPolicy "writeAttempt" call log

module private Read =
    let private getIndex (client : IDocumentClient) (pos:Store.Position) =
        let coll = DocDbCollection(client, pos.collectionUri)
        let ac = match pos.etag with None -> null | Some etag-> Client.AccessCondition(Type=Client.AccessConditionType.IfNoneMatch, Condition=etag)
        let ro = Client.RequestOptions(PartitionKey=PartitionKey(pos.streamName), AccessCondition = ac)
        coll.TryReadDocument(Store.WipBatch.IdConstant, ro)
    let private loggedGetIndex (getIndex : Store.Position -> Async<_>) (pos:Store.Position)  (log: ILogger) = async {
        let log = log |> Log.prop "stream" pos.streamName
        let! t, (ru, res : ReadResult<Store.WipBatch>) = getIndex pos |> Stopwatch.Time
        let log count bytes (f : Log.Measurement -> _) = log |> Log.event (f { stream = pos.streamName; interval = t; bytes = bytes; count = count; ru = ru })
        match res with
        | ReadResult.NotModified ->
            (log 0 0 Log.IndexNotModified).Information("Eqx {action:l} {res} {ms}ms rc={ru}", "Index", 302, (let e = t.Elapsed in e.TotalMilliseconds), ru)
        | ReadResult.NotFound ->
            (log 0 0 Log.IndexNotFound).Information("Eqx {action:l} {res} {ms}ms rc={ru}", "Index", 404, (let e = t.Elapsed in e.TotalMilliseconds), ru)
        | ReadResult.Found doc ->
            let log =
                let (Log.BatchLen bytes), count = Store.Enum.Projections doc.c, doc.c.Length
                log bytes count Log.Index
            let log = if (not << log.IsEnabled) Events.LogEventLevel.Debug then log else log |> Log.propDataProjections doc.c |> Log.prop "etag" doc._etag
            log.Information("Eqx {action:l} {res} {ms}ms rc={ru}", "Index", 200, (let e = t.Elapsed in e.TotalMilliseconds), ru)
        return ru, res }
    type [<RequireQualifiedAccess; NoComparison; NoEquality>] IndexResult = NotModified | NotFound | Found of Store.Position * Store.IOrderedEvent[]
    /// `pos` being Some implies that the caller holds a cached value and hence is ready to deal with IndexResult.UnChanged
    let loadIndex (log : ILogger) retryPolicy client (pos : Store.Position): Async<IndexResult> = async {
        let getIndex = getIndex client
        let! _rc, res = Log.withLoggedRetries retryPolicy "readAttempt" (loggedGetIndex getIndex pos) log
        match res with
        | ReadResult.NotModified -> return IndexResult.NotModified
        | ReadResult.NotFound -> return IndexResult.NotFound
        | ReadResult.Found doc ->
            let pos' = { pos with index = Some (doc.i + int64 doc.e.Length); etag=if doc._etag=null then None else Some doc._etag }
            return IndexResult.Found (pos', Store.Enum.EventsAndProjections doc |> Array.ofSeq) }

    open Microsoft.Azure.Documents.Linq
    let private genBatchesQuery (client : IDocumentClient) (pos:Store.Position) (direction: Direction) batchSize =
        let querySpec =
            match pos.index with
            | None -> SqlQuerySpec("SELECT * FROM c WHERE c.i >= 0 ORDER BY c.i " + if direction = Direction.Forward then "ASC" else "DESC")
            | Some index ->
                let f = if direction = Direction.Forward then "c.i >= @id ORDER BY c.i ASC" else "c.i < @id AND c.i >= 0 ORDER BY c.i DESC"
                SqlQuerySpec( "SELECT * FROM c WHERE " + f, SqlParameterCollection [SqlParameter("@id", index)])
        let feedOptions = new Client.FeedOptions(PartitionKey=PartitionKey(pos.streamName), MaxItemCount=Nullable batchSize)
        client.CreateDocumentQuery<Store.Batch>(pos.collectionUri, querySpec, feedOptions).AsDocumentQuery()

    // Unrolls the Batches in a response - note when reading backawards, the events are emitted in reverse order of index
    let private handleSlice (pos:Store.Position) direction (query: IDocumentQuery<Store.Batch>) (log: ILogger): Async<Store.IOrderedEvent[] * float> = async {
        let! ct = Async.CancellationToken
        let! t, (res : Client.FeedResponse<Store.Batch>) = query.ExecuteNextAsync<Store.Batch>(ct) |> Async.AwaitTaskCorrect |> Stopwatch.Time
        let batches, ru = Array.ofSeq res, res.RequestCharge
        let events = batches |> Seq.collect Store.Enum.Events |> Array.ofSeq
        if direction = Direction.Backward then Array.Reverse events // NB no Seq.rev in old FSharp.Core
        let (Log.BatchLen bytes), count = events, events.Length
        let reqMetric : Log.Measurement = { stream = pos.streamName; interval = t; bytes = bytes; count = count; ru = ru }
        let evt = Log.Slice (direction, reqMetric)
        let log = if (not << log.IsEnabled) Events.LogEventLevel.Debug then log else log |> Log.propEvents events
        let index = if count = 0 then Nullable () else Nullable <| Seq.min (seq { for x in batches -> x.i })
        (log |> Log.prop "startIndex" pos.Index |> Log.prop "bytes" bytes |> Log.event evt)
            .Information("Eqx {action:l} {count}/{batches} {direction} {ms}ms i={index} rc={ru}",
                "Query", count, batches.Length, direction, (let e = t.Elapsed in e.TotalMilliseconds), index, ru)
        return events, ru }

    let private runBatchesQuery (log : ILogger) (readSlice: IDocumentQuery<Store.Batch> -> ILogger -> Async<Store.IOrderedEvent[] * float>)
            (maxPermittedBatchReads: int option)
            (query: IDocumentQuery<Store.Batch>)
        : AsyncSeq<Store.IOrderedEvent[] * float> =
        let rec loop batchCount : AsyncSeq<Store.IOrderedEvent[] * float> = asyncSeq {
            match maxPermittedBatchReads with
            | Some mpbr when batchCount >= mpbr -> log.Information "batch Limit exceeded"; invalidOp "batch Limit exceeded"
            | _ -> ()

            let batchLog = log |> Log.prop "batchIndex" batchCount
            let! slice = readSlice query batchLog
            yield slice
            if query.HasMoreResults then
                yield! loop (batchCount + 1) }
        loop 0

    let logBatchRead direction batchSize streamName interval (responsesCount, events : Store.IOrderedEvent []) version (ru: float) (log : ILogger) =
        let (Log.BatchLen bytes), count = events, events.Length
        let reqMetric : Log.Measurement = { stream = streamName; interval = interval; bytes = bytes; count = count; ru = ru }
        let action = match direction with Direction.Forward -> "LoadF" | Direction.Backward -> "LoadB"
        let evt = Log.Event.Batch (direction, responsesCount, reqMetric)
        (log |> Log.prop "bytes" bytes |> Log.prop "batchSize" batchSize |> Log.event evt).Information(
            "Eqx {action:l} {stream} v{version} {count}/{responses} {ms}ms rc={ru}",
            action, streamName, version, count, responsesCount, (let e = interval.Elapsed in e.TotalMilliseconds), ru)

    let loadForwardsFrom (log : ILogger) retryPolicy client batchSize maxPermittedBatchReads pos: Async<Store.Position * Store.IOrderedEvent[]> = async {
        let mutable ru = 0.0
        let mutable responses = 0
        let mergeBatches (batches: AsyncSeq<Store.IOrderedEvent[] * float>) = async {
            let! (events : Store.IOrderedEvent[]) =
                batches
                |> AsyncSeq.map (fun (events, r) -> ru <- ru + r; responses <- responses + 1; events)
                |> AsyncSeq.concatSeq
                |> AsyncSeq.toArrayAsync
            return events, ru }
        let direction = Direction.Forward
        use query = genBatchesQuery client pos direction batchSize
        let pullSlice = handleSlice pos direction
        let retryingLoggingReadSlice query = Log.withLoggedRetries retryPolicy "readAttempt" (pullSlice query)
        let log = log |> Log.prop "batchSize" batchSize |> Log.prop "direction" direction |> Log.prop "stream" pos.streamName
        let slices : AsyncSeq<Store.IOrderedEvent[] * float> = runBatchesQuery log retryingLoggingReadSlice maxPermittedBatchReads query
        let! t, (events, ru) = mergeBatches slices |> Stopwatch.Time
        query.Dispose()
        let version = if events.Length = 0 then -1L else events.[events.Length].Index // No Array.tryLast in older FSharp.Core
        log |> logBatchRead direction batchSize pos.streamName t (responses,events) version ru
        return { pos with index = Some version }, events }

    let calculateUsedVersusDroppedPayload firstUsedEventNumber (xs: Store.IOrderedEvent[]) : int * int =
        let mutable used, dropped = 0, 0
        for x in xs do
            let (Log.EventLen bytes) = x
            if x.Index >= firstUsedEventNumber then used <- used + bytes
            else dropped <- dropped + bytes
        used, dropped
    let loadBackwardsUntilCompactionOrStart (log : ILogger) retryPolicy client batchSize maxPermittedBatchReads isCompactionEvent (pos : Store.Position)
        : Async<Store.Position * Store.IOrderedEvent[]> = async {
        let mutable responseCount = 0
        let mergeFromCompactionPointOrStartFromBackwardsStream (log : ILogger) (batchesBackward : AsyncSeq<Store.IOrderedEvent[] * float>)
            : Async<Store.IOrderedEvent[] * float> = async {
            let lastSlice = ref None
            let mutable ru = 0.0
            let! tempBackward =
                batchesBackward
                |> AsyncSeq.map (fun (events, r) -> lastSlice := Some events; ru <- ru + r; responseCount <- responseCount + 1; events)
                |> AsyncSeq.concatSeq
                |> AsyncSeq.takeWhileInclusive (fun x ->
                    if not (isCompactionEvent x) then true // continue the search
                    else
                        match !lastSlice with
                        | None -> log.Information("Eqx Stop stream={stream} at={index}", pos.streamName, x.Index)
                        | Some batch ->
                            let used, residual = batch |> calculateUsedVersusDroppedPayload x.Index
                            log.Information("Eqx Stop stream={stream} at={index} used={used} residual={residual}", pos.streamName, x.Index, used, residual)
                        false)
                |> AsyncSeq.toArrayAsync
            let eventsForward = Array.Reverse(tempBackward); tempBackward // sic - relatively cheap, in-place reverse of something we own
            return eventsForward, ru }
        let direction = Direction.Backward
        use query = genBatchesQuery client pos direction batchSize
        let pullSlice = handleSlice pos direction
        let retryingLoggingReadSlice query = Log.withLoggedRetries retryPolicy "readAttempt" (pullSlice query)
        let log = log |> Log.prop "batchSize" batchSize |> Log.prop "stream" pos.streamName
        let readlog = log |> Log.prop "direction" direction
        let batchesBackward : AsyncSeq<Store.IOrderedEvent[] * float> = runBatchesQuery readlog retryingLoggingReadSlice maxPermittedBatchReads query
        let! t, (events, ru) = mergeFromCompactionPointOrStartFromBackwardsStream log batchesBackward |> Stopwatch.Time
        query.Dispose()
        let version = if events.Length = 0 then -1L else events.[events.Length].Index // No Array.tryLast in older FSharp.Core // the merge put them in order so this is correct
        log |> logBatchRead direction batchSize pos.streamName t (responseCount,events) version ru
        return { pos with index = Some version } , events }

module UnionEncoderAdapters =
    let private mkEvent (x : UnionCodec.EncodedUnion<byte[]>) : Store.IEvent =
        { new Store.IEvent with
            member __.EventType = x.caseName
            member __.Data = x.payload
            member __.Meta = null }
    let encodeEvent (codec : UnionCodec.IUnionEncoder<'event, byte[]>) (x : 'event) : Store.IEvent =
        codec.Encode x |> mkEvent
    let decodeKnownEvents (codec : UnionCodec.IUnionEncoder<'event, byte[]>) (xs : Store.IOrderedEvent seq) : 'event seq =
        xs |> Seq.choose (fun x -> codec.TryDecode { caseName = x.EventType; payload = x.Data })

type [<NoComparison>]Token = { pos: Store.Position; rollingSnapshotEventIndex: int64 option }

module Token =
    let private create rollingSnapshotEventIndex batchCapacityLimit pos : Storage.StreamToken =
        { value = box { pos = pos; rollingSnapshotEventIndex = rollingSnapshotEventIndex }; batchCapacityLimit = batchCapacityLimit }
    /// No batching / compaction; we only need to retain the StreamVersion
    let ofNonCompacting (pos : Store.Position) : Storage.StreamToken =
        create None None pos
    // headroom before compaction is necessary given the stated knowledge of the last (if known) `rollingSnapshotEventIndexption`
    let private batchCapacityLimit maybeSnapshotEventIndex unstoredEventsPending (windowSize : int) (streamVersion : int64) : int =
        match maybeSnapshotEventIndex with
        | Some (rollingSnapshotEventIndex : int64) -> (windowSize - unstoredEventsPending) - int (streamVersion - rollingSnapshotEventIndex + 1L) |> max 0
        | None -> (windowSize - unstoredEventsPending) - (int streamVersion + 1) - 1 |> max 0
    let (*private*) ofRollingSnapshotEventIndex maybeSnapshotEventIndex unstoredEventsPending batchSize (pos : Store.Position) : Storage.StreamToken =
        let batchCapacityLimit = batchCapacityLimit maybeSnapshotEventIndex unstoredEventsPending batchSize pos.Index
        create maybeSnapshotEventIndex (Some batchCapacityLimit) pos
    /// Assume we have not seen any compaction events; use the batchSize and version to infer headroom
    let ofUncompactedVersion batchSize pos : Storage.StreamToken =
        ofRollingSnapshotEventIndex None 0 batchSize pos
    /// Use previousToken plus the data we are adding and the position we are adding it to infer a headroom
    let ofPreviousTokenAndEventsLength (previousToken : Storage.StreamToken) eventsLength batchSize pos : Storage.StreamToken =
        let compactedEventNumber = (unbox previousToken.value).rollingSnapshotEventIndex
        ofRollingSnapshotEventIndex compactedEventNumber eventsLength batchSize pos
    let ofPreviousTokenWithUpdatedPosition (previousToken : Storage.StreamToken) batchSize pos : Storage.StreamToken =
        let compactedEventNumber = (unbox previousToken.value).rollingSnapshotEventIndex
        ofRollingSnapshotEventIndex compactedEventNumber 0 batchSize pos
    /// Use an event just read from the stream to infer headroom
    let ofCompactionResolvedEventAndVersion (compactionEvent: Store.IOrderedEvent) batchSize pos : Storage.StreamToken =
        ofRollingSnapshotEventIndex (Some compactionEvent.Index) 0 batchSize pos
    /// Use an event we are about to write to the stream to infer headroom
    let ofPreviousStreamVersionAndCompactionEventDataIndex prevStreamVersion compactionEventDataIndex eventsLength batchSize streamVersion' : Storage.StreamToken =
        ofRollingSnapshotEventIndex (Some (prevStreamVersion + 1L + int64 compactionEventDataIndex)) eventsLength batchSize streamVersion'
    let private unpackEqxStreamVersion (x : Storage.StreamToken) = let x : Token = unbox x.value in x.pos.Index
    let private unpackEqxETag (x : Storage.StreamToken) = let x : Token = unbox x.value in x.pos.etag
    let supersedes current x =
        let currentVersion, newVersion = unpackEqxStreamVersion current, unpackEqxStreamVersion x
        let currentETag, newETag = unpackEqxETag current, unpackEqxETag x
        newVersion > currentVersion || currentETag <> newETag

type EqxConnection(client: IDocumentClient, ?readRetryPolicy (*: (int -> Async<'T>) -> Async<'T>*), ?writeRetryPolicy) =
    member __.Client = client
    member __.ReadRetryPolicy = readRetryPolicy
    member __.WriteRetryPolicy = writeRetryPolicy
    member __.Close = (client :?> Client.DocumentClient).Dispose()

type EqxBatchingPolicy(getMaxBatchSize : unit -> int, ?batchCountLimit) =
    new (maxBatchSize) = EqxBatchingPolicy(fun () -> maxBatchSize)
    member __.BatchSize = getMaxBatchSize()
    member __.MaxBatches = batchCountLimit

[<RequireQualifiedAccess; NoComparison; NoEquality>]
type GatewaySyncResult = Written of Storage.StreamToken | ConflictUnknown of Storage.StreamToken | Conflict of Storage.StreamToken * Store.IOrderedEvent[]

[<RequireQualifiedAccess; NoComparison; NoEquality>]
type LoadFromTokenResult = Unchanged | Found of Storage.StreamToken * Store.IOrderedEvent[]

type EqxGateway(conn : EqxConnection, batching : EqxBatchingPolicy) =
    let (|EventTypePredicate|) predicate (x:Store.IOrderedEvent) = predicate x.EventType
    let (|Pos|) (token: Storage.StreamToken) : Store.Position = (unbox<Token> token.value).pos
    let (|IEventDataArray|) events = [| for e in events -> e :> Store.IOrderedEvent |]
    member __.LoadBatched log maybeRollingSnapshotPredicate (pos : Store.Position)
        : Async<Storage.StreamToken * Store.IOrderedEvent[]> = async {
        let! pos, events = Read.loadForwardsFrom log conn.ReadRetryPolicy conn.Client batching.BatchSize batching.MaxBatches pos
        match maybeRollingSnapshotPredicate with
        | None -> return Token.ofNonCompacting pos, events
        | Some (EventTypePredicate isCompactionEvent) ->
            match events |> Array.tryFindBack isCompactionEvent with
            | None -> return Token.ofUncompactedVersion batching.BatchSize pos, events
            | Some resolvedEvent -> return Token.ofCompactionResolvedEventAndVersion resolvedEvent batching.BatchSize pos, events }
    member __.LoadBackwardsStoppingAtCompactionEvent log (EventTypePredicate isCompactionEvent) pos
        : Async<Storage.StreamToken * Store.IOrderedEvent[]> = async {
        let! pos, events = Read.loadBackwardsUntilCompactionOrStart log conn.ReadRetryPolicy conn.Client batching.BatchSize batching.MaxBatches isCompactionEvent pos
        match Array.tryHead events |> Option.filter isCompactionEvent with
        | None -> return Token.ofUncompactedVersion batching.BatchSize pos, events
        | Some resolvedEvent -> return Token.ofCompactionResolvedEventAndVersion resolvedEvent batching.BatchSize pos, events }
    member private __.InterpretIndexOrFallback log (EventTypePredicate isRelevantProjectionOrRollingSnapshot as etp) pos res
        : Async<Storage.StreamToken * Store.IOrderedEvent[]> = async {
        match res with
        | Read.IndexResult.NotModified  -> return invalidOp "Not handled"
        | Read.IndexResult.Found (pos, projectionsAndEvents) when projectionsAndEvents |> Array.exists isRelevantProjectionOrRollingSnapshot ->
            return Token.ofNonCompacting pos, projectionsAndEvents
        | _ -> return! __.LoadBackwardsStoppingAtCompactionEvent log etp pos }
    member __.IndexedOrBatched log isCompactionEventType pos
        : Async<Storage.StreamToken * Store.IOrderedEvent[]> = async {
        let! res = Read.loadIndex log None(* TODO conn.ReadRetryPolicy*) conn.Client pos
        return! __.InterpretIndexOrFallback log isCompactionEventType pos res }
    member __.LoadFromToken log (Pos pos as token) maybeRollingSnapshotOrProjectionPredicate tryIndex
        : Async<LoadFromTokenResult> = async {
        let ok r = LoadFromTokenResult.Found r
        if not tryIndex then
            let! pos, ((IEventDataArray xs) as events) = Read.loadForwardsFrom log conn.ReadRetryPolicy conn.Client batching.BatchSize batching.MaxBatches pos
            let ok t = ok (t,xs)
            match maybeRollingSnapshotOrProjectionPredicate with
            | None -> return ok (Token.ofNonCompacting pos)
            | Some (EventTypePredicate isCompactionEvent) ->
                match events |> Array.tryFindBack isCompactionEvent with
                | None -> return ok (Token.ofPreviousTokenAndEventsLength token events.Length batching.BatchSize pos)
                | Some resolvedEvent -> return ok (Token.ofCompactionResolvedEventAndVersion resolvedEvent batching.BatchSize pos)
        else
            let! res = Read.loadIndex log None(* TODO conn.ReadRetryPolicy*) conn.Client pos
            match res with
            | Read.IndexResult.NotModified  ->
                return LoadFromTokenResult.Unchanged
            | _ ->
                let! loaded = __.InterpretIndexOrFallback log maybeRollingSnapshotOrProjectionPredicate.Value pos res
                return ok loaded }
    member __.TrySync log (Pos pos as token) (encodedEvents: IEvent[],projections: IEvent seq) maybeRollingSnapshotPredicate
        : Async<GatewaySyncResult> = async {
        let projections = [| for x in projections -> Store.mkProjection (pos.Index + int64 encodedEvents.Length) false x |]
        let batch : Store.WipBatch = Store.mkBatch pos (encodedEvents) projections
        let! wr = Write.batch log conn.WriteRetryPolicy conn.Client pos batch
        match wr with
        | EqxSyncResult.Conflict (pos',events) ->
            return GatewaySyncResult.Conflict (Token.ofPreviousTokenAndEventsLength token events.Length batching.BatchSize pos',events)
        | EqxSyncResult.ConflictUnknown pos' ->
            return GatewaySyncResult.ConflictUnknown (Token.ofPreviousTokenWithUpdatedPosition token batching.BatchSize pos')
        | EqxSyncResult.Written pos' ->

        let token =
            match maybeRollingSnapshotPredicate with
            | None -> Token.ofNonCompacting pos'
            | Some (isCompactionEvent) ->
                match batch.e |> Array.tryFindIndexBack (fun x -> isCompactionEvent x.t) with
                | None -> Token.ofPreviousTokenAndEventsLength token batch.e.Length batching.BatchSize pos'
                | Some compactionEventIndex ->
                    Token.ofPreviousStreamVersionAndCompactionEventDataIndex pos.Index compactionEventIndex batch.e.Length batching.BatchSize pos'
        return GatewaySyncResult.Written token }

type private Collection(gateway : EqxGateway, databaseId, collectionId) =
    member __.Gateway = gateway
    member __.CollectionUri = Client.UriFactory.CreateDocumentCollectionUri(databaseId, collectionId)

[<NoComparison; NoEquality; RequireQualifiedAccess>]
type SearchStrategy<'event> =
    | EventType of string
    | Predicate of ('event -> bool)

[<NoComparison; NoEquality; RequireQualifiedAccess>]
type AccessStrategy<'event,'state> =
    | EventsAreState
    | //[<Obsolete("Superseded by IndexedSearch")>]
      RollingSnapshots of eventType: string * compact: ('state -> 'event)
    | IndexedSearch of (string -> bool) * index: ('state -> 'event seq)

type private CompactionContext(eventsLen : int, capacityBeforeCompaction : int) =
    /// Determines whether writing a Compaction event is warranted (based on the existing state and the current `Accumulated` changes)
    member __.IsCompactionDue = eventsLen > capacityBeforeCompaction

type private Category<'event, 'state>(coll : Collection, codec : UnionCodec.IUnionEncoder<'event, byte[]>, ?access : AccessStrategy<'event,'state>) =
    let (|Pos|) streamName : Store.Position = { collectionUri = coll.CollectionUri; streamName = streamName; index = None; etag = None }
    let compactionPredicate =
        match access with
        | None -> None
        | Some (AccessStrategy.IndexedSearch (predicate,_)) -> Some predicate
        | Some AccessStrategy.EventsAreState -> Some (fun _ -> true)
        | Some (AccessStrategy.RollingSnapshots (et,_)) -> Some ((=) et)
    let load (fold: 'state -> 'event seq -> 'state) initial loadF = async {
        let! token, events = loadF
        return token, fold initial (UnionEncoderAdapters.decodeKnownEvents codec events) }
    let foldI (fold: 'state -> 'event seq -> 'state) initial token events =
        token, fold initial (UnionEncoderAdapters.decodeKnownEvents codec events)
    let loadI (fold: 'state -> 'event seq -> 'state) initial loadF = async {
        let! token, events = loadF
        return foldI fold initial token events }
    let loadAlgorithm fold (pos : Store.Position) initial log =
        let batched = load fold initial (coll.Gateway.LoadBatched log None pos)
        let compacted predicate = load fold initial (coll.Gateway.LoadBackwardsStoppingAtCompactionEvent log predicate pos)
        let indexed predicate = loadI fold initial (coll.Gateway.IndexedOrBatched log predicate pos)
        match access with
        | Some (AccessStrategy.IndexedSearch (predicate,_)) -> indexed predicate
        | None -> batched
        | Some AccessStrategy.EventsAreState -> compacted (fun _ -> true)
        | Some (AccessStrategy.RollingSnapshots (et,_)) -> compacted ((=) et)
    member __.Load (fold: 'state -> 'event seq -> 'state) (initial: 'state) (Pos pos) (log : ILogger) : Async<Storage.StreamToken * 'state> =
        loadAlgorithm fold pos initial log
    member __.LoadFromToken (fold: 'state -> 'event seq -> 'state) (initial: 'state) (state: 'state) token (log : ILogger)
        : Async<Storage.StreamToken * 'state> = async {
        let indexed = match access with Some (AccessStrategy.IndexedSearch _) -> true | _ -> false
        let! res = coll.Gateway.LoadFromToken log token compactionPredicate indexed
        match res with
        | LoadFromTokenResult.Unchanged -> return token, state
        | LoadFromTokenResult.Found (token,events ) -> return foldI fold initial token events }
    member __.TrySync (fold: 'state -> 'event seq -> 'state) initial (log : ILogger)
            (token : Storage.StreamToken, state : 'state)
            (events : 'event list, state' : 'state) : Async<Storage.SyncResult<'state>> = async {
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
        let! syncRes = coll.Gateway.TrySync log token (Seq.map encode eventsIncludingSnapshots |> Array.ofSeq,Seq.map encode projections) compactionPredicate
        match syncRes with
        | GatewaySyncResult.Conflict (token',events) -> return Storage.SyncResult.Conflict (async { return foldI fold initial token' events })
        | GatewaySyncResult.ConflictUnknown token' -> return Storage.SyncResult.Conflict (__.LoadFromToken fold initial state token' log)
        | GatewaySyncResult.Written token' -> return Storage.SyncResult.Written (token', fold state eventsIncludingSnapshots) }

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
            member __.TrySync streamName (log : ILogger) (token, state) (events : 'event list, state' : 'state) : Async<Storage.SyncResult<'state>> = async {
                let! syncRes = inner.TrySync streamName log (token, state) (events,state')
                match syncRes with
                | Storage.SyncResult.Conflict resync ->             return Storage.SyncResult.Conflict (interceptAsync resync streamName)
                | Storage.SyncResult.Written (token', state') ->    return Storage.SyncResult.Written (intercept streamName (token', state')) }

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
        member __.Load (streamName : string) (log : ILogger) : Async<Storage.StreamToken * 'state> =
            loadAlgorithm streamName initial log
        member __.TrySync _streamName(* TODO remove from main interface *) (log : ILogger) (token, state) (events : 'event list, state': 'state) : Async<Storage.SyncResult<'state>> = async {
            let! syncRes = category.TrySync fold initial log (token, state) (events,state')
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
        let category = Category<'event, 'state>(Collection(gateway, databaseId, collectionId), codec, ?access = access)

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

module Initialization =
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
        colld.IndexingPolicy.ExcludedPaths <- System.Collections.ObjectModel.Collection [|ExcludedPath(Path="/*")|]
        // NB its critical to index the nominated PartitionKey field defined above or there will be runtime errors
        colld.IndexingPolicy.IncludedPaths <- System.Collections.ObjectModel.Collection [| for k in Store.Batch.IndexedFields -> IncludedPath(Path=sprintf "/%s/?" k) |]
        let! coll = client.CreateDocumentCollectionIfNotExistsAsync(dbUri, colld, Client.RequestOptions(OfferThroughput=Nullable ru)) |> Async.AwaitTaskCorrect
        return coll.Resource.Id }

    let createProc (client: IDocumentClient) (collectionUri: Uri) = async {
        let def = new StoredProcedure(Id = Write.sprocName, Body = Write.sprocBody)
        return! client.CreateStoredProcedureAsync(collectionUri, def) |> Async.AwaitTaskCorrect |> Async.Ignore }

    let initialize (client : IDocumentClient) dbName collName ru = async {
        let! dbId = createDatabase client dbName
        let dbUri = Client.UriFactory.CreateDatabaseUri dbId
        let! collId = createCollection client dbUri collName ru
        let collUri = Client.UriFactory.CreateDocumentCollectionUri (dbName, collId)
        //let! _aux = createAux client dbUri collName auxRu
        return! createProc client collUri
    }

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