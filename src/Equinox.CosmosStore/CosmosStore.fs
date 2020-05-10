namespace Equinox.CosmosStore.Core

open Azure
open Azure.Cosmos
open Equinox.Core
open FsCodec
open FSharp.Control
open Serilog
open System
open System.Text.Json
open System.Threading

/// A single Domain Event from the array held in a Batch
[<NoEquality; NoComparison>]
type Event = // TODO for STJ v5: All fields required unless explicitly optional
    {   /// Creation datetime (as opposed to system-defined _lastUpdated which is touched by triggers, replication etc.)
        t: DateTimeOffset // ISO 8601

        /// The Case (Event Type); used to drive deserialization
        c: string // required

        /// Event body, as UTF-8 encoded json ready to be injected into the Json being rendered for CosmosDB
        d: JsonElement // TODO for STJ v5: Required, but can be null so Nullary cases can work

        /// Optional metadata, as UTF-8 encoded json, ready to emit directly
        m: JsonElement // TODO for STJ v5: Optional, not serialized if missing

        /// Optional correlationId
        correlationId : string // TODO for STJ v5: Optional, not serialized if missing

        /// Optional causationId
        causationId : string // TODO for STJ v5: Optional, not serialized if missing
    }

    interface IEventData<JsonElement> with
        member __.EventType = __.c
        member __.Data = __.d
        member __.Meta = __.m
        member __.EventId = Guid.Empty
        member __.CorrelationId = __.correlationId
        member __.CausationId = __.causationId
        member __.Timestamp = __.t

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
    /// Unless running in single partition mode (which would restrict us to 10GB per collection)
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

        /// Event body - Json -> UTF-8 -> Deflate -> Base64
        [<JsonCompressedBase64Converter>]
        d: JsonElement // required

        /// Optional metadata, same encoding as `d` (can be null; not written if missing)
        [<JsonCompressedBase64Converter>]
        m: JsonElement  // TODO for STJ v5: Optional, not serialized if missing
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
type Position = { index: int64; etag: string option }

module internal Position =
    /// NB very inefficient compared to FromDocument or using one already returned to you
    let fromI (i: int64) = { index = i; etag = None }
    /// If we have strong reason to suspect a stream is empty, we won't have an etag (and Writer Stored Procedure special cases this)
    let fromKnownEmpty = fromI 0L
    /// Just Do It mode
    let fromAppendAtEnd = fromI -1L // sic - needs to yield -1
    let fromEtag (value : string) = { fromI -2L with etag = Some value }
    /// NB very inefficient compared to FromDocument or using one already returned to you
    let fromMaxIndex (xs: ITimelineEvent<JsonElement>[]) =
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
    static member internal Events(b: Tip) : ITimelineEvent<JsonElement> seq =
        b.e |> Seq.mapi (fun offset x -> FsCodec.Core.TimelineEvent.Create(b.i + int64 offset, x.c, x.d, x.m, Guid.Empty, x.correlationId, x.causationId, x.t))
    static member Events(i: int64, e: Event[], startPos : Position option, direction) : ITimelineEvent<JsonElement> seq = seq {
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
    static member Unfolds(xs: Unfold[]) : ITimelineEvent<JsonElement> seq = seq {
        for x in xs -> FsCodec.Core.TimelineEvent.Create(x.i, x.c, x.d, x.m, Guid.Empty, null, null, x.t, isUnfold=true) }
    static member EventsAndUnfolds(x: Tip): ITimelineEvent<JsonElement> seq =
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
        | Response of Direction * Measurement
        | SyncSuccess of Measurement
        | SyncResync of Measurement
        | SyncConflict of Measurement
    let prop name value (log : ILogger) = log.ForContext(name, value)
    let propData name (events: #IEventData<JsonElement> seq) (log : ILogger) =
        let render = function (j: JsonElement) when j.ValueKind <> JsonValueKind.Null -> j.GetRawText() | _ -> "null"
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
    let (|BlobLen|) = function (j: JsonElement) when j.ValueKind <> JsonValueKind.Null && j.ValueKind <> JsonValueKind.Undefined -> j.GetRawText().Length | _ -> 0
    let (|EventLen|) (x: #IEventData<_>) = let (BlobLen bytes), (BlobLen metaBytes) = x.Data, x.Meta in bytes+metaBytes
    let (|BatchLen|) = Seq.sumBy (|EventLen|)

    /// NB Caveat emptor; this is subject to unlimited change without the major version changing - while the `dotnet-templates` repo will be kept in step, and
    /// the ChangeLog will mention changes, it's critical to not assume that the presence or nature of these helpers be considered stable
    module InternalMetrics =

        module Stats =
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
                static member Restart() =
                    LogSink.Read <- Counter.Create()
                    LogSink.Write <- Counter.Create()
                    LogSink.Resync <- Counter.Create()
                    let span = epoch.Elapsed
                    epoch.Restart()
                    span
                interface Serilog.Core.ILogEventSink with
                    member __.Emit logEvent = logEvent |> function
                        | CosmosMetric (CosmosReadRc stats) -> LogSink.Read.Ingest stats
                        | CosmosMetric (CosmosWriteRc stats) -> LogSink.Write.Ingest stats
                        | CosmosMetric (CosmosResyncRc stats) -> LogSink.Resync.Ingest stats
                        | _ -> ()

        /// Relies on feeding of metrics from Log through to Stats.LogSink
        /// Use Stats.LogSink.Restart() to reset the start point (and stats) where relevant
        let dump (log: Serilog.ILogger) =
            let stats =
              [ "Read", Stats.LogSink.Read
                "Write", Stats.LogSink.Write
                "Resync", Stats.LogSink.Resync ]
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

[<AutoOpen>]
module AzureCosmosWrappers =
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
        | AggregateException (:? CosmosException as ce) -> Some ce
        | _ -> None
    // CosmosDB Error HttpStatusCode extractor
    let (|CosmosStatusCode|) (e : CosmosException) =
        e.Response.Status

    type ReadResult<'T> = Found of 'T | NotFound | NotModified

    type Azure.Core.ResponseHeaders with
        member headers.GetRequestCharge () =
            match headers.TryGetValue("x-ms-request-charge") with
            | true, charge when not <| String.IsNullOrEmpty charge -> float charge
            | _ -> 0.

[<CLIMutable; NoEquality; NoComparison>]
type SyncResponse = { etag: string; n: int64; conflicts: Unfold[] }
type ResourceThroughput =
| Default
| SetIfCreating of int
| ReplaceAlways of int
type [<RequireQualifiedAccess>] Provisioning = Container of throughput: ResourceThroughput | Database of throughput: ResourceThroughput

module SyncStoredProcedure =
    let [<Literal>] defaultName = "EquinoxRollingUnfolds3"  // NB need to rename/number for any breaking change
    let [<Literal>] body = """
// Manages the merging of the supplied Request Batch, fulfilling one of the following end-states
// 1 perform concurrency check (index=-1 -> always append; index=-2 -> check based on .etag; _ -> check .n=.index)
// 2a Verify no current Tip; if so - incoming req.e and defines the `n`ext position / unfolds
// 2b If we already have a tip, move position forward, replace unfolds
// 3 insert a new document containing the events as part of the same batch of work
// 3a in some cases, there are only changes to the `u`nfolds and no `e`vents, in which case no write should happen
function sync(req, expIndex, expEtag) {
    if (!req) throw new Error("Missing req argument");
    const collection = getContext().getCollection();
    const collectionLink = collection.getSelfLink();
    const response = getContext().getResponse();

    // Locate the Tip (-1) batch for this stream (which may not exist)
    const tipDocId = collection.getAltLink() + "/docs/" + req.id;
    const isAccepted = collection.readDocument(tipDocId, {}, function (err, current) {
        // Verify we dont have a conflicting write
        if (expIndex === -1) {
            // For Any mode, we always do an append operation
            executeUpsert(current);
        } else if (!current && ((expIndex === -2 && expEtag !== null) || expIndex > 0)) {
            // If there is no Tip page, the writer has no possible reason for writing at an index other than zero, and an etag exp must be fulfilled
            response.setBody({ etag: null, n: 0, conflicts: [] });
        } else if (current && ((expIndex === -2 && expEtag !== current._etag) || (expIndex !== -2 && expIndex !== current.n))) {
            // if we're working based on etags, the `u`nfolds very likely to bear relevant info as state-bearing unfolds
            // if there are no `u`nfolds, we need to be careful not to yield `conflicts: null`, as that signals a successful write (see below)
            response.setBody({ etag: current._etag, n: current.n, conflicts: current.u || [] });
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

            // as we've mutated the document in a manner that can conflict with other writers, our write needs to be contingent on no competing updates having taken place
            const tipAccepted = collection.replaceDocument(current._self, tip, { etag: current._etag }, callback);
            if (!tipAccepted) throw new Error("Unable to replace Tip.");
        }
        // if there's only a state update involved, we don't do an event-batch write (if we did, they'd trigger uniqueness violations)
        if (req.e.length) {
            // For now, always do an Insert, as Change Feed mechanism does not yet afford us a way to
            // a) guarantee an item per write (multiple consecutive updates can be 'squashed')
            // b) with metadata sufficient for us to determine the items added (only etags, no way to convey i/n in feed item)
            const i = tip.n - req.e.length;
            const batch = { p: tip.p, id: i.toString(), i: i, n: tip.n, e: req.e };
            const batchAccepted = collection.createDocument(collectionLink, batch, { disableAutomaticIdGeneration: true });
            if (!batchAccepted) throw new Error("Unable to insert Batch.");
        }
    }
}"""

module Initialization =
    let internal getOrCreateDatabase (client: CosmosClient) (databaseId: string) (throughput: ResourceThroughput) = async {
        let! ct = Async.CancellationToken
        let! response =
            match throughput with
            | Default -> client.CreateDatabaseIfNotExistsAsync(id = databaseId, cancellationToken = ct) |> Async.AwaitTaskCorrect
            | SetIfCreating value -> client.CreateDatabaseIfNotExistsAsync(id = databaseId, throughput = Nullable(value), cancellationToken = ct) |> Async.AwaitTaskCorrect
            | ReplaceAlways value -> async {
                let! response = client.CreateDatabaseIfNotExistsAsync(id = databaseId, throughput = Nullable(value), cancellationToken = ct) |> Async.AwaitTaskCorrect
                let! _ = response.Database.ReplaceThroughputAsync(value, cancellationToken = ct) |> Async.AwaitTaskCorrect
                return response }
        return response.Database }

    let internal getOrCreateContainer (db: CosmosDatabase) (props: ContainerProperties) (throughput: ResourceThroughput) = async {
        let! ct = Async.CancellationToken
        let! response =
            match throughput with
            | Default -> db.CreateContainerIfNotExistsAsync(props, cancellationToken = ct) |> Async.AwaitTaskCorrect
            | SetIfCreating value -> db.CreateContainerIfNotExistsAsync(props, throughput = Nullable(value), cancellationToken = ct) |> Async.AwaitTaskCorrect
            | ReplaceAlways value -> async {
                let! response = db.CreateContainerIfNotExistsAsync(props, throughput = Nullable(value), cancellationToken = ct) |> Async.AwaitTaskCorrect
                let! _ = response.Container.ReplaceThroughputAsync(value, cancellationToken = ct) |> Async.AwaitTaskCorrect
                return response }
        return response.Container }

    let internal getBatchAndTipContainerProps (containerId: string) =
        let props = ContainerProperties(id = containerId, partitionKeyPath = sprintf "/%s" Batch.PartitionKeyField)
        props.IndexingPolicy.IndexingMode <- IndexingMode.Consistent
        props.IndexingPolicy.Automatic <- true
        // Can either do a blacklist or a whitelist
        // Given how long and variable the blacklist would be, we whitelist instead
        props.IndexingPolicy.ExcludedPaths.Add(ExcludedPath(Path="/*"))
        // NB its critical to index the nominated PartitionKey field defined above or there will be runtime errors
        for k in Batch.IndexedFields do props.IndexingPolicy.IncludedPaths.Add(IncludedPath(Path = sprintf "/%s/?" k))
        props

    let createSyncStoredProcedure (container: CosmosContainer) nameOverride = async {
        let! ct = Async.CancellationToken
        let name = nameOverride |> Option.defaultValue SyncStoredProcedure.defaultName
        try let! r = container.Scripts.CreateStoredProcedureAsync(Scripts.StoredProcedureProperties(name, SyncStoredProcedure.body), cancellationToken = ct) |> Async.AwaitTaskCorrect
            return r.GetRawResponse().Headers.GetRequestCharge()
        with CosmosException ((CosmosStatusCode sc) as e) when sc = int System.Net.HttpStatusCode.Conflict -> return e.Response.Headers.GetRequestCharge() }

    let initializeContainer (client: CosmosClient) (databaseId: string) (containerId: string) (mode: Provisioning) (createStoredProcedure: bool, nameOverride: string option) = async {
        let dbThroughput = match mode with Provisioning.Database throughput -> throughput | _ -> Default
        let containerThroughput = match mode with Provisioning.Container throughput -> throughput | _ -> Default
        let! db = getOrCreateDatabase client databaseId dbThroughput
        let! container = getOrCreateContainer db (getBatchAndTipContainerProps containerId) containerThroughput

        if createStoredProcedure then
            let! (_ru : float) = createSyncStoredProcedure container nameOverride in ()

        return container }

type ContainerGateway(cosmosContainer : CosmosContainer) =

    member val CosmosContainer = cosmosContainer with get

    abstract member GetQueryIteratorByPage<'T> : query: QueryDefinition * ?options: QueryRequestOptions -> AsyncSeq<Page<'T>>
    default __.GetQueryIteratorByPage<'T>(query, ?options) =
        cosmosContainer.GetItemQueryIterator<'T>(query, requestOptions = defaultArg options null).AsPages() |> AsyncSeq.ofAsyncEnum

    abstract member TryReadItem<'T> : docId: string * partitionKey: string * ?options: ItemRequestOptions -> Async<float * ReadResult<'T>>
    default __.TryReadItem<'T>(docId, partitionKey, ?options) = async {
        let partitionKey = PartitionKey partitionKey
        let options = defaultArg options null
        let! ct = Async.CancellationToken
        // TODO use TryReadItemStreamAsync to avoid the exception https://github.com/Azure/azure-cosmos-dotnet-v3/issues/692#issuecomment-521936888
        try let! item = async { return! cosmosContainer.ReadItemAsync<'T>(docId, partitionKey, requestOptions = options, cancellationToken = ct) |> Async.AwaitTaskCorrect }
            // if item.StatusCode = System.Net.HttpStatusCode.NotModified then return item.RequestCharge, NotModified
            // NB `.Document` will NRE if a IfNoneModified precondition triggers a NotModified result
            // else

            return item.GetRawResponse().Headers.GetRequestCharge(), Found item.Value
        with CosmosException (CosmosStatusCode 404 as e) -> return e.Response.Headers.GetRequestCharge(), NotFound
            | CosmosException (CosmosStatusCode 304 as e) -> return e.Response.Headers.GetRequestCharge(), NotModified
            // NB while the docs suggest you may see a 412, the NotModified in the body of the try/with is actually what happens
            | CosmosException (CosmosStatusCode sc as e) when sc = int System.Net.HttpStatusCode.PreconditionFailed -> return e.Response.Headers.GetRequestCharge(), NotModified }

    abstract member ExecuteStoredProcedure: storedProcedureName: string * partitionKey: string * args: obj[] -> Async<Scripts.StoredProcedureExecuteResponse<SyncResponse>>
    default __.ExecuteStoredProcedure(storedProcedureName, partitionKey, args) = async {
        let! ct = Async.CancellationToken
        let partitionKey = PartitionKey partitionKey
        //let args = [| box tip; box index; box (Option.toObj etag)|]
        return! cosmosContainer.Scripts.ExecuteStoredProcedureAsync<SyncResponse>(storedProcedureName, partitionKey, args, cancellationToken = ct) |> Async.AwaitTaskCorrect }

module Sync =

    // NB don't nest in a private module, or serialization will fail miserably ;)
    [<RequireQualifiedAccess; NoEquality; NoComparison>]
    type Result =
        | Written of Position
        | Conflict of Position * events: ITimelineEvent<JsonElement>[]
        | ConflictUnknown of Position

    type [<RequireQualifiedAccess>] Exp = Version of int64 | Etag of string | Any
    let private run (gateway : ContainerGateway, stream : string) (exp, req: Tip)
        : Async<float*Result> = async {
        let ep = match exp with Exp.Version ev -> Position.fromI ev | Exp.Etag et -> Position.fromEtag et | Exp.Any -> Position.fromAppendAtEnd
        let args = [| box req; box ep.index; box (Option.toObj ep.etag)|]
        let! res = gateway.ExecuteStoredProcedure(SyncStoredProcedure.defaultName, stream, args)
        let newPos = { index = res.Value.n; etag = Option.ofObj res.Value.etag }
        return res.GetRawResponse().Headers.GetRequestCharge(), res.Value.conflicts |> function
            | null -> Result.Written newPos
            | [||] when newPos.index = 0L -> Result.Conflict (newPos, Array.empty)
            | [||] -> Result.ConflictUnknown newPos
            | xs -> Result.Conflict (newPos, Enum.Unfolds xs |> Array.ofSeq) }

    let private logged (container,stream) (exp : Exp, req: Tip) (log : ILogger)
        : Async<Result> = async {
        let verbose = log.IsEnabled Serilog.Events.LogEventLevel.Debug
        let log = if verbose then log |> Log.propEvents (Enum.Events req) |> Log.propDataUnfolds req.u else log
        let (Log.BatchLen bytes), count = Enum.Events req, req.e.Length
        let log = log |> Log.prop "bytes" bytes
        let writeLog =
            log |> Log.prop "stream" stream
                |> Log.prop "count" req.e.Length |> Log.prop "ucount" req.u.Length
                |> match exp with
                    | Exp.Etag et ->     Log.prop "expectedEtag" et
                    | Exp.Version ev ->  Log.prop "expectedVersion" ev
                    | Exp.Any ->         Log.prop "expectedVersion" -1
        let! t, (ru,result) = run (container,stream) (exp, req) |> Stopwatch.Time
        let resultLog =
            let mkMetric ru : Log.Measurement = { stream = stream; interval = t; bytes = bytes; count = count; ru = ru }
            let logConflict () = writeLog.Information("EqxCosmos Sync: Conflict writing {eventTypes}", Seq.truncate 5 (seq { for x in req.e -> x.c }))
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

    let batch (log : ILogger) retryPolicy containerStream batch: Async<Result> =
        let call = logged containerStream batch
        Log.withLoggedRetries retryPolicy "writeAttempt" call log

    let mkBatch (stream: string) (events: IEventData<_>[]) unfolds: Tip =
        {   p = stream; id = Tip.WellKnownDocumentId; n = -1L(*Server-managed*); i = -1L(*Server-managed*); _etag = null
            e = [| for e in events -> { t = e.Timestamp; c = e.EventType; d = e.Data; m = e.Meta; correlationId = e.CorrelationId; causationId = e.CausationId } |]
            u = Array.ofSeq unfolds }

    let mkUnfold compress baseIndex (unfolds: IEventData<_> seq) : Unfold seq =
        let compressIfRequested x = if compress then JsonCompressedBase64Converter.Compress x else x
        unfolds
        |> Seq.mapi (fun offset x ->
            {
                i = baseIndex + int64 offset
                c = x.EventType
                d = compressIfRequested x.Data
                m = compressIfRequested x.Meta
                t = DateTimeOffset.UtcNow
            } : Unfold)

module internal Tip =
    let private get (gateway : ContainerGateway, stream : string) (maybePos: Position option) =
        let ro = match maybePos with Some { etag=Some etag } -> ItemRequestOptions(IfNoneMatch=Nullable(Azure.ETag(etag))) | _ -> null
        gateway.TryReadItem(Tip.WellKnownDocumentId, stream, options = ro)
    let private loggedGet (get : ContainerGateway * string -> Position option -> Async<_>) (container,stream) (maybePos: Position option) (log: ILogger) = async {
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
    type [<RequireQualifiedAccess; NoComparison; NoEquality>] Result = NotModified | NotFound | Found of Position * ITimelineEvent<JsonElement>[]
    /// `pos` being Some implies that the caller holds a cached value and hence is ready to deal with IndexResult.NotModified
    let tryLoad (log : ILogger) retryPolicy containerStream (maybePos: Position option): Async<Result> = async {
        let! _rc, res = Log.withLoggedRetries retryPolicy "readAttempt" (loggedGet get containerStream maybePos) log
        match res with
        | ReadResult.NotModified -> return Result.NotModified
        | ReadResult.NotFound -> return Result.NotFound
        | ReadResult.Found tip -> return Result.Found (Position.fromTip tip, Enum.EventsAndUnfolds tip |> Array.ofSeq) }

 module internal Query =
    let private mkQuery (gateway : ContainerGateway, stream: string) maxItems (direction: Direction) startPos : AsyncSeq<Page<Batch>> =
        let query =
            let root = sprintf "SELECT c.id, c.i, c._etag, c.n, c.e FROM c WHERE c.id!=\"%s\"" Tip.WellKnownDocumentId
            let tail = sprintf "ORDER BY c.i %s" (if direction = Direction.Forward then "ASC" else "DESC")
            match startPos with
            | None -> QueryDefinition(sprintf "%s %s" root tail)
            | Some { index = positionSoExclusiveWhenBackward } ->
                let cond = if direction = Direction.Forward then "c.n > @startPos" else "c.i < @startPos"
                QueryDefinition(sprintf "%s AND %s %s" root cond tail).WithParameter("@startPos", positionSoExclusiveWhenBackward)
        let qro = QueryRequestOptions(PartitionKey = Nullable(PartitionKey stream), MaxItemCount=Nullable maxItems)
        gateway.GetQueryIteratorByPage<Batch>(query, options = qro)

    // Unrolls the Batches in a response - note when reading backwards, the events are emitted in reverse order of index
    let private processNextPage direction (streamName: string) startPos (enumerator: IAsyncEnumerator<Page<Batch>>) (log: ILogger)
        : Async<Option<ITimelineEvent<JsonElement>[] * Position option * float>> = async {
        let! t, res = enumerator.MoveNext() |> Stopwatch.Time

        return
            res
            |> Option.map (fun page ->
                let batches, ru = Array.ofSeq page.Values, page.GetRawResponse().Headers.GetRequestCharge()
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
                events, maybePosition, ru) }

    let private run (log : ILogger) (readNextPage: IAsyncEnumerator<Page<Batch>> -> ILogger -> Async<Option<ITimelineEvent<JsonElement>[] * Position option * float>>)
        (maxPermittedBatchReads: int option)
        (query: AsyncSeq<Page<Batch>>) =

        let e = query.GetEnumerator()

        let rec loop batchCount : AsyncSeq<ITimelineEvent<JsonElement>[] * Position option * float> = asyncSeq {
            match maxPermittedBatchReads with
            | Some mpbr when batchCount >= mpbr -> log.Information "batch Limit exceeded"; invalidOp "batch Limit exceeded"
            | _ -> ()

            let batchLog = log |> Log.prop "batchIndex" batchCount
            let! (page : Option<ITimelineEvent<JsonElement>[] * Position option * float>) = readNextPage e batchLog

            if page |> Option.isSome then
                yield page.Value
                yield! loop (batchCount + 1) }

        loop 0

    let private logQuery direction batchSize streamName interval (responsesCount, events : ITimelineEvent<JsonElement>[]) n (ru: float) (log : ILogger) =
        let (Log.BatchLen bytes), count = events, events.Length
        let reqMetric : Log.Measurement = { stream = streamName; interval = interval; bytes = bytes; count = count; ru = ru }
        let evt = Log.Event.Query (direction, responsesCount, reqMetric)
        let action = match direction with Direction.Forward -> "QueryF" | Direction.Backward -> "QueryB"
        (log |> Log.prop "bytes" bytes |> Log.prop "batchSize" batchSize |> Log.event evt).Information(
            "EqxCosmos {action:l} {stream} v{n} {count}/{responses} {ms}ms rc={ru}",
            action, streamName, n, count, responsesCount, (let e = interval.Elapsed in e.TotalMilliseconds), ru)

    let private calculateUsedVersusDroppedPayload stopIndex (xs: ITimelineEvent<JsonElement>[]) : int * int =
        let mutable used, dropped = 0, 0
        let mutable found = false
        for x in xs do
            let (Log.EventLen bytes) = x
            if found then dropped <- dropped + bytes
            else used <- used + bytes
            if x.Index = stopIndex then found <- true
        used, dropped

    let walk<'event> (log : ILogger) (container,stream) retryPolicy maxItems maxRequests direction startPos
        (tryDecode : ITimelineEvent<JsonElement> -> 'event option, isOrigin: 'event -> bool)
        : Async<Position * 'event[]> = async {
        let responseCount = ref 0
        let mergeBatches (log : ILogger) (batchesBackward: AsyncSeq<ITimelineEvent<JsonElement>[] * Position option * float>) = async {
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
        let readPage = processNextPage direction stream startPos
        let retryingLoggingReadPage e = Log.withLoggedRetries retryPolicy "readAttempt" (readPage e)
        let log = log |> Log.prop "batchSize" maxItems |> Log.prop "stream" stream
        let readlog = log |> Log.prop "direction" direction
        let batches : AsyncSeq<ITimelineEvent<JsonElement>[] * Position option * float> = run readlog retryingLoggingReadPage maxRequests query
        let! t, (events, maybeTipPos, ru) = mergeBatches log batches |> Stopwatch.Time
        let raws, decoded = (Array.map fst events), (events |> Seq.choose snd |> Array.ofSeq)
        let pos = match maybeTipPos with Some p -> p | None -> Position.fromMaxIndex raws

        log |> logQuery direction maxItems stream t (!responseCount,raws) pos.index ru
        return pos, decoded }

    let walkLazy<'event> (log : ILogger) (container,stream) retryPolicy maxItems maxRequests direction startPos
        (tryDecode : ITimelineEvent<JsonElement> -> 'event option, isOrigin: 'event -> bool)
        : AsyncSeq<'event[]> = asyncSeq {
        let responseCount = ref 0
        let query = mkQuery (container,stream) maxItems direction startPos
        let readPage = processNextPage direction stream startPos
        let retryingLoggingReadPage e = Log.withLoggedRetries retryPolicy "readAttempt" (readPage e)
        let log = log |> Log.prop "batchSize" maxItems |> Log.prop "stream" stream
        let mutable ru = 0.
        let allEvents = ResizeArray()
        let startTicks = System.Diagnostics.Stopwatch.GetTimestamp()

        let e = query.GetEnumerator()

        try let readlog = log |> Log.prop "direction" direction
            let mutable ok = true

            while ok do
                incr responseCount

                match maxRequests with
                | Some mpbr when !responseCount >= mpbr -> readlog.Information "batch Limit exceeded"; invalidOp "batch Limit exceeded"
                | _ -> ()

                let batchLog = readlog |> Log.prop "batchIndex" !responseCount
                let! page = retryingLoggingReadPage e batchLog

                match page with
                | Some (evts, _pos, rus) ->
                    ru <- ru + rus
                    allEvents.AddRange(evts)

                    let acc = ResizeArray()
                    for x in evts do
                        match tryDecode x with
                        | Some e when isOrigin e ->
                            let used, residual = evts |> calculateUsedVersusDroppedPayload x.Index
                            log.Information("EqxCosmos Stop stream={stream} at={index} {case} used={used} residual={residual}",
                                stream, x.Index, x.EventType, used, residual)
                            ok <- false
                            acc.Add e
                        | Some e -> acc.Add e
                        | None -> ()

                    yield acc.ToArray()
                | _ -> ok <- false
        finally
            let endTicks = System.Diagnostics.Stopwatch.GetTimestamp()
            let t = StopwatchInterval(startTicks, endTicks)
            log |> logQuery direction maxItems stream t (!responseCount,allEvents.ToArray()) -1L ru }

type [<NoComparison>] Token = { stream: string; pos: Position }
module Token =
    let create stream pos : StreamToken =
        {  value = box { stream = stream; pos = pos }
           version = pos.index }
    let (|Unpack|) (token: StreamToken) : string*Position = let t = unbox<Token> token.value in t.stream,t.pos
    let supersedes (Unpack (_,currentPos)) (Unpack (_,xPos)) =
        let currentVersion, newVersion = currentPos.index, xPos.index
        let currentETag, newETag = currentPos.etag, xPos.etag
        newVersion > currentVersion || currentETag <> newETag

[<AutoOpen>]
module Internal =
    [<RequireQualifiedAccess; NoComparison; NoEquality>]
    type InternalSyncResult = Written of StreamToken | ConflictUnknown of StreamToken | Conflict of StreamToken * ITimelineEvent<JsonElement>[]

    [<RequireQualifiedAccess; NoComparison; NoEquality>]
    type LoadFromTokenResult<'event> = Unchanged | Found of StreamToken * 'event[]

/// Defines policies for retrying with respect to transient failures calling CosmosDb (as opposed to application level concurrency conflicts)
type RetryPolicy([<O; D(null)>]?readRetryPolicy: IRetryPolicy, [<O; D(null)>]?writeRetryPolicy) =
    member __.TipRetryPolicy = readRetryPolicy
    member __.QueryRetryPolicy = readRetryPolicy
    member __.WriteRetryPolicy = writeRetryPolicy

/// Defines the policies in force regarding how to a) split up calls b) limit the number of events per page
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

/// Holds Container state, coordinating initialization activities
type internal ContainerInitializerGuard(gateway : ContainerGateway, ?initContainer : CosmosContainer -> Async<unit>) =
    let initGuard = initContainer |> Option.map (fun init -> AsyncCacheCell<unit>(init gateway.CosmosContainer))

    member __.Gateway = gateway
    member internal __.InitializationGate = match initGuard with Some g when g.PeekIsValid() |> not -> Some g.AwaitValue | _ -> None

/// Defines a process for mapping from a Stream Name to the appropriate storage area, allowing control over segregation / co-locating of data
type Containers
    (    /// Facilitates custom mapping of Stream Category Name to underlying Cosmos Database/Container names
         categoryAndStreamNameToDatabaseContainerStream : string * string -> string * string * string,
         /// Inhibit <c>CreateStoredProcedureIfNotExists</c> when a given Container is used for the first time
         [<O; D(null)>]?disableInitialization) =
    // Index of database*collection -> Initialization Context
    let containerInitGuards = System.Collections.Concurrent.ConcurrentDictionary<string*string, ContainerInitializerGuard>()

    /// Create a Container Map where all streams are stored within a single global <c>CosmosContainer</c>.
    new (databaseId, containerId, [<O; D(null)>]?disableInitialization) =
        let genStreamName (categoryName, streamId) = if categoryName = null then streamId else sprintf "%s-%s" categoryName streamId
        let catAndStreamToDatabaseContainerStream (categoryName, streamId) = databaseId, containerId, genStreamName (categoryName, streamId)
        Containers(catAndStreamToDatabaseContainerStream, ?disableInitialization = disableInitialization)

    member internal __.ResolveContainerGuardAndStreamName(client : CosmosClient, createGateway, categoryName, streamId) : ContainerInitializerGuard * string =
        let databaseId, containerId, streamName = categoryAndStreamNameToDatabaseContainerStream (categoryName, streamId)
        let createContainerInitializerGuard (d, c) =
            let init =
                if Some true = disableInitialization then None
                else Some (fun cosmosContainer -> Initialization.createSyncStoredProcedure cosmosContainer None |> Async.Ignore)
            ContainerInitializerGuard
                (   createGateway (client.GetDatabase(d).GetContainer(c)),
                    ?initContainer = init)
        let g = containerInitGuards.GetOrAdd((databaseId, containerId), createContainerInitializerGuard)
        g, streamName

type ContainerClient(gateway : ContainerGateway, batching : BatchingPolicy, retry: RetryPolicy) =
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
    member __.LoadBackwardsStopping(log, stream, (tryDecode,isOrigin)): Async<StreamToken * 'event[]> = async {
        let! pos, events = Query.walk log (gateway,stream) retry.QueryRetryPolicy batching.MaxItems batching.MaxRequests Direction.Backward None (tryDecode,isOrigin)
        System.Array.Reverse events
        return Token.create stream pos, events }
    member __.Read(log, stream, direction, startPos, (tryDecode,isOrigin)) : Async<StreamToken * 'event[]> = async {
        let! pos, events = Query.walk log (gateway,stream) retry.QueryRetryPolicy batching.MaxItems batching.MaxRequests direction startPos (tryDecode,isOrigin)
        return Token.create stream pos, events }
    member __.ReadLazy(batching: BatchingPolicy, log, stream, direction, startPos, (tryDecode,isOrigin)) : AsyncSeq<'event[]> =
        Query.walkLazy log (gateway,stream) retry.QueryRetryPolicy batching.MaxItems batching.MaxRequests direction startPos (tryDecode,isOrigin)
    member __.LoadFromUnfoldsOrRollingSnapshots(log, (stream,maybePos), (tryDecode,isOrigin)): Async<StreamToken * 'event[]> = async {
        let! res = Tip.tryLoad log retry.TipRetryPolicy (gateway,stream) maybePos
        match res with
        | Tip.Result.NotFound -> return Token.create stream Position.fromKnownEmpty, Array.empty
        | Tip.Result.NotModified -> return invalidOp "Not handled"
        | Tip.Result.Found (pos, FromUnfold tryDecode isOrigin span) -> return Token.create stream pos, span
        | _ -> return! __.LoadBackwardsStopping(log,stream, (tryDecode,isOrigin)) }
    member __.GetPosition(log, stream, ?pos): Async<StreamToken> = async {
        let! res = Tip.tryLoad log retry.TipRetryPolicy (gateway,stream) pos
        match res with
        | Tip.Result.NotFound -> return Token.create stream Position.fromKnownEmpty
        | Tip.Result.NotModified -> return Token.create stream pos.Value
        | Tip.Result.Found (pos, _unfoldsAndEvents) -> return Token.create stream pos }
    member __.LoadFromToken(log, (stream,pos), (tryDecode, isOrigin)): Async<LoadFromTokenResult<'event>> = async {
        let! res = Tip.tryLoad log retry.TipRetryPolicy (gateway,stream) (Some pos)
        match res with
        | Tip.Result.NotFound -> return LoadFromTokenResult.Found (Token.create stream Position.fromKnownEmpty,Array.empty)
        | Tip.Result.NotModified -> return LoadFromTokenResult.Unchanged
        | Tip.Result.Found (pos, FromUnfold tryDecode isOrigin span) -> return LoadFromTokenResult.Found (Token.create stream pos, span)
        | _ ->  let! res = __.Read(log, stream, Direction.Forward, Some pos, (tryDecode,isOrigin))
                return LoadFromTokenResult.Found res }
    member __.Sync(log, stream, (exp, batch: Tip)): Async<InternalSyncResult> = async {
        if Array.isEmpty batch.e && Array.isEmpty batch.u then invalidOp "Must write either events or unfolds."
        let! wr = Sync.batch log retry.WriteRetryPolicy (gateway,stream) (exp,batch)
        match wr with
        | Sync.Result.Conflict (pos',events) -> return InternalSyncResult.Conflict (Token.create stream pos',events)
        | Sync.Result.ConflictUnknown pos' -> return InternalSyncResult.ConflictUnknown (Token.create stream pos')
        | Sync.Result.Written pos' -> return InternalSyncResult.Written (Token.create stream pos') }

type internal Category<'event, 'state, 'context>(container : ContainerClient, codec : IEventCodec<'event,JsonElement,'context>) =
    let (|TryDecodeFold|) (fold: 'state -> 'event seq -> 'state) initial (events: ITimelineEvent<JsonElement> seq) : 'state = Seq.choose codec.TryDecode events |> fold initial
    member __.Load(includeUnfolds, stream, fold, initial, isOrigin, log : ILogger): Async<StreamToken * 'state> = async {
        let! token, events =
            if not includeUnfolds then container.LoadBackwardsStopping(log, stream, (codec.TryDecode,isOrigin))
            else container.LoadFromUnfoldsOrRollingSnapshots(log, (stream, None), (codec.TryDecode,isOrigin))
        return token, fold initial events }
    member __.LoadFromToken(Token.Unpack (stream,pos), state: 'state as current) fold isOrigin (log : ILogger): Async<StreamToken * 'state> = async {
        let! res = container.LoadFromToken(log, (stream, pos), (codec.TryDecode,isOrigin))
        match res with
        | LoadFromTokenResult.Unchanged -> return current
        | LoadFromTokenResult.Found (token', events') -> return token', fold state events' }
    member __.Sync(Token.Unpack (stream,pos), state as current, events, mapUnfolds, fold, isOrigin, compress, log, context): Async<SyncResult<'state>> = async {
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
        let projections = Sync.mkUnfold compress baseIndex projectionsEncoded
        let batch = Sync.mkBatch stream eventsEncoded projections
        let! res = container.Sync(log, stream, (exp,batch))
        match res with
        | InternalSyncResult.Conflict (token',TryDecodeFold fold state events') -> return SyncResult.Conflict (async { return token', events' })
        | InternalSyncResult.ConflictUnknown _token' -> return SyncResult.Conflict (__.LoadFromToken current fold isOrigin log)
        | InternalSyncResult.Written token' -> return SyncResult.Written (token', state') }

module Caching =
    /// Forwards all state changes in all streams of an ICategory to a `tee` function
    type CategoryTee<'event, 'state, 'context>(inner: ICategory<'event, 'state, string,'context>, tee : string -> StreamToken * 'state -> Async<unit>) =
        let intercept streamName tokenAndState = async {
            let! _ = tee streamName tokenAndState
            return tokenAndState }
        let loadAndIntercept load streamName = async {
            let! tokenAndState = load
            return! intercept streamName tokenAndState }
        interface ICategory<'event, 'state, string, 'context> with
            member __.Load(log, streamName, opt) : Async<StreamToken * 'state> =
                loadAndIntercept (inner.Load(log, streamName, opt)) streamName
            member __.TrySync(log : ILogger, (Token.Unpack (stream,_) as streamToken), state, events : 'event list, context, compress)
                : Async<SyncResult<'state>> = async {
                let! syncRes = inner.TrySync(log, streamToken, state, events, context, compress)
                match syncRes with
                | SyncResult.Conflict resync -> return SyncResult.Conflict(loadAndIntercept resync stream)
                | SyncResult.Written(token', state') ->
                    let! intercepted = intercept stream (token', state')
                    return SyncResult.Written intercepted }

    let applyCacheUpdatesWithSlidingExpiration
            (cache : ICache)
            (prefix : string)
            (slidingExpiration : TimeSpan)
            (category : ICategory<'event, 'state, string, 'context>)
            : ICategory<'event, 'state, string, 'context> =
        let mkCacheEntry (initialToken : StreamToken, initialState : 'state) = CacheEntry<'state>(initialToken, initialState, Token.supersedes)
        let options = CacheItemOptions.RelativeExpiration slidingExpiration
        let addOrUpdateSlidingExpirationCacheEntry streamName value = cache.UpdateIfNewer(prefix + streamName, options, mkCacheEntry value)
        CategoryTee<'event, 'state, 'context>(category, addOrUpdateSlidingExpirationCacheEntry) :> _

type internal Folder<'event, 'state, 'context>
    (   category: Category<'event, 'state, 'context>, fold: 'state -> 'event seq -> 'state, initial: 'state,
        isOrigin: 'event -> bool,
        mapUnfolds: Choice<unit,('event list -> 'state -> 'event seq),('event list -> 'state -> 'event list * 'event list)>,
        ?readCache) =
    let inspectUnfolds = match mapUnfolds with Choice1Of3 () -> false | _ -> true
    let batched log stream = category.Load(inspectUnfolds, stream, fold, initial, isOrigin, log)
    interface ICategory<'event, 'state, string, 'context> with
        member __.Load(log, streamName, opt): Async<StreamToken * 'state> =
            match readCache with
            | None -> batched log streamName
            | Some (cache : ICache, prefix : string) -> async {
                match! cache.TryGet(prefix + streamName) with
                | None -> return! batched log streamName
                | Some tokenAndState when opt = Some Equinox.AllowStale -> return tokenAndState
                | Some tokenAndState -> return! category.LoadFromToken tokenAndState fold isOrigin log }
        member __.TrySync(log : ILogger, streamToken, state, events : 'event list, context, compress)
            : Async<SyncResult<'state>> = async {
            let! res = category.Sync((streamToken,state), events, mapUnfolds, fold, isOrigin, compress, log, context)
            match res with
            | SyncResult.Conflict resync ->         return SyncResult.Conflict resync
            | SyncResult.Written (token',state') -> return SyncResult.Written (token',state') }

namespace Equinox.CosmosStore

open Azure.Cosmos
open Equinox
open Equinox.Core
open Equinox.CosmosStore.Core
open FsCodec
open FSharp.Control
open Serilog
open System

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

/// Holds all relevant state for a Store within a given CosmosDB Database
/// - The (singleton) CosmosDB CosmosClient (there should be a single one of these per process)
/// - The (singleton) Core.Containers instance, which maintains the per Container Stored Procedure initialization state
type CosmosStoreConnection
    (   client : CosmosClient,
        /// Singleton used to cache initialization state per <c>CosmosContainer</c>.
        containers : Containers,
        /// Admits a hook to enable customization of how <c>Equinox.Cosmos</c> handles the low level interactions with the underlying <c>CosmosContainer</c>.
        ?createGateway) =
    let createGateway = match createGateway with Some creator -> creator | None -> ContainerGateway
    new (client, databaseId : string, containerId : string,
         /// Inhibit <c>CreateStoredProcedureIfNotExists</c> when a given Container is used for the first time
         [<O; D(null)>]?disableInitialization,
         /// Admits a hook to enable customization of how <c>Equinox.Cosmos</c> handles the low level interactions with the underlying <c>CosmosContainer</c>.
         [<O; D(null)>]?createGateway : CosmosContainer -> ContainerGateway) =
        let containers = Containers(databaseId, containerId, ?disableInitialization = disableInitialization)
        CosmosStoreConnection(client, containers, ?createGateway = createGateway)
    member __.Client = client
    member internal __.ResolveContainerGuardAndStreamName(categoryName, streamId) =
        containers.ResolveContainerGuardAndStreamName(client, createGateway, categoryName, streamId)

/// Defines a set of related access policies for a given CosmosDB, together with a Containers map defining mappings from (category,id) to (databaseId,containerId,streamName)
type CosmosStoreContext(connection : CosmosStoreConnection, batchingPolicy, retryPolicy) =
    new(client : CosmosStoreConnection, ?defaultMaxItems, ?getDefaultMaxItems, ?maxRequests, ?readRetryPolicy, ?writeRetryPolicy) =
        let retry = RetryPolicy(?readRetryPolicy = readRetryPolicy, ?writeRetryPolicy = writeRetryPolicy)
        let batching = BatchingPolicy(?defaultMaxItems = defaultMaxItems, ?getDefaultMaxItems = getDefaultMaxItems, ?maxRequests = maxRequests)
        CosmosStoreContext(client, batching, retry)
    member __.Batching = batchingPolicy
    member __.Retries = retryPolicy
    member internal __.ResolveContainerClientAndStreamIdAndInit(categoryName, streamId) =
        let cg, streamId = connection.ResolveContainerGuardAndStreamName(categoryName, streamId)
        let cc = ContainerClient(cg.Gateway, batchingPolicy, retryPolicy)
        cc, streamId, cg.InitializationGate

type CosmosStoreCategory<'event, 'state, 'context>(context : CosmosStoreContext, codec, fold, initial, caching, access) =
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
    let categories = System.Collections.Concurrent.ConcurrentDictionary<string, ICategory<_, _, string, 'context>>()
    let resolveCategory (categoryName, container) =
        let createCategory _name =
            let cosmosCat = Category<'event, 'state, 'context>(container, codec)
            let folder = Core.Folder<'event, 'state, 'context>(cosmosCat, fold, initial, isOrigin, mapUnfolds, ?readCache = readCacheOption)
            match caching with
            | CachingStrategy.NoCaching -> folder :> ICategory<_, _, string, 'context>
            | CachingStrategy.SlidingWindow(cache, window) -> Caching.applyCacheUpdatesWithSlidingExpiration cache null window folder
        categories.GetOrAdd(categoryName, createCategory)

    let resolveStream (categoryName, container, streamId, maybeContainerInitializationGate) opt context compress =
        let category = resolveCategory (categoryName, container)
        { new IStream<'event, 'state> with
            member __.Load log = category.Load(log, streamId, opt)
            member __.TrySync(log: ILogger, token: StreamToken, originState: 'state, events: 'event list) =
                match maybeContainerInitializationGate with
                | None -> category.TrySync(log, token, originState, events, context, compress)
                | Some init -> async {
                    do! init ()
                    return! category.TrySync(log, token, originState, events, context, compress) } }

    let resolveStreamConfig = function
        | StreamName.CategoryAndId (categoryName, streamId) ->
            let containerClient, streamId, init = context.ResolveContainerClientAndStreamIdAndInit(categoryName, streamId)
            categoryName, containerClient, streamId, init

    member __.Resolve
        (   streamName : StreamName,
            /// Resolver options
            [<O; D null>]?option,
            /// Context to be passed to IEventCodec
            [<O; D null>]?context,
            /// Determines whether the data and metadata payloads of the `u`nfolds in the Tip document are base64 encoded and compressed; defaults to true
            [<O; D true>]?compressUnfolds) =
        let compress = defaultArg compressUnfolds true
        match resolveStreamConfig streamName, option with
        | streamArgs,(None|Some AllowStale) ->
            resolveStream streamArgs option context compress
        | (_, _, streamId, _) as streamArgs,Some AssumeEmpty ->
            let stream = resolveStream streamArgs option context compress
            Stream.ofMemento (Token.create streamId Position.fromKnownEmpty,initial) stream

    member __.FromMemento
        (   Token.Unpack (stream,_pos) as streamToken,
            state,
            /// Determines whether the data and metadata payloads of the `u`nfolds in the Tip document are base64 encoded and compressed; defaults to true
            [<O; D true>]?compressUnfolds) =
        let compress = defaultArg compressUnfolds true
        let skipInitialization = None
        let (categoryName, container, streamId, _maybeInit) = resolveStreamConfig (StreamName.parse stream)
        let stream = resolveStream (categoryName, container, streamId, skipInitialization) None None compress
        Stream.ofMemento (streamToken,state) stream

[<RequireQualifiedAccess; NoComparison>]
type Discovery =
    /// Separated Account Uri and Key (for interop with previous versions)
    | AccountUriAndKey of databaseUri: Uri * key:string
    /// Cosmos SDK Connection String
    | ConnectionString of connectionString : string

type CosmosStoreClientFactory
    (   /// Timeout to apply to individual reads/write round-trips going to CosmosDb
        requestTimeout: TimeSpan,
        /// Maximum number of times attempt when failure reason is a 429 from CosmosDb, signifying RU limits have been breached
        maxRetryAttemptsOnRateLimitedRequests: int,
        /// Maximum number of seconds to wait (especially if a higher wait delay is suggested by CosmosDb in the 429 response)
        maxRetryWaitTimeOnRateLimitedRequests: TimeSpan,
        /// Connection limit for Gateway Mode (default 1000)
        [<O; D(null)>]?gatewayModeMaxConnectionLimit,
        /// Connection mode (default: ConnectionMode.Gateway (lowest perf, least trouble))
        [<O; D(null)>]?mode : ConnectionMode,
        /// consistency mode  (default: ConsistencyLevel.Session)
        [<O; D(null)>]?defaultConsistencyLevel : ConsistencyLevel) =

    /// CosmosClientOptions for this Connector as configured
    member val Options =
        let maxAttempts, maxWait, timeout = Nullable maxRetryAttemptsOnRateLimitedRequests, Nullable maxRetryWaitTimeOnRateLimitedRequests, requestTimeout
        let serializerOptions = FsCodec.SystemTextJson.Options.CreateDefault()
        let co =
            CosmosClientOptions(
                MaxRetryAttemptsOnRateLimitedRequests = maxAttempts, MaxRetryWaitTimeOnRateLimitedRequests = maxWait, RequestTimeout = timeout,
                Serializer = CosmosJsonSerializer serializerOptions)
        match mode with
        | Some ConnectionMode.Direct -> co.ConnectionMode <- ConnectionMode.Direct
        | None | Some ConnectionMode.Gateway | Some _ (* enum total match :( *) -> co.ConnectionMode <- ConnectionMode.Gateway // default; only supports Https
        match gatewayModeMaxConnectionLimit with
        | Some _ when co.ConnectionMode = ConnectionMode.Direct -> invalidArg "gatewayModeMaxConnectionLimit" "Not admissible in Direct mode"
        | x -> if co.ConnectionMode = ConnectionMode.Gateway then co.GatewayModeMaxConnectionLimit <- defaultArg x 1000
        match defaultConsistencyLevel with
        | Some x -> co.ConsistencyLevel <- Nullable x
        | None -> ()
        // TODO translate, or not
        // https://github.com/Azure/azure-cosmos-dotnet-v3/issues/1232
        // https://github.com/Azure/azure-cosmos-dotnet-v2/issues/605
        // https://docs.microsoft.com/en-us/azure/cosmos-db/local-emulator#running-on-mac-or-linux
//        if defaultArg bypassCertificateValidation false then
//            let inhibitCertCheck = new System.Net.Http.HttpClientHandler(ServerCertificateCustomValidationCallback = fun _ _ _ _ -> true)
//            co.TransportClientHandlerFactory <- inhibitCertCheck
        co

    abstract member Create: discovery: Discovery -> CosmosClient
    default __.Create discovery = discovery |> function
        | Discovery.AccountUriAndKey (databaseUri=uri; key=key) -> new CosmosClient(string uri, key, __.Options)
        | Discovery.ConnectionString cs -> new CosmosClient(cs, __.Options)

namespace Equinox.CosmosStore.Core

open FsCodec
open FSharp.Control
open System.Runtime.InteropServices
open System.Text.Json

/// Outcome of appending events, specifying the new and/or conflicting events, together with the updated Target write position
[<RequireQualifiedAccess; NoComparison>]
type AppendResult<'t> =
    | Ok of pos: 't
    | Conflict of index: 't * conflictingEvents: ITimelineEvent<JsonElement>[]
    | ConflictUnknown of index: 't

/// Encapsulates the core facilities Equinox.Cosmos offers for operating directly on Events in Streams.
type EventsContext
    (   context : Equinox.CosmosStore.CosmosStoreContext, container : ContainerClient,
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
    let maxCountPredicate count =
        let acc = ref (max (count-1) 0)
        fun _ ->
            if !acc = 0 then true else
            decr acc
            false

    let yieldPositionAndData res = async {
        let! (Token.Unpack (_,pos')), data = res
        return pos', data }

    new (client : Azure.Cosmos.CosmosClient, log, databaseId : string, containerId : string, ?defaultMaxItems, ?getDefaultMaxItems) =
        let inner = Equinox.CosmosStore.CosmosStoreContext(Equinox.CosmosStore.CosmosStoreConnection(client, databaseId, containerId))
        let cc, _streamId, _init = inner.ResolveContainerClientAndStreamIdAndInit(null, null)
        EventsContext(inner, cc, log, ?defaultMaxItems = defaultMaxItems, ?getDefaultMaxItems = getDefaultMaxItems)

    member __.ResolveStream(streamName) =
        let _cc, streamId, init = context.ResolveContainerClientAndStreamIdAndInit(null, streamName)
        streamId, init
    member __.CreateStream(streamName) : string = __.ResolveStream streamName |> fst

    member internal __.GetLazy((stream, startPos), ?batchSize, ?direction) : AsyncSeq<ITimelineEvent<JsonElement>[]> =
        let direction = defaultArg direction Direction.Forward
        let batching = BatchingPolicy(defaultArg batchSize batching.MaxItems)
        container.ReadLazy(batching, log, stream, direction, startPos, (Some,fun _ -> false))

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
            return! container.Read(log, stream, direction, startPos, (Some,isOrigin)) }

    /// Establishes the current position of the stream in as efficient a manner as possible
    /// (The ideal situation is that the preceding token is supplied as input in order to avail of 1RU low latency state checks)
    member __.Sync(stream, ?position: Position) : Async<Position> = async {
        let! (Token.Unpack (_,pos')) = container.GetPosition(log, stream, ?pos=position)
        return pos' }

    /// Reads in batches of `batchSize` from the specified `Position`, allowing the reader to efficiently walk away from a running query
    /// ... NB as long as they Dispose!
    member __.Walk(stream, batchSize, ?position, ?direction) : AsyncSeq<ITimelineEvent<JsonElement>[]> =
        __.GetLazy((stream, position), batchSize, ?direction=direction)

    /// Reads all Events from a `Position` in a given `direction`
    member __.Read(stream, ?position, ?maxCount, ?direction) : Async<Position*ITimelineEvent<JsonElement>[]> =
        __.GetInternal((stream, position), ?maxCount=maxCount, ?direction=direction) |> yieldPositionAndData

    /// Appends the supplied batch of events, subject to a consistency check based on the `position`
    /// Callers should implement appropriate idempotent handling, or use Equinox.Stream for that purpose
    member __.Sync(stream, position, events: IEventData<_>[]) : Async<AppendResult<Position>> = async {
        // Writes go through the stored proc, which we need to provision per-collection
        // Having to do this here in this way is far from ideal, but work on caching, external snapshots and caching is likely
        //   to move this about before we reach a final destination in any case
        match __.ResolveStream stream |> snd with
        | None -> ()
        | Some init -> do! init ()
        let batch = Sync.mkBatch stream events Seq.empty
        let! res = container.Sync(log, stream, (Sync.Exp.Version position.index, batch))
        match res with
        | InternalSyncResult.Written (Token.Unpack (_,pos)) -> return AppendResult.Ok pos
        | InternalSyncResult.Conflict (Token.Unpack (_,pos),events) -> return AppendResult.Conflict (pos, events)
        | InternalSyncResult.ConflictUnknown (Token.Unpack (_,pos)) -> return AppendResult.ConflictUnknown pos }

    /// Low level, non-idempotent call appending events to a stream without a concurrency control mechanism in play
    /// NB Should be used sparingly; Equinox.Stream enables building equivalent equivalent idempotent handling with minimal code.
    member __.NonIdempotentAppend(stream, events: IEventData<_>[]) : Async<Position> = async {
        let! res = __.Sync(stream, Position.fromAppendAtEnd, events)
        match res with
        | AppendResult.Ok token -> return token
        | x -> return x |> sprintf "Conflict despite it being disabled %A" |> invalidOp }

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
    let private dropPosition (f: Async<Position*ITimelineEvent<JsonElement>[]>): Async<ITimelineEvent<JsonElement>[]> = async {
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
    let getAll (ctx: EventsContext) (streamName: string) (MinPosition index: int64) (batchSize: int): FSharp.Control.AsyncSeq<ITimelineEvent<JsonElement>[]> =
        ctx.Walk(ctx.CreateStream streamName, batchSize, ?position=index)

    /// Returns an async array of events in the stream starting at the specified sequence number,
    /// number of events to read is specified by batchSize
    /// Returns an empty sequence if the stream is empty or if the sequence number is larger than the largest
    /// sequence number in the stream.
    let get (ctx: EventsContext) (streamName: string) (MinPosition index: int64) (maxCount: int): Async<ITimelineEvent<JsonElement>[]> =
        ctx.Read(ctx.CreateStream streamName, ?position=index, maxCount=maxCount) |> dropPosition

    /// Appends a batch of events to a stream at the specified expected sequence number.
    /// If the specified expected sequence number does not match the stream, the events are not appended
    /// and a failure is returned.
    let append (ctx: EventsContext) (streamName: string) (index: int64) (events: IEventData<_>[]): Async<AppendResult<int64>> =
        ctx.Sync(ctx.CreateStream streamName, Position.fromI index, events) |> stripSyncResult

    /// Appends a batch of events to a stream at the the present Position without any conflict checks.
    /// NB typically, it is recommended to ensure idempotency of operations by using the `append` and related API as
    /// this facilitates ensuring consistency is maintained, and yields reduced latency and Request Charges impacts
    /// (See equivalent APIs on `Context` that yield `Position` values)
    let appendAtEnd (ctx: EventsContext) (streamName: string) (events: IEventData<_>[]): Async<int64> =
        ctx.NonIdempotentAppend(ctx.CreateStream streamName, events) |> stripPosition

    /// Returns an async sequence of events in the stream backwards starting from the specified sequence number,
    /// reading in batches of the specified size.
    /// Returns an empty sequence if the stream is empty or if the sequence number is smaller than the smallest
    /// sequence number in the stream.
    let getAllBackwards (ctx: EventsContext) (streamName: string) (MaxPosition index: int64) (batchSize: int): AsyncSeq<ITimelineEvent<JsonElement>[]> =
        ctx.Walk(ctx.CreateStream streamName, batchSize, ?position=index, direction=Direction.Backward)

    /// Returns an async array of events in the stream backwards starting from the specified sequence number,
    /// number of events to read is specified by batchSize
    /// Returns an empty sequence if the stream is empty or if the sequence number is smaller than the smallest
    /// sequence number in the stream.
    let getBackwards (ctx: EventsContext) (streamName: string) (MaxPosition index: int64) (maxCount: int): Async<ITimelineEvent<JsonElement>[]> =
        ctx.Read(ctx.CreateStream streamName, ?position=index, maxCount=maxCount, direction=Direction.Backward) |> dropPosition

    /// Obtains the `index` from the current write Position
    let getNextIndex (ctx: EventsContext) (streamName: string) : Async<int64> =
        ctx.Sync(ctx.CreateStream streamName) |> stripPosition
