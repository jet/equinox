namespace Equinox.DynamoStore.Core

open Equinox.Core
open FSharp.AWS.DynamoDB
open FSharp.Control
open FsCodec
open Serilog
open System
open System.IO

[<Struct; NoEquality; NoComparison>]
type InternalBody = { encoding: int; data: MemoryStream }
module private InternalBody =
    let ofStreamAndEncoding (stream: MemoryStream option, encoding: int option): InternalBody =
        let stream = Option.toObj stream
        { encoding = defaultArg encoding 0; data = stream }
    let toStreamAndEncoding (encoded: InternalBody) =
        Option.ofObj encoded.data, match encoded.encoding with 0 -> None | x -> Some x
    let bytes (x: InternalBody) =
        if x.data = null then 0
        else int x.data.Length

/// A single Domain Event from the array held in a Batch
[<NoEquality; NoComparison>]
type Event =
    {   /// Index number within stream, not persisted (computed from Batch's `n` and the index within `e`)
        i: int

        /// Creation Timestamp, as set by the application layer at the point of rendering the Event
        t: DateTimeOffset

        /// The Event Type (Case) that defines the content of the Data (and Metadata) fields
        c: string

        /// Main event body; required
        d: InternalBody

        /// Optional metadata, encoded as per 'd'; can be Empty
        m: InternalBody

        /// CorrelationId; stored as x (signifying transactionId), or null
        correlationId: string option

        /// CausationId; stored as y (signifying why), or null
        causationId: string option }
    interface ITimelineEvent<InternalBody> with
        member x.Index = x.i
        member _.IsUnfold = false
        member _.Context = null
        member x.Size = Event.Bytes x
        member x.EventType = x.c
        member x.Data = x.d
        member x.Meta = x.m
        member _.EventId = Guid.Empty
        member x.CorrelationId = Option.toObj x.correlationId
        member x.CausationId = Option.toObj x.causationId
        member x.Timestamp = x.t
    static member Bytes(x: Event) =
        let inline len x = match x with Some (s: string) -> s.Length | None -> 0
        x.c.Length + InternalBody.bytes x.d + InternalBody.bytes x.m + len x.correlationId + len x.causationId + 20 (*t*) + 20 (*overhead*)
module Event =
    let arrayBytes (xs: Event[]) = Array.sumBy Event.Bytes xs

/// Compaction/Snapshot/Projection Event based on the state at a given point in time `i`
[<NoEquality; NoComparison>]
type Unfold =
    {   /// Base: Stream Position (Version) of State from which this Unfold Event was generated. An unfold from State Version 1 is i=1 and includes event i=1
        i: int64

        /// Generation datetime
        t: DateTimeOffset

        /// The Case (Event Type) of this snapshot, used to drive deserialization
        c: string // required

        /// Event body
        d: InternalBody // required

        /// Optional metadata, can be Empty
        m: InternalBody }
    interface ITimelineEvent<InternalBody> with
        member x.Index = x.i
        member _.IsUnfold = true
        member _.Context = null
        member x.Size = Unfold.Bytes x
        member x.EventType = x.c
        member x.Data = x.d
        member x.Meta = x.m
        member _.EventId = Guid.Empty
        member _.CorrelationId = null
        member _.CausationId = null
        member x.Timestamp = x.t
    static member Bytes(x: Unfold) = x.c.Length + InternalBody.bytes x.d + InternalBody.bytes x.m + 50
module Unfold =
    let arrayBytes (xs: Unfold[]) = match xs with null -> 0 | u -> Array.sumBy Unfold.Bytes u

/// The abstract storage format for a Batch of Events represented in a DynamoDB Item
/// NOTE See Batch.Schema buddy type for what actually gets stored
/// NOTE names are intended to generally align with CosmosStore naming. Key Diffs:
/// - no mandatory `id` and/or requirement for it to be a `string` -> replaced with `i` as an int64
///   (also Tip magic value is tipMagicI: Int32.MaxValue, not "-1")
/// - etag is managed explicitly (on Cosmos DB, its managed by the service and named "_etag")
[<NoEquality; NoComparison>]
type Batch =
    {   p: string // "{streamName}"

        /// (Tip Batch only) Number of bytes held in predecessor Batches
        b: int option

        /// base 'i' value for the Events held herein
        i: int64 // tipMagicI for the Tip

        /// Marker on which compare-and-swap operations on Tip are predicated
        etag: string

        /// `i` value for successor batch (to facilitate identifying which Batch a given startPos is within)
        n: int64

        /// The Domain Events (as opposed to Unfolded Events in `u`) for this page of the stream
        e: Event[]

        /// Compaction/Snapshot/Projection quasi-events
        u: Unfold[] }
module Batch =
    /// NOTE QueryIAndNOrderByNAscending and others rely on this, when used as the [<RangeKey>], sorting after the other items
    let tipMagicI = int64 Int32.MaxValue
    let tableKeyForStreamTip stream = TableKey.Combined(stream, tipMagicI)
    let isTip i = i = tipMagicI
    /// The concrete storage format
    [<NoEquality; NoComparison>]
    type Schema =
        {   [<HashKey>]
            p: string
            [<RangeKey>]
            i: int64 // tipMagicI for the Tip
            b: int option // iff Tip: bytes in predecessor batches
            etag: string option
            n: int64
            /// Count of events appended to stream with this insert/append.
            /// N/A for calves; all writes go via Tip as only item updates guarantee ordered arrival at Lambda via DDB streams
            a: int option
            /// NOTE the per-event e.c values are actually stored here, so they can be selected out without hydrating the bodies
            c: string[]
            /// NOTE as per Event, but without c and t fields; we instead unroll those as arrays at top level
            e: EventSchema[]
            u: UnfoldSchema[] }
    and [<NoEquality; NoComparison>] EventSchema =
        {   t: DateTimeOffset // NOTE there has to be a single non-`option` field per record, or a trailing insert will be stripped
            d: MemoryStream option; D: int option     // D carries encoding, None -> 0 // required
            m: MemoryStream option; M: int option     // M carries encoding, None -> 0
            x: string option
            y: string option }
    and [<NoEquality; NoComparison>] UnfoldSchema =
        {   i: int64
            t: DateTimeOffset
            c: string // required
            d: MemoryStream option; D: int option     // D carries encoding, None -> 0 // required
            m: MemoryStream option; M: int option }   // M carries encoding, None -> 0
    let private toEventSchema (x: Event): EventSchema =
        let (d, D), (m, M) = InternalBody.toStreamAndEncoding x.d, InternalBody.toStreamAndEncoding x.m
        { t = x.t; d = d; D = D; m = m; M = M; x = x.correlationId; y = x.causationId }
    let eventsToSchema (xs: Event[]): (*case*) string[] * EventSchema[] =
        xs |> Array.map (fun x -> x.c), xs |> Array.map toEventSchema
    let private toUnfoldSchema (x: Unfold): UnfoldSchema =
        let (d, D), (m, M) = InternalBody.toStreamAndEncoding x.d, InternalBody.toStreamAndEncoding x.m
        { i = x.i; t = x.t; c = x.c; d = d; D = D; m = m; M = M }
    let unfoldsToSchema = Array.map toUnfoldSchema
    let private ofUnfoldSchema (x: UnfoldSchema): Unfold =
        { i = x.i; t = x.t; c = x.c; d = InternalBody.ofStreamAndEncoding (x.d, x.D); m = InternalBody.ofStreamAndEncoding (x.m, x.M) }
    let ofSchema (x: Schema): Batch =
        let baseIndex = int x.n - x.e.Length
        let events =
            Seq.zip x.c x.e
            |> Seq.mapi (fun i (c, e) ->
                let data, meta = InternalBody.ofStreamAndEncoding (e.d, e.D), InternalBody.ofStreamAndEncoding (e.m, e.M)
                { i = baseIndex + i; t = e.t; d = data; m = meta; correlationId = e.x; causationId = e.y; c = c })
        { p = x.p; b = x.b; i = x.i; etag = Option.toObj x.etag; n = x.n; e = Seq.toArray events; u = x.u |> Array.map ofUnfoldSchema }
    let enumEvents (minIndex, maxIndex) (x: Batch): Event seq =
        let indexMin, indexMax = defaultArg minIndex 0L, defaultArg maxIndex Int64.MaxValue
        // If we're loading from a nominated position, we need to discard items in the batch before/after the start on the start page
        x.e |> Seq.filter (fun e -> let i = int64 e.i in i >= indexMin && int64 i < indexMax)

    /// Computes base Index for the Item (`i` can bear the the magic value TipI when the Item is the Tip)
    let baseIndex (x: Batch) = x.n - x.e.LongLength
    let bytesUnfolds (x: Batch) = Unfold.arrayBytes x.u
    let bytesBase (x: Batch) = 80 + x.p.Length + String.length x.etag + Event.arrayBytes x.e
    let bytesTotal (xs: Batch seq) = xs |> Seq.sumBy (fun x -> bytesBase x + bytesUnfolds x)

type EncodedBody = (struct (int * ReadOnlyMemory<byte>))
module EncodedBody =

    let private decodeBody (raw: InternalBody): EncodedBody =
        raw.encoding, if raw.data = null then ReadOnlyMemory.Empty else raw.data.ToArray() |> ReadOnlyMemory
    let internal ofInternal = Core.TimelineEvent.Map decodeBody
    let internal toInternal struct (encoding, encodedBody: ReadOnlyMemory<byte>): InternalBody =
        {   encoding = encoding
            data = match encodedBody with d when d.IsEmpty -> null | d -> new MemoryStream(d.ToArray(), writable = false) }

// We only capture the total RUs, without attempting to split them into read/write as the service does not actually populate the associated fields
// See https://github.com/aws/aws-sdk-go/issues/2699#issuecomment-514378464
[<Struct>]
type RequestConsumption = { total: float }

[<Struct; RequireQualifiedAccess>]
type Direction = Forward | Backward override this.ToString() = match this with Forward -> "Forward" | Backward -> "Backward"

module Log =

    /// <summary>Name of Property used for <c>Metric</c> in <c>LogEvent</c>s.</summary>
    let [<Literal>] PropertyTag = "ddbEvt"

    [<NoEquality; NoComparison>]
    type Measurement =
       {   table: string; stream: string
           interval: StopwatchInterval; bytes: int; count: int; ru: float }
       member x.Category = x.stream |> StreamName.Internal.trust |> StreamName.Category.ofStreamName
    let inline metric table stream t bytes count rc: Measurement =
        { table = table; stream = stream; interval = t; bytes = bytes; count = count; ru = rc.total }
    [<RequireQualifiedAccess; NoEquality; NoComparison>]
    type Metric =
        /// Individual read request for the Tip
        | Tip of Measurement
        /// Individual read request for the Tip, not found
        | TipNotFound of Measurement
        /// Tip read but etag unchanged, signifying payload not inspected as it can be trusted not to have been altered
        /// (NOTE the read is still fully charged on Dynamo, as opposed to Cosmos where it is 1 RU regardless of size)
        | TipNotModified of Measurement

        /// Summarizes a set of Responses for a given Read request
        | Query of Direction * responses: int * Measurement
        /// Individual read request in a Batch
        /// Charges are rolled up into Query Metric (so do not double count)
        | QueryResponse of Direction * Measurement

        | SyncAppend of Measurement
        | SyncCalve of Measurement
        | SyncAppendConflict of Measurement
        | SyncCalveConflict of Measurement

        /// Summarizes outcome of request to trim batches from head of a stream and events in Tip
        /// count in Measurement is number of batches (documents) deleted
        /// bytes in Measurement is number of events deleted
        | Prune of responsesHandled: int * Measurement
        /// Handled response from listing of batches in a stream
        /// Charges are rolled up into the Prune Metric (so do not double count)
        | PruneResponse of Measurement
        /// Deleted an individual Batch
        | Delete of Measurement
        /// Trimmed the Tip
        | Trim of Measurement
    let [<return: Struct>] (|MetricEvent|_|) (logEvent: Serilog.Events.LogEvent): Metric voption =
        let mutable p = Unchecked.defaultof<_>
        logEvent.Properties.TryGetValue(PropertyTag, &p) |> ignore
        match p with Log.ScalarValue (:? Metric as e) -> ValueSome e | _ -> ValueNone

    /// Attach a property to the captured event record to hold the metric information
    let internal event (value: Metric) = Internal.Log.withScalarProperty PropertyTag value
    let internal prop name value (log: ILogger) = log.ForContext(name, value)
    [<RequireQualifiedAccess>]
    type Operation = Tip | Tip404 | Tip304 | Query | Append | Calve | AppendConflict | CalveConflict | Prune | Delete | Trim
    let (|Op|QueryRes|PruneRes|) = function
        | Metric.Tip s                        -> Op (Operation.Tip, s)
        | Metric.TipNotFound s                -> Op (Operation.Tip404, s)
        | Metric.TipNotModified s             -> Op (Operation.Tip304, s)

        | Metric.Query (_, _, s)              -> Op (Operation.Query, s)
        | Metric.QueryResponse (direction, s) -> QueryRes (direction, s)

        | Metric.SyncAppend s                 -> Op (Operation.Append, s)
        | Metric.SyncCalve s                  -> Op (Operation.Calve, s)
        | Metric.SyncAppendConflict s         -> Op (Operation.AppendConflict, s)
        | Metric.SyncCalveConflict s          -> Op (Operation.CalveConflict, s)

        | Metric.Prune (_, s)                 -> Op (Operation.Prune, s)
        | Metric.PruneResponse s              -> PruneRes s
        | Metric.Delete s                     -> Op (Operation.Delete, s)
        | Metric.Trim s                       -> Op (Operation.Trim, s)

    module InternalMetrics =

        module Stats =

            type internal Counter =
                 { mutable rux100: int64; mutable count: int64; mutable ms: int64 }
                 static member Create() = { rux100 = 0L; count = 0L; ms = 0L }
                 member x.Ingest(ms, ru) =
                     Interlocked.Increment(&x.count) |> ignore
                     Interlocked.Add(&x.rux100, int64 (ru * 100.)) |> ignore
                     Interlocked.Add(&x.ms, ms) |> ignore
            type internal Counters() =
                 let tables = System.Collections.Concurrent.ConcurrentDictionary<string, Counter>()
                 let create (_name: string) = Counter.Create()
                 member _.Ingest(table, ms, ru) = tables.GetOrAdd(table, create).Ingest(ms, ru)
                 member _.Tables = tables.Keys
                 member _.TryTable table = match tables.TryGetValue table with true, t -> Some t | false, _ -> None
            type Epoch() =
                let epoch = System.Diagnostics.Stopwatch.StartNew()
                member val internal Tip = Counters() with get, set
                member val internal Query = Counters() with get, set
                member val internal Append = Counters() with get, set
                member val internal Calve = Counters() with get, set
                member val internal Append409 = Counters() with get, set
                member val internal Calve409 = Counters() with get, set
                member val internal Prune = Counters() with get, set
                member val internal Delete = Counters() with get, set
                member val internal Trim = Counters() with get, set
                member _.Stop() = epoch.Stop()
                member _.Elapsed = epoch.Elapsed
            let inline private (|TableMsRu|) ({ table = t; interval = i; ru = ru }: Measurement) =
                t, int64 i.ElapsedMilliseconds, ru
            type LogSink() =
                static let mutable epoch = Epoch()
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
                            | Op ((Operation.Tip | Operation.Tip404 | Operation.Tip304), TableMsRu m)  ->
                                                                                epoch.Tip.Ingest m
                            | Op (Operation.Query, TableMsRu m)  ->             epoch.Query.Ingest m
                            | QueryRes (_direction,          _)        ->       ()
                            | Op (Operation.Append,           TableMsRu m)  ->  epoch.Append.Ingest m
                            | Op (Operation.Calve,            TableMsRu m)  ->  epoch.Calve.Ingest m
                            | Op (Operation.AppendConflict,   TableMsRu m) ->   epoch.Append409.Ingest m
                            | Op (Operation.CalveConflict,    TableMsRu m)  ->  epoch.Calve409.Ingest m
                            | Op (Operation.Prune,            TableMsRu m)  ->  epoch.Prune.Ingest m
                            | PruneRes                        _        ->       ()
                            | Op (Operation.Delete,           TableMsRu m)  ->  epoch.Delete.Ingest m
                            | Op (Operation.Trim,             TableMsRu m)  ->  epoch.Trim.Ingest m
                        | _ -> ()

        /// Relies on feeding of metrics from Log through to Stats.LogSink
        /// Use Stats.LogSink.Restart() to reset the start point (and stats) where relevant
        let dump (log: ILogger) =
            let res = Stats.LogSink.Restart()
            let stats =
              [ nameof res.Tip,       res.Tip
                nameof res.Query,     res.Query
                nameof res.Append,    res.Append
                nameof res.Append409, res.Append409
                nameof res.Calve,     res.Calve
                nameof res.Calve409,  res.Calve409
                nameof res.Prune,     res.Prune
                nameof res.Delete,    res.Delete
                nameof res.Trim,      res.Trim ]
            for table in stats |> Seq.collect (fun (_n, stat) -> stat.Tables) |> Seq.distinct |> Seq.sort do
                let mutable rows, totalCount, totalRRu, totalWRu, totalMs = 0, 0L, 0., 0., 0L
                let logActivity name count ru lat =
                    let aru, ams = (if count = 0L then Double.NaN else ru/float count), (if count = 0L then Double.NaN else float lat/float count)
                    let rut = name |> function
                        | "TOTAL" -> "" | nameof res.Tip | nameof res.Query | nameof res.Prune -> totalRRu <- totalRRu + ru; "R"
                        | _ ->                                                                    totalWRu <- totalWRu + ru; "W"
                    log.Information("{table} {name}: {count:n0}r {ru:n0}{rut:l}CU Average {avgRu:n1}CU {lat:n0}ms", table, name, count, ru, rut, aru, ams)
                for name, stat in stats do
                    match stat.TryTable table with
                    | Some stat when stat.count <> 0L ->
                        let ru = float stat.rux100 / 100.
                        totalCount <- totalCount + stat.count
                        totalMs <- totalMs + stat.ms
                        logActivity name stat.count ru stat.ms
                        rows <- rows + 1
                    | _ -> ()
                if rows > 1 then logActivity "TOTAL" totalCount (totalRRu + totalWRu) totalMs
                let measures: (string * (TimeSpan -> float)) list = [ "s", fun x -> x.TotalSeconds(*; "m", fun x -> x.TotalMinutes; "h", fun x -> x.TotalHours*) ]
                let logPeriodicRate name count rru wru = log.Information("{table} {rru:n1}R/{wru:n1}W CU @ {count:n0} rp{unit}", table, rru, wru, count, name)
                for uom, f in measures do let d = f res.Elapsed in if d <> 0. then logPeriodicRate uom (float totalCount/d |> int64) (totalRRu/d) (totalWRu/d)

module Initialization =

    open Amazon.DynamoDBv2
    let private prepare (client: IAmazonDynamoDB) tableName maybeThroughput: Async<unit> =
        let context = TableContext<Batch.Schema>(client, tableName)
        match maybeThroughput with Some throughput -> context.VerifyOrCreateTableAsync(throughput) |> Async.Ignore | None -> context.VerifyTableAsync()

    /// Verify the specified <c>tableName</c> is present and adheres to the correct schema.
    let verify (client: IAmazonDynamoDB) tableName: Async<unit> =
        let context = TableContext<Batch.Schema>(client, tableName)
        context.VerifyTableAsync()

    [<RequireQualifiedAccess>]
    type StreamingMode = Off | New | NewAndOld
    let toStreaming = function
        | StreamingMode.Off -> Streaming.Disabled
        | StreamingMode.New -> Streaming.Enabled StreamViewType.NEW_IMAGE
        | StreamingMode.NewAndOld -> Streaming.Enabled StreamViewType.NEW_AND_OLD_IMAGES

    /// Create the specified <c>tableName</c> if it does not exist. Will throw if it exists but the schema mismatches.
    let createIfNotExists (client: IAmazonDynamoDB) tableName (throughput, streamingMode): Async<unit> =
        let context = TableContext<Batch.Schema>(client, tableName)
        context.VerifyOrCreateTableAsync(throughput, toStreaming streamingMode) |> Async.Ignore

    /// Provision (or re-provision) the specified table with the specified <c>Throughput</c>. Will throw if schema mismatches.
    let provision (client: IAmazonDynamoDB) tableName (throughput, streamingMode) = async {
        let context = TableContext<Batch.Schema>(client, tableName)
        let! desc = context.VerifyOrCreateTableAsync(throughput, toStreaming streamingMode)
        return! context.UpdateTableIfRequiredAsync(throughput, toStreaming streamingMode, currentTableDescription = desc) }

    /// Yields result of <c>DescribeTable</c>; Will throw if table does not exist, or creation is in progress
    let describe (client: IAmazonDynamoDB) tableName: Async<Model.TableDescription> =
        let context = TableContext<Batch.Schema>(client, tableName)
        context.UpdateTableIfRequiredAsync()

    /// Yields the <c>StreamsARN</c> if (but only if) it streaming is presently active
    let tryGetActiveStreamsArn (x: Model.TableDescription) =
        match x.StreamSpecification with
        | ss when ss <> null && ss.StreamEnabled -> x.LatestStreamArn
        | _ -> null

type private Metrics() =
    let mutable t = 0.
    member _.Add(x: RequestMetrics) =
        for x in x.ConsumedCapacity do
            t <- t + x.CapacityUnits
    member _.Consumed: RequestConsumption = { total = t }

module private Async =

    let inline startImmediateAsTask (computation: Async<'T>) ct: Task<'T> = Async.StartImmediateAsTask(computation, ct)
    let inline executeAsTask ct (computation: Async<'T>) : Task<'T> = startImmediateAsTask computation ct

type internal BatchIndices = { isTip: bool; index: int64; n: int64 }
type StoreTable(name, createContext: (RequestMetrics -> unit) -> TableContext<Batch.Schema>) =

    member _.CreateContext(collector) = createContext collector
    member val Name = name

    /// As per Equinox.CosmosStore, we assume the table to be provisioned correctly (see DynamoStoreContext.Establish(ConnectMode) re validating on startup)
    static member Create(client, tableName) =
        let createContext collector = TableContext<Batch.Schema>(client, tableName, metricsCollector = collector)
        StoreTable(tableName, createContext)

    member _.TryGetTip(stream: string, consistentRead, ct): Task<Batch option * RequestConsumption> = task {
        let rm = Metrics()
        let context = createContext rm.Add
        let pk = Batch.tableKeyForStreamTip stream
        let! item = context.TryGetItemAsync(pk, consistentRead) |> Async.executeAsTask ct
        return item |> Option.map Batch.ofSchema, rm.Consumed }
    member _.TryUpdateTip(stream: string, updateExpr: Quotations.Expr<Batch.Schema -> Batch.Schema>, ct, ?precondition): Task<Batch * RequestConsumption> = task {
        let rm = Metrics()
        let context = createContext rm.Add
        let pk = Batch.tableKeyForStreamTip stream
        let! item = context.UpdateItemAsync(pk, updateExpr, ?precondition = precondition) |> Async.executeAsTask ct
        return item |> Batch.ofSchema, rm.Consumed }
    member _.QueryBatches(stream, consistentRead, minN, maxI, backwards, batchSize, ct): taskSeq<int * StopwatchInterval * Batch[] * RequestConsumption> =
        let compile = (createContext ignore).Template.PrecomputeConditionalExpr
        let kc = match maxI with
                 | Some maxI -> compile <@ fun (b: Batch.Schema) -> b.p = stream && b.i < maxI @>
                 | None -> compile <@ fun (b: Batch.Schema) -> b.p = stream @>
        let fc = match minN with
                 | Some minN -> compile <@ fun (b: Batch.Schema) -> b.n > minN @> |> Some
                 | None -> None
        let rec aux (i, le) = taskSeq {
            // TOCONSIDER could avoid projecting `p`
            let rm = Metrics()
            let context = createContext rm.Add
            let! t, res = context.QueryPaginatedAsync(kc, ?filterCondition = fc, limit = batchSize, ?exclusiveStartKey = le,
                                                      scanIndexForward = not backwards, consistentRead = consistentRead)
                          |> Async.startImmediateAsTask |> Stopwatch.time ct
            yield i, t, Array.map Batch.ofSchema res.Records, rm.Consumed
            match res.LastEvaluatedKey with
            | None -> ()
            | le -> yield! aux (i + 1, le) }
        aux (0, None)
    member internal _.QueryIAndNOrderByNAscending(stream, maxItems, ct): taskSeq<int * StopwatchInterval * BatchIndices[] * RequestConsumption> =
        let rec aux (index, lastEvaluated) = taskSeq {
            let rm = Metrics()
            let context = createContext rm.Add
            let keyCond = <@ fun (b: Batch.Schema) -> b.p = stream @>
            let proj = <@ fun (b: Batch.Schema) -> b.i, b.c, b.n @> // TOCONSIDER want len of c, but b.e.Length explodes in empty array case, so no choice but to return the full thing
            let! t, res = context.QueryProjectedPaginatedAsync(keyCond, proj, ?exclusiveStartKey = lastEvaluated, scanIndexForward = true, limit = maxItems)
                          |> Async.startImmediateAsTask |> Stopwatch.time ct
            yield index, t, [| for i, c, n in res -> { isTip = Batch.isTip i; index = n - int64 c.Length; n = n } |], rm.Consumed
            match res.LastEvaluatedKey with
            | None -> ()
            | le -> yield! aux (index + 1, le) }
        aux (0, None)
    member _.DeleteItem(stream: string, i, ct): Task<RequestConsumption> = task {
        let rm = Metrics()
        let context = createContext rm.Add
        let pk = TableKey.Combined(stream, i)
        let! _item = context.DeleteItemAsync(pk) |> Async.executeAsTask ct
        return rm.Consumed }

/// Represents the State of the Stream for the purposes of deciding how to map a Sync request to DynamoDB operations
[<NoComparison; NoEquality>]
type Position =
    { index: int64; etag: string; calvedBytes: int; baseBytes: int; unfoldsBytes: int; events: Event[] }
    override x.ToString() = sprintf "{ n=%d; etag=%s; e=%d; b=%d+%d }" x.index x.etag x.events.Length x.baseBytes x.unfoldsBytes
module internal Position =

    // NOTE a write of Some 0 to x.b round-trips as None
    let fromTip (x: Batch) = { index = x.n; etag = x.etag; events = x.e; calvedBytes = defaultArg x.b 0; baseBytes = Batch.bytesBase x; unfoldsBytes = Batch.bytesUnfolds x }
    let fromElements (p, b, n, e, u, etag) = fromTip { p = p; b = Some b; i = Unchecked.defaultof<_>; n = n; e = e; u = u; etag = etag }
    let tryFromBatch (x: Batch) = if Batch.isTip x.i then fromTip x |> Some else None
    let toIndex = function Some p -> p.index | None -> 0
    let toEtag = function Some p -> p.etag | None -> null
    let toVersionAndStreamBytes = function Some p -> p.index, p.calvedBytes + p.baseBytes | None -> 0, 0
    let null_ i = { index = i; etag = null; calvedBytes = 0; baseBytes = 0; unfoldsBytes = 0; events = Array.empty }
    let flatten = function Some p -> p | None -> null_ 0
    let orMinusOneSentinel = function Some p -> p | None -> null_ -1L

module internal Sync =

    let private (|DynamoDbConflict|_|): exn -> _ = function
        | Precondition.CheckFailed
        | TransactWriteItemsRequest.TransactionCanceledConditionalCheckFailed -> Some ()
        | _ -> None

    let private cce: Quotations.Expr<Batch.Schema -> bool> -> ConditionExpression<Batch.Schema> = template.PrecomputeConditionalExpr
    let private cue (ue: Quotations.Expr<Batch.Schema -> Batch.Schema>) = template.PrecomputeUpdateExpr ue

    let private batchDoesNotExistCondition =    cce <@ fun t -> NOT_EXISTS t.i @>
    let private putItemIfNotExists item =       TransactWrite.Put (item, Some batchDoesNotExistCondition)

    let private updateTip stream updater cond = TransactWrite.Update (Batch.tableKeyForStreamTip stream, Some (cce cond), cue updater)

    [<RequireQualifiedAccess; NoComparison; NoEquality>]
    type Req =
        | Append of tipWasEmpty: bool * events: Event[]
        | Calve  of calfEvents: Event[] * appendedEvents: Event[]
    [<RequireQualifiedAccess>]
    type internal Exp =
        | Version of int64
        | Etag of string
    let private generateRequests (stream: string) (req, u, exp, b', n') etag'
        : TransactWrite<Batch.Schema> list =
        let u = Batch.unfoldsToSchema u
        // TOCONSIDER figure out way to remove special casing (replaceTipEvents): Array.append to empty e/c triggers exception
        let replaceTipEvents, tipA, (tipC, tipE), (maybeCalf: Batch.Schema option) =
            match req with
            | Req.Append (tipWasEmpty, eventsToAppendToTip) ->
                let replaceTipEvents = tipWasEmpty && eventsToAppendToTip.Length <> 0
                replaceTipEvents, eventsToAppendToTip.Length, Batch.eventsToSchema eventsToAppendToTip, None
            | Req.Calve (calf, appendedEvents) ->
                let calf: Batch.Schema =
                    let tipIndex = n' - appendedEvents.LongLength
                    let calfIndex = tipIndex - calf.LongLength
                    let calfC, calfE = Batch.eventsToSchema calf
                    { p = stream; i = calfIndex; a = None; b = None; etag = None; u = [||]; c = calfC; e = calfE; n = tipIndex }
                true, Array.length appendedEvents, Batch.eventsToSchema appendedEvents, Some calf
        let genFreshTipItem (): Batch.Schema =
            { p = stream; i = Batch.tipMagicI; a = Some tipA; b = Some b'; etag = Some etag'; u = u; n = n'; e = tipE; c = tipC }
        let updateTipIf condExpr =
            let updExpr: Quotations.Expr<Batch.Schema -> Batch.Schema> =
                if replaceTipEvents  then <@ fun t -> { t with a = Some tipA; b = Some b'; etag = Some etag'; u = u; n = n'; e = tipE; c = tipC } @>
                elif tipE.Length = 0 then <@ fun t -> { t with a = Some 0;    b = Some b'; etag = Some etag'; u = u } @>
                else                      <@ fun t -> { t with a = Some tipA; b = Some b'; etag = Some etag'; u = u; n = n'; e = Array.append t.e tipE; c = Array.append t.c tipC } @>
            updateTip stream updExpr condExpr
        [   match maybeCalf with
            | Some calfItem ->                 putItemIfNotExists calfItem
            | None ->                          ()
            match exp with
            | Exp.Version 0L| Exp.Etag null -> putItemIfNotExists (genFreshTipItem ())
            | Exp.Etag etag ->                 updateTipIf <@ fun t -> t.etag = Some etag @>
            | Exp.Version ver ->               updateTipIf <@ fun t -> t.n = ver @> ]

    [<RequireQualifiedAccess; NoEquality; NoComparison>]
    type private Res =
        | Written of etag': string
        | ConflictUnknown
    let private transact (table: StoreTable, stream: string) requestArgs ct: Task<struct (RequestConsumption * Res)> = task {
        let etag' = let g = Guid.NewGuid() in g.ToString "N"
        let actions = generateRequests stream requestArgs etag'
        let rm = Metrics()
        try do! let context = table.CreateContext(rm.Add)
                match actions with
                | [ TransactWrite.Put (item, Some cond) ] -> context.PutItemAsync(item, cond) |> Async.Ignore
                | [ TransactWrite.Update (key, Some cond, updateExpr) ] -> context.UpdateItemAsync(key, updateExpr, cond) |> Async.Ignore
                | actions -> context.TransactWriteItems actions |> Async.Ignore
                |> Async.executeAsTask ct
            return struct (rm.Consumed, Res.Written etag')
        with DynamoDbConflict ->
            return rm.Consumed, Res.ConflictUnknown }

    let private transactLogged (table, stream) (baseBytes, baseEvents, req, unfolds, exp, b', n', ct) (log: ILogger)
        : Task<Res> = task {
        let! t, ({ total = ru } as rc, result) = transact (table, stream) (req, unfolds, exp, b', n') |> Stopwatch.time ct
        let calfBytes, calfCount, tipBytes, tipEvents, appended = req |> function
            | Req.Append (_tipWasEmpty, appends) ->   0, 0, baseBytes + Event.arrayBytes appends, baseEvents + appends.Length, appends
            | Req.Calve (calf, tip) ->                Event.arrayBytes calf, calf.Length, baseBytes + Event.arrayBytes tip, tip.Length, tip
        let exp, log = exp |> function
            | Exp.Etag etag ->  "e="+etag,       log |> Log.prop "expectedEtag" etag
            | Exp.Version ev -> "v="+string ev,  log |> Log.prop "expectedVersion" ev
        let outcome, log =
            let reqMetric = Log.metric table.Name stream t (calfBytes + tipBytes) (appended.Length + unfolds.Length) rc
            match result with
            | Res.Written etag' -> "OK",         log |> Log.event ((if calfBytes = 0 then Log.Metric.SyncAppend else Log.Metric.SyncCalve) reqMetric)
                                                     |> Log.prop "nextPos" n'
                                                     |> Log.prop "nextEtag" etag'
            | Res.ConflictUnknown -> "Conflict", log |> Log.event ((if calfBytes = 0 then Log.Metric.SyncAppendConflict else Log.Metric.SyncCalveConflict) reqMetric)
                                                     |> Log.prop "conflict" true
                                                     |> Log.prop "eventTypes" (Seq.truncate 5 (seq { for x in appended -> x.c }))
        let appendedBytes, unfoldsBytes = Event.arrayBytes appended, Unfold.arrayBytes unfolds
        if calfBytes <> 0 then
             log.Information("EqxDynamo {action:l}{act:l} {outcome:l} {stream:l} {ms:f1}ms {ru}RU {exp:l} {appendedE}e {appendedB}b Tip {baseE}->{tipE}e {baseB}->{tipB}b Unfolds {unfolds} {unfoldsBytes}b Calf {calfEvents} {calfBytes}b",
                             "Sync", "Calve",  outcome, stream, t.ElapsedMilliseconds, ru, exp, appended.Length, appendedBytes, baseEvents, tipEvents, baseBytes, tipBytes, unfolds.Length, unfoldsBytes, calfCount, calfBytes)
        else log.Information("EqxDynamo {action:l}{act:l} {outcome:l} {stream:l} {ms:f1}ms {ru}RU {exp:l} {appendedE}e {appendedB}b Events {events} {tipB}b Unfolds {unfolds} {unfoldsB}b",
                             "Sync", "Append", outcome, stream, t.ElapsedMilliseconds, ru, exp, appended.Length, appendedBytes, tipEvents, tipBytes, unfolds.Length, unfoldsBytes)
        return result }

    [<RequireQualifiedAccess; NoEquality; NoComparison>]
    type Result =
        | Written of etag: string * predecessorBytes: int * events: Event[] * unfolds: Unfold[]
        | ConflictUnknown

    let handle log (maxEvents, maxBytes, maxEventBytes) (table, stream)
            (pos, exp, n', events: IEventData<EncodedBody>[], unfolds: IEventData<EncodedBody>[], ct) = task {
        let baseIndex = int n' - events.Length
        let events: Event[] = events |> Array.mapi (fun i e ->
            {   i = baseIndex + i; t = e.Timestamp; c = e.EventType; d = EncodedBody.toInternal e.Data; m = EncodedBody.toInternal e.Meta
                correlationId = Option.ofObj e.CorrelationId; causationId = Option.ofObj e.CausationId })
        let unfolds: Unfold[] = unfolds |> Array.map (fun (x: IEventData<_>) ->
            {   i = n'; t = x.Timestamp; c = x.EventType; d = EncodedBody.toInternal x.Data; m = EncodedBody.toInternal x.Meta })
        if Array.isEmpty events && Array.isEmpty unfolds then invalidOp "Must write either events or unfolds."
        let cur = Position.flatten pos
        let req, predecessorBytes', tipEvents' =
            if (maxEvents |> Option.exists (fun limit -> cur.events.Length + events.Length > limit)
                || Event.arrayBytes cur.events + Event.arrayBytes events > maxEventBytes
                || Event.arrayBytes cur.events + Event.arrayBytes events + Unfold.arrayBytes unfolds > maxBytes)
               && (not << Array.isEmpty) cur.events then // even if a rule says we should calve, we don't want to produce empty ones
                Req.Calve (cur.events, events), cur.calvedBytes + Event.arrayBytes cur.events, events
            else Req.Append (Array.isEmpty cur.events, events), cur.calvedBytes, Array.append cur.events events
        match! transactLogged (table, stream) (cur.baseBytes, cur.events.Length, req, unfolds, exp pos, predecessorBytes', n', ct) log with
        | Res.Written etag' -> return Result.Written (etag', predecessorBytes', tipEvents', unfolds)
        | Res.ConflictUnknown -> return Result.ConflictUnknown }

module internal Tip =

    [<RequireQualifiedAccess>]
    type Res<'T> =
        | Found of 'T
        | NotFound
        | NotModified
    let private get (table: StoreTable, stream: string) consistentRead (maybePos: Position option) ct = task {
        match! table.TryGetTip(stream, consistentRead, ct) with
        | Some { etag = fe }, rc when fe = Position.toEtag maybePos -> return rc, Res.NotModified
        | Some t, rc -> return rc, Res.Found t
        | None, rc -> return rc, Res.NotFound }
    let private loggedGet (get: StoreTable * string -> bool  -> Position option -> CancellationToken -> Task<_>) (table, stream) consistentRead (maybePos: Position option) (log: ILogger) ct = task {
        let log = log |> Log.prop "stream" stream
        let! t, ({ total = ru } as rc, res: Res<_>) = get (table, stream) consistentRead maybePos |> Stopwatch.time ct
        let logMetric bytes count (f: Log.Measurement -> _) = log |> Log.event (f (Log.metric table.Name stream t bytes count rc))
        match res with
        | Res.NotModified ->
            (logMetric 0 0 Log.Metric.TipNotModified).Information("EqxDynamo {action:l} {res} {stream:l} {ms:f1}ms {ru}RU",
                                                                  "Tip", 304, stream, t.ElapsedMilliseconds, ru)
        | Res.NotFound ->
            (logMetric 0 0 Log.Metric.TipNotFound).Information("EqxDynamo {action:l} {res} {stream:l} {ms:f1}ms {ru}RU",
                                                               "Tip", 404, stream, t.ElapsedMilliseconds, ru)
        | Res.Found tip ->
            let eventsCount, unfoldsCount, bb, ub = tip.e.Length, tip.u.Length, Batch.bytesBase tip, Batch.bytesUnfolds tip
            let log = logMetric (bb + ub) (eventsCount + unfoldsCount) Log.Metric.Tip
            let log = match maybePos with Some p -> log |> Log.prop "startPos" p |> Log.prop "startEtag" p | None -> log
            log.Information("EqxDynamo {action:l} {res} {stream:l} v{n} {ms:f1}ms {ru}RU {etag} {events}e {unfolds}u {baseBytes}+{unfoldsBytes}b",
                            "Tip", 200, stream, tip.n, t.ElapsedMilliseconds, ru, tip.etag, eventsCount, unfoldsCount, bb, ub)
        return ru, res }
    let private enumEventsAndUnfolds (minIndex, maxIndex) (x: Batch): ITimelineEvent<InternalBody>[] =
        Seq.append<ITimelineEvent<_>> (Batch.enumEvents (minIndex, maxIndex) x |> Seq.cast) (x.u |> Seq.cast)
        // where Index is equal, unfolds get delivered after the events so the fold semantics can be 'idempotent'
        |> Seq.sortBy (fun x -> x.Index, x.IsUnfold)
        |> Array.ofSeq
    /// `pos` being Some implies that the caller holds a cached value and hence is ready to deal with Result.NotModified
    let tryLoad (log: ILogger) tableStream consistentRead (maybePos: Position option, maxIndex) ct: Task<Res<Position * int64 * ITimelineEvent<InternalBody>[]>> = task {
        let! _rc, res = loggedGet get tableStream consistentRead maybePos log ct
        match res with
        | Res.NotModified -> return Res.NotModified
        | Res.NotFound -> return Res.NotFound
        | Res.Found tip ->
            let minIndex = maybePos |> Option.map (fun x -> x.index)
            return Res.Found (Position.fromTip tip, Batch.baseIndex tip, tip |> enumEventsAndUnfolds (minIndex, maxIndex)) }

module internal Query =

    let private mkQuery (log: ILogger) (table: StoreTable, stream: string) consistentRead maxItems (direction: Direction, minIndex, maxIndex) ct =
        let minN, maxI = minIndex, maxIndex
        log.Debug("EqxDynamo Query {stream}; n>{minIndex} i<{maxIndex}", stream, Option.toNullable minIndex, Option.toNullable maxIndex)
        table.QueryBatches(stream, consistentRead, minN, maxI, (direction = Direction.Backward), maxItems, ct)

    // Unrolls the Batches in a response
    // NOTE when reading backwards, the events are emitted in reverse Index order to suit the takeWhile consumption
    let private mapPage direction (table: StoreTable, stream: string) (minIndex, maxIndex, maxItems) (maxRequests: int option)
            (log: ILogger) (i, t, batches: Batch[], rc)
        : Event[] * Position option * RequestConsumption =
        match maxRequests with
        | Some mr when i >= mr -> log.Information("EqxDynamo {action:l} Page {page} Limit exceeded", i); invalidOp "batch Limit exceeded"
        | _ -> ()
        let unwrapBatch (x: Batch) =
            Batch.enumEvents (minIndex, maxIndex) x
            |> if direction = Direction.Backward then Seq.rev else id
        let events = batches |> Seq.collect unwrapBatch |> Array.ofSeq
        let usedEventsCount, usedBytes, totalBytes = events.Length, Event.arrayBytes events, Batch.bytesTotal batches
        let baseIndex = if usedEventsCount = 0 then Nullable () else Nullable (Seq.map Batch.baseIndex batches |> Seq.min)
        let minI, maxI = match events with [||] -> Nullable (), Nullable () | xs -> Nullable events[0].i, Nullable events[xs.Length - 1].i
        (log|> Log.event (Log.Metric.QueryResponse (direction, Log.metric table.Name stream t totalBytes usedEventsCount rc)))
            .Information("EqxDynamo {action:l} {page} {minIndex}-{maxIndex} {ms:f1}ms {ru}RU {batches}/{batchSize}@{index} {count}e {bytes}/{totalBytes}b {direction:l}",
                         "Page", i, minI, maxI, t.ElapsedMilliseconds, rc.total, batches.Length, maxItems, baseIndex, usedEventsCount, usedBytes, totalBytes, direction)
        let maybePosition = batches |> Array.tryPick Position.tryFromBatch
        events, maybePosition, rc

    let private logQuery (direction, minIndex, maxIndex) (table: StoreTable, stream) interval (responsesCount, events: Event[]) n (rc: RequestConsumption) (log: ILogger) =
        let count, bytes = events.Length, Event.arrayBytes events
        let reqMetric = Log.metric table.Name stream interval bytes count rc
        let evt = Log.Metric.Query (direction, responsesCount, reqMetric)
        let action = match direction with Direction.Forward -> "QueryF" | Direction.Backward -> "QueryB"
        (log|> Log.event evt).Information(
            "EqxDynamo {action:l} {stream:l} v{n} {ms:f1}ms {ru}RU {count}e/{responses} {bytes}b >{minN} <{maxI}",
            action, stream, n, interval.ElapsedMilliseconds, rc.total, count, responsesCount, bytes, Option.toNullable minIndex, Option.toNullable maxIndex)

    let private calculateUsedVersusDroppedPayload stopIndex (xs: Event[]) : int * int =
        let mutable used, dropped = 0, 0
        let mutable found = false
        for x in xs do
            let bytes = Event.Bytes x
            if found then dropped <- dropped + bytes
            else used <- used + bytes
            if x.i = stopIndex then found <- true
        used, dropped

    [<RequireQualifiedAccess; NoComparison; NoEquality>]
    type ScanResult<'event> = { found: bool; minIndex: int64; next: int64; maybeTipPos: Position option; events: 'event[] }

    let scanTip (tryDecode: ITimelineEvent<EncodedBody> -> 'event voption, isOrigin: 'event -> bool) (pos: Position, i: int64, xs: ITimelineEvent<InternalBody>[])
        : ScanResult<'event> =
        let items = ResizeArray(xs.Length)
        let isOrigin' e =
            match EncodedBody.ofInternal e |> tryDecode with
            | ValueNone -> false
            | ValueSome e ->
                items.Insert(0, e) // WalkResult always renders events ordered correctly - here we're aiming to align with Enum.EventsAndUnfolds
                isOrigin e
        let f, e = xs |> Seq.tryFindBack isOrigin' |> Option.isSome, items.ToArray()
        { found = f; maybeTipPos = Some pos; minIndex = i; next = pos.index + 1L; events = e }

    let scan<'event> (log: ILogger) (table, stream) consistentRead maxItems maxRequests direction
        (tryDecode: ITimelineEvent<EncodedBody> -> 'event voption, isOrigin: 'event -> bool)
        (minIndex, maxIndex, ct)
        : Task<ScanResult<'event> option> = task {
        let mutable found = false
        let mutable responseCount = 0
        let mergeBatches (log: ILogger) (batchesBackward: taskSeq<Event[] * Position option * RequestConsumption>) = task {
            let mutable lastResponse, maybeTipPos, ru = None, None, 0.
            let! events =
                batchesBackward
                |> TaskSeq.collectSeq (fun (events, maybePos, rc) ->
                    if Option.isNone maybeTipPos then maybeTipPos <- maybePos
                    lastResponse <- Some events; ru <- ru + rc.total
                    responseCount <- responseCount + 1
                    seq { for x in events -> struct (x, x |> EncodedBody.ofInternal |> tryDecode) })
                |> TaskSeq.takeWhileInclusive (function
                    | struct (x, ValueSome e) when isOrigin e ->
                        found <- true
                        let log = log |> Log.prop "stream" stream
                        let logLevel = if x.i = 0 then Events.LogEventLevel.Debug else Events.LogEventLevel.Information
                        match lastResponse with
                        | None -> log.Write(logLevel, "EqxDynamo Stop @{index} {case}", x.i, x.c)
                        | Some batch ->
                            let used, residual = batch |> calculateUsedVersusDroppedPayload x.i
                            log.Write(logLevel, "EqxDynamo Stop @{index} {case} used {used}b residual {residual}b", x.i, x.c, used, residual)
                        false
                    | _ -> true)
                |> TaskSeq.toArrayAsync
            return events, maybeTipPos, { total = ru } }
        let log = log |> Log.prop "batchSize" maxItems |> Log.prop "stream" stream
        let readLog = log |> Log.prop "direction" direction
        let batches ct: taskSeq<Event[] * Position option * RequestConsumption> =
            mkQuery readLog (table, stream) consistentRead maxItems (direction, minIndex, maxIndex) ct
            |> TaskSeq.map (mapPage direction (table, stream) (minIndex, maxIndex, maxItems) maxRequests readLog)
        let! t, (events, maybeTipPos, ru) = batches >> mergeBatches log |> Stopwatch.time ct
        let raws = Array.map ValueTuple.fst events
        let decoded = if direction = Direction.Forward then Array.chooseV ValueTuple.snd events else let xs = Array.chooseV ValueTuple.snd events in Array.Reverse xs; xs
        let minMax = (None, raws) ||> Array.fold (fun acc x -> let i = int64 x.i in Some (match acc with None -> i, i | Some (n, x) -> min n i, max x i))
        let version =
            match maybeTipPos, minMax with
            | Some { index = max }, _
            | _, Some (_, max) -> max + 1L
            | None, None -> 0L
        log |> logQuery (direction, minIndex, maxIndex) (table, stream) t (responseCount, raws) version ru
        match minMax, maybeTipPos with
        | Some (i, m), _ -> return Some ({ found = found; minIndex = i; next = m + 1L; maybeTipPos = maybeTipPos; events = decoded }: ScanResult<_>)
        | None, Some { index = tipI } -> return Some { found = found; minIndex = tipI; next = tipI; maybeTipPos = maybeTipPos; events = [||] }
        | None, _ -> return None }

    let walkLazy<'event> (log: ILogger) (table, stream) maxItems maxRequests
        (tryDecode: ITimelineEvent<EncodedBody> -> 'event option, isOrigin: 'event -> bool)
        (direction, minIndex, maxIndex) ct
        : taskSeq<'event[]> = taskSeq {
        let query = mkQuery log (table, stream) (*consistentRead*)false maxItems (direction, minIndex, maxIndex)

        let readPage = mapPage direction (table, stream) (minIndex, maxIndex, maxItems) maxRequests
        let log = log |> Log.prop "batchSize" maxItems |> Log.prop "stream" stream
        let readLog = log |> Log.prop "direction" direction
        let startTicks = System.Diagnostics.Stopwatch.GetTimestamp()
        let query = query ct |> TaskSeq.map (readPage readLog)
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

                let events, _pos, rc = e.Current

                ru <- ru + rc.total
                allEvents.AddRange(events)

                let acc = ResizeArray()
                for x in events do
                    match x |> EncodedBody.ofInternal |> tryDecode with
                    | Some e when isOrigin e ->
                        let used, residual = events |> calculateUsedVersusDroppedPayload x.i
                        log.Information("EqxDynamo Stop stream={stream} at={index} {case} used={used} residual={residual}",
                                        stream, x.i, x.c, used, residual)
                        ok <- false
                        acc.Add e
                    | Some e -> acc.Add e
                    | None -> ()
                i <- i + 1
                yield acc.ToArray()
        finally
            let endTicks = System.Diagnostics.Stopwatch.GetTimestamp()
            let t = StopwatchInterval(startTicks, endTicks)
            log |> logQuery (direction, minIndex, maxIndex) (table, stream) t (i, allEvents.ToArray()) -1L { total = ru } }
    type [<NoComparison; NoEquality; RequireQualifiedAccess>] LoadRes = Pos of Position | Empty | Next of int64
    let toPosition = function LoadRes.Pos p -> Some p | LoadRes.Next _ | LoadRes.Empty -> None
    /// Manages coalescing of spans of events obtained from various sources:
    /// 1) Tip Data and/or Conflicting events
    /// 2) Querying Primary for predecessors of what's obtained from 1
    /// 3) Querying Archive for predecessors of what's obtained from 2
    let load (log: ILogger) (minIndex, maxIndex) (tip: ScanResult<'event> option)
            (primary: int64 option * int64 option * CancellationToken -> Task<ScanResult<'event> option>)
            // Choice1Of2 -> indicates whether it's acceptable to ignore missing events; Choice2Of2 -> Fallback store
            (fallback: Choice<bool, int64 option * int64 option * CancellationToken -> Task<ScanResult<'event> option>>) ct
            : Task<Position option * 'event[]> = task {
        let minI = defaultArg minIndex 0L
        match tip with
        | Some { found = true; maybeTipPos = Some p; events = e } -> return Some p, e
        | Some { minIndex = i; maybeTipPos = Some p; events = e } when i <= minI -> return Some p, e
        | _ ->

        let i, events, pos =
            match tip with
            | Some { minIndex = i; maybeTipPos = p; events = e } -> Some i, e, p
            | None -> maxIndex, Array.empty, None
        let! primary = primary (minIndex, i, ct)
        let events, res =
            match primary with
            | None -> events, match pos with Some p -> LoadRes.Pos p | None -> LoadRes.Empty
            | Some primary ->
                Array.append primary.events events,
                match pos |> Option.orElse primary.maybeTipPos with Some p -> LoadRes.Pos p | None -> LoadRes.Next primary.next
        let inline logMissing (minIndex, maxIndex) message =
            if log.IsEnabled Events.LogEventLevel.Debug then
                (log|> fun log -> match minIndex with None -> log | Some mi -> log |> Log.prop "minIndex" mi
                    |> fun log -> match maxIndex with None -> log | Some mi -> log |> Log.prop "maxIndex" mi)
                    .Debug(message)

        match primary, fallback with
        | Some { found = true }, _ -> return toPosition res, events // origin found in primary, no need to look in fallback
        | Some { minIndex = i }, _ when i <= minI -> return toPosition res, events // primary had required earliest event Index, no need to look at fallback
        | None, _ when Option.isNone tip -> return toPosition res, events // initial load where no documents present in stream
        | _, Choice1Of2 allowMissing ->
            logMissing (minIndex, i) "Origin event not found; no Archive Table supplied"
            if allowMissing then return toPosition res, events
            else return failwith "Origin event not found; no Archive Table supplied"
        | _, Choice2Of2 fallback ->

        let maxIndex = match primary with Some p -> Some p.minIndex | None -> maxIndex // if no batches in primary, high water mark from tip is max
        let! fallback = fallback (minIndex, maxIndex, ct)
        let events =
            match fallback with
            | Some s -> Array.append s.events events
            | None -> events
        match fallback with
        | Some { minIndex = i } when i <= minI -> ()
        | Some { found = true } -> ()
        | _ -> logMissing (minIndex, maxIndex) "Origin event not found in Archive Table"
        return toPosition res, events }

// Manages deletion of (full) Batches, and trimming of events in Tip, maintaining ordering guarantees by never updating non-Tip batches
// Additionally, the nature of the fallback algorithm requires that deletions be carried out in sequential order so as not to leave gaps
// NOTE: module is public so BatchIndices can be deserialized into
module internal Prune =

    let until (log: ILogger) (table: StoreTable, stream: string) maxItems indexInclusive ct: Task<int * int * int64> = task {
        let log = log |> Log.prop "stream2" stream
        let deleteItem i count: Task<RequestConsumption> = task {
            let! t, rc = (fun ct -> table.DeleteItem(stream, i, ct)) |> Stopwatch.time ct
            let reqMetric = Log.metric table.Name stream t -1 count rc
            let log = let evt = Log.Metric.Delete reqMetric in log |> Log.event evt
            log.Information("EqxDynamo {action:l} {i} {ms:f1}ms {ru}RU", "Delete", i, t.ElapsedMilliseconds, rc)
            return rc }
        let trimTip expectedN count = task {
            match! table.TryGetTip(stream, (*consistentRead = *)false, ct) with
            | None, _rc -> return failwith "unexpected NotFound"
            | Some tip, _rc when tip.n <> expectedN -> return failwith $"Concurrent write detected; Expected n=%d{expectedN} actual=%d{tip.n}"
            | Some tip, tipRc ->

            let tC, tE = Batch.eventsToSchema tip.e
            let tC', tE' = Array.skip count tC, Array.skip count tE
            let updEtag = let g = Guid.NewGuid() in g.ToString "N"
            let condExpr: Quotations.Expr<Batch.Schema -> bool> = <@ fun t -> t.etag = Some tip.etag @>
            let updateExpr: Quotations.Expr<Batch.Schema -> _> = <@ fun t -> { t with etag = Some updEtag; c = tC'; e = tE' } @>
            let! t, (_updated, updRc) = (fun ct -> table.TryUpdateTip(stream, updateExpr, ct, condExpr)) |> Stopwatch.time ct
            let rc = { total = tipRc.total + updRc.total }
            let reqMetric = Log.metric table.Name stream t -1 count rc
            let log = let evt = Log.Metric.Trim reqMetric in log |> Log.event evt
            log.Information("EqxDynamo {action:l} {count} {ms:f1}ms {ru}RU", "Trim", count, t.ElapsedMilliseconds, rc.total)
            return rc }
        let log = log |> Log.prop "index" indexInclusive
        // need to sort by n to guarantee we don't ever leave an observable gap in the sequence
        let query ct = table.QueryIAndNOrderByNAscending(stream, maxItems, ct)
        let mapPage (i, t: StopwatchInterval, batches: BatchIndices[], rc) =
            let next = Array.tryLast batches |> Option.map (fun x -> x.n)
            let reqMetric = Log.metric table.Name stream t -1 batches.Length rc
            let log = let evt = Log.Metric.PruneResponse reqMetric in log |> Log.prop "batchIndex" i |> Log.event evt
            log.Information("EqxDynamo {action:l} {batches} {ms:f1}ms n={next} {ru}RU",
                            "PruneResponse", batches.Length, t.ElapsedMilliseconds, Option.toNullable next, rc.total)
            batches, rc
        let! pt, outcomes =
            let isRelevant (x: BatchIndices) = x.index <= indexInclusive || x.isTip
            let handle (batches: BatchIndices[], rc) = task {
                let mutable delCharges, batchesDeleted, trimCharges, batchesTrimmed, eventsDeleted, eventsDeferred = 0., 0, 0., 0, 0, 0
                let mutable lwm = None
                for x in batches |> Seq.takeWhile (fun x -> isRelevant x || lwm = None) do
                    let batchSize = x.n - x.index |> int
                    let eligibleEvents = max 0 (min batchSize (int (indexInclusive + 1L - x.index)))
                    if x.isTip then // Even if we remove the last event from the Tip, we need to retain a) unfolds b) position (n)
                        if eligibleEvents <> 0 then
                            let! charge = trimTip x.n eligibleEvents
                            trimCharges <- trimCharges + charge.total
                            batchesTrimmed <- batchesTrimmed + 1
                            eventsDeleted <- eventsDeleted + eligibleEvents
                        if lwm = None then
                            lwm <- Some (x.index + int64 eligibleEvents)
                    elif x.n <= indexInclusive + 1L then
                        let! charge = deleteItem x.index batchSize
                        delCharges <- delCharges + charge.total
                        batchesDeleted <- batchesDeleted + 1
                        eventsDeleted <- eventsDeleted + batchSize
                    else // can't update a non-Tip batch, or it'll be ordered wrong from a CFP perspective
                        eventsDeferred <- eventsDeferred + eligibleEvents
                        if lwm = None then
                            lwm <- Some x.index
                return (rc, delCharges, trimCharges), lwm, (batchesDeleted + batchesTrimmed, eventsDeleted, eventsDeferred) }
            let hasRelevantItems (batches, _rc) = batches |> Array.exists isRelevant
            let load ct =
                query ct
                |> TaskSeq.map mapPage
                |> TaskSeq.takeWhile hasRelevantItems
                |> TaskSeq.mapAsync handle
                |> TaskSeq.toArrayAsync
            load |> Stopwatch.time ct
        let mutable lwm, queryCharges, delCharges, trimCharges, responses, batches, eventsDeleted, eventsDeferred = None, 0., 0., 0., 0, 0, 0, 0
        let accumulate ((qc, dc, tc), bLwm, (bCount, eDel, eDef)) =
            lwm <- max lwm bLwm
            queryCharges <- queryCharges + qc.total
            delCharges <- delCharges + dc
            trimCharges <- trimCharges + tc
            responses <- responses + 1
            batches <- batches + bCount
            eventsDeleted <- eventsDeleted + eDel
            eventsDeferred <- eventsDeferred + eDef
        outcomes |> Array.iter accumulate
        let reqMetric = Log.metric table.Name stream pt eventsDeleted batches { total = queryCharges }
        let log = let evt = Log.Metric.Prune (responses, reqMetric) in log |> Log.event evt
        let lwm = lwm |> Option.defaultValue 0L // If we've seen no batches at all, then the write position is 0L
        log.Information("EqxDynamo {action:l} {events}/{batches} lwm={lwm} {ms:f1}ms queryRu={queryRu} deleteRu={deleteRu} trimRu={trimRu}",
                        "Prune", eventsDeleted, batches, lwm, pt.ElapsedMilliseconds, queryCharges, delCharges, trimCharges)
        return eventsDeleted, eventsDeferred, lwm }

type [<NoComparison; NoEquality>] Token = { pos: Position option }
module Token =

    let create_ pos: StreamToken =
        let v, b = Position.toVersionAndStreamBytes pos
        { value = box { pos = pos }; version = v; streamBytes = b }
    let create: Position -> StreamToken = Some >> create_
    let empty = create_ None
    let (|Unpack|) (token: StreamToken): Position option = let t = unbox<Token> token.value in t.pos

    // TOCONSIDER for RollingState, comparing etags is not meaningful, and they are under our control;
    //            => should be replaced with a `revision` that increments if `index` is not changing as part of a write
    //            see also same function in CosmosStore - ideally the scheme would align
    let isStale (Unpack currentPos) (Unpack candidatePos) =
        match currentPos, candidatePos with
        | Some _, None -> true
        | None, _ -> false
        | Some current, Some candidate -> current.index > candidate.index

[<AutoOpen>]
module Internal =

    [<RequireQualifiedAccess; NoComparison; NoEquality>]
    type InternalSyncResult = Written of StreamToken | ConflictUnknown

    [<RequireQualifiedAccess; NoComparison; NoEquality>]
    type LoadFromTokenResult<'event> = Unchanged | Found of StreamToken * 'event[]

    // Item writes are charged in 1K blocks; reads in 4K blocks.
    // Selecting an appropriate limit is a trade-off between
    // - Read costs, Table Item counts and database usage (fewer, larger Items will imply lower querying and storage costs)
    // - Tip Write costs - appending an event also incurs a cost to rewrite the existing content of the Tip
    // - Uncached roundtrip latency - using the normal access strategies, the Tip is loaded as a point read before querying is triggered.
    //   Ideally that gets all the data - smaller tip sizes reduce the likelihood of that
    // - Calving costs - Calving a batch from the Tip every time is cost-prohibitive for at least the following reasons
    //   - TransactWriteItems is more than twice the cost in Write RU vs a normal UpdateItem, with associated latency impacts
    //   - various other considerations, e.g. we need to re-read the Tip next time around see https://stackoverflow.com/a/71706015/11635
    let defaultTipMaxBytes = 32 * 1024
    // In general we want to minimize round-trips, but we'll get better diagnostic feedback under load if we constrain our
    // queries to shorter pages. The effect of this is of course highly dependent on the max Item size, which is
    // dictated by the TipOptions - in the default configuration that's controlled by defaultTipMaxBytes
    let defaultMaxItems = 32

/// Defines the policies in force regarding how to split up calls when loading Event Batches via queries
type QueryOptions
    (   // Max number of Batches to return per paged query response. Default: 32.
        [<O; D null>] ?maxItems: int,
        // Dynamic version of `maxItems`, allowing one to react to dynamic configuration changes. Default: use `maxItems` value.
        [<O; D null>] ?getMaxItems: unit -> int,
        // Maximum number of trips to permit when slicing the work into multiple responses based on `MaxItems`. Default: unlimited.
        [<O; D null>] ?maxRequests,
        // Inhibit throwing when events are missing, but no fallback Table has been supplied. Default: false.
        [<O; D null>] ?ignoreMissingEvents) =
    let getMaxItems = defaultArg getMaxItems (fun () -> defaultArg maxItems defaultMaxItems)
    /// Limit for Maximum number of `Batch` records in a single query batch response
    member _.MaxItems = getMaxItems ()
    /// Maximum number of trips to permit when slicing the work into multiple responses based on `MaxItems`
    member _.MaxRequests = maxRequests
    /// Whether to inhibit throwing when events are missing, but no Archive Table has been supplied as a fallback
    member val IgnoreMissingEvents = defaultArg ignoreMissingEvents false

/// Defines the policies in force regarding accumulation/retention of Events in Tip
type TipOptions
    (   // Maximum serialized size to permit to accumulate in Tip before events get moved out to a standalone Batch. Default: 32K.
        [<O; D null>] ?maxBytes,
        // Optional maximum number of events permitted in Tip. When this is exceeded, events are moved out to a standalone Batch. Default: limited by MaxBytes
        [<O; D null>] ?maxEvents) =
    /// Maximum number of events permitted in Tip. When this is exceeded, events are moved out to a standalone Batch. Default: limited by MaxBytes
    member val MaxEvents: int option = maxEvents
    /// Maximum serialized size allowed for the Tip (including snapshots) before accumulated events are forced out to a standalone Batch in order
    /// to reduce the WCU impact of the buffered events on the cost of appending events. Default: 32K.
    /// When combined with MaxEventBytes, either condition can trigger the Calving (and increasing this one opts into accepting large WCU costs
    /// due to large snapshots without compromising on packing density of calved batches).
    member val MaxBytes = defaultArg maxBytes defaultTipMaxBytes
    /// Maximum serialized size of events to accumulate in Tip before a Calve operation is forced (independent of capacity consumed by unfolds). Default: 32K.
    member val MaxEventBytes = defaultArg maxBytes defaultTipMaxBytes

type internal StoreClient(table: StoreTable, fallback: StoreTable option, query: QueryOptions, tip: TipOptions) =

    let loadTip log stream consistentRead pos = Tip.tryLoad log (table, stream) consistentRead (pos, None)

    // Always yields events forward, regardless of direction
    member _.Read(log, stream, consistentRead, direction, (tryDecode, isOrigin), ct, ?minIndex, ?maxIndex, ?tip): Task<StreamToken * 'event[]> = task {
        let tip = tip |> Option.map (Query.scanTip (tryDecode, isOrigin))
        let maxIndex = match maxIndex with
                       | Some _ as mi -> mi
                       | None when Option.isSome tip -> Some Batch.tipMagicI
                       | None -> None
        let walk log table = Query.scan log (table, stream) consistentRead query.MaxItems query.MaxRequests direction (tryDecode, isOrigin)
        let walkFallback =
            match fallback with
            | None -> Choice1Of2 query.IgnoreMissingEvents
            | Some f -> Choice2Of2 (walk (log |> Log.prop "fallback" true) f)

        let log = log |> Log.prop "stream" stream
        let! pos, events = Query.load log (minIndex, maxIndex) tip (walk log table) walkFallback ct
        return Token.create_ pos, events }
    member _.ReadLazy(log, batching: QueryOptions, stream, direction, (tryDecode, isOrigin), ct, ?minIndex, ?maxIndex): taskSeq<'event[]> =
        Query.walkLazy log (table, stream) batching.MaxItems batching.MaxRequests (tryDecode, isOrigin) (direction, minIndex, maxIndex) ct

    member store.Load(log, (stream, maybePos), consistentRead, (tryDecode, isOrigin), checkUnfolds: bool, ct): Task<StreamToken * 'event[]> =
        if not checkUnfolds then store.Read(log, stream, consistentRead, Direction.Backward, (tryDecode, isOrigin), ct)
        else task {
            match! loadTip log stream consistentRead maybePos ct with
            | Tip.Res.NotFound -> return Token.empty, Array.empty
            | Tip.Res.NotModified -> return invalidOp "Not applicable"
            | Tip.Res.Found (pos, i, xs) -> return! store.Read(log, stream, consistentRead, Direction.Backward, (tryDecode, isOrigin), ct, tip = (pos, i, xs)) }
    member _.GetPosition(log, stream, ct, ?pos): Task<StreamToken> = task {
        match! loadTip log stream (*consistentRead*)false pos ct with
        | Tip.Res.NotFound -> return Token.empty
        | Tip.Res.NotModified -> return Token.create pos.Value
        | Tip.Res.Found (pos, _i, _unfoldsAndEvents) -> return Token.create pos }
    member store.Reload(log, (stream, maybePos: Position option), consistentRead, (tryDecode, isOrigin), ct, ?preview): Task<LoadFromTokenResult<'event>> =
        let read tipContent = task {
            let! res = store.Read(log, stream, consistentRead, Direction.Backward, (tryDecode, isOrigin), ct, minIndex = Position.toIndex maybePos, tip = tipContent)
            return LoadFromTokenResult.Found res }
        match preview with
        | Some (pos, i, xs) -> read (pos, i, xs)
        | None -> task {
            match! loadTip log stream consistentRead maybePos ct with
            | Tip.Res.NotFound -> return LoadFromTokenResult.Found (Token.empty, Array.empty)
            | Tip.Res.NotModified -> return LoadFromTokenResult.Unchanged
            | Tip.Res.Found (pos, i, xs) -> return! read (pos, i, xs) }

    member _.Sync(log, stream, pos, exp, n': int64, eventsEncoded, unfoldsEncoded, ct): Task<InternalSyncResult> = task {
        match! Sync.handle log (tip.MaxEvents, tip.MaxBytes, tip.MaxEventBytes) (table, stream) (pos, exp, n', eventsEncoded, unfoldsEncoded, ct) with
        | Sync.Result.Written (etag', b', events, unfolds) ->
            return InternalSyncResult.Written (Token.create (Position.fromElements (stream, b', n', events, unfolds, etag')))
        | Sync.Result.ConflictUnknown -> return InternalSyncResult.ConflictUnknown }

    member _.Prune(log, stream, index, ct) =
        Prune.until log (table, stream) query.MaxItems index ct

type internal StoreCategory<'event, 'state, 'req>
    (   store: StoreClient,
        codec: IEventCodec<'event, EncodedBody, 'req>, fold, initial: 'state, isOrigin: 'event -> bool,
        checkUnfolds, mapUnfolds: Choice<unit, 'event[] -> 'state -> 'event[], 'event[] -> 'state -> 'event[] * 'event[]>) =
    let fold s xs = (fold : Func<'state, 'event[], 'state>).Invoke(s, xs)
    let fetch state f = task { let! token', events = f in return struct (token', fold state events) }
    let reload (log, streamNam, requireLeader, (Token.Unpack pos as streamToken), state) ct: Task<struct (StreamToken * 'state)> = task {
        match! store.Reload(log, (streamNam, pos), requireLeader, (codec.TryDecode, isOrigin), ct) with
        | LoadFromTokenResult.Unchanged -> return struct (streamToken, state)
        | LoadFromTokenResult.Found (token', events) -> return token', fold state events }
    interface ICategory<'event, 'state, 'req> with
        member _.Empty = Token.empty, initial
        member _.Load(log, _categoryName, _streamId, streamName, _maxAge, requireLeader, ct) =
            fetch initial (store.Load(log, (streamName, None), requireLeader, (codec.TryDecode, isOrigin), checkUnfolds, ct))
        member _.Sync(log, _categoryName, _streamId, streamName, req, (Token.Unpack pos as streamToken), state, events, ct) = task {
            let state' = fold state events
            let exp, events, eventsEncoded, unfoldsEncoded =
                let encode e = codec.Encode(req, e)
                let expVer = Position.toIndex >> Sync.Exp.Version
                match mapUnfolds with
                | Choice1Of3 () ->        expVer, events, Array.map encode events, Array.empty
                | Choice2Of3 unfold ->    expVer, events, Array.map encode events, Array.map encode (unfold events state')
                | Choice3Of3 transmute -> let events', unfolds = transmute events state'
                                          Position.toEtag >> Sync.Exp.Etag, events', Array.map encode events', Array.map encode unfolds
            let baseVer = Position.toIndex pos + int64 (Array.length events)
            match! store.Sync(log, streamName, pos, exp, baseVer, eventsEncoded, unfoldsEncoded, ct) with
            | InternalSyncResult.Written token' -> return SyncResult.Written (token', state')
            | InternalSyncResult.ConflictUnknown -> return SyncResult.Conflict (reload (log, streamName, true, streamToken, state)) }
    interface Caching.IReloadable<'state> with member _.Reload(log, sn, leader, token, state, ct) = reload (log, sn, leader, token, state) ct

namespace Equinox.DynamoStore

open Equinox.Core
open Equinox.DynamoStore.Core
open System

type ProvisionedThroughput = FSharp.AWS.DynamoDB.ProvisionedThroughput
type Throughput = FSharp.AWS.DynamoDB.Throughput

type StreamViewType = Amazon.DynamoDBv2.StreamViewType
type Streaming = FSharp.AWS.DynamoDB.Streaming

[<NoComparison; NoEquality>]
type ConnectMode =
    | SkipVerify
    | Verify
    | CreateIfNotExists of Throughput
module internal ConnectMode =
    let apply client tableName = function
        | SkipVerify -> async { () }
        | Verify -> Initialization.verify client tableName
        | CreateIfNotExists throughput -> Initialization.createIfNotExists client tableName (throughput, Initialization.StreamingMode.New)

type [<RequireQualifiedAccess>] ConnectionMode = AwsEnvironment of systemName: string | AwsKeyCredentials of serviceUrl: string

/// Manages Creation and configuration of an IAmazonDynamoDB connection
type DynamoStoreConnector(clientConfig: Amazon.DynamoDBv2.AmazonDynamoDBConfig, ?credentials: Amazon.Runtime.AWSCredentials) =

    /// Connect explicitly with a triplet of serviceUrl, accessKey, secretKey. No fallback behaviors are applied.
    /// timeout: Required; AWS SDK Default: 100s
    /// maxRetries: Required; AWS SDK Default: 10
    new(serviceUrl, accessKey, secretKey, timeout: TimeSpan, retries) =
        let m = Amazon.Runtime.RequestRetryMode.Standard
        let clientConfig = Amazon.DynamoDBv2.AmazonDynamoDBConfig(ServiceURL = serviceUrl, RetryMode = m, MaxErrorRetry = retries, Timeout = timeout)
        DynamoStoreConnector(clientConfig, Amazon.Runtime.BasicAWSCredentials(accessKey, secretKey))

    /// Connect to a nominated SystemName with endpoints and credentials gathered implicitly from well-known environment variables and/or configuration etc
    /// systemName: Amazon SystemName, e.g. "us-west-1"
    /// timeout: Required; AWS SDK Default: 100s
    /// maxRetries: Required; AWS SDK Default: 10
    new(systemName, timeout: TimeSpan, retries) =
        let regionEndpoint = Amazon.RegionEndpoint.GetBySystemName(systemName)
        let m = Amazon.Runtime.RequestRetryMode.Standard
        let clientConfig = Amazon.DynamoDBv2.AmazonDynamoDBConfig(RegionEndpoint = regionEndpoint, RetryMode = m, MaxErrorRetry = retries, Timeout = timeout)
        DynamoStoreConnector(clientConfig)

    member val Options = clientConfig
    member x.Retries = x.Options.MaxErrorRetry
    member x.Timeout = let t = x.Options.Timeout in t.Value
    member x.Endpoint =
        match x.Options.ServiceURL with
        | null -> ConnectionMode.AwsEnvironment x.Options.RegionEndpoint.SystemName
        | x -> ConnectionMode.AwsKeyCredentials x

    member _.CreateDynamoDbClient() =
        match credentials with
        | None -> new Amazon.DynamoDBv2.AmazonDynamoDBClient(clientConfig) // this uses credentials=FallbackCredentialsFactory.GetCredentials()
        | Some credentials -> new Amazon.DynamoDBv2.AmazonDynamoDBClient(credentials, clientConfig)
        :> Amazon.DynamoDBv2.IAmazonDynamoDB
    member x.CreateDynamoStoreClient() = x.CreateDynamoDbClient() |> DynamoStoreClient

/// Holds the DynamoDB Client(s). There should not need to be more than a single instance per process
and DynamoStoreClient(client: Amazon.DynamoDBv2.IAmazonDynamoDB,
        // Client to use for fallback tables.
        // Events that have been archived and purged (and hence are missing from the primary) are retrieved from this Table
        [<O; D null>] ?fallbackClient: Amazon.DynamoDBv2.IAmazonDynamoDB) =
    /// Verifies or Creates the underlying Tables comprising the Store
    member x.Establish(tableName: string, [<O; D null>] ?archiveTableName, [<O; D null>] ?mode: ConnectMode): Async<unit> = async {
        let init ddb t = ConnectMode.apply ddb t (defaultArg mode ConnectMode.Verify)
        do! init x.Primary tableName
        match archiveTableName with None -> () | Some archiveTable-> do! init x.Fallback archiveTable }
    member val internal Primary = client
    member val internal Fallback = defaultArg fallbackClient client

/// Defines the policies for accessing a given Table (And optional fallback Table for retrieval of archived data).
type DynamoStoreContext(client: DynamoStoreClient, tableName, tipOptions, queryOptions, ?archiveTableName) =
    member val TipOptions = tipOptions
    member val QueryOptions = queryOptions
    new(client: DynamoStoreClient, tableName,
        // Maximum serialized event size to permit to accumulate in Tip before they get moved out to a standalone Batch. Default: 32K.
        [<O; D null>] ?maxBytes,
        // Maximum number of events permitted in Tip. When this is exceeded, events are moved out to a standalone Batch. Default: limited by maxBytes
        [<O; D null>] ?tipMaxEvents,
        // Max number of Batches to return per paged query response. Default: 32.
        [<O; D null>] ?queryMaxItems,
        // Maximum number of trips to permit when slicing the work into multiple responses limited by `queryMaxItems`. Default: unlimited.
        [<O; D null>] ?queryMaxRequests,
        // Inhibit throwing when events are missing, but no Archive Table has been supplied as a fallback
        [<O; D null>] ?ignoreMissingEvents,
        [<O; D null>] ?archiveTableName) =
        let tipOptions = TipOptions(?maxBytes = maxBytes, ?maxEvents = tipMaxEvents)
        let queryOptions = QueryOptions(?maxItems = queryMaxItems, ?maxRequests = queryMaxRequests, ?ignoreMissingEvents = ignoreMissingEvents)
        DynamoStoreContext(client, tableName, tipOptions, queryOptions, ?archiveTableName = archiveTableName)
    /// Verifies or Creates the underlying Tables comprising the Store before creating a `DynamoStoreContext`
    static member Establish(client: DynamoStoreClient, tableName: string,
            [<O; D null>] ?maxBytes, [<O; D null>] ?tipMaxEvents,
            [<O; D null>] ?queryMaxItems, [<O; D null>] ?queryMaxRequests,
            [<O; D null>] ?ignoreMissingEvents, [<O; D null>] ?archiveTableName,
            [<O; D null>] ?mode: ConnectMode): Async<DynamoStoreContext> = async {
        do! client.Establish(tableName, ?archiveTableName = archiveTableName, ?mode = mode)
        return DynamoStoreContext(client, tableName,
            ?maxBytes = maxBytes, ?tipMaxEvents = tipMaxEvents,
            ?queryMaxItems = queryMaxItems, ?queryMaxRequests = queryMaxRequests,
            ?ignoreMissingEvents = ignoreMissingEvents, ?archiveTableName = archiveTableName) }
    member val internal StoreClient =
        let primary = StoreTable.Create(client.Primary, tableName)
        let fallback = archiveTableName |> Option.map(fun t -> StoreTable.Create(client.Fallback, t))
        StoreClient(primary, fallback, queryOptions, tipOptions)

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
    | Snapshot of isOrigin: ('event -> bool) * toSnapshot: ('state -> 'event)
    /// Allow any events that pass the `isOrigin` test to be used in lieu of folding all the events from the start of the stream
    /// When writing, uses `toSnapshots` to 'unfold' the <c>'state</c>, representing it as one or more Event records to be stored in
    /// the Tip with efficient read cost.
    | MultiSnapshot of isOrigin: ('event -> bool) * toSnapshots: ('state -> 'event[])
    /// Instead of actually storing the events representing the decisions, only ever update a snapshot stored in the Tip document
    /// <remarks>In this mode, Optimistic Concurrency Control is necessarily based on the etag</remarks>
    | RollingState of toSnapshot: ('state -> 'event)
    /// Allow produced events to be filtered, transformed or removed completely and/or to be transmuted to unfolds.
    /// <remarks>
    /// In this mode, Optimistic Concurrency Control is based on the etag (rather than the normal Expected Version strategy)
    /// in order that conflicting updates to the state not involving the writing of an event can trigger retries.
    /// </remarks>
    | Custom of isOrigin: ('event -> bool) * transmute: ('event[] -> 'state -> 'event[] * 'event[])

type DynamoStoreCategory<'event, 'state, 'req> =
    inherit Equinox.Category<'event, 'state, 'req>
    new(context: DynamoStoreContext, name, codec, fold, initial, access,
        // For DynamoDB, caching is typically a central aspect of managing RU consumption to maintain performance and capacity.
        // Omitting can make sense in specific cases; if streams are short, or there's always a usable snapshot in the Tip
        // NOTE Using NoCaching with DynamoDB can have significant implications for the scalability of your application, both in terms of latency and running costs.
        // While the cost of a cache miss can be ameliorated to varying degrees by employing an appropriate `AccessStrategy`
        //   [that works well and has been validated for your scenario with real data], even a cache with a low Hit Rate provides
        //   a direct benefit in terms of the number of Read and/or Write Request Charge Units (RCU)s that need to be provisioned for your Tables.
        // NOTE Unless a <c>LoadOption</c> is used, each cache hit still involves a read roundtrip (RU charges incurred, transport latency) though deserialization is skipped due to etag match
        // NOTE re SlidingWindowPrefixed: the recommended approach is to track all relevant data in the state, and/or have the `unfold` function ensure _all_ relevant events get held in the `u`nfolds in Tip
        caching) =
        let isOrigin, checkUnfolds, mapUnfolds =
            match access with
            | AccessStrategy.Unoptimized ->                      (fun _ -> false), false, Choice1Of3 ()
            | AccessStrategy.LatestKnownEvent ->                 (fun _ -> true),  true,  Choice2Of3 (fun events _ -> events |> Array.last |> Array.singleton)
            | AccessStrategy.Snapshot (isOrigin, toSnapshot) ->  isOrigin,         true,  Choice2Of3 (fun _ -> toSnapshot >> Array.singleton<'event>)
            | AccessStrategy.MultiSnapshot (isOrigin, unfold) -> isOrigin,         true,  Choice2Of3 (fun _ -> unfold)
            | AccessStrategy.RollingState toSnapshot ->          (fun _ -> true),  true,  Choice3Of3 (fun _ state  -> Array.empty, toSnapshot state |> Array.singleton)
            | AccessStrategy.Custom (isOrigin, transmute) ->     isOrigin,         true,  Choice3Of3 transmute
        { inherit Equinox.Category<'event, 'state, 'req>(name,
            StoreCategory<'event, 'state, 'req>(context.StoreClient, codec, fold, initial, isOrigin, checkUnfolds, mapUnfolds)
            |> Caching.apply Token.isStale caching); __ = () }; val private __: unit // __ can be removed after Rider 2023.2

module Exceptions =

    let isProvisionedThroughputExceeded (x: exn) = x :? Amazon.DynamoDBv2.Model.ProvisionedThroughputExceededException
    let [<return: Struct>] (|ProvisionedThroughputExceeded|_|) e = if isProvisionedThroughputExceeded e then ValueSome () else ValueNone

namespace Equinox.DynamoStore.Core

open Equinox.Core
open FsCodec
open FSharp.Control

/// Outcome of appending events, specifying the new and/or conflicting events, together with the updated Target write position
[<RequireQualifiedAccess; NoComparison>]
type AppendResult<'t> =
    | Ok of pos: 't
    | ConflictUnknown

/// Encapsulates the core facilities Equinox.DynamoStore offers for operating directly on Events in Streams.
type EventsContext
    (   context: Equinox.DynamoStore.DynamoStoreContext,
        // Logger to write to - see https://github.com/serilog/serilog/wiki/Provided-Sinks for how to wire to your logger
        log: Serilog.ILogger) =
    do if log = null then nullArg "log"
    let resolve streamName = context.StoreClient, StreamName.toString streamName
    let maxCountPredicate count =
        let acc = ref (max (count-1) 0)
        fun _ ->
            if acc.Value = 0 then true else
            acc.Value <- acc.Value - 1
            false

    member internal _.GetLazy(streamName, ct, ?queryMaxItems, ?direction, ?minIndex, ?maxIndex): taskSeq<ITimelineEvent<EncodedBody>[]> =
        let direction = defaultArg direction Direction.Forward
        let batching = match queryMaxItems with Some qmi -> QueryOptions(qmi) | _ -> context.QueryOptions
        let store, stream = resolve streamName
        store.ReadLazy(log, batching, stream, direction, (Some, fun _ -> false), ct, ?minIndex = minIndex, ?maxIndex = maxIndex)

    member internal _.GetInternal(streamName, ct, ?minIndex, ?maxIndex, ?maxCount, ?direction) = task {
        let direction = defaultArg direction Direction.Forward
        if maxCount = Some 0 then
            // Search semantics include the first hit so we need to special case this anyway
            let startPos = (if direction = Direction.Backward then maxIndex else minIndex) |> Option.map Position.null_
            return Token.create (Position.flatten startPos), Array.empty
        else
            let isOrigin =
                match maxCount with
                | Some limit -> maxCountPredicate limit
                | None -> fun _ -> false
            let store, stream = resolve streamName
            let! token, events = store.Read(log, stream, (*consistentRead*)false, direction, (ValueSome, isOrigin), ct, ?minIndex = minIndex, ?maxIndex = maxIndex)
            if direction = Direction.Backward then System.Array.Reverse events
            return token, events }

    /// Establishes the current position of the stream in as efficient a manner as possible
    /// (The ideal situation is that the preceding token is supplied as input in order to avail of efficient validation of an unchanged state)
    member _.Sync(streamName, ct, [<O; D null>] ?position: Position): Task<Position> = task {
        let store, stream = resolve streamName
        let! Token.Unpack pos' = store.GetPosition(log, stream, ct, ?pos = position)
        return Position.flatten pos' }

    /// Query (with MaxItems set to `queryMaxItems`) from the specified `Position`, allowing the reader to efficiently walk away from a running query
    /// ... NB as long as they Dispose!
    member x.Walk(streamName, queryMaxItems, ct, [<O; D null>] ?minIndex, [<O; D null>] ?maxIndex, [<O; D null>] ?direction)
        : taskSeq<ITimelineEvent<EncodedBody>[]> =
        x.GetLazy(streamName, queryMaxItems, ct, ?direction = direction, ?minIndex = minIndex, ?maxIndex = maxIndex)

    /// Reads all Events from a `Position` in a given `direction`
    member x.Read(streamName, ct, [<O; D null>] ?minIndex, [<O; D null>] ?maxIndex, [<O; D null>] ?maxCount, [<O; D null>] ?direction)
        : Task<ITimelineEvent<EncodedBody>[]> = task {
        let! _pos', data = x.GetInternal(streamName, ct, ?minIndex = minIndex, ?maxIndex = maxIndex, ?maxCount = maxCount, ?direction = direction)
        return data }
#if APPEND_SUPPORT

    /// Appends the supplied batch of events, subject to a consistency check based on the `position`
    /// Callers should implement appropriate idempotent handling, or use Equinox.Decider for that purpose
    member x.Sync(streamName, position, events: IEventData<_>[]): Async<AppendResult<Position>> = async {
        let store, stream = resolve streamName
        match! x.Sync(log, stream, Some position, Position.toIndex >> Sync.Exp.Version, position.index, events, Seq.empty) with
        | InternalSyncResult.Written (Token.Unpack pos) -> return AppendResult.Ok (Position.flatten pos)
        | InternalSyncResult.ConflictUnknown -> return AppendResult.ConflictUnknown }
#endif

    member _.Prune(streamName, index, ct): Task<int * int * int64> =
        let store, stream = resolve streamName
        store.Prune(log, stream, index, ct)

/// Provides mechanisms for building `EventData` records to be supplied to the `Events` API
type EventData() =
    /// Creates an Event record, suitable for supplying to Append et al
    static member FromUtf8Bytes(eventType, data, ?meta): IEventData<_> = FsCodec.Core.EventData.Create(eventType, data, ?meta = meta) :> _

/// Api as defined in the Equinox Specification
/// Note the DynamoContext APIs can yield better performance due to the fact that a Position tracks the etag of the Stream's Tip
module Events =

    let private (|PositionIndex|) (x: Position) = x.index
    let private stripSyncResult (f: Async<AppendResult<Position>>): Async<AppendResult<int64>> = async {
        match! f with
        | AppendResult.Ok (PositionIndex index)-> return AppendResult.Ok index
        | AppendResult.ConflictUnknown -> return AppendResult.ConflictUnknown }
    let private stripPosition (f: Async<Position>): Async<int64> = async {
        let! (PositionIndex index) = f
        return index }

    /// Returns an async sequence of events in the stream starting at the specified sequence number,
    /// reading in batches of the specified size.
    /// Returns an empty sequence if the stream is empty or if the sequence number is larger than the largest
    /// sequence number in the stream.
    let getAll (ctx: EventsContext) (streamName: StreamName) (index: int64) (batchSize: int) ct: taskSeq<ITimelineEvent<EncodedBody>[]> =
        ctx.Walk(streamName, ct, batchSize, minIndex = index)

    /// Returns an async array of events in the stream starting at the specified sequence number,
    /// number of events to read is specified by batchSize
    /// Returns an empty sequence if the stream is empty or if the sequence number is larger than the largest
    /// sequence number in the stream.
    let get (ctx: EventsContext) (streamName: StreamName) (index: int64) (maxCount: int): Async<ITimelineEvent<EncodedBody>[]> = Async.call <| fun ct ->
        ctx.Read(streamName, ct, ?minIndex = (if index = 0 then None else Some index), maxCount = maxCount)

#if APPEND_SUPPORT
    /// Appends a batch of events to a stream at the specified expected sequence number.
    /// If the specified expected sequence number does not match the stream, the events are not appended
    /// and a failure is returned.
    let append (ctx: EventsContext) (streamName: StreamName) (index: int64) (events: IEventData<_>[]): Async<AppendResult<int64>> =
        ctx.Sync(streamName, Sync.Exp.Version index, events) |> stripSyncResult

#endif
    /// Requests deletion of events up and including the specified <c>index</c>.
    /// Due to the need to preserve ordering of data in the stream, only complete Batches will be removed.
    /// If the <c>index</c> is within the Tip, events are removed via an etag-checked update. Does not alter the unfolds held in the Tip, or remove the Tip itself.
    /// Returns count of events deleted this time, events that could not be deleted due to partial batches, and the stream's lowest remaining sequence number.
    let pruneUntil (ctx: EventsContext) (streamName: StreamName) (index: int64): Async<int * int * int64> = Async.call <| fun ct ->
        ctx.Prune(streamName, index, ct)

    /// Returns an async sequence of events in the stream backwards starting from the specified sequence number,
    /// reading in batches of the specified size.
    /// Returns an empty sequence if the stream is empty or if the sequence number is smaller than the smallest
    /// sequence number in the stream.
    let getAllBackwards (ctx: EventsContext) (streamName: StreamName) (index: int64) (batchSize: int) ct: taskSeq<ITimelineEvent<EncodedBody>[]> =
        ctx.Walk(streamName, ct, batchSize, maxIndex = index, direction = Direction.Backward)

    /// Returns an async array of events in the stream backwards starting from the specified sequence number,
    /// number of events to read is specified by batchSize
    /// Returns an empty sequence if the stream is empty or if the sequence number is smaller than the smallest
    /// sequence number in the stream.
    let getBackwards (ctx: EventsContext) (streamName: StreamName) (index: int64) (maxCount: int): Async<ITimelineEvent<EncodedBody>[]> = Async.call <| fun ct ->
        ctx.Read(streamName, ct, ?maxIndex = (match index with int64.MaxValue -> None | i -> Some (i + 1L)), maxCount = maxCount, direction = Direction.Backward)

  /// Obtains the `index` from the current write Position
    let getNextIndex (ctx: EventsContext) (streamName: StreamName): Async<int64> =
        Async.call (fun ct -> ctx.Sync(streamName, ct))
        |> stripPosition
