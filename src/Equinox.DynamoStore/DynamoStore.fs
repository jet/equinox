namespace Equinox.DynamoStore.Core

open Amazon.DynamoDBv2
open Equinox.Core
open FsCodec
open FSharp.AWS.DynamoDB
open FSharp.Control
open Serilog
open System

type EventBody = byte[]
module EventBody = let (|Bytes|) : EventBody -> int = function null -> 0 | x -> x.Length

/// A single Domain Event from the array held in a Batch
[<NoEquality; NoComparison>]
type Event =
    {   /// Creation Timestamp, as set by the application layer at the point of rendering the Event
        t : DateTimeOffset

        /// The Event Type (Case) that defines the content of the Data (and Metadata) fields
        c : string

        /// Event body, as UTF-8 encoded json
        d : EventBody // required

        /// Optional metadata, encoded as per 'd'
        m : EventBody // can be null

        /// CorrelationId; stored as x (signifying transactionId), or null
        correlationId : string option

        /// CausationId; stored as y (signifying why), or null
        causationId : string option
    }
    interface IEventData<EventBody> with
        member x.EventType = x.c
        member x.Data = x.d
        member x.Meta = x.m
        member _.EventId = Guid.Empty
        member x.CorrelationId = Option.toObj x.correlationId
        member x.CausationId = Option.toObj x.causationId
        member x.Timestamp = x.t
module Event =
    let bytes (x: #IEventData<_>) = let EventBody.Bytes db, EventBody.Bytes mb = x.Data, x.Meta in x.EventType.Length + db + mb + 100
    let arrayBytes xs = Array.sumBy bytes xs
    let (|Bytes|) (x : Event) = bytes x

/// Compaction/Snapshot/Projection Event based on the state at a given point in time `i`
type [<NoEquality; NoComparison>] Unfold =
    {   /// Base: Stream Position (Version) of State from which this Unfold was generated
        i : int64

        /// Generation datetime
        t : DateTimeOffset

        /// The Case (Event Type) of this snapshot, used to drive deserialization
        c : string // required

        /// Event body - Json -> byte[] -> (maybe) Deflate
        d : EventBody // required

        /// Optional metadata, same encoding as `d` (can be null, which is mapped to empty)
        m : EventBody }
module Unfold =
    let bytes (x : Unfold) = let EventBody.Bytes db, EventBody.Bytes mb = x.d, x.m in x.c.Length + db + mb + 50
    let arrayBytes (xs : Unfold[]) = match xs with null -> 0 | u -> Array.sumBy bytes u

/// The abstract storage format for a 'normal' (frozen, not Tip) Batch of Events (without any Unfolds)
/// NOTE See BatchSchema for what actually gets stored
/// NOTE names are intended to generally align with CosmosStore naming. Key Diffs:
/// - no mandatory `id` and/or requirement for it to be a `string` -> replaced with `i` as an int64 (also Tip magic value is tipMagicI: int.MaxValue, not "-1")
/// - etag is managed explicitly (on Cosmos DB, its managed by the service and named "_etag")
/// NOTE see the BatchSchema buddy type for what the store internally has
[<NoEquality; NoComparison>]
type Batch =
    {   p : string // "{streamName}"

        /// base 'i' value for the Events held herein
        i : int64 // tipMagicI for the Tip

        /// Marker on which compare-and-swap operations on Tip are predicated
        etag : string

        /// `i` value for successor batch (to facilitate identifying which Batch a given startPos is within)
        n : int64

        /// The Domain Events (as opposed to Unfolded Events, see Tip) at this offset in the stream
        e : Event[]

        /// Compaction/Snapshot/Projection quasi-events
        u : Unfold[] }
module Batch =
    /// The concrete storage format
    [<NoEquality; NoComparison>]
    type Schema =
        {   [<HashKey>]
            p : string
            [<RangeKey>]
            i : int64 // tipMagicI for the Tip
            etag : string option
            n : int64
            // Count of items written in the most recent insert/update - used by the DDB Streams Consumer to identify the fresh events
            a : int
            // NOTE the per-event e.c and e.t values are actually stored here, so they can be selected out without hydrating the bodies
            c : string[]
            t : DateTimeOffset[]
            // NOTE as per Event, but without c and t fields; we instead unroll those as arrays at top level
            e : EventSchema[]
            u : Unfold[] }
     and EventSchema =
        {   d : EventBody
            m : EventBody
            x : string option
            y : string option }
    /// Computes base Index for the Item (`i` can bear the the magic value TipI when the Item is the Tip)
    let baseIndex (x : Batch) = x.n - x.e.LongLength
    let tipMagicI = int64 Int32.MaxValue
    let baseBytes (x : Batch) = 80 + x.p.Length + String.length x.etag + Event.arrayBytes x.e
    let unfoldsBytes (x : Batch) = Unfold.arrayBytes x.u
    let allBytes (xs : Batch seq) = xs |> Seq.sumBy (fun x -> baseBytes x + unfoldsBytes x)
    let toSchema (xs : Event[]) : (*case*) string[] * (*timestamp*) DateTimeOffset[] * EventSchema[] =
        let toEventSchema (x : Event) : EventSchema = { d = x.d; m = (match x.m with null -> Array.empty | x -> x); x = x.correlationId; y = x.causationId }
        xs |> Array.map (fun x -> x.c), xs |> Array.map (fun x -> x.t), xs |> Array.map toEventSchema
    let ofSchema (x : Schema) : Batch =
        let e' = (x.e, x.c, x.t) |||> Array.map3 (fun e c t -> { t = t; d = e.d; m = e.m; correlationId = e.x; causationId = e.y; c = c })
        { p = x.p; i = x.i; etag = Option.toObj x.etag; n = x.n; e = e'; u = x.u }

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
                yield FsCodec.Core.TimelineEvent.Create(index, x.c, x.d, x.m, Guid.Empty, Option.toObj x.correlationId, Option.toObj x.causationId, x.t) }
    static member internal Events(b : Batch, ?minIndex, ?maxIndex) =
        Enum.Events(Batch.baseIndex b, b.e, ?minIndex = minIndex, ?maxIndex = maxIndex)
    static member Unfolds(xs : Unfold[]) : ITimelineEvent<EventBody> seq = seq {
        for x in xs -> FsCodec.Core.TimelineEvent.Create(x.i, x.c, x.d, x.m, Guid.Empty, null, null, x.t, isUnfold = true) }
    static member EventsAndUnfolds(x : Batch, ?minIndex, ?maxIndex) : ITimelineEvent<EventBody> seq =
        Enum.Events(x, ?minIndex = minIndex, ?maxIndex = maxIndex)
        |> Seq.append (Enum.Unfolds x.u)
        // where Index is equal, unfolds get delivered after the events so the fold semantics can be 'idempotent'
        |> Seq.sortBy (fun x -> x.Index, x.IsUnfold)

type IRetryPolicy = abstract member Execute : (int -> Async<'T>) -> Async<'T>

// We only capture the total RUs, without attempting to split them into read/write as the service does not actually populate the associated fields
// See https://github.com/aws/aws-sdk-go/issues/2699#issuecomment-514378464
type [<Struct>] RequestConsumption = { total : float }

module [<AutoOpen>] internal Helpers =

    let tms (t : StopwatchInterval) = let e = t.Elapsed in e.TotalMilliseconds

module Log =

    /// <summary>Name of Property used for <c>Metric</c> in <c>LogEvent</c>s.</summary>
    let [<Literal>] PropertyTag = "ddbEvt"

    [<NoEquality; NoComparison>]
    type Measurement =
       {   table : string; stream : string
           interval : StopwatchInterval; bytes : int; count : int; ru : float }
    let inline metric table stream t bytes count rc : Measurement =
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
        | Query of Direction * responses : int * Measurement
        /// Individual read request in a Batch
        /// Charges are rolled up into Query Metric (so do not double count)
        | QueryResponse of Direction * Measurement

        | SyncSuccess of Measurement
        | SyncConflict of Measurement

        /// Summarizes outcome of request to trim batches from head of a stream and events in Tip
        /// count in Measurement is number of batches (documents) deleted
        /// bytes in Measurement is number of events deleted
        | Prune of responsesHandled : int * Measurement
        /// Handled response from listing of batches in a stream
        /// Charges are rolled up into the Prune Metric (so do not double count)
        | PruneResponse of Measurement
        /// Deleted an individual Batch
        | Delete of Measurement
        /// Trimmed the Tip
        | Trim of Measurement
    let internal prop name value (log : ILogger) = log.ForContext(name, value)
    let internal propData name (events : #IEventData<EventBody> seq) (log : ILogger) =
        let render (body : EventBody) : string = match body with null -> null | b -> System.Text.Encoding.UTF8.GetString b
        let items = seq { for e in events do yield sprintf "{\"%s\": %s}" e.EventType (render e.Data) }
        log.ForContext(name, sprintf "[%s]" (String.concat ",\n\r" items))
    let internal propDataEvents = propData "events"
    let internal propDataUnfolds = Enum.Unfolds >> propData "unfolds"

    let internal withLoggedRetries<'t> (retryPolicy : IRetryPolicy option) (contextLabel : string) (f : ILogger -> Async<'t>) log : Async<'t> =
        match retryPolicy with
        | None -> f log
        | Some retryPolicy ->
            let withLoggingContextWrapping count =
                let log = if count = 1 then log else log |> prop contextLabel count
                f log
            retryPolicy.Execute withLoggingContextWrapping
    /// Include a LogEvent property bearing metrics
    // Sidestep Log.ForContext converting to a string; see https://github.com/serilog/serilog/issues/1124
    let internal event (value : Metric) (log : ILogger) =
        let enrich (e : Serilog.Events.LogEvent) =
            e.AddPropertyIfAbsent(Serilog.Events.LogEventProperty(PropertyTag, Serilog.Events.ScalarValue(value)))
        log.ForContext({ new Serilog.Core.ILogEventEnricher with member _.Enrich(evt, _) = enrich evt })
    let internal (|SerilogScalar|_|) : Serilog.Events.LogEventPropertyValue -> obj option = function
        | :? Serilog.Events.ScalarValue as x -> Some x.Value
        | _ -> None
    let (|MetricEvent|_|) (logEvent : Serilog.Events.LogEvent) : Metric option =
        match logEvent.Properties.TryGetValue(PropertyTag) with
        | true, SerilogScalar (:? Metric as e) -> Some e
        | _ -> None
    [<RequireQualifiedAccess>]
    type Operation = Tip | Tip404 | Tip304 | Query | Write | Conflict | Prune | Delete | Trim
    let (|Op|QueryRes|PruneRes|) = function
        | Metric.Tip s                        -> Op (Operation.Tip, s)
        | Metric.TipNotFound s                -> Op (Operation.Tip404, s)
        | Metric.TipNotModified s             -> Op (Operation.Tip304, s)

        | Metric.Query (_, _, s)              -> Op (Operation.Query, s)
        | Metric.QueryResponse (direction, s) -> QueryRes (direction, s)

        | Metric.SyncSuccess s                -> Op (Operation.Write, s)
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
                     System.Threading.Interlocked.Increment(&x.count) |> ignore
                     System.Threading.Interlocked.Add(&x.rux100, int64 (ru * 100.)) |> ignore
                     System.Threading.Interlocked.Add(&x.ms, ms) |> ignore
            let inline private (|RuMs|) ({ interval = i; ru = ru } : Measurement) = ru, int64 (tms i)
            type LogSink() =
                static let epoch = System.Diagnostics.Stopwatch.StartNew()
                static member val internal Read = Counter.Create() with get, set
                static member val internal Write = Counter.Create() with get, set
                static member val internal Conflict = Counter.Create() with get, set
                static member val internal Prune = Counter.Create() with get, set
                static member val internal Delete = Counter.Create() with get, set
                static member val internal Trim = Counter.Create() with get, set
                static member Restart() =
                    LogSink.Read <- Counter.Create()
                    LogSink.Write <- Counter.Create()
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
                            | Op ((Operation.Tip | Operation.Tip404 | Operation.Tip304 | Operation.Query), RuMs m)  ->
                                                                            LogSink.Read.Ingest m
                            | QueryRes (_direction,          _)        ->   ()
                            | Op (Operation.Write,            RuMs m)  ->  LogSink.Write.Ingest m
                            | Op (Operation.Conflict,         RuMs m)  ->  LogSink.Conflict.Ingest m
                            | Op (Operation.Prune,            RuMs m)  ->  LogSink.Prune.Ingest m
                            | PruneRes                        _        ->   ()
                            | Op (Operation.Delete,           RuMs m)  ->  LogSink.Delete.Ingest m
                            | Op (Operation.Trim,             RuMs m)  ->  LogSink.Trim.Ingest m
                        | _ -> ()

        /// Relies on feeding of metrics from Log through to Stats.LogSink
        /// Use Stats.LogSink.Restart() to reset the start point (and stats) where relevant
        let dump (log : ILogger) =
            let stats =
              [ "Read", Stats.LogSink.Read
                "Write", Stats.LogSink.Write
                "Conflict", Stats.LogSink.Conflict
                "Prune", Stats.LogSink.Prune
                "Delete", Stats.LogSink.Delete
                "Trim", Stats.LogSink.Trim ]
            let mutable rows, totalCount, totalRRu, totalWRu, totalMs = 0, 0L, 0., 0., 0L
            let logActivity name count ru lat =
                let aru, ams = (if count = 0L then Double.NaN else ru/float count), (if count = 0L then Double.NaN else float lat/float count)
                let rut = match name with "TOTAL" -> "" | "Read" | "Prune" -> totalRRu <- totalRRu + ru; "R" | _ -> totalWRu <- totalWRu + ru; "W"
                log.Information("{name}: {count:n0} requests costing {ru:n0}{rut:l}RU (average: {avgRu:n1}); Average latency: {lat:n0}ms",
                    name, count, ru, rut, aru, ams)
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
            let logPeriodicRate name count rru wru = log.Information("rp{name} {count:n0} = ~{rru:n1}R/{wru:n1}W RU", name, count, rru, wru)
            for uom, f in measures do let d = f duration in if d <> 0. then logPeriodicRate uom (float totalCount/d |> int64) (totalRRu/d) (totalWRu/d)

type Metrics() =
    let mutable t = 0.
    member _.Add(x : RequestMetrics) =
        for x in x.ConsumedCapacity do
            t <- t + x.CapacityUnits
    member _.Consumed : RequestConsumption = { total = t }

type BatchIndices = { isTip : bool; index : int64; n : int64 }
type Container(tableName, createContext : (RequestMetrics -> unit) -> TableContext<Batch.Schema>) =
    /// As per Equinox.CosmosStore, we assume the table to be provisioned correctly (see DynamoStoreClient.Connect(ConnectMode) re validating on startup)
    static member Create(client, tableName) =
        let createContext collector = TableContext<Batch.Schema>(client, tableName, metricsCollector = collector)
        Container(tableName, createContext)

    static member TableKeyForStreamTip(stream) =
        TableKey.Combined(stream, Batch.tipMagicI)
    member x.DeleteItem(stream : string, i) : Async<RequestConsumption> = async {
        let rm = Metrics()
        let context = createContext rm.Add
        let pk = TableKey.Combined(stream, i)
        let! _item = context.DeleteItemAsync(pk)
        return rm.Consumed }
    member x.TryGetTip(stream : string) : Async<Batch option * RequestConsumption> = async {
        let rm = Metrics()
        let context = createContext rm.Add
        let pk = Container.TableKeyForStreamTip stream
        let! item = context.TryGetItemAsync(pk)
        return item |> Option.map Batch.ofSchema, rm.Consumed }
    member x.TryUpdateTip(stream : string, updateExpr : Quotations.Expr<Batch.Schema -> Batch.Schema>, ?precondition) : Async<Batch * RequestConsumption> = async {
        let rm = Metrics()
        let context = createContext rm.Add
        let pk = Container.TableKeyForStreamTip stream
        let! item = context.UpdateItemAsync(pk, updateExpr, ?precondition = precondition)
        return item |> Batch.ofSchema, rm.Consumed }
    member _.EnumIAndNOrderByNAscending(stream, maxItems) : AsyncSeq<int * StopwatchInterval * BatchIndices array * RequestConsumption> =
        let rec aux (index, lastEvaluated) = asyncSeq {
            let rm = Metrics()
            let context = createContext rm.Add
            let keyCond = <@ fun (b : Batch.Schema) -> b.p = stream @>
            let proj = <@ fun (b : Batch.Schema) -> b.i, b.t, b.n @> // b.t/e/xc.Length explodes in empty array case, so no choice but to return the full thing
            let! t, res = context.QueryProjectedPaginatedAsync(keyCond, proj, ?exclusiveStartKey = lastEvaluated, scanIndexForward = true, limit = maxItems) |> Stopwatch.Time
            yield index, t, [| for i, ts, n in res -> { isTip = i = Batch.tipMagicI; index = n - int64 ts.Length; n = n } |], rm.Consumed
            match res.LastEvaluatedKey with
            | None -> ()
            | le -> yield! aux (index + 1, le) }
        aux (0, None)
    member _.EnumBatches(stream, minN, maxI, backwards, batchSize) : AsyncSeq<int * StopwatchInterval * Batch[] * RequestConsumption> =
        let compile = (createContext ignore).Template.PrecomputeConditionalExpr
        let kc = match maxI with
                 | Some maxI -> compile <@ fun (b : Batch.Schema) -> b.p = stream && b.i < maxI @>
                 | None -> compile <@ fun (b : Batch.Schema) -> b.p = stream @>
        let fc = match minN with
                 | Some minN -> compile <@ fun (b : Batch.Schema) -> b.n > minN @> |> Some
                 | None -> None
        let rec aux (i, lastEvaluated) = asyncSeq {
            // TOCONSIDER could avoid projecting `p`
            let rm = Metrics()
            let context = createContext rm.Add
            let! t, res = context.QueryPaginatedAsync(kc, ?filterCondition = fc, limit = batchSize, ?exclusiveStartKey = lastEvaluated, scanIndexForward = not backwards) |> Stopwatch.Time
            yield i, t, Array.map Batch.ofSchema res.Records, rm.Consumed
            match res.LastEvaluatedKey with
            | None -> ()
            | le -> yield! aux (i + 1, le)
        }
        aux (0, None)
    member _.Context(collector) = createContext collector
    member _.TableName = tableName

/// Represents the State of the Stream for the purposes of deciding how to map a Sync request to DynamoDB operations
[<NoComparison; NoEquality>]
type Position =
    { index : int64; etag : string; baseBytes : int; unfoldsBytes : int; events : Event[] }
    override x.ToString() = sprintf "{ n=%d; etag=%s; e=%d; b=%d+%d }" x.index x.etag x.events.Length x.baseBytes x.unfoldsBytes
module internal Position =

    let fromTip (x : Batch) = { index = x.n; etag = x.etag; events = x.e; baseBytes = Batch.baseBytes x; unfoldsBytes = Batch.unfoldsBytes x }
    let fromElements (p, n, e, u, etag) = fromTip { p = p; i = Unchecked.defaultof<_>; n = n; e = e; u = u; etag = etag }
    let tryFromBatch (x : Batch) = if x.i = Batch.tipMagicI then fromTip x |> Some else None
    let toIndex = function Some p -> p.index | None -> 0
    let toEtag = function Some p -> p.etag | None -> null
    let null_ i = { index = i; etag = null; baseBytes = 0; unfoldsBytes = 0; events = Array.empty }
    let flatten = function Some p -> p | None -> null_ 0

module internal Sync =

    [<RequireQualifiedAccess>]
    type internal Exp = Version of int64 | Etag of string

    [<RequireQualifiedAccess; NoEquality; NoComparison>]
    type Result =
        | Written of etag : string
        | ConflictUnknown

    let private cce (ce : Quotations.Expr<Batch.Schema -> bool>) = template.PrecomputeConditionalExpr ce
    let private cue (ue : Quotations.Expr<Batch.Schema -> Batch.Schema>) = template.PrecomputeUpdateExpr ue
    let private batchDoesNotExistCondition = cce <@ fun t -> NOT_EXISTS t.i @>
    let private putItemIfNotExists item = TransactWrite.Put (item, Some batchDoesNotExistCondition)
    let private updateTip stream updater cond = TransactWrite.Update (Container.TableKeyForStreamTip stream, Some (cce cond), cue updater)

    let private gen (stream : string) (tipEmpty, exp, n', calve : Event[], append : Event[], u, eventCount) etag' : TransactWrite<Batch.Schema> list =
        let tipEventCount = append.LongLength
        let tipIndex = n' - tipEventCount
        let appA = min eventCount append.Length
        let insertCalf () =
            let calfEventCount = calve.LongLength
            let calfC, calfT, calfE = Batch.toSchema calve
            let calveA = eventCount - appA
            putItemIfNotExists { p = stream; i = tipIndex - calfEventCount; n = tipIndex; a = calveA; c = calfC; t = calfT; e = calfE; etag = None; u = [||] }
        let appC, appT, appE = Batch.toSchema append
        let insertFreshTip () =
            putItemIfNotExists { p = stream; i = Batch.tipMagicI; n = n'; a = appA; c = appC; t = appT; e = appE; etag = Some etag'; u = u }
        let updateTipIf condExpr =
            let updExpr : Quotations.Expr<Batch.Schema -> Batch.Schema> =
                // TOCONSIDER figure out whether there is a way to stop DDB choking on the Array.append below when its empty instead of this special casing
                if calve.Length <> 0 || (tipEmpty && tipEventCount <> 0) then
                     <@ fun t -> { t with a = appA; c = appC; t = appT; e = appE; n = n'
                                          etag = Some etag'; u = u } @>
                elif tipEventCount <> 0 then
                     <@ fun t -> { t with a = appA; c = Array.append t.c appC; t = Array.append t.t appT; e = Array.append t.e appE; n = t.n + tipEventCount
                                          etag = Some etag'; u = u } @>
                else <@ fun t -> { t with etag = Some etag'; u = u } @>
            updateTip stream updExpr condExpr
        [   if calve.Length > 0 then
                insertCalf ()
            match exp with
            | Exp.Version 0L | Exp.Etag null -> insertFreshTip ()
            | Exp.Etag etag ->                  updateTipIf <@ fun t -> t.etag = Some etag @>
            | Exp.Version ver ->                updateTipIf <@ fun t -> t.n = ver @> ]

    let private (|DynamoDbConflict|_|) : exn -> _ = function
        | Precondition.CheckFailed
        | TransactWriteItemsRequest.TransactionCanceledConditionalCheckFailed -> Some ()
        | _ -> None
    let private run (container : Container, stream : string) (tipEmpty, exp, n', calve, append, unfolds, eventCount)
        : Async<struct (RequestConsumption * Result)> = async {
        let etag' = let g = Guid.NewGuid() in g.ToString "N"
        let actions = gen stream (tipEmpty, exp, n', calve, append, unfolds, eventCount) etag'
        let rm = Metrics()
        try do! let context = container.Context(rm.Add)
                match actions with
                | [ TransactWrite.Put (item, Some cond) ] -> context.PutItemAsync(item, cond) |> Async.Ignore
                | [ TransactWrite.Update (key, Some cond, updateExpr) ] -> context.UpdateItemAsync(key, updateExpr, cond) |> Async.Ignore
                | actions -> context.TransactWriteItems actions
            return rm.Consumed, Result.Written etag'
        with DynamoDbConflict -> return rm.Consumed, Result.ConflictUnknown }

    let private logged (container, stream) (tipEmpty, exp : Exp, n', calve, append, unfolds, eventCount) (log : ILogger)
        : Async<Result> = async {
        let! t, ({ total = ru } as rc, result) = run (container, stream) (tipEmpty, exp, n', calve, append, unfolds, eventCount) |> Stopwatch.Time
        let calveBytes, tipBytes = Event.arrayBytes calve, Event.arrayBytes append + Unfold.arrayBytes unfolds
        let ucount = Array.length unfolds
        let log =
            let reqMetric = Log.metric container.TableName stream t (calveBytes + tipBytes) (eventCount+ucount) rc
            if log.IsEnabled Events.LogEventLevel.Debug then log |> Log.propDataEvents (Enum.Events(n', append)) |> Log.propDataUnfolds unfolds else log
            |> match exp with
                | Exp.Etag et ->            Log.prop "expectedEtag" et
                | Exp.Version ev ->         Log.prop "expectedVersion" ev
            |> match result with
                | Result.Written etag' ->   Log.prop "nextPos" n' >> Log.prop "nextEtag" etag' >> Log.event (Log.Metric.SyncSuccess reqMetric)
                | Result.ConflictUnknown -> Log.prop "conflict" true
                                            >> Log.prop "eventTypes" (Seq.truncate 5 (seq { for x in append -> x.c }))
                                            >> Log.event (Log.Metric.SyncConflict reqMetric)
        log.Information("EqxDynamo {action:l} {stream:l} {ecount}+{ucount} {ms:f1}ms {ru}RU {tipBytes:n0}+{calveBytes:n0}b {exp}",
            "Sync", stream, eventCount, ucount, tms t, ru, tipBytes, calveBytes, exp)
        return result }

    let batch (log : ILogger) retryPolicy containerStream expBatch : Async<Result> =
        let call = logged containerStream expBatch
        Log.withLoggedRetries retryPolicy "writeAttempt" call log

module Initialization =

    let private prepare (client : IAmazonDynamoDB) tableName maybeThroughput : Async<unit> =
        let context = TableContext<Batch.Schema>(client, tableName)
        match maybeThroughput with Some throughput -> context.VerifyOrCreateTableAsync(throughput) | None -> context.VerifyTableAsync()

    /// Verify the specified <c>tableName</c> is present and adheres to the correct schema.
    let verify (client : IAmazonDynamoDB) tableName : Async<unit> =
        let context = TableContext<Batch.Schema>(client, tableName)
        context.VerifyTableAsync()

    [<RequireQualifiedAccess>]
    type StreamingMode = Off | Default | NewAndOld
    let toStreaming = function
        | StreamingMode.Off -> Streaming.Disabled
        | StreamingMode.Default -> Streaming.Enabled StreamViewType.NEW_IMAGE
        | StreamingMode.NewAndOld -> Streaming.Enabled StreamViewType.NEW_AND_OLD_IMAGES

    /// Create the specified <c>tableName</c> if it does not exist. Will throw if it exists but the schema mismatches.
    let createIfNotExists (client : IAmazonDynamoDB) tableName (throughput, streamingMode) : Async<unit> =
        let context = TableContext<Batch.Schema>(client, tableName)
        context.VerifyOrCreateTableAsync(throughput, toStreaming streamingMode)

    /// Provision (or re-provision) the specified table with the specified <c>Throughput</c>. Will throw if schema mismatches.
    let provision (client : IAmazonDynamoDB) tableName (throughput, streamingMode) = async {
        let context = TableContext<Batch.Schema>(client, tableName)
        do! context.VerifyOrCreateTableAsync(throughput, toStreaming streamingMode)
        do! context.UpdateTableIfRequiredAsync(throughput, toStreaming streamingMode) }

module internal Tip =

    [<RequireQualifiedAccess>]
    type Res<'T> = Found of 'T | NotFound | NotModified
    let private get (container : Container, stream : string) (maybePos : Position option) = async {
        match! container.TryGetTip(stream) with
        | Some { etag = fe }, rc when fe = Position.toEtag maybePos -> return rc, Res.NotModified
        | Some t, rc -> return rc, Res.Found t
        | None, rc -> return rc, Res.NotFound }
    let private loggedGet (get : Container * string -> Position option -> Async<_>) (container, stream) (maybePos : Position option) (log : ILogger) = async {
        let log = log |> Log.prop "stream" stream
        let! t, ({ total = ru } as rc, res : Res<_>) = get (container, stream) maybePos |> Stopwatch.Time
        let logMetric bytes count (f : Log.Measurement -> _) = log |> Log.event (f (Log.metric container.TableName stream t bytes count rc))
        match res with
        | Res.NotModified ->
            (logMetric 0 0 Log.Metric.TipNotModified).Information("EqxDynamo {action:l} {stream:l} {res} {ms:f1}ms {ru}RU", "Tip", stream, 304, tms t, ru)
        | Res.NotFound ->
            (logMetric 0 0 Log.Metric.TipNotFound).Information("EqxDynamo {action:l} {stream:l} {res} {ms:f1}ms {ru}RU", "Tip", stream, 404, tms t, ru)
        | Res.Found tip ->
            let ecount, ucount, bb, ub = tip.e.Length, tip.u.Length, Batch.baseBytes tip, Batch.unfoldsBytes tip
            let log = logMetric (bb + ub) (ecount + ucount) Log.Metric.Tip
            let log = if log.IsEnabled Events.LogEventLevel.Debug then log |> Log.propDataUnfolds tip.u else log
            let log = match maybePos with Some p -> log |> Log.prop "startPos" p |> Log.prop "startEtag" p | None -> log
            let log = log |> Log.prop "etag" tip.etag //|> Log.prop "n" tip.n
            log.Information("EqxDynamo {action:l} {stream:l} {res} {ms:f1}ms {ru}RU v{n} {events}+{unfolds}e {baseBytes}+{unfoldsBytes}b", "Tip", stream, 200, tms t, ru, tip.n, ecount, ucount, bb, ub)
        return ru, res }
    type [<RequireQualifiedAccess; NoComparison; NoEquality>] Result = NotModified | NotFound | Found of Position * i : int64 * ITimelineEvent<EventBody>[]
    /// `pos` being Some implies that the caller holds a cached value and hence is ready to deal with Result.NotModified
    let tryLoad (log : ILogger) retryPolicy containerStream (maybePos : Position option, maxIndex) : Async<Result> = async {
        let! _rc, res = Log.withLoggedRetries retryPolicy "readAttempt" (loggedGet get containerStream maybePos) log
        match res with
        | Res.NotModified -> return Result.NotModified
        | Res.NotFound -> return Result.NotFound
        | Res.Found tip ->
            let minIndex = maybePos |> Option.map (fun x -> x.index)
            return Result.Found (Position.fromTip tip, Batch.baseIndex tip, Enum.EventsAndUnfolds(tip, ?maxIndex = maxIndex, ?minIndex = minIndex) |> Array.ofSeq) }

module internal Query =

    let private mkQuery (log : ILogger) (container : Container, stream : string) maxItems (direction : Direction, minIndex, maxIndex) =
        let minN, maxI = minIndex, maxIndex
        log.Debug("EqxDynamo Query {stream}; minIndex={minIndex} maxIndex={maxIndex}", stream, minIndex, maxIndex)
        container.EnumBatches(stream, minN, maxI, (direction = Direction.Backward), maxItems)

    // Unrolls the Batches in a response
    // NOTE when reading backwards, the events are emitted in reverse Index order to suit the takeWhile consumption
    let private mapPage direction (container : Container, stream : string) (minIndex, maxIndex) (maxRequests : int option)
            (log : ILogger) (i, t, batches : Batch[], rc)
        : ITimelineEvent<EventBody>[] * Position option * RequestConsumption =
        let log = log |> Log.prop "batchIndex" i
        match maxRequests with
        | Some mr when i >= mr -> log.Information "batch Limit exceeded"; invalidOp "batch Limit exceeded"
        | _ -> ()
        let unwrapBatch (b : Batch) =
            Enum.Events(b, ?minIndex = minIndex, ?maxIndex = maxIndex)
            |> if direction = Direction.Backward then Seq.rev else id
        let events = batches |> Seq.collect unwrapBatch |> Array.ofSeq
        let count, bytes = events.Length, Batch.allBytes batches
        let reqMetric = Log.metric container.TableName stream t bytes count rc
        let log = let evt = Log.Metric.QueryResponse (direction, reqMetric) in log |> Log.event evt
        let log = if log.IsEnabled Events.LogEventLevel.Debug then log |> Log.propDataEvents events else log
        let index = if count = 0 then Nullable () else Nullable (Seq.map Batch.baseIndex batches |> Seq.min)
        (log|> Log.prop "bytes" bytes
            |> match minIndex with None -> id | Some i -> Log.prop "minIndex" i
            |> match maxIndex with None -> id | Some i -> Log.prop "maxIndex" i)
            .Information("EqxDynamo {action:l} {count}/{batches} {direction} {ms:f1}ms i={index} {ru}RU",
                "Response", count, batches.Length, direction, tms t, index, rc.total)
        let maybePosition = batches |> Array.tryPick Position.tryFromBatch
        events, maybePosition, rc

    let private logQuery direction (container : Container, stream) interval (responsesCount, events : ITimelineEvent<EventBody>[]) n (rc : RequestConsumption) (log : ILogger) =
        let count, bytes = events.Length, Event.arrayBytes events
        let reqMetric = Log.metric container.TableName stream interval bytes count rc
        let evt = Log.Metric.Query (direction, responsesCount, reqMetric)
        let action = match direction with Direction.Forward -> "QueryF" | Direction.Backward -> "QueryB"
        (log |> Log.prop "bytes" bytes |> Log.event evt).Information(
            "EqxDynamo {action:l} {stream} v{n} {count}/{responses} {ms:f1}ms {ru}RU",
            action, stream, n, count, responsesCount, tms interval, rc.total)

    let private calculateUsedVersusDroppedPayload stopIndex (xs : ITimelineEvent<EventBody>[]) : int * int =
        let mutable used, dropped = 0, 0
        let mutable found = false
        for x in xs do
            let bytes = Event.bytes x
            if found then dropped <- dropped + bytes
            else used <- used + bytes
            if x.Index = stopIndex then found <- true
        used, dropped

    [<RequireQualifiedAccess; NoComparison; NoEquality>]
    type ScanResult<'event> = { found : bool; minIndex : int64; next : int64; maybeTipPos : Position option; events : 'event[] }

    let scanTip (tryDecode : #IEventData<EventBody> -> 'event option, isOrigin : 'event -> bool) (pos : Position, i : int64, xs : #ITimelineEvent<EventBody>[]) : ScanResult<'event> =
        let items = ResizeArray()
        let isOrigin' e =
            match tryDecode e with
            | None -> false
            | Some e ->
                items.Insert(0, e) // WalkResult always renders events ordered correctly - here we're aiming to align with Enum.EventsAndUnfolds
                isOrigin e
        let f, e = xs |> Seq.tryFindBack isOrigin' |> Option.isSome, items.ToArray()
        { found = f; maybeTipPos = Some pos; minIndex = i; next = pos.index + 1L; events = e }

    // Yields events in ascending Index order
    let scan<'event> (log : ILogger) (container, stream) maxItems maxRequests direction
        (tryDecode : ITimelineEvent<EventBody> -> 'event option, isOrigin : 'event -> bool)
        (minIndex, maxIndex)
        : Async<ScanResult<'event> option> = async {
        let mutable found = false
        let mutable responseCount = 0
        let mergeBatches (log : ILogger) (batchesBackward : AsyncSeq<ITimelineEvent<EventBody>[] * Position option * RequestConsumption>) = async {
            let mutable lastResponse, maybeTipPos, ru = None, None, 0.
            let! events =
                batchesBackward
                |> AsyncSeq.map (fun (events, maybePos, rc) ->
                    if Option.isNone maybeTipPos then maybeTipPos <- maybePos
                    lastResponse <- Some events; ru <- ru + rc.total
                    responseCount <- responseCount + 1
                    seq { for x in events -> x, tryDecode x })
                |> AsyncSeq.concatSeq
                |> AsyncSeq.takeWhileInclusive (function
                    | x, Some e when isOrigin e ->
                        found <- true
                        match lastResponse with
                        | None -> log.Information("EqxDynamo Stop stream={stream} at={index} {case}", stream, x.Index, x.EventType)
                        | Some batch ->
                            let used, residual = batch |> calculateUsedVersusDroppedPayload x.Index
                            log.Information("EqxDynamo Stop stream={stream} at={index} {case} used={used} residual={residual}",
                                stream, x.Index, x.EventType, used, residual)
                        false
                    | _ -> true)
                |> AsyncSeq.toArrayAsync
            return events, maybeTipPos, { total = ru } }
        let log = log |> Log.prop "batchSize" maxItems |> Log.prop "stream" stream
        let readLog = log |> Log.prop "direction" direction
        let batches : AsyncSeq<ITimelineEvent<EventBody>[] * Position option * RequestConsumption> =
            mkQuery readLog (container, stream) maxItems (direction, minIndex, maxIndex)
            |> AsyncSeq.map (mapPage direction (container, stream) (minIndex, maxIndex) maxRequests readLog)
        let! t, (events, maybeTipPos, ru) = mergeBatches log batches |> Stopwatch.Time
        let raws = Array.map fst events
        let decoded = if direction = Direction.Forward then Array.choose snd events else Seq.choose snd events |> Seq.rev |> Array.ofSeq
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
        let query = mkQuery log (container, stream) maxItems (direction, minIndex, maxIndex)

        let readPage = mapPage direction (container, stream) (minIndex, maxIndex) maxRequests
        let log = log |> Log.prop "batchSize" maxItems |> Log.prop "stream" stream
        let readLog = log |> Log.prop "direction" direction
        let query = query |> AsyncSeq.map (readPage readLog)
        let startTicks = System.Diagnostics.Stopwatch.GetTimestamp()
        let allEvents = ResizeArray()
        let mutable i, ru = 0, 0.
        try let mutable ok = true
            let e = query.GetEnumerator()
            while ok do
                let batchLog = readLog |> Log.prop "batchIndex" i
                match maxRequests with
                | Some mr when i+1 >= mr -> batchLog.Information "batch Limit exceeded"; invalidOp "batch Limit exceeded"
                | _ -> ()

                match! e.MoveNext() with
                | None -> ok <- false // rest of block does not happen, while exits
                | Some (events, _pos, rc) ->

                ru <- ru + rc.total
                allEvents.AddRange(events)

                let acc = ResizeArray()
                for x in events do
                    match tryDecode x with
                    | Some e when isOrigin e ->
                        let used, residual = events |> calculateUsedVersusDroppedPayload x.Index
                        log.Information("EqxDynamo Stop stream={stream} at={index} {case} used={used} residual={residual}",
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
            log |> logQuery direction (container, stream) t (i, allEvents.ToArray()) -1L { total = ru } }
    type [<NoComparison; NoEquality>] LoadRes = Pos of Position | Empty | Next of int64
    let toPosition = function Pos p -> Some p | Empty -> None | Next _ -> failwith "unexpected"
    /// Manages coalescing of spans of events obtained from various sources:
    /// 1) Tip Data and/or Conflicting events
    /// 2) Querying Primary for predecessors of what's obtained from 1
    /// 3) Querying Archive for predecessors of what's obtained from 2
    let load (log : ILogger) (minIndex, maxIndex) (tip : ScanResult<'event> option)
            (primary : int64 option * int64 option -> Async<ScanResult<'event> option>)
            // Choice1Of2 -> indicates whether it's acceptable to ignore missing events; Choice2Of2 -> Fallback store
            (fallback : Choice<bool, int64 option * int64 option -> Async<ScanResult<'event> option>>)
            : Async<Position option * 'event[]> = async {
        let minI = defaultArg minIndex 0L
        match tip with
        | Some { found = true; maybeTipPos = Some p; events = e } -> return Some p, e
        | Some { minIndex = i; maybeTipPos = Some p; events = e } when i <= minI -> return Some p, e
        | _ ->

        let i, events, pos =
            match tip with
            | Some { minIndex = i; maybeTipPos = p; events = e } -> Some i, e, p
            | None -> maxIndex, Array.empty, None
        let! primary = primary (minIndex, i)
        let events, pos =
            match primary with
            | None -> events, match pos with Some p -> Pos p | None -> Empty
            | Some primary -> Array.append primary.events events, match pos |> Option.orElse primary.maybeTipPos with Some p -> Pos p | None -> Next primary.next
        let inline logMissing (minIndex, maxIndex) message =
            if log.IsEnabled Events.LogEventLevel.Debug then
                (log|> fun log -> match minIndex with None -> log | Some mi -> log |> Log.prop "minIndex" mi
                    |> fun log -> match maxIndex with None -> log | Some mi -> log |> Log.prop "maxIndex" mi)
                    .Debug(message)

        match primary, fallback with
        | Some { found = true }, _ -> return toPosition pos, events // origin found in primary, no need to look in fallback
        | Some { minIndex = i }, _ when i <= minI -> return toPosition pos, events // primary had required earliest event Index, no need to look at fallback
        | None, _ when Option.isNone tip -> return toPosition pos, events // initial load where no documents present in stream
        | _, Choice1Of2 allowMissing ->
            logMissing (minIndex, i) "Origin event not found; no Archive Table supplied"
            if allowMissing then return toPosition pos, events
            else return failwithf "Origin event not found; no Archive Table supplied"
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
        | _ -> logMissing (minIndex, maxIndex) "Origin event not found in Archive Table"
        return toPosition pos, events }

// Manages deletion of (full) Batches, and trimming of events in Tip, maintaining ordering guarantees by never updating non-Tip batches
// Additionally, the nature of the fallback algorithm requires that deletions be carried out in sequential order so as not to leave gaps
// NOTE: module is public so BatchIndices can be deserialized into
module Prune =

    let until (log : ILogger) (container : Container, stream : string) maxItems indexInclusive : Async<int * int * int64> = async {
        let log = log |> Log.prop "stream" stream
        let deleteItem i count : Async<RequestConsumption> = async {
            let! t, rc = container.DeleteItem(stream, i) |> Stopwatch.Time
            let reqMetric = Log.metric container.TableName stream t -1 count rc
            let log = let evt = Log.Metric.Delete reqMetric in log |> Log.event evt
            log.Information("EqxDynamo {action:l} {i} {ms:f1}ms {ru}RU", "Delete", i, tms t, rc)
            return rc
        }
        let trimTip expectedN count = async {
            match! container.TryGetTip(stream) with
            | None, _rc -> return failwith "unexpected NotFound"
            | Some tip, _rc when tip.n <> expectedN -> return failwithf "Concurrent write detected; Expected n=%d actual=%d" expectedN tip.n
            | Some tip, tipRc ->

            let tC, tT, tE = Batch.toSchema tip.e
            let tC', tT', tE' = Array.skip count tC, Array.skip count tT, Array.skip count tE
            let updEtag = let g = Guid.NewGuid() in g.ToString "N"
            let condExpr : Quotations.Expr<Batch.Schema -> bool> = <@ fun t -> t.etag = Some tip.etag @>
            let updateExpr : Quotations.Expr<Batch.Schema -> _> = <@ fun t -> { t with etag = Some updEtag; c = tC'; t = tT'; e = tE' } @>
            let! t, (_updated, updRc) = container.TryUpdateTip(stream, updateExpr, condExpr) |> Stopwatch.Time
            let rc = { total = tipRc.total + updRc.total }
            let reqMetric = Log.metric container.TableName stream t -1 count rc
            let log = let evt = Log.Metric.Trim reqMetric in log |> Log.event evt
            log.Information("EqxDynamo {action:l} {count} {ms:f1}ms {ru}RU", "Trim", count, tms t, rc)
            return rc
        }
        let log = log |> Log.prop "index" indexInclusive
        // need to sort by n to guarantee we don't ever leave an observable gap in the sequence
        let query = container.EnumIAndNOrderByNAscending(stream, maxItems)
        let mapPage (i, t : StopwatchInterval, batches : BatchIndices[], rc) =
            let next = Array.tryLast batches |> Option.map (fun x -> x.n) |> Option.toNullable
            let reqMetric = Log.metric container.TableName stream t -1 batches.Length rc
            let log = let evt = Log.Metric.PruneResponse reqMetric in log |> Log.prop "batchIndex" i |> Log.event evt
            log.Information("EqxDynamo {action:l} {batches} {ms:f1}ms n={next} {ru}RU", "PruneResponse", batches.Length, tms t, next, rc)
            batches, rc
        let! pt, outcomes =
            let isRelevant (x : BatchIndices) = x.index <= indexInclusive || x.isTip
            let handle (batches : BatchIndices[], rc) = async {
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
                return (rc, delCharges, trimCharges), lwm, (batchesDeleted + batchesTrimmed, eventsDeleted, eventsDeferred)
            }
            let hasRelevantItems (batches, _rc) = batches |> Array.exists isRelevant
            query
            |> AsyncSeq.map mapPage
            |> AsyncSeq.takeWhile hasRelevantItems
            |> AsyncSeq.mapAsync handle
            |> AsyncSeq.toArrayAsync
            |> Stopwatch.Time
        let mutable queryCharges, delCharges, trimCharges, responses, batches, eventsDeleted, eventsDeferred = 0., 0., 0., 0, 0, 0, 0
        let mutable lwm = None
        for (qc, dc, tc), bLwm, (bCount, eDel, eDef) in outcomes do
            lwm <- max lwm bLwm
            queryCharges <- queryCharges + qc.total
            delCharges <- delCharges + dc
            trimCharges <- trimCharges + tc
            responses <- responses + 1
            batches <- batches + bCount
            eventsDeleted <- eventsDeleted + eDel
            eventsDeferred <- eventsDeferred + eDef
        let reqMetric = Log.metric container.TableName stream pt eventsDeleted batches { total = queryCharges }
        let log = let evt = Log.Metric.Prune (responses, reqMetric) in log |> Log.event evt
        let lwm = lwm |> Option.defaultValue 0L // If we've seen no batches at all, then the write position is 0L
        log.Information("EqxDynamo {action:l} {events}/{batches} lwm={lwm} {ms:f1}ms queryRu={queryRu} deleteRu={deleteRu} trimRu={trimRu}",
                "Prune", eventsDeleted, batches, lwm, tms pt, queryCharges, delCharges, trimCharges)
        return eventsDeleted, eventsDeferred, lwm
    }

type [<NoComparison; NoEquality>] Token = { pos : Position option }
module Token =

    let create_ pos : StreamToken = { value = box { pos = pos }; version = Position.toIndex pos }
    let create : Position -> StreamToken = Some >> create_
    let empty = create_ None
    let (|Unpack|) (token : StreamToken) : Position option = let t = unbox<Token> token.value in t.pos
    let supersedes (Unpack currentPos) (Unpack xPos) =
        match currentPos, xPos with
        | Some currentPos, Some xPos ->
            let currentVersion, newVersion = currentPos.index, xPos.index
            let currentETag, newETag = currentPos.etag, xPos.etag
            newVersion > currentVersion || currentETag <> newETag
        | None, Some _ -> true
        | Some _, None
        | None, None  -> false

[<AutoOpen>]
module Internal =

    [<RequireQualifiedAccess; NoComparison; NoEquality>]
    type InternalSyncResult = Written of StreamToken | ConflictUnknown

    [<RequireQualifiedAccess; NoComparison; NoEquality>]
    type LoadFromTokenResult<'event> = Unchanged | Found of StreamToken * 'event[]

/// Defines the policies in force regarding how to split up calls when loading Event Batches via queries
type QueryOptions
    (   // Max number of Batches to return per paged query response. Default: 10.
        [<O; D(null)>]?maxItems : int,
        // Dynamic version of `maxItems`, allowing one to react to dynamic configuration changes. Default: use `maxItems` value.
        [<O; D(null)>]?getMaxItems : unit -> int,
        // Maximum number of trips to permit when slicing the work into multiple responses based on `MaxItems`. Default: unlimited.
        [<O; D(null)>]?maxRequests) =
    let getMaxItems = defaultArg getMaxItems (fun () -> defaultArg maxItems 10)
    /// Limit for Maximum number of `Batch` records in a single query batch response
    member _.MaxItems = getMaxItems ()
    /// Maximum number of trips to permit when slicing the work into multiple responses based on `MaxItems`
    member _.MaxRequests = maxRequests

/// Defines the policies in force regarding
/// - accumulation/retention of Events in Tip
/// - retrying read and write operations for the Tip
type TipOptions
    (   // Maximum serialized size to permit to accumulate in Tip before events get moved out to a standalone Batch. Default: 32K.
        [<O; D(null)>]?maxBytes,
        // Optional maximum number of events permitted in Tip. When this is exceeded, events are moved out to a standalone Batch. Default: limited by MaxBytes
        [<O; D(null)>]?maxEvents,
        // Inhibit throwing when events are missing, but no fallback Table has been supplied. Default: false.
        [<O; D(null)>]?ignoreMissingEvents,
        [<O; D(null)>]?readRetryPolicy,
        [<O; D(null)>]?writeRetryPolicy) =
//        // Compress Unfolds in Tip. Default: <c>true</c>.
//        [<O; D null>]?compressUnfolds,
//        // Compress Data in Tip. Default: <c>true</c>.
//        [<O; D null>]?compressTipData,
//        // Compress Data in non-Tip batches. Default: <c>true</c>.
//        [<O; D null>]?compressCalvedData) =
//    let compress = (compressUnfolds = Some true), (compressTipData = Some true), (compressCalvedData = Some true)
    /// Maximum number of events permitted in Tip. When this is exceeded, events are moved out to a standalone Batch. Default: limited by MaxBytes
    member val MaxEvents : int option = maxEvents
    /// Maximum serialized size to permit to accumulate in Tip before events get moved out to a standalone Batch. Default: 32K.
    member val MaxBytes = defaultArg maxBytes 32 * 1024
    /// Whether to inhibit throwing when events are missing, but no Archive Table has been supplied as a fallback
    member val IgnoreMissingEvents = defaultArg ignoreMissingEvents false

    member val ReadRetryPolicy = readRetryPolicy
    member val WriteRetryPolicy = writeRetryPolicy

type StoreClient(container : Container, fallback : Container option, query : QueryOptions, tip : TipOptions) =

    let loadTip log stream pos = Tip.tryLoad log tip.ReadRetryPolicy (container, stream) (pos, None)
    let ignoreMissing = tip.IgnoreMissingEvents

    // Always yields events forward, regardless of direction
    member internal _.Read(log, stream, direction, (tryDecode, isOrigin), ?minIndex, ?maxIndex, ?tip) : Async<StreamToken * 'event[]> = async {
        let tip = tip |> Option.map (Query.scanTip (tryDecode, isOrigin))
        let maxIndex = match maxIndex with
                       | Some _ as mi -> mi
                       | None when Option.isSome tip -> Some Batch.tipMagicI
                       | None -> None
        let walk log container = Query.scan log (container, stream) query.MaxItems query.MaxRequests direction (tryDecode, isOrigin)
        let walkFallback =
            match fallback with
            | None -> Choice1Of2 ignoreMissing
            | Some f -> Choice2Of2 (walk (log |> Log.prop "fallback" true) f)

        let log = log |> Log.prop "stream" stream
        let! pos, events = Query.load log (minIndex, maxIndex) tip (walk log container) walkFallback
        return Token.create_ pos, events }
    member _.ReadLazy(log, batching : QueryOptions, stream, direction, (tryDecode, isOrigin), ?minIndex, ?maxIndex) : AsyncSeq<'event[]> =
        Query.walkLazy log (container, stream) batching.MaxItems batching.MaxRequests (tryDecode, isOrigin) (direction, minIndex, maxIndex)

    member store.Load(log, (stream, maybePos), (tryDecode, isOrigin), checkUnfolds : bool) : Async<StreamToken * 'event[]> =
        if not checkUnfolds then store.Read(log, stream, Direction.Backward, (tryDecode, isOrigin))
        else async {
            match! loadTip log stream maybePos with
            | Tip.Result.NotFound -> return Token.empty, Array.empty
            | Tip.Result.NotModified -> return invalidOp "Not applicable"
            | Tip.Result.Found (pos, i, xs) -> return! store.Read(log, stream, Direction.Backward, (tryDecode, isOrigin), tip = (pos, i, xs)) }
    member _.GetPosition(log, stream, ?pos) : Async<StreamToken> = async {
        match! loadTip log stream pos with
        | Tip.Result.NotFound -> return Token.empty
        | Tip.Result.NotModified -> return Token.create pos.Value
        | Tip.Result.Found (pos, _i, _unfoldsAndEvents) -> return Token.create pos }
    member store.Reload(log, (stream, maybePos : Position option), (tryDecode, isOrigin), ?preview): Async<LoadFromTokenResult<'event>> =
        let read tipContent = async {
            let! res = store.Read(log, stream, Direction.Backward, (tryDecode, isOrigin), minIndex = Position.toIndex maybePos, tip = tipContent)
            return LoadFromTokenResult.Found res }
        match preview with
        | Some (pos, i, xs) -> read (pos, i, xs)
        | None -> async {
            match! loadTip log stream maybePos with
            | Tip.Result.NotFound -> return LoadFromTokenResult.Found (Token.empty, Array.empty)
            | Tip.Result.NotModified -> return LoadFromTokenResult.Unchanged
            | Tip.Result.Found (pos, i, xs) -> return! read (pos, i, xs) }

    member internal _.Sync(log, stream, pos, exp, n', eventsEncoded, unfoldsEncoded : IEventData<_> seq) : Async<InternalSyncResult> = async {
        let mkEvent (e : IEventData<EventBody>) =
            {   t = e.Timestamp; c = e.EventType; d = e.Data; m = e.Meta; correlationId = Option.ofObj e.CorrelationId; causationId = Option.ofObj e.CausationId }
        let events = Seq.map mkEvent eventsEncoded |> Array.ofSeq
        let unfolds =
            unfoldsEncoded
            |> Seq.mapi (fun offset x ->
                {   i = n' + int64 offset
                    c = x.EventType
                    d = x.Data
                    m = match x.Meta with null -> Array.empty | x -> x
                    t = DateTimeOffset.UtcNow
                } : Unfold)
            |> Seq.toArray
        if Array.isEmpty events && Array.isEmpty unfolds then invalidOp "Must write either events or unfolds."
        let cur = Position.flatten pos
        let calve, append, e' =
            let eventOverflow = tip.MaxEvents |> Option.exists (fun limit -> events.Length + cur.events.Length > limit)
            if eventOverflow || cur.baseBytes + Unfold.arrayBytes unfolds + Event.arrayBytes events > tip.MaxBytes then
                let calfEvents, residualEvents = ResizeArray(events.Length + cur.events.Length), ResizeArray()
                let mutable calfFull, calfSize = false, 1024
                for e in Seq.append cur.events events do
                    match calfFull, calfSize + Event.bytes e with
                    | false, calfSize' when calfSize' < 400 * 1024 -> calfSize <- calfSize'; calfEvents.Add e
                    | _ -> calfFull <- true; residualEvents.Add e
                let tipEvents = residualEvents.ToArray()
                calfEvents.ToArray(), tipEvents, tipEvents
            else Array.empty, events, Array.append cur.events events
        let tipEmpty, eventCount = Array.isEmpty cur.events, Array.length events
        match! Sync.batch log tip.WriteRetryPolicy (container, stream) (tipEmpty, exp pos, n', calve, append, unfolds, eventCount) with
        | Sync.Result.ConflictUnknown -> return InternalSyncResult.ConflictUnknown
        | Sync.Result.Written etag' -> return InternalSyncResult.Written (Token.create (Position.fromElements (stream, n', e', unfolds, etag'))) }

    member _.Prune(log, stream, index) =
        Prune.until log (container, stream) query.MaxItems index

type internal Category<'event, 'state, 'context>(store : StoreClient, codec : IEventCodec<'event, EventBody, 'context>) =
    member _.Load(log, stream, initial, checkUnfolds, fold, isOrigin) : Async<StreamToken * 'state> = async {
        let! token, events = store.Load(log, (stream, None), (codec.TryDecode, isOrigin), checkUnfolds)
        return token, fold initial events }
    member _.Reload(log, stream, (Token.Unpack pos as streamToken), state, fold, isOrigin, ?preloaded) : Async<StreamToken * 'state> = async {
        match! store.Reload(log, (stream, pos), (codec.TryDecode, isOrigin), ?preview = preloaded) with
        | LoadFromTokenResult.Unchanged -> return streamToken, state
        | LoadFromTokenResult.Found (token', events) -> return token', fold state events }
    member cat.Sync(log, stream, (Token.Unpack pos as streamToken), state, events, mapUnfolds, fold, isOrigin, context) : Async<SyncResult<'state>> = async {
        let state' = fold state (Seq.ofList events)
        let exp, events, eventsEncoded, unfoldsEncoded =
            let encode e = codec.Encode(context, e)
            let expVer = Position.toIndex >> Sync.Exp.Version
            match mapUnfolds with
            | Choice1Of3 () ->     expVer, events, Seq.map encode events |> Array.ofSeq, Seq.empty
            | Choice2Of3 unfold -> expVer, events, Seq.map encode events |> Array.ofSeq, Seq.map encode (unfold events state')
            | Choice3Of3 transmute ->
                let events', unfolds = transmute events state'
                Position.toEtag >> Sync.Exp.Etag, events', Seq.map encode events' |> Array.ofSeq, Seq.map encode unfolds
        let baseVer = Position.toIndex pos + int64 (List.length events)
        match! store.Sync(log, stream, pos, exp, baseVer, eventsEncoded, unfoldsEncoded) with
        | InternalSyncResult.ConflictUnknown -> return SyncResult.Conflict (cat.Reload(log, stream, streamToken, state, fold, isOrigin))
        | InternalSyncResult.Written token' -> return SyncResult.Written (token', state') }

module internal Caching =

    let applyCacheUpdatesWithSlidingExpiration (cache : ICache, prefix : string) (slidingExpiration : TimeSpan) =
        let mkCacheEntry (initialToken : StreamToken, initialState : 'state) = CacheEntry<'state>(initialToken, initialState, Token.supersedes)
        let options = CacheItemOptions.RelativeExpiration slidingExpiration
        fun streamName value ->
            cache.UpdateIfNewer(prefix + streamName, options, mkCacheEntry value)

    let applyCacheUpdatesWithFixedTimeSpan (cache : ICache, prefix : string) (period : TimeSpan) =
        let mkCacheEntry (initialToken : StreamToken, initialState : 'state) = CacheEntry<'state>(initialToken, initialState, Token.supersedes)
        fun streamName value ->
            let expirationPoint = let creationDate = DateTimeOffset.UtcNow in creationDate.Add period
            let options = CacheItemOptions.AbsoluteExpiration expirationPoint
            cache.UpdateIfNewer(prefix + streamName, options, mkCacheEntry value)

    type CachingCategory<'event, 'state, 'context>
        (   category : Category<'event, 'state, 'context>,
            fold : 'state -> 'event seq -> 'state, initial : 'state, isOrigin : 'event -> bool,
            tryReadCache, updateCache,
            checkUnfolds, mapUnfolds : Choice<unit, 'event list -> 'state -> 'event seq, 'event list -> 'state -> 'event list * 'event list>) =
        let cache streamName inner = async {
            let! tokenAndState = inner
            do! updateCache streamName tokenAndState
            return tokenAndState }
        interface ICategory<'event, 'state, string, 'context> with
            member _.Load(log, streamName, allowStale) : Async<StreamToken * 'state> = async {
                match! tryReadCache streamName with
                | None -> return! category.Load(log, streamName, initial, checkUnfolds, fold, isOrigin) |> cache streamName
                | Some tokenAndState when allowStale -> return tokenAndState // read already updated TTL, no need to write
                | Some (token, state) -> return! category.Reload(log, streamName, token, state, fold, isOrigin) |> cache streamName }
            member _.TrySync(log : ILogger, streamName, streamToken, state, events : 'event list, context) : Async<SyncResult<'state>> = async {
                match! category.Sync(log, streamName, streamToken, state, events, mapUnfolds, fold, isOrigin, context) with
                | SyncResult.Conflict resync ->
                    return SyncResult.Conflict (cache streamName resync)
                | SyncResult.Written (token', state') ->
                    do! updateCache streamName (token', state')
                    return SyncResult.Written (token', state') }

namespace Equinox.DynamoStore

open Equinox.Core
open Equinox.DynamoStore.Core
open System

/// Manages Creating an IAmazonDynamoDB
type DynamoStoreConnector(credentials : Amazon.Runtime.AWSCredentials, clientConfig : Amazon.DynamoDBv2.AmazonDynamoDBConfig) =

    new (serviceUrl, accessKey, secretKey) =
        let credentials = Amazon.Runtime.BasicAWSCredentials(accessKey, secretKey)
        let clientConfig = Amazon.DynamoDBv2.AmazonDynamoDBConfig(ServiceURL = serviceUrl)
        DynamoStoreConnector(credentials, clientConfig)

    member _.Endpoint = clientConfig.ServiceURL

    member _.CreateClient() = new Amazon.DynamoDBv2.AmazonDynamoDBClient(credentials, clientConfig) :> Amazon.DynamoDBv2.IAmazonDynamoDB

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
        | CreateIfNotExists throughput -> Initialization.createIfNotExists client tableName (throughput, Initialization.StreamingMode.Default)

/// Holds all relevant state for a Store
/// - The Client (IAmazonDynamoDB). there should be a single one of these per process, plus an optional fallback one for pruning scenarios.
/// - The (singleton) per-Table initialization state // TOCONSIDER remove if we don't end up with any
type DynamoStoreClient
    (   // Facilitates custom mapping of Stream Category Name to underlying Table and Stream names
        categoryAndStreamIdToTableAndStreamNames : string * string -> string * string,
        createContainer : string -> Container,
        createFallbackContainer : string -> Container option,
        [<O; D(null)>]?primaryTableToArchive : string -> string) =
    let primaryTableToSecondary = defaultArg primaryTableToArchive id
    new(client : Amazon.DynamoDBv2.IAmazonDynamoDB, tableName : string,
        // Table name to use for archive store. Default: (if <c>archiveClient</c> specified) use same <c>tableName</c> but via <c>archiveClient</c>
        [<O; D(null)>]?archiveTableName,
        [<O; D(null)>]?archiveClient : Amazon.DynamoDBv2.IAmazonDynamoDB) =
        let genStreamName (categoryName, streamId) = if categoryName = null then streamId else sprintf "%s-%s" categoryName streamId
        let catAndStreamToTableStream (categoryName, streamId) = tableName, genStreamName (categoryName, streamId)
        let primaryContainer t = Container.Create(client, t)
        let fallbackContainer =
            if Option.isNone archiveClient && Option.isNone archiveTableName then fun _ -> None
            else fun t -> Some (Container.Create(defaultArg archiveClient client, defaultArg archiveTableName t))
        DynamoStoreClient(catAndStreamToTableStream, primaryContainer, fallbackContainer)
    member internal _.ResolveContainerFallbackAndStreamName(categoryName, streamId) : Container * Container option * string =
        let tableName, streamName = categoryAndStreamIdToTableAndStreamNames (categoryName, streamId)
        let fallbackTableName = primaryTableToSecondary tableName
        let primaryContainer, fallbackContainer = createContainer tableName, createFallbackContainer fallbackTableName
        primaryContainer, fallbackContainer, streamName

    /// Connect to an Equinox.DynamoStore in the specified Table
    /// Events that have been archived and purged (and hence are missing from the primary) are retrieved from the fallback where that is provided
    static member Connect(client, tableName : string, ?archiveTableName, ?mode : ConnectMode) : Async<DynamoStoreClient> = async {
        let init t = ConnectMode.apply client t (defaultArg mode ConnectMode.Verify)
        do! init tableName
        match archiveTableName with None -> () | Some archiveTable-> do! init archiveTable
        return DynamoStoreClient(client, tableName, ?archiveTableName = archiveTableName) }

/// Defines a set of related access policies for a given Table, together with a Containers map defining mappings from (category, streamId) to (tableName, streamName)
type DynamoStoreContext(storeClient : DynamoStoreClient, tipOptions, queryOptions) =
    new(    storeClient : DynamoStoreClient,
            // Maximum serialized event size to permit to accumulate in Tip before they get moved out to a standalone Batch. Default: 32K.
            [<O; D null>]?maxBytes,
            // Maximum number of events permitted in Tip. When this is exceeded, events are moved out to a standalone Batch. Default: limited by maxBytes
            [<O; D null>]?tipMaxEvents,
            // Inhibit throwing when events are missing, but no Archive Table has been supplied as a fallback
            [<O; D null>]?ignoreMissingEvents,
            // Max number of Batches to return per paged query response. Default: 10.
            [<O; D null>]?queryMaxItems,
            // Maximum number of trips to permit when slicing the work into multiple responses limited by `queryMaxItems`. Default: unlimited.
            [<O; D null>]?queryMaxRequests) =
        let tipOptions = TipOptions(?maxBytes = maxBytes, ?maxEvents = tipMaxEvents, ?ignoreMissingEvents = ignoreMissingEvents)
        let queryOptions = QueryOptions(?maxItems = queryMaxItems, ?maxRequests = queryMaxRequests)
        DynamoStoreContext(storeClient, tipOptions, queryOptions)
    member val StoreClient = storeClient
    member val QueryOptions = queryOptions
    member val TipOptions = tipOptions
    member internal x.ResolveContainerClientAndStreamId(categoryName, streamId) =
        let container, fallback, streamId = storeClient.ResolveContainerFallbackAndStreamName(categoryName, streamId)
        StoreClient(container, fallback, x.QueryOptions, x.TipOptions), streamId

/// For DynamoDB, caching is critical in order to reduce RU consumption.
/// As such, it can often be omitted, particularly if streams are short or there are snapshots being maintained
[<NoComparison; NoEquality; RequireQualifiedAccess>]
type CachingStrategy =
    /// Do not apply any caching strategy for this Stream.
    /// NB opting not to leverage caching when using DynamoDB can have significant implications for the scalability
    ///   of your application, both in terms of latency and running costs.
    /// While the cost of a cache miss can be ameliorated to varying degrees by employing an appropriate `AccessStrategy`
    ///   [that works well and has been validated for your scenario with real data], even a cache with a low Hit Rate provides
    ///   a direct benefit in terms of the number of Read and/or Write Request Charge Units (RCU)s that need to be provisioned for your Tables.
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
    /// <remarks>In this mode, Optimistic Concurrency Control is necessarily based on the etag</remarks>
    | RollingState of toSnapshot : ('state -> 'event)
    /// Allow produced events to be filtered, transformed or removed completely and/or to be transmuted to unfolds.
    /// <remarks>
    /// In this mode, Optimistic Concurrency Control is based on the etag (rather than the normal Expected Version strategy)
    /// in order that conflicting updates to the state not involving the writing of an event can trigger retries.
    /// </remarks>
    | Custom of isOrigin : ('event -> bool) * transmute : ('event list -> 'state -> 'event list*'event list)

type DynamoStoreCategory<'event, 'state, 'context>(context : DynamoStoreContext, codec, fold, initial, caching, access) =
    let categories = System.Collections.Concurrent.ConcurrentDictionary<string, ICategory<'event, 'state, string, 'context>>()
    let resolveCategory (categoryName, container) =
        let createCategory _name : ICategory<_, _, string, 'context> =
            let tryReadCache, updateCache =
                match caching with
                | CachingStrategy.NoCaching -> (fun _ -> async { return None }), fun _ _ -> async { () }
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
            Caching.CachingCategory<'event, 'state, 'context>(cosmosCat, fold, initial, isOrigin, tryReadCache, updateCache, checkUnfolds, mapUnfolds) :> _
        categories.GetOrAdd(categoryName, createCategory)
    let resolve (FsCodec.StreamName.CategoryAndId (categoryName, streamId)) =
        let container, streamName = context.ResolveContainerClientAndStreamId(categoryName, streamId)
        resolveCategory (categoryName, container), streamName, None
    let empty = Token.empty, initial
    let storeCategory = StoreCategory(resolve, empty)
    member _.Resolve(streamName, ?context) = storeCategory.Resolve(streamName, ?context = context)

namespace Equinox.DynamoStore.Core

open Equinox.Core
open FsCodec
open FSharp.Control

/// Outcome of appending events, specifying the new and/or conflicting events, together with the updated Target write position
[<RequireQualifiedAccess; NoComparison>]
type AppendResult<'t> =
    | Ok of pos : 't
    | ConflictUnknown

/// Encapsulates the core facilities Equinox.DynamoStore offers for operating directly on Events in Streams.
type EventsContext internal
    (   context : Equinox.DynamoStore.DynamoStoreContext, store : StoreClient,
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
        return Position.flatten pos', data }

    new (context : Equinox.DynamoStore.DynamoStoreContext, log) =
        let storeClient, _streamId = context.ResolveContainerClientAndStreamId(null, null)
        EventsContext(context, storeClient, log)

    member x.StreamId(streamName) : string = context.ResolveContainerClientAndStreamId(null, streamName) |> snd

    member internal _.GetLazy(stream, ?queryMaxItems, ?direction, ?minIndex, ?maxIndex) : AsyncSeq<ITimelineEvent<EventBody>[]> =
        let direction = defaultArg direction Direction.Forward
        let batching = match queryMaxItems with Some qmi -> QueryOptions(qmi) | _ -> context.QueryOptions
        store.ReadLazy(log, batching, stream, direction, (Some, fun _ -> false), ?minIndex = minIndex, ?maxIndex = maxIndex)

    member internal _.GetInternal(stream, ?minIndex, ?maxIndex, ?maxCount, ?direction) = async {
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
            let! token, events = store.Read(log, stream, direction, (Some, isOrigin), ?minIndex = minIndex, ?maxIndex = maxIndex)
            if direction = Direction.Backward then System.Array.Reverse events
            return token, events }

    /// Establishes the current position of the stream in as efficient a manner as possible
    /// (The ideal situation is that the preceding token is supplied as input in order to avail of 1RU low latency state checks)
    member _.Sync(stream, [<O; D null>] ?position : Position) : Async<Position> = async {
        let! Token.Unpack pos' = store.GetPosition(log, stream, ?pos = position)
        return Position.flatten pos' }

    /// Query (with MaxItems set to `queryMaxItems`) from the specified `Position`, allowing the reader to efficiently walk away from a running query
    /// ... NB as long as they Dispose!
    member x.Walk(stream, queryMaxItems, [<O; D null>] ?minIndex, [<O; D null>] ?maxIndex, [<O; D null>] ?direction) : AsyncSeq<ITimelineEvent<EventBody>[]> =
        x.GetLazy(stream, queryMaxItems, ?direction = direction, ?minIndex = minIndex, ?maxIndex = maxIndex)

    /// Reads all Events from a `Position` in a given `direction`
    member x.Read(stream, [<O; D null>] ?minIndex, [<O; D null>] ?maxIndex, [<O; D null>] ?maxCount, [<O; D null>] ?direction) : Async<Position * ITimelineEvent<EventBody>[]> =
        x.GetInternal(stream, ?minIndex = minIndex, ?maxIndex = maxIndex, ?maxCount = maxCount, ?direction = direction) |> yieldPositionAndData
#if APPEND_SUPPORT

    /// Appends the supplied batch of events, subject to a consistency check based on the `position`
    /// Callers should implement appropriate idempotent handling, or use Equinox.Decider for that purpose
    member x.Sync(stream, position, events : IEventData<_>[]) : Async<AppendResult<Position>> = async {
        match! store.Sync(log, stream, Some position, Position.toIndex >> Sync.Exp.Version, position.index, events, Seq.empty) with
        | InternalSyncResult.Written (Token.Unpack pos) -> return AppendResult.Ok (Position.flatten pos)
        | InternalSyncResult.ConflictUnknown -> return AppendResult.ConflictUnknown }
#endif

    member _.Prune(stream, index) : Async<int * int * int64> =
        store.Prune(log, stream, index)

/// Provides mechanisms for building `EventData` records to be supplied to the `Events` API
type EventData() =
    /// Creates an Event record, suitable for supplying to Append et al
    static member FromUtf8Bytes(eventType, data, ?meta) : IEventData<_> = FsCodec.Core.EventData.Create(eventType, data, ?meta = meta) :> _

/// Api as defined in the Equinox Specification
/// Note the DynamoContext APIs can yield better performance due to the fact that a Position tracks the etag of the Stream's Tip
module Events =

    let private (|PositionIndex|) (x : Position) = x.index
    let private stripSyncResult (f : Async<AppendResult<Position>>) : Async<AppendResult<int64>> = async {
        match! f with
        | AppendResult.Ok (PositionIndex index)-> return AppendResult.Ok index
        | AppendResult.ConflictUnknown -> return AppendResult.ConflictUnknown }
    let private stripPosition (f : Async<Position>) : Async<int64> = async {
        let! (PositionIndex index) = f
        return index }
    let private dropPosition (f : Async<Position * ITimelineEvent<EventBody>[]>) : Async<ITimelineEvent<EventBody>[]> = async {
        let! _, xs = f
        return xs }

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
    let get (ctx : EventsContext) (streamName : string) (index : int64) (maxCount : int) : Async<ITimelineEvent<EventBody>[]> =
        ctx.Read(ctx.StreamId streamName, ?minIndex = (if index = 0 then None else Some index), maxCount = maxCount) |> dropPosition

#if APPEND_SUPPORT
    /// Appends a batch of events to a stream at the specified expected sequence number.
    /// If the specified expected sequence number does not match the stream, the events are not appended
    /// and a failure is returned.
    let append (ctx : EventsContext) (streamName : string) (index : int64) (events : IEventData<_>[]) : Async<AppendResult<int64>> =
        ctx.Sync(ctx.StreamId streamName, Sync.Exp.Version index, events) |> stripSyncResult

#endif
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
    let getBackwards (ctx : EventsContext) (streamName : string) (index : int64) (maxCount : int) : Async<ITimelineEvent<EventBody>[]> =
        ctx.Read(ctx.StreamId streamName, ?maxIndex = (match index with int64.MaxValue -> None | i -> Some (i + 1L)), maxCount = maxCount, direction = Direction.Backward) |> dropPosition

  /// Obtains the `index` from the current write Position
    let getNextIndex (ctx : EventsContext) (streamName : string) : Async<int64> =
        ctx.Sync(ctx.StreamId streamName) |> stripPosition
