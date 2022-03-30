namespace Equinox.DynamoStore.Core

open Amazon.DynamoDBv2
open Equinox.Core
open FsCodec
open FSharp.AWS.DynamoDB
open FSharp.Control
open Serilog
open System

type EventBody = byte[]

/// A single Domain Event from the array held in a Batch
[<NoEquality; NoComparison>]
type Event =
    {   /// Creation datetime (as opposed to system-defined _lastUpdated which is touched by triggers, replication etc.)
        t : DateTimeOffset

        /// The Case (Event Type); used to drive deserialization
        c : string

        /// Event body, as UTF-8 encoded json
        d : EventBody // required

        /// Optional metadata, encoded as per 'd'
        m : EventBody // can be null

        /// correlationId, or null
        correlationId : string option

        /// causationId, or null
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

/// A 'normal' (frozen, not Tip) Batch of Events (without any Unfolds)
[<NoEquality; NoComparison>]
type Batch =
    {   [<HashKey>]
        p : string // "{streamName}"

        /// base 'i' value for the Events held herein
        [<RangeKey>]
        i : int64 // WellKnownI for the Tip

        /// `i` value for successor batch (to facilitate identifying which Batch a given startPos is within)
        n : int64

        /// The Domain Events (as opposed to Unfolded Events, see Tip) at this offset in the stream
        e: Event[]

        /// Marker on which compare-and-swap operations on Tip are predicated
        etag : string }
    /// Computes base Index for the Item (`i` can bear the the magic value WellKnownI when the Item is the Tip)
    member x.Base = x.n - x.e.LongLength

/// Compaction/Snapshot/Projection Event based on the state at a given point in time `i`
[<NoEquality; NoComparison>]
type Unfold =
    {   /// Base: Stream Position (Version) of State from which this Unfold was generated
        i : int64

        /// Generation datetime
        t : DateTimeOffset

        /// The Case (Event Type) of this snapshot, used to drive deserialization
        c : string // required

        /// Event body - Json -> byte[] -> (maybe) Deflate
        d : EventBody // required

        /// Optional metadata, same encoding as `d` (can be null; not written if missing)
        m : EventBody }

/// The special-case 'Pending' Batch Format used to read the currently active (and mutable) document
/// Stored representation has the following diffs vs a 'normal' (frozen/completed) Batch:
/// a) `n` = WellKnownI
/// b) contains unfolds (`u`)
[<NoEquality; NoComparison>]
type Tip =
    {   /// Partition key, as per Batch
        [<HashKey>]
        p : string // "{streamName}"

        /// base 'i' value for the Events held herein
        [<RangeKey>]
        i : int64 // WellKnownI for the Tip (in which case i is computed as: n - e.Length)

        /// `i` value for successor batch (to facilitate identifying which Batch a given startPos is within)
        n : int64

        /// Domain Events, will eventually move out to a Batch
        e : Event[]

        /// Compaction/Snapshot/Projection events
        u : Unfold[]

        /// Marker on which compare-and-swap operations on Tip are predicated
        etag : string }
    /// Computes base Index for the Item (`i` can bear the the magic value WellKnownI when the Item is the Tip)
    member x.Base = x.n - x.e.LongLength
    static member internal WellKnownI = int64 Int32.MaxValue

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
    let fromTip (x : Tip) = { index = x.n; etag = match x.etag with null -> None | x -> Some x }
    /// If we encounter the tip (id=-1) item/document, we're interested in its etag so we can re-sync for 1 RU
    let tryFromBatch (x : Batch) =
        if x.i <> Tip.WellKnownI then None
        else Some { index = x.n; etag = match x.etag with null -> None | x -> Some x }

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
    static member internal Events(t : Tip, ?minIndex, ?maxIndex) : ITimelineEvent<EventBody> seq =
        Enum.Events(t.Base, t.e, ?minIndex = minIndex, ?maxIndex = maxIndex)
    static member internal Events(b : Batch, ?minIndex, ?maxIndex) =
        Enum.Events(b.Base, b.e, ?minIndex = minIndex, ?maxIndex = maxIndex)
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
    let [<Literal>] PropertyTag = "dynamoEvt"

    [<NoEquality; NoComparison>]
    type Measurement =
       {   table : string; stream : string
           interval : StopwatchInterval; bytes : int; count : int; rru : float; wru : float }
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
    let internal prop name value (log : ILogger) = log.ForContext(name, value)
    let internal propData name (events : #IEventData<EventBody> seq) (log : ILogger) =
        let render (body : EventBody) : string = body (*.ToArray()*) |> System.Text.Encoding.UTF8.GetString
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
    /// Include a LogEvent property bearing metrics
    // Sidestep Log.ForContext converting to a string; see https://github.com/serilog/serilog/issues/1124
    let internal event (value : Metric) (log : ILogger) =
        let enrich (e : Serilog.Events.LogEvent) =
            e.AddPropertyIfAbsent(Serilog.Events.LogEventProperty(PropertyTag, Serilog.Events.ScalarValue(value)))
        log.ForContext({ new Serilog.Core.ILogEventEnricher with member _.Enrich(evt, _) = enrich evt })
    let internal (|BlobLen|) (x : EventBody) = match x with null -> 0 | x -> int x.Length
    let internal eventLen (x: #IEventData<_>) = let BlobLen bytes, BlobLen metaBytes = x.Data, x.Meta in bytes + metaBytes + 80 // TODO revise for Dynamo
    let internal batchLen = Seq.sumBy eventLen
    let internal (|SerilogScalar|_|) : Serilog.Events.LogEventPropertyValue -> obj option = function
        | :? Serilog.Events.ScalarValue as x -> Some x.Value
        | _ -> None
    let (|MetricEvent|_|) (logEvent : Serilog.Events.LogEvent) : Metric option =
        match logEvent.Properties.TryGetValue(PropertyTag) with
        | true, SerilogScalar (:? Metric as e) -> Some e
        | _ -> None
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
                 { mutable rrux100 : int64; mutable wrux100 : int64; mutable count : int64; mutable ms : int64 }
                 static member Create() = { rrux100 = 0L; wrux100 = 0L; count = 0L; ms = 0L }
                 member x.Ingest(rru, wru, ms) =
                     System.Threading.Interlocked.Increment(&x.count) |> ignore
                     System.Threading.Interlocked.Add(&x.rrux100, int64 (rru * 100.)) |> ignore
                     System.Threading.Interlocked.Add(&x.wrux100, int64 (wru * 100.)) |> ignore
                     System.Threading.Interlocked.Add(&x.ms, ms) |> ignore
            let inline private (|RcsMs|) ({ interval = i; rru = rru; wru = wru } : Measurement) =
                rru, wru, let e = i.Elapsed in int64 e.TotalMilliseconds
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
                            | Op ((Operation.Tip | Operation.Tip404 | Operation.Tip304 | Operation.Query), RcsMs m)  ->
                                                                            LogSink.Read.Ingest m
                            | QueryRes (_direction,          _)        ->   ()
                            | Op (Operation.Write,            RcsMs m)  ->  LogSink.Write.Ingest m
                            | Op (Operation.Conflict,         RcsMs m)  ->  LogSink.Conflict.Ingest m
                            | Op (Operation.Resync,           RcsMs m)  ->  LogSink.Resync.Ingest m
                            | Op (Operation.Prune,            RcsMs m)  ->  LogSink.Prune.Ingest m
                            | PruneRes                        _        ->   ()
                            | Op (Operation.Delete,           RcsMs m)  ->  LogSink.Delete.Ingest m
                            | Op (Operation.Trim,             RcsMs m)  ->  LogSink.Trim.Ingest m
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
            let mutable rows, totalCount, totalRRc, totalWRc, totalMs = 0, 0L, 0., 0., 0L
            let logActivity name count rrc wrc lat =
                let arrc, awrc, ams = (if count = 0L then Double.NaN else rrc/float count), (if count = 0L then Double.NaN else wrc/float count), (if count = 0L then Double.NaN else float lat/float count)
                log.Information("{name}: {count:n0} requests costing {rru:n0}/{wru:n0} R/W RU (average: {avgR:n2} {avgW:n2}); Average latency: {lat:n0}ms",
                    name, count, rrc, wrc, arrc, awrc, ams)
            for name, stat in stats do
                if stat.count <> 0L then
                    let rru = float stat.rrux100 / 100.
                    let wru = float stat.wrux100 / 100.
                    totalCount <- totalCount + stat.count
                    totalRRc <- totalRRc + rru
                    totalWRc <- totalWRc + wru
                    totalMs <- totalMs + stat.ms
                    logActivity name stat.count rru wru stat.ms
                    rows <- rows + 1
            // Yes, there's a minor race here between the use of the values and the reset
            let duration = Stats.LogSink.Restart()
            if rows > 1 then logActivity "TOTAL" totalCount totalRRc totalWRc totalMs
            let measures : (string * (TimeSpan -> float)) list = [ "s", fun x -> x.TotalSeconds(*; "m", fun x -> x.TotalMinutes; "h", fun x -> x.TotalHours*) ]
            let logPeriodicRate name count rru wru = log.Information("rp{name} {count:n0} = ~{rru:n0} RRU ~{wru:n0} WRU", name, count, rru, wru)
            for uom, f in measures do let d = f duration in if d <> 0. then logPeriodicRate uom (float totalCount/d |> int64) (totalRRc/d) (totalWRc/d)

type Container(tip : TableContext<Tip>) =
    let batches = lazy tip.WithRecordType<Batch>()

    /// As per Equinox.CosmosStore, we assume the table to be provisioned correctly
    static member Create(client, tableName) =
        // TODO Use CreateUnverified, document/automate how to init the test tables
        let context = TableContext.CreateAsync<_>(client, tableName, verifyTable = true, createIfNotExists = true) |> Async.RunSynchronously
        Container(context)

    member _.TableKeyForStreamTip stream =
        TableKey.Combined(stream, Tip.WellKnownI)
    member _.TableName = tip.TableName
    member x.TryGetTip(stream) = async {
        try let! item = tip.GetItemAsync(x.TableKeyForStreamTip stream) // TODO replace with TryGetItemAsync when released
            return Some item // TODO expose ru metrics
        with :? ResourceNotFoundException -> return None }
    member _.EnumBatches(stream, minN, maxI, backwards, batchSize) : AsyncSeq<int * StopwatchInterval * Batch[]> = // TODO add emission of timing and request charges
        let table = batches.Value
        let compile ex = table.Template.PrecomputeConditionalExpr ex
        let kc = match maxI with
                 | Some maxI -> compile <@ fun (b : Batch) -> b.p = stream && b.i < maxI @>
                 | None -> compile <@ fun (b : Batch) -> b.p = stream @>
        let fc = match minN with
                 | Some minN -> compile <@ fun (b : Batch) -> b.n > minN @> |> Some
                 | None -> None
        let rec aux (i, lastEvaluated) = asyncSeq {
            // TOCONSIDER could avoid projecting `p`
            let! t, res = table.QueryPaginatedAsync(kc, ?filterCondition = fc, limit = batchSize, ?exclusiveStartKey = lastEvaluated, scanIndexForward = not backwards) |> Stopwatch.Time
            yield i, t, res.Records
            match res.LastEvaluatedKey with
            | None -> ()
            | le -> return aux (i + 1, le)
        }
        aux (0, None)
    member _.Tip = tip

type ConsumedMetrics = { read : float; write : float }
type ReadResult<'T> = Found of 'T | NotFound | NotModified
// TODO remove Conflicts/e unless there turns out to be a way to get them
[<NoEquality; NoComparison>]
type SyncResponse = { etag : string; n : int64; conflicts : Unfold[]; e : Event[] }

[<RequireQualifiedAccess>]
type internal SyncExp = Version of int64 | Etag of string //| Any

module internal Sync =

    // NB don't nest in a private module, or serialization will fail miserably ;)
    [<RequireQualifiedAccess; NoEquality; NoComparison>]
    type Result =
        | Written of Position
        | Conflict of Position * events : ITimelineEvent<EventBody>[]
        | ConflictUnknown

    let private run (container : Container, stream : string) (maxEventsInTip, maxStringifyLen) (exp, req : Tip)
        : Async<ConsumedMetrics * Result> = async {
        let eventCount = int64 req.e.Length
        let updEtag = let g = Guid.NewGuid() in g.ToString "N"
        let execute = async {
            let update (condExpr : Quotations.Expr<Tip -> bool>) =
                let updateExpr : Quotations.Expr<Tip -> Tip> =
                    if eventCount = 0 then
                        <@ fun t -> { t with etag = updEtag
                                             u = req.u } @>
                    else
                        <@ fun t -> { t with e = Array.append t.e req.e
                                             n = t.n + eventCount
                                             etag = updEtag
                                             u = req.u } @>
                container.Tip.UpdateItemAsync(container.TableKeyForStreamTip stream, updateExpr, condExpr)
            match exp with
            | SyncExp.Version 0L
            | SyncExp.Etag null ->
                let condExpr : Quotations.Expr<Tip -> bool> = <@ fun t -> NOT_EXISTS t.etag @>
                let item : Tip = { p = stream; i = Tip.WellKnownI; n = eventCount; e = req.e; u = req.u; etag = updEtag }
                let! _tk = container.Tip.PutItemAsync(item, condExpr)
                return item
            | SyncExp.Etag eetag ->
                let condExpr : Quotations.Expr<Tip -> bool> = <@ fun t -> t.etag = eetag @>
                return! update condExpr
            | SyncExp.Version ever ->
                let condExpr : Quotations.Expr<Tip -> bool> = <@ fun t -> t.n = ever @>
                return! update condExpr
            (*| SyncExp.Any -> return failwith "E_NOTIMPL Position.fromAppendAtEnd" *)}
        try let! updated = execute
            let newPos = { index = updated.n; etag = Some updated.etag }
            return { read = 0.; write = 0. }, Result.Written newPos
        with :? ConditionalCheckFailedException ->
            return { read = 0. ; write = 0. }, Result.ConflictUnknown }

    let private logged (container, stream) (maxEventsInTip, maxStringifyLen) (exp : SyncExp, req : Tip) (log : ILogger)
        : Async<Result> = async {
        let! t, (ru, result) = run (container, stream) (maxEventsInTip, maxStringifyLen) (exp, req) |> Stopwatch.Time
        let verbose = log.IsEnabled Serilog.Events.LogEventLevel.Debug
        let count, bytes = req.e.Length, if verbose then Enum.Events req |> Log.batchLen else 0
        let log =
            let inline mkMetric ru : Log.Measurement =
                { table = container.TableName; stream = stream; interval = t; bytes = bytes; count = count; rru = ru.read; wru = ru.write }
            let inline propConflict log = log |> Log.prop "conflict" true |> Log.prop "eventTypes" (Seq.truncate 5 (seq { for x in req.e -> x.c }))
            if verbose then log |> Log.propEvents (Enum.Events req) |> Log.propDataUnfolds req.u else log
            |> match exp with
                | SyncExp.Etag et ->     Log.prop "expectedEtag" et
                | SyncExp.Version ev ->  Log.prop "expectedVersion" ev
//TODO                | SyncExp.Any ->         Log.prop "expectedVersion" -1
            |> match result with
                | Result.Written pos ->
                    Log.prop "nextExpectedVersion" pos >> Log.event (Log.Metric.SyncSuccess (mkMetric ru))
                | Result.ConflictUnknown ->
                    propConflict >> Log.event (Log.Metric.SyncConflict (mkMetric ru))
                | Result.Conflict (pos', xs) ->
                    if verbose then Log.propData "conflicts" xs else id
                    >> Log.prop "nextExpectedVersion" pos' >> propConflict >> Log.event (Log.Metric.SyncResync (mkMetric ru))
        log.Information("EqxDynamo {action:l} {stream} {count}+{ucount} {ms:f1}ms {rru}/{wru}RU {bytes:n0}b {exp}",
            "Sync", stream, count, req.u.Length, (let e = t.Elapsed in e.TotalMilliseconds), ru.read, ru.write, bytes, exp)
        return result }

    let batch (log : ILogger) (retryPolicy, maxEventsInTip, maxStringifyLen) containerStream expBatch : Async<Result> =
        let call = logged containerStream (maxEventsInTip, maxStringifyLen) expBatch
        Log.withLoggedRetries retryPolicy "writeAttempt" call log

    let private mkEvent (e : IEventData<EventBody>) =
        {   t = e.Timestamp; c = e.EventType; d = e.Data; m = e.Meta; correlationId = Option.ofObj e.CorrelationId; causationId = Option.ofObj e.CausationId }
    let mkBatch (stream : string) (events : IEventData<EventBody>[]) unfolds : Tip =
        {   p = stream; n = -1L(*TODO NOT Server-managed*); i = -1L(*TODO NOT Server-managed*); etag = null
            e = Array.map mkEvent events; u = Array.ofSeq unfolds }
    let mkUnfold compressor baseIndex (unfolds : IEventData<_> seq) : Unfold seq =
        unfolds
        |> Seq.mapi (fun offset x ->
                {   i = baseIndex + int64 offset
                    c = x.EventType
                    d = compressor x.Data
                    m = compressor x.Meta
                    t = DateTimeOffset.UtcNow
                } : Unfold)

// TODO remove when FSharp.AWS.DynamoDB merges support
[<NoComparison; NoEquality>]
type Throughput =
    | OnDemand
    | Provisioned of ProvisionedThroughput

module Initialization =

    let private prepare (client : IAmazonDynamoDB) tableName maybeThroughput =
        TableContext.CreateAsync<Batch>(client, tableName, createIfNotExists = true, ?provisionedThroughput = maybeThroughput)

    /// Verify the specified <c>tableName</c> is present and adheres to the correct schema.
    let verify (client : IAmazonDynamoDB) tableName = prepare client tableName None |> Async.Ignore

    /// Create the specified <c>tableName</c> if it does not exist. Will throw if it exists but the schema mismatches.
    let createIfNotExists (client : IAmazonDynamoDB) tableName = function
        | Throughput.OnDemand -> failwith "TODO"
        | Throughput.Provisioned throughput -> prepare client tableName (Some throughput) |> Async.Ignore

    /// Provision (or re-provision) the specified table with the specified <c>Throughput</c>. Will throw if schema mismatches.
    let provision (client : IAmazonDynamoDB) tableName throughput = async {
        match throughput with
        | Throughput.OnDemand -> failwith "TODO"
        | Throughput.Provisioned throughput ->
            let! tc = prepare client tableName (Some throughput)
            // TODO remove try/catch and replace with (upcoming) TableContext.ProvisionAsync
            try do! tc.UpdateProvisionedThroughputAsync throughput with _ -> () }

    /// Holds Container state, coordinating initialization activities
    type internal ContainerInitializerGuard(container : Container, fallback : Container option, ?initContainer : Container -> Async<unit>) =
        let initGuard = initContainer |> Option.map (fun init -> AsyncCacheCell<unit>(init container))

        member _.Container = container
        member _.Fallback = fallback
        member internal _.InitializationGate = match initGuard with Some g when not (g.IsValid())  -> Some g.AwaitValue | _ -> None

module internal Tip =

    let private get (container : Container, stream : string) (maybePos : Position option) = async {
        let expectedEtag = maybePos |> Option.bind (fun x -> x.etag)
        let rc = { read = 0.; write = 0. }
        match! container.TryGetTip stream with
        | Some { etag = fe } when Option.ofObj fe = expectedEtag -> return rc, ReadResult.NotModified
        | Some t -> return rc, ReadResult.Found t
        | None -> return rc, ReadResult.NotFound }
    let private loggedGet (get : Container * string -> Position option -> Async<_>) (container, stream) (maybePos : Position option) (log : ILogger) = async {
        let log = log |> Log.prop "stream" stream
        let! t, (ru : ConsumedMetrics, res : ReadResult<Tip>) = get (container, stream) maybePos |> Stopwatch.Time
        let verbose = log.IsEnabled Events.LogEventLevel.Debug
        let log bytes count (f : Log.Measurement -> _) = log |> Log.event (f { table = container.TableName; stream = stream; interval = t; bytes = bytes; count = count; rru = ru.read; wru = ru.write })
        match res with
        | ReadResult.NotModified ->
            (log 0 0 Log.Metric.TipNotModified).Information("EqxDynamo {action:l} {stream} {res} {ms}ms {rru}/{wru}RU", "Tip", stream, 304, (let e = t.Elapsed in e.TotalMilliseconds), ru.read, ru.write)
        | ReadResult.NotFound ->
            (log 0 0 Log.Metric.TipNotFound).Information("EqxDynamo {action:l} {stream} {res} {ms}ms {rru}/{wru}RU", "Tip", stream, 404, (let e = t.Elapsed in e.TotalMilliseconds), ru.read, ru.write)
        | ReadResult.Found tip ->
            let log =
                let count, bytes = tip.u.Length, if verbose then Enum.Unfolds tip.u |> Log.batchLen else 0
                log bytes count Log.Metric.Tip
            let log = if verbose then log |> Log.propDataUnfolds tip.u else log
            let log = match maybePos with Some p -> log |> Log.propStartPos p |> Log.propStartEtag p | None -> log
            let log = log |> Log.prop "etag" tip.etag |> Log.prop "n" tip.n
            log.Information("EqxDynamo {action:l} {stream} {res} {ms}ms {rru}/{wru}RU", "Tip", stream, 200, (let e = t.Elapsed in e.TotalMilliseconds), ru.read, ru.write)
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
            return Result.Found (Position.fromTip tip, tip.Base, Enum.EventsAndUnfolds(tip, ?maxIndex = maxIndex, ?minIndex = minIndex) |> Array.ofSeq) }

module internal Query =

    let private mkQuery (log : ILogger) (container : Container, stream : string) maxItems (direction : Direction, minIndex, maxIndex) =
        let minN, maxI = minIndex, maxIndex
        log.Debug("EqxDynamo Query {stream}; minIndex={minIndex} maxIndex={maxIndex}", stream, minIndex, maxIndex)
        container.EnumBatches(stream, minN, maxI, (direction = Direction.Backward), maxItems)

    // Unrolls the Batches in a response
    // NOTE when reading backwards, the events are emitted in reverse Index order to suit the takeWhile consumption
    let private mapPage direction (container : Container, streamName : string) (minIndex, maxIndex) (maxRequests : int option)
            (log : ILogger) (i, t, res : Batch[])
        : ITimelineEvent<EventBody>[] * Position option * ConsumedMetrics =
        let log = log |> Log.prop "batchIndex" i
        match maxRequests with
        | Some mr when i >= mr -> log.Information "batch Limit exceeded"; invalidOp "batch Limit exceeded"
        | _ -> ()
        let batches, ru = Array.ofSeq res, { read = 0.; write = 0. }
        let unwrapBatch (b : Batch) =
            Enum.Events(b, ?minIndex = minIndex, ?maxIndex = maxIndex)
            |> if direction = Direction.Backward then System.Linq.Enumerable.Reverse else id
        let events = batches |> Seq.collect unwrapBatch |> Array.ofSeq
        let verbose = log.IsEnabled Events.LogEventLevel.Debug
        let count, bytes = events.Length, if verbose then events |> Log.batchLen else 0
        let reqMetric : Log.Measurement = { table = container.TableName; stream = streamName; interval = t; bytes = bytes; count = count; rru = ru.read; wru = ru.write }
        let log = let evt = Log.Metric.QueryResponse (direction, reqMetric) in log |> Log.event evt
        let log = if verbose then log |> Log.propEvents events else log
        let index = if count = 0 then Nullable () else Nullable <| Seq.min (seq { for x in batches -> x.Base })
        (log|> Log.prop "bytes" bytes
            |> match minIndex with None -> id | Some i -> Log.prop "minIndex" i
            |> match maxIndex with None -> id | Some i -> Log.prop "maxIndex" i)
            .Information("EqxDynamo {action:l} {count}/{batches} {direction} {ms}ms i={index} {rru}/{wru}RU",
                "Response", count, batches.Length, direction, (let e = t.Elapsed in e.TotalMilliseconds), index, ru.read, ru.write)
        let maybePosition = batches |> Array.tryPick Position.tryFromBatch
        events, maybePosition, ru

    let private logQuery direction queryMaxItems (container : Container, streamName) interval (responsesCount, events : ITimelineEvent<EventBody>[]) n (rc : ConsumedMetrics) (log : ILogger) =
        let verbose = log.IsEnabled Events.LogEventLevel.Debug
        let count, bytes = events.Length, if verbose then events |> Log.batchLen else 0
        let reqMetric : Log.Measurement = { table = container.TableName; stream = streamName; interval = interval; bytes = bytes; count = count; rru = rc.read; wru = rc.write }
        let evt = Log.Metric.Query (direction, responsesCount, reqMetric)
        let action = match direction with Direction.Forward -> "QueryF" | Direction.Backward -> "QueryB"
        (log |> Log.prop "bytes" bytes |> Log.prop "queryMaxItems" queryMaxItems |> Log.event evt).Information(
            "EqxDynamo {action:l} {stream} v{n} {count}/{responses} {ms}ms {rru}/{wru}RU",
            action, streamName, n, count, responsesCount, (let e = interval.Elapsed in e.TotalMilliseconds), rc.read, rc.write)

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
        let mergeBatches (log : ILogger) (batchesBackward : AsyncSeq<ITimelineEvent<EventBody>[] * Position option * ConsumedMetrics>) = async {
            let mutable lastResponse, maybeTipPos, rru, wru = None, None, 0., 0.
            let! events =
                batchesBackward
                |> AsyncSeq.map (fun (events, maybePos, rc) ->
                    if maybeTipPos = None then maybeTipPos <- maybePos
                    lastResponse <- Some events; rru <- rru + rc.read; wru <- wru + rc.write
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
            return events, maybeTipPos, { read = rru; write = wru } }
        let log = log |> Log.prop "batchSize" maxItems |> Log.prop "stream" stream
        let readLog = log |> Log.prop "direction" direction
        let batches : AsyncSeq<ITimelineEvent<EventBody>[] * Position option * ConsumedMetrics> =
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
        log |> logQuery direction maxItems (container, stream) t (responseCount, raws) version ru
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
        let mutable i, rru, wru = 0, 0., 0.
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

                rru <- rru + rc.read; wru <- wru + rc.write
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
            log |> logQuery direction maxItems (container, stream) t (i, allEvents.ToArray()) -1L { read = rru; write = wru } }

    /// Manages coalescing of spans of events obtained from various sources:
    /// 1) Tip Data and/or Conflicting events
    /// 2) Querying Primary for predecessors of what's obtained from 1
    /// 2) Querying Secondary for predecessors of what's obtained from 1
    let load (log : ILogger) (minIndex, maxIndex) (tip : ScanResult<'event> option)
            (primary : int64 option * int64 option -> Async<ScanResult<'event> option>)
            // Choice1Of2 -> indicates whether it's acceptable to ignore missing events; Choice2Of2 -> Fallback store
            (secondary : Choice<bool, int64 option * int64 option -> Async<ScanResult<'event> option>>)
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

        match primary, secondary with
        | Some { found = true }, _ -> return pos, events // origin found in primary, no need to look in secondary
        | Some { minIndex = i }, _ when i <= minI -> return pos, events // primary had required earliest event Index, no need to look at secondary
        | None, _ when Option.isNone tip -> return pos, events // initial load where no documents present in stream
        | _, Choice1Of2 allowMissing ->
            logMissing (minIndex, i) "Origin event not found; no secondary container supplied"
            if allowMissing then return pos, events
            else return failwithf "Origin event not found; no secondary container supplied"
        | _, Choice2Of2 secondary ->

        let maxIndex = match primary with Some p -> Some p.minIndex | None -> maxIndex // if no batches in primary, high water mark from tip is max
        let! secondary = secondary (minIndex, maxIndex)
        let events =
            match secondary with
            | Some s -> Array.append s.events events
            | None -> events
        match secondary with
        | Some { minIndex = i } when i <= minI -> ()
        | Some { found = true } -> ()
        | _ -> logMissing (minIndex, maxIndex) "Origin event not found in secondary container"
        return pos, events }

#if PRUNE_SUPPORT
// Manages deletion of (full) Batches, and trimming of events in Tip, maintaining ordering guarantees by never updating non-Tip batches
// Additionally, the nature of the fallback algorithm requires that deletions be carried out in sequential order so as not to leave gaps
// NOTE: module is public so BatchIndices can be deserialized into
module Prune =

    type BatchIndices = { id : string; i : int64; n : int64 }

    let until (log : ILogger) (container : Container, stream : string) maxItems indexInclusive : Async<int * int * int64> = async {
        let log = log |> Log.prop "stream" stream
        let! ct = Async.CancellationToken
        let deleteItem id count : Async<float> = async {
            let ro = ItemRequestOptions(EnableContentResponseOnWrite = Nullable false) // https://devblogs.microsoft.com/cosmosdb/enable-content-response-on-write/
            let! t, res = container.DeleteItemAsync(id, PartitionKey stream, ro, ct) |> Async.AwaitTaskCorrect |> Stopwatch.Time
            let rc, ms = res.RequestCharge, (let e = t.Elapsed in e.TotalMilliseconds)
            let reqMetric : Log.Measurement = { table = container.TableName; stream = stream; interval = t; bytes = -1; count = count; ru = rc }
            let log = let evt = Log.Metric.Delete reqMetric in log |> Log.event evt
            log.Information("EqxDynamo {action:l} {id} {ms}ms {ru}RU", "Delete", id, ms, rc)
            return rc
        }
        let trimTip expectedI count = async {
            match! container.TryReadItem<Tip>(PartitionKey stream, Tip.WellKnownDocumentId) with
            | _, ReadResult.NotModified -> return failwith "unexpected NotModified; no etag supplied"
            | _, ReadResult.NotFound -> return failwith "unexpected NotFound"
            | _, ReadResult.Found tip when tip.i <> expectedI -> return failwithf "Concurrent write detected; Expected i=%d actual=%d" expectedI tip.i
            | tipRu, ReadResult.Found tip ->

            let tip = { tip with i = tip.i + int64 count; e = Array.skip count tip.e }
            let ro = ItemRequestOptions(EnableContentResponseOnWrite = Nullable false, IfMatchEtag = tip.etag)
            let! t, updateRes = container.ReplaceItemAsync(tip, tip.id, Nullable (PartitionKey stream), ro, ct) |> Async.AwaitTaskCorrect |> Stopwatch.Time
            let rc, ms = tipRu + updateRes.RequestCharge, (let e = t.Elapsed in e.TotalMilliseconds)
            let reqMetric : Log.Measurement = { table = container.TableName; stream = stream; interval = t; bytes = -1; count = count; ru = rc }
            let log = let evt = Log.Metric.Trim reqMetric in log |> Log.event evt
            log.Information("EqxDynamo {action:l} {count} {ms}ms {ru}RU", "Trim", count, ms, rc)
            return rc
        }
        let log = log |> Log.prop "index" indexInclusive
        let query : FeedIterator<BatchIndices> =
             let qro = QueryRequestOptions(PartitionKey = Nullable (PartitionKey stream), MaxItemCount = Nullable maxItems)
             // sort by i to guarantee we don't ever leave an observable gap in the sequence
             container.GetItemQueryIterator<_>(QueryDefinition "SELECT c.id, c.i, c.n FROM c ORDER by c.i", requestOptions = qro)
        let mapPage i (t : StopwatchInterval) (page : FeedResponse<BatchIndices>) =
            let batches, rc, ms = Array.ofSeq page, page.RequestCharge, (let e = t.Elapsed in e.TotalMilliseconds)
            let next = Array.tryLast batches |> Option.map (fun x -> x.n) |> Option.toNullable
            let reqMetric : Log.Measurement = { table = container.TableName; stream = stream; interval = t; bytes = -1; count = batches.Length; ru = rc }
            let log = let evt = Log.Metric.PruneResponse reqMetric in log |> Log.prop "batchIndex" i |> Log.event evt
            log.Information("EqxDynamo {action:l} {batches} {ms}ms n={next} {ru}RU", "PruneResponse", batches.Length, ms, next, rc)
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
        let reqMetric : Log.Measurement = { table = container.TableName; stream = stream; interval = pt; bytes = eventsDeleted; count = batches; ru = queryCharges }
        let log = let evt = Log.Metric.Prune (responses, reqMetric) in log |> Log.event evt
        let lwm = lwm |> Option.defaultValue 0L // If we've seen no batches at all, then the write position is 0L
        log.Information("EqxDynamo {action:l} {events}/{batches} lwm={lwm} {ms}ms queryRu={queryRu} deleteRu={deleteRu} trimRu={trimRu}",
                "Prune", eventsDeleted, batches, lwm, (let e = pt.Elapsed in e.TotalMilliseconds), queryCharges, delCharges, trimCharges)
        return eventsDeleted, eventsDeferred, lwm
    }
#endif

type [<NoComparison>] Token = { pos : Position }
module Token =

    let create pos : StreamToken = { value = box { pos = pos }; version = pos.index }
    let (|Unpack|) (token : StreamToken) : Position = let t = unbox<Token> token.value in t.pos
    let supersedes (Unpack currentPos) (Unpack xPos) =
        let currentVersion, newVersion = currentPos.index, xPos.index
        let currentETag, newETag = currentPos.etag, xPos.etag
        newVersion > currentVersion || currentETag <> newETag

[<AutoOpen>]
module Internal =

    [<RequireQualifiedAccess; NoComparison; NoEquality>]
    type InternalSyncResult = Written of StreamToken | ConflictUnknown | Conflict of Position * ITimelineEvent<EventBody>[]

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
    (   // Maximum number of events permitted in Tip. When this is exceeded, events are moved out to a standalone Batch.
        maxEvents,
        // Maximum serialized size to permit to accumulate in Tip before they get moved out to a standalone Batch. Default: 30_000.
        [<O; D(null)>]?maxJsonLength,
        // Inhibit throwing when events are missing, but no fallback Container has been supplied. Default: false.
        [<O; D(null)>]?ignoreMissingEvents,
        [<O; D(null)>]?readRetryPolicy,
        [<O; D(null)>]?writeRetryPolicy) =
    /// Maximum number of events permitted in Tip. When this is exceeded, events are moved out to a standalone Batch.
    member val MaxEvents : int = maxEvents
    /// Maximum serialized size to permit to accumulate in Tip before they get moved out to a standalone Batch. Default: 30_000.
    member val MaxJsonLength = defaultArg maxJsonLength 30_000
    /// Whether to inhibit throwing when events are missing, but no fallback Container has been supplied
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
                       | None when Option.isSome tip -> Some Tip.WellKnownI
                       | None -> None
        let walk log container = Query.scan log (container, stream) query.MaxItems query.MaxRequests direction (tryDecode, isOrigin)
        let walkFallback =
            match fallback with
            | None -> Choice1Of2 ignoreMissing
            | Some f -> Choice2Of2 (walk (log |> Log.prop "secondary" true) f)

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
        | Sync.Result.ConflictUnknown -> return InternalSyncResult.ConflictUnknown
        | Sync.Result.Written pos' -> return InternalSyncResult.Written (Token.create pos') }

#if PRUNE_SUPPORT
    member _.Prune(log, stream, index) =
        Prune.until log (container, stream) query.MaxItems index
#endif

type internal Category<'event, 'state, 'context>(store : StoreClient, codec : IEventCodec<'event, EventBody, 'context>) =
    member _.Load(log, stream, initial, checkUnfolds, fold, isOrigin) : Async<StreamToken * 'state> = async {
        let! token, events = store.Load(log, (stream, None), (codec.TryDecode, isOrigin), checkUnfolds)
        return token, fold initial events }
    member _.Reload(log, streamName, (Token.Unpack pos as streamToken), state, fold, isOrigin, ?preloaded) : Async<StreamToken * 'state> = async {
        match! store.Reload(log, (streamName, pos), (codec.TryDecode, isOrigin), ?preview = preloaded) with
        | LoadFromTokenResult.Unchanged -> return streamToken, state
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
        let compressor = id // (body : byte[]) : EventBody = new System.IO.MemoryStream(body) // TODO if compressUnfolds then JsonCompressedBase64Converter.Compress else JsonHelper.fixup
        let projections = Sync.mkUnfold compressor baseIndex projectionsEncoded
        let batch = Sync.mkBatch streamName eventsEncoded projections
        match! store.Sync(log, streamName, exp, batch) with
        | InternalSyncResult.Conflict (pos', tipEvents) -> return SyncResult.Conflict (cat.Reload(log, streamName, streamToken, state, fold, isOrigin, (pos', pos.index, tipEvents)))
        | InternalSyncResult.ConflictUnknown -> return SyncResult.Conflict (cat.Reload(log, streamName, streamToken, state, fold, isOrigin))
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
            checkUnfolds, compressUnfolds,
            mapUnfolds : Choice<unit, 'event list -> 'state -> 'event seq, 'event list -> 'state -> 'event list * 'event list>) =
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
                match! category.Sync(log, streamName, streamToken, state, events, mapUnfolds, fold, isOrigin, context, compressUnfolds) with
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

[<NoComparison; NoEquality>]
type ConnectMode =
    | SkipVerify
    | Verify
    | CreateIfNotExists of Throughput
module ConnectMode =
    let apply client tableName = function
        | SkipVerify -> async { () }
        | Verify -> Initialization.verify client tableName
        | CreateIfNotExists throughput -> Initialization.createIfNotExists client tableName throughput

/// Holds all relevant state for a Store
/// - The Client (IAmazonDynamoDB). there should be a single one of these per process, plus an optional fallback one for pruning scenarios.
/// - The (singleton) per-Table initialization state // TOCONSIDER remove if we don't end up with any
type DynamoStoreClient
    (   // Facilitates custom mapping of Stream Category Name to underlying Table and Stream names
        categoryAndStreamIdToTableAndStreamNames : string * string -> string * string,
        createContainer : string -> Container,
        createSecondaryContainer : string -> Container option,
        [<O; D(null)>]?primaryContainerToSecondary : string -> string) =
    let primaryTableToSecondary = defaultArg primaryContainerToSecondary id
    // Index of tableName -> Initialization Context
    let containerInitGuards = System.Collections.Concurrent.ConcurrentDictionary<string, Initialization.ContainerInitializerGuard>()
    new(client : Amazon.DynamoDBv2.IAmazonDynamoDB, tableName : string,
        // Table name to use for fallback/archive store. Default: (if <c>fallbackClient</c> specified) use same <c>tableName</c> but via <c>fallbackClient</c>
        [<O; D(null)>]?fallbackTableName,
        [<O; D(null)>]?fallbackClient : Amazon.DynamoDBv2.IAmazonDynamoDB) =
        let genStreamName (categoryName, streamId) = if categoryName = null then streamId else sprintf "%s-%s" categoryName streamId
        let catAndStreamToTableStream (categoryName, streamId) = tableName, genStreamName (categoryName, streamId)
        let primaryContainer t = Container.Create(client, t)
        let secondaryContainer =
            if Option.isNone fallbackClient && Option.isNone fallbackTableName then fun _ -> None
            else fun t -> Some (Container.Create(defaultArg fallbackClient client, defaultArg fallbackTableName t))
        DynamoStoreClient(catAndStreamToTableStream, primaryContainer, secondaryContainer)
    // TOCONSIDER remove this facility unless we end up using it
    member internal _.ResolveContainerGuardAndStreamName(categoryName, streamId) : Initialization.ContainerInitializerGuard * string =
        let createContainerInitializerGuard tableName =
            let init = Some (fun _container -> async { () })
            let fallbackTableName = primaryTableToSecondary tableName
            let primaryContainer, fallbackContainer = createContainer tableName, createSecondaryContainer fallbackTableName
            Initialization.ContainerInitializerGuard(primaryContainer, fallbackContainer, ?initContainer = init)
        let tableName, streamName = categoryAndStreamIdToTableAndStreamNames (categoryName, streamId)
        let g = containerInitGuards.GetOrAdd(tableName, createContainerInitializerGuard)
        g, streamName

    /// Connect to an Equinox.DynamoStore in the specified Table
    /// Events that have been archived and purged (and hence are missing from the primary) are retrieved from the fallback where that is provided
    static member Connect(client, tableName : string, ?fallbackTableName, ?mode : ConnectMode) : Async<DynamoStoreClient> = async {
        let init t = ConnectMode.apply client t (defaultArg mode ConnectMode.Verify)
        do! init tableName
        match fallbackTableName with None -> () | Some fallbackTable -> do! init fallbackTable
        return DynamoStoreClient(client, tableName, ?fallbackTableName = fallbackTableName) }

/// Defines a set of related access policies for a given Table, together with a Containers map defining mappings from (category, streamId) to (tableName, streamName)
type DynamoStoreContext(storeClient : DynamoStoreClient, tipOptions, queryOptions) =
    new(    storeClient : DynamoStoreClient,
            // Maximum number of events permitted in Tip. When this is exceeded, events are moved out to a standalone Batch.
            tipMaxEvents,
            // Maximum serialized event size to permit to accumulate in Tip before they get moved out to a standalone Batch. Default: 30_000.
            [<O; D null>]?tipMaxJsonLength,
            // Inhibit throwing when events are missing, but no fallback Table has been supplied
            [<O; D null>]?ignoreMissingEvents,
            // Max number of Batches to return per paged query response. Default: 10.
            [<O; D null>]?queryMaxItems,
            // Maximum number of trips to permit when slicing the work into multiple responses limited by `queryMaxItems`. Default: unlimited.
            [<O; D null>]?queryMaxRequests) =
        let tipOptions = TipOptions(maxEvents = tipMaxEvents, ?maxJsonLength = tipMaxJsonLength, ?ignoreMissingEvents = ignoreMissingEvents)
        let queryOptions = QueryOptions(?maxItems = queryMaxItems, ?maxRequests = queryMaxRequests)
        DynamoStoreContext(storeClient, tipOptions, queryOptions)
    member val StoreClient = storeClient
    member val QueryOptions = queryOptions
    member val TipOptions = tipOptions
    member internal x.ResolveContainerClientAndStreamIdAndInit(categoryName, streamId) =
        let cg, streamId = storeClient.ResolveContainerGuardAndStreamName(categoryName, streamId)
        let store = StoreClient(cg.Container, cg.Fallback, x.QueryOptions, x.TipOptions)
        store, streamId, cg.InitializationGate

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

type DynamoStoreCategory<'event, 'state, 'context>
    (   context : DynamoStoreContext, codec, fold, initial, caching, access,
        // TOCONSIDER compress events?
        // Compress Unfolds in Tip. Default: <c>true</c>.
        [<O; D null>]?compressUnfolds) =
    let compressUnfolds = defaultArg compressUnfolds true
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
            Caching.CachingCategory<'event, 'state, 'context>(cosmosCat, fold, initial, isOrigin, tryReadCache, updateCache, checkUnfolds, compressUnfolds, mapUnfolds) :> _
        categories.GetOrAdd(categoryName, createCategory)
    let resolve (FsCodec.StreamName.CategoryAndId (categoryName, streamId)) =
        let container, streamName, maybeContainerInitializationGate = context.ResolveContainerClientAndStreamIdAndInit(categoryName, streamId)
        resolveCategory (categoryName, container), streamName, maybeContainerInitializationGate
    let empty = Token.create Position.fromKnownEmpty, initial
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
    // TOCONSIDER remove
    | Conflict of index : 't * conflictingEvents : ITimelineEvent<EventBody>[]
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
        return pos', data }

    let getRange direction startPos =
        let startPos = startPos |> Option.map (fun x -> x.index)
        match direction with
        | Direction.Forward -> startPos, None
        | Direction.Backward -> None, startPos

    new (context : Equinox.DynamoStore.DynamoStoreContext, log) =
        let store, _streamId, _init = context.ResolveContainerClientAndStreamIdAndInit(null, null)
        EventsContext(context, store, log)

    member _.ResolveStream(streamName) =
        let _cc, streamId, init = context.ResolveContainerClientAndStreamIdAndInit(null, streamName)
        streamId, init
    member x.StreamId(streamName) : string = x.ResolveStream streamName |> fst

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
            let! token, events = store.Read(log, stream, direction, (Some, isOrigin), ?minIndex = minIndex, ?maxIndex = maxIndex)
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
    member x.Read(stream, [<O; D null>] ?position, [<O; D null>] ?maxCount, [<O; D null>] ?direction) : Async<Position * ITimelineEvent<EventBody>[]> =
        x.GetInternal((stream, position), ?maxCount = maxCount, ?direction = direction) |> yieldPositionAndData

    /// Appends the supplied batch of events, subject to a consistency check based on the `position`
    /// Callers should implement appropriate idempotent handling, or use Equinox.Decider for that purpose
    member x.Sync(stream, position, events : IEventData<_>[]) : Async<AppendResult<Position>> = async {
        // TODO FIX COMMENT
        // Writes go through the stored proc, which we need to provision per container
        // Having to do this here in this way is far from ideal, but work on caching, external snapshots and caching is likely
        //   to move this about before we reach a final destination in any case
        match x.ResolveStream stream |> snd with
        | None -> ()
        | Some init -> do! init ()
        let batch = Sync.mkBatch stream events Seq.empty
        match! store.Sync(log, stream, SyncExp.Version position.index, batch) with
        | InternalSyncResult.Written (Token.Unpack pos) -> return AppendResult.Ok pos
        | InternalSyncResult.Conflict (pos, events) -> return AppendResult.Conflict (pos, events)
        | InternalSyncResult.ConflictUnknown -> return AppendResult.ConflictUnknown }

    /// Low level, non-idempotent call appending events to a stream without a concurrency control mechanism in play
    /// NB Should be used sparingly; Equinox.Decider enables building equivalent equivalent idempotent handling with minimal code.
    member x.NonIdempotentAppend(stream, events : IEventData<_>[]) : Async<Position> = async {
        match! x.Sync(stream, Position.fromAppendAtEnd, events) with
        | AppendResult.Ok token -> return token
        | x -> return x |> sprintf "Conflict despite it being disabled %A" |> invalidOp }

#if PRUNE_SUPPORT
    member _.Prune(stream, index) : Async<int * int * int64> =
        store.Prune(log, stream, index)
#endif

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
        | AppendResult.Conflict (PositionIndex index, events) -> return AppendResult.Conflict (index, events)
        | AppendResult.ConflictUnknown -> return AppendResult.ConflictUnknown }
    let private stripPosition (f : Async<Position>) : Async<int64> = async {
        let! (PositionIndex index) = f
        return index }
    let private dropPosition (f : Async<Position * ITimelineEvent<EventBody>[]>) : Async<ITimelineEvent<EventBody>[]> = async {
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

#if PRUNE_SUPPORT
    /// Requests deletion of events up and including the specified <c>index</c>.
    /// Due to the need to preserve ordering of data in the stream, only complete Batches will be removed.
    /// If the <c>index</c> is within the Tip, events are removed via an etag-checked update. Does not alter the unfolds held in the Tip, or remove the Tip itself.
    /// Returns count of events deleted this time, events that could not be deleted due to partial batches, and the stream's lowest remaining sequence number.
    let pruneUntil (ctx : EventsContext) (streamName : string) (index : int64) : Async<int * int * int64> =
        ctx.Prune(ctx.StreamId streamName, index)
#endif

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
