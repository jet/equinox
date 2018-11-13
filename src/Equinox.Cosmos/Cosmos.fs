namespace Equinox.Cosmos

open Equinox
open Equinox.Store
open FSharp.Control
open Microsoft.Azure.Documents
open Microsoft.Azure.Documents.Linq
open Newtonsoft.Json
open Newtonsoft.Json.Linq
open Serilog
open System


[<AutoOpen>]
module DocDbExtensions =
    type Client.RequestOptions with
        /// Simplified ETag precondition builder
        member options.ETag
            with get () =
                match options.AccessCondition with
                | null -> null
                | ac -> ac.Condition

            and set etag =
                if String.IsNullOrEmpty etag then () else
                options.AccessCondition <- Client.AccessCondition(Type = Client.AccessConditionType.IfMatch, Condition = etag)

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
    let (|HasValue|_|) (x:Nullable<_>) = if x.HasValue then Some x.Value else None
    /// DocumentDB Error HttpStatusCode extractor
    let (|DocDbStatusCode|_|) (e : DocumentClientException) =
        match e.StatusCode with
        | HasValue x -> Some x
        | _ -> None

    type ReadResult<'T> = Found of 'T | NotFound | PreconditionFailed
    type DocDbCollection(client : IDocumentClient, collectionUri) =
        member __.TryReadDocument(documentId : string, ?options : Client.RequestOptions): Async<float * ReadResult<'T>> = async {
            let! ct = Async.CancellationToken
            let options = defaultArg options null
            let docLink = sprintf "%O/docs/%s" collectionUri documentId
            try let! document = async { return! client.ReadDocumentAsync<'T>(docLink, options = options, cancellationToken = ct) |> Async.AwaitTaskCorrect }
                return document.RequestCharge, Found document.Document
            with
            | DocDbException (DocDbStatusCode System.Net.HttpStatusCode.NotFound as e) -> return e.RequestCharge, NotFound
            | DocDbException (DocDbStatusCode System.Net.HttpStatusCode.PreconditionFailed as e) -> return e.RequestCharge, PreconditionFailed }

module Store =
    [<NoComparison>]
    type Position =
        {   collectionUri: Uri; streamName: string; index: int64 option; etag: string option }
        member __.Index : int64 = defaultArg __.index -1L
        member __.IndexRel (offset: int) : int64 = __.index |> function
            | Some index -> index+int64 offset
            | None -> failwithf "Cannot IndexRel %A" __

    type EventData = { eventType: string; data: byte[]; metadata: byte[] }
    type IEventData =
        /// The Event Type, used to drive deserialization
        abstract member EventType : string
        /// Event body, as UTF-8 encoded json ready to be injected into the Json being rendered for DocDb
        abstract member DataUtf8 : byte[]
        /// Optional metadata (null, or same as d, not written if missing)
        abstract member MetaUtf8 : byte[]

    [<NoEquality; NoComparison; JsonObject(ItemRequired=Required.Always)>]
    type Event =
        {   (* DocDb-mandated essential elements *)

            // DocDb-mandated Partition Key, must be maintained within the document
            // Not actually required if running in single partition mode, but for simplicity, we always write it
            p: string // "{streamName}"

            // DocDb-mandated unique row key; needs to be unique within any partition it is maintained; must be a string
            // At the present time, one can't perform an ORDER BY on this field, hence we also have i, which is identical
            id: string // "{index}"

            // Same as `id`; necessitated by fact that it's not presently possible to do an ORDER BY on the row key
            i: int64 // {index}

            (* Event payload elements *)

            /// Creation date (as opposed to sytem-defined _lastUpdated which is rewritten by triggers adnd/or replication)
            c: DateTimeOffset // ISO 8601

            /// The Event Type, used to drive deserialization
            t: string // required

            /// Event body, as UTF-8 encoded json ready to be injected into the Json being rendered for DocDb
            [<JsonConverter(typeof<VerbatimUtf8JsonConverter>)>]
            d: byte[] // required

            /// Optional metadata (null, or same as d, not written if missing)
            [<JsonConverter(typeof<VerbatimUtf8JsonConverter>); JsonProperty(Required=Required.Default, NullValueHandling=NullValueHandling.Ignore)>]
            m: byte[] } // optional
        /// Unless running in single partion mode (which would restrict us to 10GB per collection)
        /// we need to nominate a partition key that will be in every document
        static member PartitionKeyField = "p"
        /// As one cannot sort by the implicit `id` field, we have an indexed `i` field which we use for sort and range query purporses
        static member IndexedFields = [Event.PartitionKeyField; "i"]
        static member Create (pos: Position) offset (ed: EventData) : Event =
            {   p = pos.streamName; id = string (pos.IndexRel offset); i = pos.IndexRel offset
                c = DateTimeOffset.UtcNow
                t = ed.eventType; d = ed.data; m = ed.metadata }
        interface IEventData with
            member __.EventType = __.t
            member __.DataUtf8 = __.d
            member __.MetaUtf8 = __.m

    /// Manages injecting prepared json into the data being submitted to DocDb as-is, on the basis we can trust it to be valid json as DocDb will need it to be
    and VerbatimUtf8JsonConverter() =
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

    [<NoEquality; NoComparison; JsonObject(ItemRequired=Required.Always)>]
    type IndexEvent =
        {   p: string // "{streamName}"
            id: string // "{-1}"

            //w: int64 // 100: window size
            /// last index/i value
            m: int64 // {index}

            /// Compacted projections based on version identified by `m`
            c: IndexProjection[]

            (*// Potential schema to manage Pending Events together with compaction events based on each one
              // This scheme is more complete than the simple `c` encoding, which relies on every writer being able to write all salient snapshots
              // For instance, in the case of blue/green deploys, older versions need to be able to coexist without destroying the perf for eachother
              "x": [
                { "i":0,
                  "c":"ISO 8601"
                  "e":[
                    [{"t":"added","d":"..."},{"t":"compacted/1","d":"..."}],
                    [{"t":"removed","d":"..."}],
                  ]
                }
              ] *)
            //x: JObject[][]
            }
        static member IdConstant = "-1"
        static member Create (pos: Position) eventCount (eds: EventData[]) : IndexEvent =
            {   p = pos.streamName; id = IndexEvent.IdConstant; m = pos.IndexRel eventCount
                c = [| for ed in eds -> { t = ed.eventType; d = ed.data; m = ed.metadata } |] }
    and IndexProjection =
        {   /// The Event Type, used to drive deserialization
            t: string // required

            /// Event body, as UTF-8 encoded json ready to be injected into the Json being rendered for DocDb
            [<JsonConverter(typeof<VerbatimUtf8JsonConverter>)>]
            d: byte[] // required

            /// Optional metadata (null, or same as d, not written if missing)
            [<JsonConverter(typeof<VerbatimUtf8JsonConverter>); JsonProperty(Required=Required.Default, NullValueHandling=NullValueHandling.Ignore)>]
            m: byte[] } // optional
        interface IEventData with
            member __.EventType = __.t
            member __.DataUtf8 = __.d
            member __.MetaUtf8 = __.m
    (* Pseudocode:
    function sync(p, expectedVersion, windowSize, events) {
        if (i == 0) then {
            coll.insert(p,0,{ p:p, id:-1, w:windowSize, m:flatLen(events)})
        } else {
            const i = doc.find(p=p && id=-1)
            if(i.m <> expectedVersion) then emit from expectedVersion else
            i.x.append(events)
            for (var (i, c, e: [ {e1}, ...]) in events) {
                coll.insert({p:p, id:i, i:i, c:c, e:e1)
            }
            // trim i.x to w total items in i.[e]
            coll.update(p,id,i)
        }
    } *)
[<RequireQualifiedAccess>]
type Direction = Forward | Backward with
    override this.ToString() = match this with Forward -> "Forward" | Backward -> "Backward"

module Log =
    [<NoEquality; NoComparison>]
    type Measurement = { stream: string; interval: StopwatchInterval; bytes: int; count: int; ru: float }
    [<NoEquality; NoComparison>]
    type Event =
        | WriteSuccess of Measurement
        | WriteConflict of Measurement
        /// Individual read request in a Batch
        | Slice of Direction * Measurement
        /// Individual read request for the Index
        | Index of Measurement
        /// Individual read request for the Index, not found
        | IndexNotFound of Measurement
        /// Index read with Single RU Request Charge due to correct use of etag in cache
        | IndexCached of Measurement
        /// Summarizes a set of Slices read together
        | Batch of Direction * slices: int * Measurement
    let prop name value (log : ILogger) = log.ForContext(name, value)
    let propEvents name (kvps : System.Collections.Generic.KeyValuePair<string,string> seq) (log : ILogger) =
        let items = seq { for kv in kvps do yield sprintf "{\"%s\": %s}" kv.Key kv.Value }
        log.ForContext(name, sprintf "[%s]" (String.concat ",\n\r" items))
    let propEventData name (events : Store.EventData[]) (log : ILogger) =
        log |> propEvents name (seq { for x in events -> Collections.Generic.KeyValuePair<_,_>(x.eventType, System.Text.Encoding.UTF8.GetString x.data)})
    let propResolvedEvents name (events : Store.Event[]) (log : ILogger) =
        log |> propEvents name (seq { for x in events -> Collections.Generic.KeyValuePair<_,_>(x.t, System.Text.Encoding.UTF8.GetString x.d)})
    let propIEventDatas name (events : Store.IEventData[]) (log : ILogger) =
        log |> propEvents name (seq { for x in events -> Collections.Generic.KeyValuePair<_,_>(x.EventType, System.Text.Encoding.UTF8.GetString x.DataUtf8)})
    let propProjectionEvents name (events : Store.IndexProjection[]) (log : ILogger) =
        log |> propEvents name (seq { for x in events -> Collections.Generic.KeyValuePair<_,_>(x.t, System.Text.Encoding.UTF8.GetString x.d)})

    open Serilog.Events
    /// Attach a property to the log context to hold the metrics
    // Sidestep Log.ForContext converting to a string; see https://github.com/serilog/serilog/issues/1124
    let event (value : Event) (log : ILogger) =
        let enrich (e : LogEvent) = e.AddPropertyIfAbsent(LogEventProperty("cosmosEvt", ScalarValue(value)))
        log.ForContext({ new Serilog.Core.ILogEventEnricher with member __.Enrich(evt,_) = enrich evt })
    let withLoggedRetries<'t> retryPolicy (contextLabel : string) (f : ILogger -> Async<'t>) log: Async<'t> =
        match retryPolicy with
        | None -> f log
        | Some retryPolicy ->
            let withLoggingContextWrapping count =
                let log = if count = 1 then log else log |> prop contextLabel count
                f log
            retryPolicy withLoggingContextWrapping
    let (|BlobLen|) = function null -> 0 | (x : byte[]) -> x.Length

[<RequireQualifiedAccess; NoEquality; NoComparison>]
type EqxSyncResult =
    | Written of Store.Position
    | ConflictUnknown of Store.Position
    | Conflict of Store.Position * events: Store.IEventData[]

module private Write =
    [<CLIMutable; NoEquality; NoComparison; JsonObject(ItemRequired=Required.AllowNull)>]
    type WriteResponse = { etag: string; conflicts: Store.IndexProjection[] }
    let [<Literal>] sprocName = "AtomicMultiDocInsert"

    let private writeEventsAsync (client: IDocumentClient) (pos: Store.Position) (events: Store.EventData seq,maybeIndexEvents): Async<float*EqxSyncResult> = async {
        let sprocUri = sprintf "%O/sprocs/%s" pos.collectionUri sprocName
        let opts = Client.RequestOptions(PartitionKey=PartitionKey(pos.streamName))
        let! ct = Async.CancellationToken
        let events = events |> Seq.mapi (fun i ed -> Store.Event.Create pos (i+1) ed |> JsonConvert.SerializeObject) |> Seq.toArray
        if events.Length = 0 then invalidArg "eventsData" "must be non-empty"
        let index : Store.IndexEvent =
            match maybeIndexEvents with
            | None | Some [||] -> Unchecked.defaultof<_>
            | Some eds -> Store.IndexEvent.Create pos (events.Length) eds
        try
            let! (res : Client.StoredProcedureResponse<WriteResponse>) = client.ExecuteStoredProcedureAsync(sprocUri, opts, ct, box events, box pos.Index, box pos.etag, box index) |> Async.AwaitTaskCorrect
            match res.RequestCharge, (match res.Response.etag with null -> None | x -> Some x), res.Response.conflicts with
            | rc,e,null -> return rc, EqxSyncResult.Written { pos with index = Some (pos.IndexRel events.Length); etag=e }
            | rc,e,[||] -> return rc, EqxSyncResult.ConflictUnknown { pos with etag=e }
            | rc,e, xs  -> return rc, EqxSyncResult.Conflict ({ pos with index = Some (pos.IndexRel xs.Length); etag=e }, Array.map (fun x -> x :> _) xs)
        with DocDbException ex when ex.Message.Contains "already" -> // TODO this does not work for the SP
            return ex.RequestCharge, EqxSyncResult.ConflictUnknown { pos with etag=None } }

    let bytes events =
        let eventDataLen ({ data = Log.BlobLen bytes; metadata = Log.BlobLen metaBytes } : Store.EventData) = bytes + metaBytes
        events |> Array.sumBy eventDataLen

    let private writeEventsLogged client (pos : Store.Position) (events : Store.EventData[], maybeIndexEvents) (log : ILogger): Async<EqxSyncResult> = async {
        let log = if (not << log.IsEnabled) Events.LogEventLevel.Debug then log else log |> Log.propEventData "Json" events
        let bytes, count = bytes events, events.Length
        let log = log |> Log.prop "bytes" bytes
        let writeLog = log |> Log.prop "stream" pos.streamName |> Log.prop "expectedVersion" pos.Index |> Log.prop "count" count
        let! t, (ru,result) = writeEventsAsync client pos (events,maybeIndexEvents) |> Stopwatch.Time
        let resultLog =
            let mkMetric ru : Log.Measurement = { stream = pos.streamName; interval = t; bytes = bytes; count = count; ru = ru }
            let logConflict () = writeLog.Information("Eqx TrySync WrongExpectedVersion writing {EventTypes}", [| for x in events -> x.eventType |])
            match result with
            | EqxSyncResult.Written pos ->
                log |> Log.event (Log.WriteSuccess (mkMetric ru)) |> Log.prop "nextExpectedVersion" pos
            | EqxSyncResult.ConflictUnknown pos ->
                logConflict ()
                log |> Log.event (Log.WriteConflict (mkMetric ru)) |> Log.prop "nextExpectedVersion" pos |> Log.prop "conflict" true
            | EqxSyncResult.Conflict (pos, xs) ->
                logConflict ()
                let log = if (not << log.IsEnabled) Events.LogEventLevel.Debug then log else log |> Log.prop "nextExpectedVersion" pos |> Log.propIEventDatas "conflictJson" xs
                log |> Log.event (Log.WriteConflict (mkMetric ru)) |> Log.prop "conflict" true
        resultLog.Information("Eqx {action:l} {count} {ms}ms rc={ru}", "Write", events.Length, (let e = t.Elapsed in e.TotalMilliseconds), ru)
        return result }

    let writeEvents (log : ILogger) retryPolicy client pk (events : Store.EventData[],maybeIndexEvents): Async<EqxSyncResult> =
        let call = writeEventsLogged client pk (events,maybeIndexEvents)
        Log.withLoggedRetries retryPolicy "writeAttempt" call log

module private Read =
    let private getIndex (client : IDocumentClient) (pos:Store.Position) =
        let coll = DocDbCollection(client, pos.collectionUri)
        let ac = match pos.etag with None -> null | Some etag-> Client.AccessCondition(Type=Client.AccessConditionType.IfNoneMatch, Condition=etag)
        let ro = Client.RequestOptions(PartitionKey=PartitionKey(pos.streamName), AccessCondition = ac)
        coll.TryReadDocument(Store.IndexEvent.IdConstant, ro)
    let private loggedGetIndex (getIndex : Store.Position -> Async<_>) (pos:Store.Position)  (log: ILogger) = async {
        let log = log |> Log.prop "stream" pos.streamName
        let! t, (ru, res : ReadResult<Store.IndexEvent>) = getIndex pos |> Stopwatch.Time
        let log count bytes (f : Log.Measurement -> _) = log |> Log.event (f { stream = pos.streamName; interval = t; bytes = bytes; count = count; ru = ru })
        match res with
        | ReadResult.PreconditionFailed ->
            (log 0 0 Log.IndexCached).Information("Eqx {action:l} {ms}ms rc={ru}", "IndexCached", (let e = t.Elapsed in e.TotalMilliseconds), ru)
        | ReadResult.NotFound ->
            (log 0 0 Log.IndexNotFound).Information("Eqx {action:l} {ms}ms rc={ru}", "IndexNotFound", (let e = t.Elapsed in e.TotalMilliseconds), ru)
        | ReadResult.Found doc ->
            let log =
                let (|EventLen|) (x : Store.IndexProjection) = match x.d, x.m with Log.BlobLen bytes, Log.BlobLen metaBytes -> bytes + metaBytes
                let bytes, count = doc.c |> Array.sumBy (|EventLen|), doc.c.Length
                log bytes count Log.Index
            let log = if (not << log.IsEnabled) Events.LogEventLevel.Debug then log else log |> Log.propProjectionEvents "Json" doc.c
            log.Information("Eqx {action:l} {ms}ms rc={ru}", "Index", (let e = t.Elapsed in e.TotalMilliseconds), ru)
        return ru, res }
    type [<RequireQualifiedAccess; NoComparison>] IndexResult = Unchanged | NotFound | Found of Store.Position * Store.IndexProjection[]
    /// `pos` being Some implies that the caller holds a cached value and hence is ready to deal with IndexResult.UnChanged
    let loadIndex (log : ILogger) retryPolicy client (pos : Store.Position): Async<IndexResult> = async {
        let getIndex = getIndex client
        let! _rc, res = Log.withLoggedRetries retryPolicy "readAttempt" (loggedGetIndex getIndex pos) log
        match res with
        | ReadResult.PreconditionFailed -> return IndexResult.Unchanged
        | ReadResult.NotFound -> return IndexResult.NotFound
        | ReadResult.Found index -> return IndexResult.Found ({ pos with index = Some index.m }, index.c) }

    let private getQuery (client : IDocumentClient) (pos:Store.Position) (direction: Direction) batchSize =
        let querySpec =
            match pos.index with
            | None -> SqlQuerySpec(if direction = Direction.Forward then "SELECT * FROM c ORDER BY c.i ASC" else "SELECT * FROM c ORDER BY c.i DESC")
            | Some index ->
                let f = if direction = Direction.Forward then "c.i >= @id ORDER BY c.i ASC" else "c.i < @id ORDER BY c.i DESC"
                SqlQuerySpec( "SELECT * FROM c WHERE " + f, SqlParameterCollection (Seq.singleton (SqlParameter("@id", index))))
        let feedOptions = new Client.FeedOptions(PartitionKey=PartitionKey(pos.streamName), MaxItemCount=Nullable batchSize)
        client.CreateDocumentQuery<Store.Event>(pos.collectionUri, querySpec, feedOptions).AsDocumentQuery()

    let (|EventLen|) (x : Store.Event) = match x.d, x.m with Log.BlobLen bytes, Log.BlobLen metaBytes -> bytes + metaBytes
    let bytes events = events |> Array.sumBy (|EventLen|)

    let private loggedQueryExecution (pos:Store.Position) direction (query: IDocumentQuery<Store.Event>) (log: ILogger): Async<Store.Event[] * float> = async {
        let! ct = Async.CancellationToken
        let! t, (res : Client.FeedResponse<Store.Event>) = query.ExecuteNextAsync<Store.Event>(ct) |> Async.AwaitTaskCorrect |> Stopwatch.Time
        let slice, ru = Array.ofSeq res, res.RequestCharge
        let bytes, count = bytes slice, slice.Length
        let reqMetric : Log.Measurement = { stream = pos.streamName; interval = t; bytes = bytes; count = count; ru = ru }
        let evt = Log.Slice (direction, reqMetric)
        let log = if (not << log.IsEnabled) Events.LogEventLevel.Debug then log else log |> Log.propResolvedEvents "Json" slice
        let index = match slice |> Array.tryHead with Some head -> head.id | None -> null
        (log |> Log.prop "startIndex" pos.Index |> Log.prop "bytes" bytes |> Log.event evt)
            .Information("Eqx {action:l} {count} {ms}ms i={index} rc={ru}", "Read", count, (let e = t.Elapsed in e.TotalMilliseconds), index, ru)
        return slice, ru }

    let private readBatches (log : ILogger) (readSlice: IDocumentQuery<Store.Event> -> ILogger -> Async<Store.Event[] * float>)
            (maxPermittedBatchReads: int option)
            (query: IDocumentQuery<Store.Event>)
        : AsyncSeq<Store.Event[] * float> =
        let rec loop batchCount : AsyncSeq<Store.Event[] * float> = asyncSeq {
            match maxPermittedBatchReads with
            | Some mpbr when batchCount >= mpbr -> log.Information "batch Limit exceeded"; invalidOp "batch Limit exceeded"
            | _ -> ()

            let batchLog = log |> Log.prop "batchIndex" batchCount
            let! slice = readSlice query batchLog
            yield slice
            if query.HasMoreResults then
                yield! loop (batchCount + 1) }
        loop 0

    let logBatchRead direction streamName interval events batchSize version (ru: float) (log : ILogger) =
        let bytes, count = bytes events, events.Length
        let reqMetric : Log.Measurement = { stream = streamName; interval = interval; bytes = bytes; count = count; ru = ru }
        let batches = (events.Length - 1)/batchSize + 1
        let action = match direction with Direction.Forward -> "LoadF" | Direction.Backward -> "LoadB"
        let evt = Log.Event.Batch (direction, batches, reqMetric)
        (log |> Log.prop "bytes" bytes |> Log.event evt).Information(
            "Eqx {action:l} stream={stream} {count}/{batches} {ms}ms i={index} rc={ru}",
            action, streamName, count, batches, (let e = interval.Elapsed in e.TotalMilliseconds), version, ru)

    let private lastEventIndex (xs:Store.Event seq) : int64 =
        match xs |> Seq.tryLast with
        | None -> -1L
        | Some last -> int64 last.id

    let loadForwardsFrom (log : ILogger) retryPolicy client batchSize maxPermittedBatchReads (pos): Async<Store.Position * Store.Event[]> = async {
        let mutable ru = 0.0
        let mergeBatches (batches: AsyncSeq<Store.Event[] * float>) = async {
            let! (events : Store.Event[]) =
                batches
                |> AsyncSeq.map (fun (events, r) -> ru <- ru + r; events)
                |> AsyncSeq.concatSeq
                |> AsyncSeq.toArrayAsync
            return events, ru }
        use query = getQuery client pos Direction.Forward batchSize
        let call q = loggedQueryExecution pos Direction.Forward q
        let retryingLoggingReadSlice q = Log.withLoggedRetries retryPolicy "readAttempt" (call q)
        let direction = Direction.Forward
        let log = log |> Log.prop "batchSize" batchSize |> Log.prop "direction" direction |> Log.prop "stream" pos.streamName
        let batches : AsyncSeq<Store.Event[] * float> = readBatches log retryingLoggingReadSlice maxPermittedBatchReads query
        let! t, (events, ru) = mergeBatches batches |> Stopwatch.Time
        query.Dispose()
        let version = lastEventIndex events
        log |> logBatchRead direction pos.streamName t events batchSize version ru
        return { pos with index = Some version }, events }

    let partitionPayloadFrom firstUsedEventNumber : Store.Event[] -> int * int =
        let acc (tu,tr) ((EventLen bytes) as y) = if y.id < firstUsedEventNumber then tu, tr + bytes else tu + bytes, tr
        Array.fold acc (0,0)
    let loadBackwardsUntilCompactionOrStart (log : ILogger) retryPolicy client batchSize maxPermittedBatchReads isCompactionEvent (pos : Store.Position)
        : Async<Store.Position * Store.Event[]> = async {
        let mergeFromCompactionPointOrStartFromBackwardsStream (log : ILogger) (batchesBackward : AsyncSeq<Store.Event[] * float>)
            : Async<Store.Event[] * float> = async {
            let lastBatch = ref None
            let mutable ru = 0.0
            let! tempBackward =
                batchesBackward
                |> AsyncSeq.map (fun (events, r) -> lastBatch := Some events; ru <- ru + r; events)
                |> AsyncSeq.concatSeq
                |> AsyncSeq.takeWhileInclusive (fun x ->
                    if not (isCompactionEvent x) then true // continue the search
                    else
                        match !lastBatch with
                        | None -> log.Information("Eqx Stop stream={stream} at={eventNumber}", pos.streamName, x.id)
                        | Some batch ->
                            let used, residual = batch |> partitionPayloadFrom x.id
                            log.Information("Eqx Stop stream={stream} at={eventNumber} used={used} residual={residual}", pos.streamName, x.id, used, residual)
                        false)
                |> AsyncSeq.toArrayAsync
            let eventsForward = Array.Reverse(tempBackward); tempBackward // sic - relatively cheap, in-place reverse of something we own
            return eventsForward, ru }
        use query = getQuery client pos Direction.Backward batchSize
        let call q = loggedQueryExecution pos Direction.Backward q
        let retryingLoggingReadSlice q = Log.withLoggedRetries retryPolicy "readAttempt" (call q)
        let log = log |> Log.prop "batchSize" batchSize |> Log.prop "stream" pos.streamName
        let direction = Direction.Backward
        let readlog = log |> Log.prop "direction" direction
        let batchesBackward : AsyncSeq<Store.Event[] * float> = readBatches readlog retryingLoggingReadSlice maxPermittedBatchReads query
        let! t, (events, ru) = mergeFromCompactionPointOrStartFromBackwardsStream log batchesBackward |> Stopwatch.Time
        query.Dispose()
        let version = lastEventIndex events
        log |> logBatchRead direction pos.streamName t events batchSize version ru
        return { pos with index = Some version } , events }

module UnionEncoderAdapters =
    let private encodedEventOfStoredEvent (x : Store.Event) : UnionCodec.EncodedUnion<byte[]> =
        { caseName = x.t; payload = x.d }
    let private encodedEventOfStoredEventI (x : Store.IEventData) : UnionCodec.EncodedUnion<byte[]> =
        { caseName = x.EventType; payload = x.DataUtf8 }
    let private eventDataOfEncodedEvent (x : UnionCodec.EncodedUnion<byte[]>) : Store.EventData =
        { eventType = x.caseName; data = x.payload; metadata = null }
    let encodeEvents (codec : UnionCodec.IUnionEncoder<'event, byte[]>) (xs : 'event seq) : Store.EventData[] =
        xs |> Seq.map (codec.Encode >> eventDataOfEncodedEvent) |> Seq.toArray
    let decodeKnownEventsI (codec : UnionCodec.IUnionEncoder<'event, byte[]>) (xs : Store.IEventData seq) : 'event seq =
        xs |> Seq.map encodedEventOfStoredEventI |> Seq.choose codec.TryDecode
    let decodeKnownEvents (codec : UnionCodec.IUnionEncoder<'event, byte[]>) (xs : Store.Event seq) : 'event seq =
        xs |> Seq.map encodedEventOfStoredEvent |> Seq.choose codec.TryDecode

type [<NoComparison>]Token = { pos: Store.Position; compactionEventNumber: int64 option }

module Token =
    let private create compactionEventNumber batchCapacityLimit pos : Storage.StreamToken =
        { value = box { pos = pos; compactionEventNumber = compactionEventNumber }; batchCapacityLimit = batchCapacityLimit }
    /// No batching / compaction; we only need to retain the StreamVersion
    let ofNonCompacting (pos : Store.Position) : Storage.StreamToken =
        create None None pos
    // headroom before compaction is necessary given the stated knowledge of the last (if known) `compactionEventNumberOption`
    let private batchCapacityLimit compactedEventNumberOption unstoredEventsPending (batchSize : int) (streamVersion : int64) : int =
        match compactedEventNumberOption with
        | Some (compactionEventNumber : int64) -> (batchSize - unstoredEventsPending) - int (streamVersion - compactionEventNumber + 1L) |> max 0
        | None -> (batchSize - unstoredEventsPending) - (int streamVersion + 1) - 1 |> max 0
    let (*private*) ofCompactionEventNumber compactedEventNumberOption unstoredEventsPending batchSize (pos : Store.Position) : Storage.StreamToken =
        let batchCapacityLimit = batchCapacityLimit compactedEventNumberOption unstoredEventsPending batchSize pos.Index
        create compactedEventNumberOption (Some batchCapacityLimit) pos
    /// Assume we have not seen any compaction events; use the batchSize and version to infer headroom
    let ofUncompactedVersion batchSize pos : Storage.StreamToken =
        ofCompactionEventNumber None 0 batchSize pos
    /// Use previousToken plus the data we are adding and the position we are adding it to infer a headroom
    let ofPreviousTokenAndEventsLength (previousToken : Storage.StreamToken) eventsLength batchSize pos : Storage.StreamToken =
        let compactedEventNumber = (unbox previousToken.value).compactionEventNumber
        ofCompactionEventNumber compactedEventNumber eventsLength batchSize pos
    let ofPreviousTokenWithUpdatedPosition (previousToken : Storage.StreamToken) batchSize pos : Storage.StreamToken =
        let compactedEventNumber = (unbox previousToken.value).compactionEventNumber
        ofCompactionEventNumber compactedEventNumber 0 batchSize pos
    /// Use an event just read from the stream to infer headroom
    let ofCompactionResolvedEventAndVersion (compactionEvent: Store.Event) batchSize pos : Storage.StreamToken =
        ofCompactionEventNumber (Some (int64 compactionEvent.id)) 0 batchSize pos
    /// Use an event we are about to write to the stream to infer headroom
    let ofPreviousStreamVersionAndCompactionEventDataIndex prevStreamVersion compactionEventDataIndex eventsLength batchSize streamVersion' : Storage.StreamToken =
        ofCompactionEventNumber (Some (prevStreamVersion + 1L + int64 compactionEventDataIndex)) eventsLength batchSize streamVersion'
    let private unpackEqxStreamVersion (x : Storage.StreamToken) = let x : Token = unbox x.value in x.pos.Index
    let supersedes current x =
        let currentVersion, newVersion = unpackEqxStreamVersion current, unpackEqxStreamVersion x
        newVersion > currentVersion

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
type GatewaySyncResult = Written of Storage.StreamToken | ConflictUnknown of Storage.StreamToken | Conflict of Storage.StreamToken * Store.IEventData[]

[<RequireQualifiedAccess; NoComparison; NoEquality>]
type LoadFromTokenResult = Unchanged | Found of Storage.StreamToken * Store.IEventData[]

type EqxGateway(conn : EqxConnection, batching : EqxBatchingPolicy) =
    let isResolvedEventEventType predicate (x:Store.Event) = predicate x.t
    let tryIsResolvedEventEventType predicateOption = predicateOption |> Option.map isResolvedEventEventType
    //let isResolvedEventDataEventType predicate (x:Store.Event) = predicate x.t
    //let tryIsEventDataEventType predicateOption = predicateOption |> Option.map isResolvedEventDataEventType
    let (|Pos|) (token: Storage.StreamToken) : Store.Position = (unbox<Token> token.value).pos
    let (|IEventDataArray|) events = [| for e in events -> e :> Store.IEventData |]
    member private __.InterpretIndexOrFallback log isCompactionEventType pos res: Async<Storage.StreamToken * Store.IEventData[]> = async {
        match res with
        | Read.IndexResult.Found (pos, projectionsAndEvents) when projectionsAndEvents |> Array.exists (fun x -> isCompactionEventType x.t) ->
            return Token.ofNonCompacting pos, projectionsAndEvents |> Seq.cast<Store.IEventData> |> Array.ofSeq
        | Read.IndexResult.Unchanged  -> return invalidOp "Not handled"
        | _ ->
            let! streamToken, events = __.LoadBackwardsStoppingAtCompactionEvent log isCompactionEventType pos
            return streamToken, events |> Seq.cast<Store.IEventData> |> Array.ofSeq }
    member __.LoadBatched log isCompactionEventType (pos : Store.Position): Async<Storage.StreamToken * Store.Event[]> = async {
        let! pos, events = Read.loadForwardsFrom log conn.ReadRetryPolicy conn.Client batching.BatchSize batching.MaxBatches pos
        match tryIsResolvedEventEventType isCompactionEventType with
        | None -> return Token.ofNonCompacting pos, events
        | Some isCompactionEvent ->
            match events |> Array.tryFindBack isCompactionEvent with
            | None -> return Token.ofUncompactedVersion batching.BatchSize pos, events
            | Some resolvedEvent -> return Token.ofCompactionResolvedEventAndVersion resolvedEvent batching.BatchSize pos, events }
    member __.LoadBackwardsStoppingAtCompactionEvent log isCompactionEventType pos: Async<Storage.StreamToken * Store.Event[]> = async {
        let isCompactionEvent = isResolvedEventEventType isCompactionEventType
        let! pos, events =
            Read.loadBackwardsUntilCompactionOrStart log conn.ReadRetryPolicy conn.Client batching.BatchSize batching.MaxBatches isCompactionEvent pos
        match Array.tryHead events |> Option.filter isCompactionEvent with
        | None -> return Token.ofUncompactedVersion batching.BatchSize pos, events
        | Some resolvedEvent -> return Token.ofCompactionResolvedEventAndVersion resolvedEvent batching.BatchSize pos, events }
    member __.IndexedOrBatched log isCompactionEventType pos: Async<Storage.StreamToken * Store.IEventData[]> = async {
        let! res = Read.loadIndex log None(* TODO conn.ReadRetryPolicy*) conn.Client pos
        return! __.InterpretIndexOrFallback log isCompactionEventType pos res }
    member __.LoadFromToken log (Pos pos as token) isCompactionEventType tryIndex: Async<LoadFromTokenResult> = async {
        let ok r = LoadFromTokenResult.Found r
        if not tryIndex then
            let! pos, ((IEventDataArray xs) as events) = Read.loadForwardsFrom log conn.ReadRetryPolicy conn.Client batching.BatchSize batching.MaxBatches pos
            let ok t = ok (t,xs)
            match tryIsResolvedEventEventType isCompactionEventType with
            | None -> return ok (Token.ofNonCompacting pos)
            | Some isCompactionEvent ->
                match events |> Array.tryFindBack isCompactionEvent with
                | None -> return ok (Token.ofPreviousTokenAndEventsLength token events.Length batching.BatchSize pos)
                | Some resolvedEvent -> return ok (Token.ofCompactionResolvedEventAndVersion resolvedEvent batching.BatchSize pos)
        else
            let! res = Read.loadIndex log None(* TODO conn.ReadRetryPolicy*) conn.Client pos
            match res with
            | Read.IndexResult.Unchanged  ->
                return LoadFromTokenResult.Unchanged
            | _ ->
                let! loaded = __.InterpretIndexOrFallback log isCompactionEventType.Value pos res
                return ok loaded }
    member __.TrySync log (Pos pos as token) (encodedEvents: Store.EventData[],maybeIndexEvents) isCompactionEventType: Async<GatewaySyncResult> = async {
        let! wr = Write.writeEvents log conn.WriteRetryPolicy conn.Client pos (encodedEvents,maybeIndexEvents)
        match wr with
        | EqxSyncResult.Conflict (pos',events) ->
            return GatewaySyncResult.Conflict (Token.ofPreviousTokenAndEventsLength token events.Length batching.BatchSize pos',events)
        | EqxSyncResult.ConflictUnknown pos' ->
            return GatewaySyncResult.ConflictUnknown (Token.ofPreviousTokenWithUpdatedPosition token batching.BatchSize pos')
        | EqxSyncResult.Written pos' ->

        let token =
            match isCompactionEventType with
            | None -> Token.ofNonCompacting pos'
            | Some isCompactionEvent ->
                let isEventDataEventType predicate (x:Store.EventData) = predicate x.eventType
                match encodedEvents |> Array.tryFindIndexBack (isEventDataEventType isCompactionEvent) with
                | None -> Token.ofPreviousTokenAndEventsLength token encodedEvents.Length batching.BatchSize pos'
                | Some compactionEventIndex ->
                    Token.ofPreviousStreamVersionAndCompactionEventDataIndex pos.Index compactionEventIndex encodedEvents.Length batching.BatchSize pos'
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
        token, fold initial (UnionEncoderAdapters.decodeKnownEventsI codec events)
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
        let events, index =
            match access with
            | None | Some AccessStrategy.EventsAreState ->
                events, None
            | Some (AccessStrategy.RollingSnapshots (_,f)) ->
                let cc = CompactionContext(List.length events, token.batchCapacityLimit.Value)
                (if cc.IsCompactionDue then events @ [f state'] else events), None
            | Some (AccessStrategy.IndexedSearch (_,index)) ->
                events, Some (index state')
        let encodedEvents : Store.EventData[] = UnionEncoderAdapters.encodeEvents codec (Seq.ofList events)
        let maybeIndexEvents : Store.EventData[] option = index |> Option.map (UnionEncoderAdapters.encodeEvents codec)
        let! syncRes = coll.Gateway.TrySync log token (encodedEvents,maybeIndexEvents) compactionPredicate
        match syncRes with
        | GatewaySyncResult.Conflict (token',events) -> return Storage.SyncResult.Conflict  (async { return foldI fold initial token' events })
        | GatewaySyncResult.ConflictUnknown token' -> return Storage.SyncResult.Conflict (__.LoadFromToken fold initial state token' log)
        | GatewaySyncResult.Written token' -> return Storage.SyncResult.Written (token', fold state (Seq.ofList events)) }

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
        pkd.Paths.Add(sprintf "/%s" Store.Event.PartitionKeyField)
        let colld = DocumentCollection(Id = collName, PartitionKey = pkd)

        colld.IndexingPolicy.IndexingMode <- IndexingMode.Consistent
        colld.IndexingPolicy.Automatic <- true
        // Can either do a blacklist or a whitelist
        // Given how long and variable the blacklist would be, we whitelist instead
        colld.IndexingPolicy.ExcludedPaths <- System.Collections.ObjectModel.Collection [|ExcludedPath(Path="/*")|]
        // NB its critical to index the nominated PartitionKey field defined above or there will be runtime errors
        colld.IndexingPolicy.IncludedPaths <- System.Collections.ObjectModel.Collection [| for k in Store.Event.IndexedFields -> IncludedPath(Path=sprintf "/%s/?" k) |]
        let! coll = client.CreateDocumentCollectionIfNotExistsAsync(dbUri, colld, Client.RequestOptions(OfferThroughput=Nullable ru)) |> Async.AwaitTaskCorrect
        return coll.Resource.Id }

    let createProc (client: IDocumentClient) (collectionUri: Uri) = async {
        let f = """function multidocInsert(docs, expectedVersion, etag, index) {
            var response = getContext().getResponse();
            var collection = getContext().getCollection();
            var collectionLink = collection.getSelfLink();
            if (!docs) throw new Error("docs argument is missing.");
            if(index) {
                var assignedEtag = null;
                function callback(err, doc, options) {
                    if (err) throw err;
                    assignedEtag = doc._etag
                }
                if (-1 == expectedVersion) {
                    collection.createDocument(collectionLink, index, { disableAutomaticIdGeneration : true}, callback);
                } else {
                    collection.replaceDocument(collection.getAltLink() + "/docs/" + index.id, index, { etag : etag }, callback);
                }
                // can also contain { conflicts: [{t, d}] } representing conflicting events since expectedVersion
                // null/missing signifies events have been written, with no conflict
                response.setBody({ etag: assignedEtag, conflicts: null });
            } else {
                response.setBody({ etag: null, conflicts: null });
            }
            for (var i=0; i<docs.length; i++) {
                collection.createDocument(collectionLink, docs[i]);
            }
        }"""
        let def = new StoredProcedure(Id = Write.sprocName, Body = f)
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