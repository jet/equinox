﻿namespace Equinox.Cosmos

open Equinox
open Equinox.Store
open FSharp.Control
open Microsoft.Azure.Documents
open Microsoft.Azure.Documents.Linq
open Newtonsoft.Json
open Newtonsoft.Json.Linq
open Serilog
open System

module Store =
    [<NoComparison>]
    type Position =
        {   collectionUri: Uri; streamName: string; index: int64 option }
        member __.Index : int64 = defaultArg __.index -1L
        member __.IndexRel (offset: int) : int64 = __.index |> function
            | Some index -> index+int64 offset
            | None -> failwithf "Cannot IndexRel %A" __

    type EventData = { eventType: string; data: byte[]; metadata: byte[] }

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
        | Slice of Direction * Measurement
        | Batch of Direction * slices: int * Measurement
    let prop name value (log : ILogger) = log.ForContext(name, value)
    let propEvents name (kvps : System.Collections.Generic.KeyValuePair<string,string> seq) (log : ILogger) =
        let items = seq { for kv in kvps do yield sprintf "{\"%s\": %s}" kv.Key kv.Value }
        log.ForContext(name, sprintf "[%s]" (String.concat ",\n\r" items))
    let propEventData name (events : Store.EventData[]) (log : ILogger) =
        log |> propEvents name (seq { for x in events -> Collections.Generic.KeyValuePair<_,_>(x.eventType, System.Text.Encoding.UTF8.GetString x.data)})
    let propResolvedEvents name (events : Store.Event[]) (log : ILogger) =
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
type EqxSyncResult = Written of Store.Position * requestCharge: float | Conflict of requestCharge: float

module private Write =
    let [<Literal>] sprocName = "AtomicMultiDocInsert"
    let append (client: IDocumentClient) (pos: Store.Position) (eventsData: Store.EventData seq): Async<Store.Position * float> = async {
        let sprocUri = sprintf "%O/sprocs/%s" pos.collectionUri sprocName
        let opts = Client.RequestOptions(PartitionKey=PartitionKey(pos.streamName))
        let! ct = Async.CancellationToken
        let events = eventsData |> Seq.mapi (fun i ed -> Store.Event.Create pos (i+1) ed |> JsonConvert.SerializeObject) |> Seq.toArray
        if events.Length = 0 then invalidArg "eventsData" "must be non-empty"
        let! res = client.ExecuteStoredProcedureAsync<bool>(sprocUri, opts, ct, box events) |> Async.AwaitTaskCorrect
        return { pos with index = Some (pos.IndexRel events.Length) }, res.RequestCharge }

    /// Yields `EqxSyncResult.Written`, or `EqxSyncResult.Conflict` to signify WrongExpectedVersion
    let private writeEventsAsync (log : ILogger) client pk (events : Store.EventData[]): Async<EqxSyncResult> = async {
        try
            let! wr = append client pk events
            return EqxSyncResult.Written wr
        with :? DocumentClientException as ex when ex.Message.Contains "already" -> // TODO this does not work for the SP
            log.Information(ex, "Eqx TrySync WrongExpectedVersionException writing {EventTypes}", [| for x in events -> x.eventType |])
            return EqxSyncResult.Conflict ex.RequestCharge }

    let bytes events =
        let eventDataLen ({ data = Log.BlobLen bytes; metadata = Log.BlobLen metaBytes } : Store.EventData) = bytes + metaBytes
        events |> Array.sumBy eventDataLen

    let private writeEventsLogged client (pos : Store.Position) (events : Store.EventData[]) (log : ILogger): Async<EqxSyncResult> = async {
        let log = if (not << log.IsEnabled) Events.LogEventLevel.Debug then log else log |> Log.propEventData "Json" events
        let bytes, count = bytes events, events.Length
        let log = log |> Log.prop "bytes" bytes
        let writeLog = log |> Log.prop "stream" pos.streamName |> Log.prop "expectedVersion" pos.Index |> Log.prop "count" count
        let! t, result = writeEventsAsync writeLog client pos events |> Stopwatch.Time
        let conflict, (ru: float), resultLog =
            let mkMetric ru : Log.Measurement = { stream = pos.streamName; interval = t; bytes = bytes; count = count; ru = ru }
            match result with
            | EqxSyncResult.Conflict ru -> true, ru, log |> Log.event (Log.WriteConflict (mkMetric ru))
            | EqxSyncResult.Written (x, ru) -> false, ru, log |> Log.event (Log.WriteSuccess (mkMetric ru)) |> Log.prop "nextExpectedVersion" x
        resultLog.Information("Eqx{action:l} count={count} conflict={conflict}, rus={ru}", "Write", events.Length, conflict, ru)
        return result }

    let writeEvents (log : ILogger) retryPolicy client pk (events : Store.EventData[]): Async<EqxSyncResult> =
        let call = writeEventsLogged client pk events
        Log.withLoggedRetries retryPolicy "writeAttempt" call log

module private Read =
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

    let private loggedQueryExecution (pos:Store.Position) direction (query: IDocumentQuery<Store.Event>) (log: ILogger): Async<Store.Event[] * float> = async {
        let! t, (res : Client.FeedResponse<Store.Event>) = query.ExecuteNextAsync<Store.Event>() |> Async.AwaitTaskCorrect |> Stopwatch.Time
        let slice, ru = Array.ofSeq res, res.RequestCharge
        let bytes, count = slice |> Array.sumBy (|EventLen|), slice.Length
        let reqMetric : Log.Measurement = { stream = pos.streamName; interval = t; bytes = bytes; count = count; ru = ru }
        let evt = Log.Slice (direction, reqMetric)
        let log = if (not << log.IsEnabled) Events.LogEventLevel.Debug then log else log |> Log.propResolvedEvents "Json" slice
        let index = match slice |> Array.tryHead with Some head -> head.id | None -> null
        (log |> Log.prop "startIndex" pos.Index |> Log.prop "bytes" bytes |> Log.event evt)
            .Information("Eqx{action:l} count={count} index={index} rus={ru}", "Read", count, index, ru)
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

    let bytes events = events |> Array.sumBy (|EventLen|)

    let logBatchRead direction streamName interval events batchSize version (ru: float) (log : ILogger) =
        let bytes, count = bytes events, events.Length
        let reqMetric : Log.Measurement = { stream = streamName; interval = interval; bytes = bytes; count = count; ru = ru }
        let batches = (events.Length - 1)/batchSize + 1
        let action = match direction with Direction.Forward -> "LoadF" | Direction.Backward -> "LoadB"
        let evt = Log.Event.Batch (direction, batches, reqMetric)
        (log |> Log.prop "bytes" bytes |> Log.event evt).Information(
            "Eqx{action:l} stream={stream} count={count}/{batches} index={index} rus={ru}",
            action, streamName, count, batches, version, ru)

    let private lastEventIndex (xs:Store.Event seq) : int64 =
        match xs |> Seq.tryLast with
        | None -> -1L
        | Some last -> int64 last.id

    let loadForwardsFrom (log : ILogger) retryPolicy client batchSize maxPermittedBatchReads (pos,_strongConsistency): Async<Store.Position * Store.Event[]> = async {
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
                        | None -> log.Information("EqxStop stream={stream} at={eventNumber}", pos.streamName, x.id)
                        | Some batch ->
                            let used, residual = batch |> partitionPayloadFrom x.id
                            log.Information("EqxStop stream={stream} at={eventNumber} used={used} residual={residual}", pos.streamName, x.id, used, residual)
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
    let private eventDataOfEncodedEvent (x : UnionCodec.EncodedUnion<byte[]>) : Store.EventData =
        { eventType = x.caseName; data = x.payload; metadata = null }
    let encodeEvents (codec : UnionCodec.IUnionEncoder<'event, byte[]>) (xs : 'event seq) : Store.EventData[] =
        xs |> Seq.map (codec.Encode >> eventDataOfEncodedEvent) |> Seq.toArray
    let decodeKnownEvents (codec : UnionCodec.IUnionEncoder<'event, byte[]>) (xs : Store.Event[]) : 'event seq =
        xs |> Seq.map encodedEventOfStoredEvent |> Seq.choose codec.TryDecode

type [<NoComparison>]Token = { pos: Store.Position; compactionEventNumber: int64 option }

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
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

type EqxConnection(client: IDocumentClient, ?readRetryPolicy, ?writeRetryPolicy) =
    member __.Client = client
    member __.ReadRetryPolicy = readRetryPolicy
    member __.WriteRetryPolicy = writeRetryPolicy
    member __.Close = (client :?> Client.DocumentClient).Dispose()

type EqxBatchingPolicy(getMaxBatchSize : unit -> int, ?batchCountLimit) =
    new (maxBatchSize) = EqxBatchingPolicy(fun () -> maxBatchSize)
    member __.BatchSize = getMaxBatchSize()
    member __.MaxBatches = batchCountLimit

[<RequireQualifiedAccess; NoComparison; NoEquality>]
type GatewaySyncResult = Written of Storage.StreamToken | Conflict

type EqxGateway(conn : EqxConnection, batching : EqxBatchingPolicy) =
    let isResolvedEventEventType predicate (x:Store.Event) = predicate x.t
    let tryIsResolvedEventEventType predicateOption = predicateOption |> Option.map isResolvedEventEventType
    let (|Pos|) (token: Storage.StreamToken) : Store.Position = (unbox<Token> token.value).pos
    member __.LoadBatched log isCompactionEventType (pos : Store.Position): Async<Storage.StreamToken * Store.Event[]> = async {
        let! pos, events = Read.loadForwardsFrom log conn.ReadRetryPolicy conn.Client batching.BatchSize batching.MaxBatches (pos,false)
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
    member __.LoadFromToken log (Pos pos as token) isCompactionEventType synchronized: Async<Storage.StreamToken * Store.Event[]> = async {
        let! pos, events = Read.loadForwardsFrom log conn.ReadRetryPolicy conn.Client batching.BatchSize batching.MaxBatches (pos,synchronized)
        match tryIsResolvedEventEventType isCompactionEventType with
        | None -> return Token.ofNonCompacting pos, events
        | Some isCompactionEvent ->
            match events |> Array.tryFindBack isCompactionEvent with
            | None -> return Token.ofPreviousTokenAndEventsLength token events.Length batching.BatchSize pos, events
            | Some resolvedEvent -> return Token.ofCompactionResolvedEventAndVersion resolvedEvent batching.BatchSize pos, events }
    member __.TrySync log (Pos pos as token) (encodedEvents: Store.EventData[]) isCompactionEventType: Async<GatewaySyncResult> = async {
        let! wr = Write.writeEvents log conn.WriteRetryPolicy conn.Client pos encodedEvents
        match wr with
        | EqxSyncResult.Conflict _ -> return GatewaySyncResult.Conflict
        | EqxSyncResult.Written (wr, _) ->

        let version' = wr
        let token =
            match isCompactionEventType with
            | None -> Token.ofNonCompacting version'
            | Some isCompactionEvent ->
                let isEventDataEventType predicate (x:Store.EventData) = predicate x.eventType
                match encodedEvents |> Array.tryFindIndexBack (isEventDataEventType isCompactionEvent) with
                | None -> Token.ofPreviousTokenAndEventsLength token encodedEvents.Length batching.BatchSize version'
                | Some compactionEventIndex ->
                    Token.ofPreviousStreamVersionAndCompactionEventDataIndex pos.Index compactionEventIndex encodedEvents.Length batching.BatchSize version'
        return GatewaySyncResult.Written token }

type private Collection(gateway : EqxGateway, databaseId, collectionId) =
    member __.Gateway = gateway
    member __.CollectionUri = Client.UriFactory.CreateDocumentCollectionUri(databaseId, collectionId)

type private Category<'event, 'state>(coll : Collection, codec : UnionCodec.IUnionEncoder<'event, byte[]>, ?compactionStrategy) =
    let (|Pos|) streamName : Store.Position = { collectionUri = coll.CollectionUri; streamName = streamName; index = None }
    let loadAlgorithm load (Pos pos) initial log =
        let batched = load initial (coll.Gateway.LoadBatched log None pos)
        let compacted predicate = load initial (coll.Gateway.LoadBackwardsStoppingAtCompactionEvent log predicate pos)
        match compactionStrategy with
        | Some predicate -> compacted predicate
        | None -> batched
    let load (fold: 'state -> 'event seq -> 'state) initial loadF = async {
        let! token, events = loadF
        return token, fold initial (UnionEncoderAdapters.decodeKnownEvents codec events) }
    member __.Load (fold: 'state -> 'event seq -> 'state) (initial: 'state) streamName (log : ILogger) : Async<Storage.StreamToken * 'state> =
        loadAlgorithm (load fold) streamName initial log
    member __.LoadFromToken (fold: 'state -> 'event seq -> 'state) (state: 'state) token (log : ILogger) : Async<Storage.StreamToken * 'state> =
        (load fold) state (coll.Gateway.LoadFromToken log token compactionStrategy false)
    member __.TrySync (fold: 'state -> 'event seq -> 'state) (log : ILogger) (token, state) (events : 'event list) : Async<Storage.SyncResult<'state>> = async {
        let encodedEvents : Store.EventData[] = UnionEncoderAdapters.encodeEvents codec (Seq.ofList events)
        let! syncRes = coll.Gateway.TrySync log token encodedEvents compactionStrategy
        match syncRes with
        | GatewaySyncResult.Conflict ->         return Storage.SyncResult.Conflict  (load fold state (coll.Gateway.LoadFromToken log token compactionStrategy true))
        | GatewaySyncResult.Written token' ->   return Storage.SyncResult.Written   (token', fold state (Seq.ofList events)) }

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
            member __.TrySync streamName (log : ILogger) (token, state) (events : 'event list) : Async<Storage.SyncResult<'state>> = async {
                let! syncRes = inner.TrySync streamName log (token, state) events
                match syncRes with
                | Storage.SyncResult.Conflict resync ->             return Storage.SyncResult.Conflict (interceptAsync resync streamName)
                | Storage.SyncResult.Written (token', state') ->    return Storage.SyncResult.Written (token', state') }

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
        let cached token state = category.LoadFromToken fold state token log
        match readCache with
        | None -> batched
        | Some (cache : Caching.Cache, prefix : string) ->
            match cache.TryGet(prefix + streamName) with
            | None -> batched
            | Some (token, state) -> cached token state
    interface ICategory<'event, 'state> with
        member __.Load (streamName : string) (log : ILogger) : Async<Storage.StreamToken * 'state> =
            loadAlgorithm streamName initial log
        member __.TrySync _streamName(* TODO remove from main interface *) (log : ILogger) (token, state) (events : 'event list) : Async<Storage.SyncResult<'state>> = async {
            let! syncRes = category.TrySync fold log (token, state) events
            match syncRes with
            | Storage.SyncResult.Conflict resync ->         return Storage.SyncResult.Conflict resync
            | Storage.SyncResult.Written (token',state') -> return Storage.SyncResult.Written (token',state') }

[<NoComparison; NoEquality; RequireQualifiedAccess>]
type CompactionStrategy =
    | EventType of string
    | Predicate of (string -> bool)

[<NoComparison; NoEquality; RequireQualifiedAccess>]
type CachingStrategy =
    | SlidingWindow of Caching.Cache * window: TimeSpan
    /// Prefix is used to distinguish multiple folds per stream
    | SlidingWindowPrefixed of Caching.Cache * window: TimeSpan * prefix: string

type EqxStreamBuilder<'event, 'state>(gateway : EqxGateway, codec, fold, initial, ?compaction, ?caching) =
    member __.Create (databaseId, collectionId, streamName) : Equinox.IStream<'event, 'state> =
        let compactionPredicateOption =
            match compaction with
            | None -> None
            | Some (CompactionStrategy.Predicate predicate) -> Some predicate
            | Some (CompactionStrategy.EventType eventType) -> Some (fun x -> x = eventType)
        let category = Category<'event, 'state>(Collection(gateway, databaseId, collectionId), codec, ?compactionStrategy = compactionPredicateOption)

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
        let f ="""function multidocInsert(docs) {
            var response = getContext().getResponse();
            var collection = getContext().getCollection();
            var collectionLink = collection.getSelfLink();
            if (!docs) throw new Error("docs argument is missing.");
            for (var i=0; i<docs.length; i++) {
                collection.createDocument(collectionLink, docs[i]);
            }
            response.setBody(true);
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
            log.Information("Connecting to Cosmos with Connection Name {connectionName}", sanitizedName)
            do! client.OpenAsync() |> Async.AwaitTaskCorrect
            return client :> IDocumentClient }

        match discovery with Discovery.UriAndKey(databaseUri=uri; key=key) -> connect (uri,key)

    /// Yields a DocDbConnection configured per the specified strategy
    member __.Connect(name, discovery : Discovery) : Async<EqxConnection> = async {
        let! conn = connect(name, discovery)
        return EqxConnection(conn, ?readRetryPolicy=readRetryPolicy, ?writeRetryPolicy=writeRetryPolicy) }