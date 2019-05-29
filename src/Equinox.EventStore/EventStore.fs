namespace Equinox.EventStore

open EventStore.ClientAPI
open Equinox
open Equinox.Store
open FSharp.Control
open Serilog // NB must shadow EventStore.ClientAPI.ILogger
open System

[<RequireQualifiedAccess>]
type Direction = Forward | Backward with
    override this.ToString() = match this with Forward -> "Forward" | Backward -> "Backward"

module Log =
    [<NoEquality; NoComparison>]
    type Measurement = { stream: string; interval: StopwatchInterval; bytes: int; count: int }
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
    let propEventData name (events : EventData[]) (log : ILogger) =
        log |> propEvents name (seq {
            for x in events do
                if x.IsJson then
                    yield System.Collections.Generic.KeyValuePair<_,_>(x.Type, System.Text.Encoding.UTF8.GetString x.Data) })
    let propResolvedEvents name (events : ResolvedEvent[]) (log : ILogger) =
        log |> propEvents name (seq {
            for x in events do
                let e = x.Event
                if e.IsJson then
                    yield System.Collections.Generic.KeyValuePair<_,_>(e.EventType, System.Text.Encoding.UTF8.GetString e.Data) })

    open Serilog.Events
    /// Attach a property to the log context to hold the metrics
    // Sidestep Log.ForContext converting to a string; see https://github.com/serilog/serilog/issues/1124
    let event (value : Event) (log : ILogger) =
        let enrich (e : LogEvent) = e.AddPropertyIfAbsent(LogEventProperty("esEvt", ScalarValue(value)))
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
type EsSyncResult = Written of EventStore.ClientAPI.WriteResult | Conflict

module private Write =
    /// Yields `EsSyncResult.Written` or `EsSyncResult.Conflict` to signify WrongExpectedVersion
    let private writeEventsAsync (log : ILogger) (conn : IEventStoreConnection) (streamName : string) (version : int64) (events : EventData[])
        : Async<EsSyncResult> = async {
        try
            let! wr = conn.AppendToStreamAsync(streamName, version, events) |> Async.AwaitTaskCorrect
            return EsSyncResult.Written wr
        with :? EventStore.ClientAPI.Exceptions.WrongExpectedVersionException as ex ->
            log.Information(ex, "Ges TrySync WrongExpectedVersionException writing {EventTypes}", [| for x in events -> x.Type |])
            return EsSyncResult.Conflict }
    let eventDataBytes events =
        let eventDataLen (x : EventData) = match x.Data, x.Metadata with Log.BlobLen bytes, Log.BlobLen metaBytes -> bytes + metaBytes
        events |> Array.sumBy eventDataLen
    let private writeEventsLogged (conn : IEventStoreConnection) (streamName : string) (version : int64) (events : EventData[]) (log : ILogger)
        : Async<EsSyncResult> = async {
        let log = if (not << log.IsEnabled) Events.LogEventLevel.Debug then log else log |> Log.propEventData "Json" events
        let bytes, count = eventDataBytes events, events.Length
        let log = log |> Log.prop "bytes" bytes
        let writeLog = log |> Log.prop "stream" streamName |> Log.prop "expectedVersion" version |> Log.prop "count" count
        let! t, result = writeEventsAsync writeLog conn streamName version events |> Stopwatch.Time
        let reqMetric : Log.Measurement = { stream = streamName; interval = t; bytes = bytes; count = count}
        let resultLog, evt =
            match result, reqMetric with
            | EsSyncResult.Conflict, m -> log, Log.WriteConflict m
            | EsSyncResult.Written x, m ->
                log |> Log.prop "nextExpectedVersion" x.NextExpectedVersion |> Log.prop "logPosition" x.LogPosition,
                Log.WriteSuccess m
        (resultLog |> Log.event evt).Information("Ges{action:l} count={count} conflict={conflict}",
            "Write", events.Length, match evt with Log.WriteConflict _ -> true | _ -> false)
        return result }
    let writeEvents (log : ILogger) retryPolicy (conn : IEventStoreConnection) (streamName : string) (version : int64) (events : EventData[])
        : Async<EsSyncResult> =
        let call = writeEventsLogged conn streamName version events
        Log.withLoggedRetries retryPolicy "writeAttempt" call log

module private Read =
    let private readSliceAsync (conn : IEventStoreConnection) (streamName : string) (direction : Direction) (batchSize : int) (startPos : int64)
        : Async<StreamEventsSlice> = async {
        let call =
            match direction with
            | Direction.Forward ->  conn.ReadStreamEventsForwardAsync(streamName, startPos, batchSize, resolveLinkTos = false)
            | Direction.Backward -> conn.ReadStreamEventsBackwardAsync(streamName, startPos, batchSize, resolveLinkTos = false)
        return! call |> Async.AwaitTaskCorrect }
    let (|ResolvedEventLen|) (x : ResolvedEvent) = match x.Event.Data, x.Event.Metadata with Log.BlobLen bytes, Log.BlobLen metaBytes -> bytes + metaBytes
    let private loggedReadSlice conn streamName direction batchSize startPos (log : ILogger) : Async<StreamEventsSlice> = async {
        let! t, slice = readSliceAsync conn streamName direction batchSize startPos |> Stopwatch.Time
        let bytes, count = slice.Events |> Array.sumBy (|ResolvedEventLen|), slice.Events.Length
        let reqMetric : Log.Measurement ={ stream = streamName; interval = t; bytes = bytes; count = count}
        let evt = Log.Slice (direction, reqMetric)
        let log = if (not << log.IsEnabled) Events.LogEventLevel.Debug then log else log |> Log.propResolvedEvents "Json" slice.Events
        (log |> Log.prop "startPos" startPos |> Log.prop "bytes" bytes |> Log.event evt).Information("Ges{action:l} count={count} version={version}",
            "Read", count, slice.LastEventNumber)
        return slice }
    let private readBatches (log : ILogger) (readSlice : int64 -> ILogger -> Async<StreamEventsSlice>)
            (maxPermittedBatchReads : int option) (startPosition : int64)
        : AsyncSeq<int64 option * ResolvedEvent[]> =
        let rec loop batchCount pos : AsyncSeq<int64 option * ResolvedEvent[]> = asyncSeq {
            match maxPermittedBatchReads with
            | Some mpbr when batchCount >= mpbr -> log.Information "batch Limit exceeded"; invalidOp "batch Limit exceeded"
            | _ -> ()

            let batchLog = log |> Log.prop "batchIndex" batchCount
            let! slice = readSlice pos batchLog
            match slice.Status with
            | SliceReadStatus.StreamDeleted -> raise <| EventStore.ClientAPI.Exceptions.StreamDeletedException(slice.Stream)
            | SliceReadStatus.StreamNotFound -> yield Some (int64 ExpectedVersion.NoStream), Array.empty
            | SliceReadStatus.Success ->
                let version = if batchCount = 0 then Some slice.LastEventNumber else None
                yield version, slice.Events
                if not slice.IsEndOfStream then
                    yield! loop (batchCount + 1) slice.NextEventNumber
            | x -> raise <| System.ArgumentOutOfRangeException("SliceReadStatus", x, "Unknown result value") }
        loop 0 startPosition
    let resolvedEventBytes events = events |> Array.sumBy (|ResolvedEventLen|)
    let logBatchRead direction streamName t events batchSize version (log : ILogger) =
        let bytes, count = resolvedEventBytes events, events.Length
        let reqMetric : Log.Measurement = { stream = streamName; interval = t; bytes = bytes; count = count}
        let batches = (events.Length - 1)/batchSize + 1
        let action = match direction with Direction.Forward -> "LoadF" | Direction.Backward -> "LoadB"
        let evt = Log.Event.Batch (direction, batches, reqMetric)
        (log |> Log.prop "bytes" bytes |> Log.event evt).Information(
            "Ges{action:l} stream={stream} count={count}/{batches} version={version}",
            action, streamName, count, batches, version)
    let loadForwardsFrom (log : ILogger) retryPolicy conn batchSize maxPermittedBatchReads streamName startPosition
        : Async<int64 * ResolvedEvent[]> = async {
        let mergeBatches (batches: AsyncSeq<int64 option * ResolvedEvent[]>) = async {
            let mutable versionFromStream = None
            let! (events : ResolvedEvent[]) =
                batches
                |> AsyncSeq.map (function None, events -> events | (Some _) as reportedVersion, events -> versionFromStream <- reportedVersion; events)
                |> AsyncSeq.concatSeq
                |> AsyncSeq.toArrayAsync
            let version = match versionFromStream with Some version -> version | None -> invalidOp "no version encountered in event batch stream"
            return version, events }
        let call pos = loggedReadSlice conn streamName Direction.Forward batchSize pos
        let retryingLoggingReadSlice pos = Log.withLoggedRetries retryPolicy "readAttempt" (call pos)
        let direction = Direction.Forward
        let log = log |> Log.prop "batchSize" batchSize |> Log.prop "direction" direction |> Log.prop "stream" streamName
        let batches : AsyncSeq<int64 option * ResolvedEvent[]> = readBatches log retryingLoggingReadSlice maxPermittedBatchReads startPosition
        let! t, (version, events) = mergeBatches batches |> Stopwatch.Time
        log |> logBatchRead direction streamName t events batchSize version
        return version, events }
    let partitionPayloadFrom firstUsedEventNumber : ResolvedEvent[] -> int * int =
        let acc (tu,tr) ((ResolvedEventLen bytes) as y) = if y.Event.EventNumber < firstUsedEventNumber then tu, tr + bytes else tu + bytes, tr
        Array.fold acc (0,0)
    let loadBackwardsUntilCompactionOrStart (log : ILogger) retryPolicy conn batchSize maxPermittedBatchReads streamName (tryDecode,isOrigin)
        : Async<int64 * (ResolvedEvent * 'event option)[]> = async {
        let mergeFromCompactionPointOrStartFromBackwardsStream (log : ILogger) (batchesBackward : AsyncSeq<int64 option * ResolvedEvent[]>)
            : Async<int64 * (ResolvedEvent*'event option)[]> = async {
            let versionFromStream, lastBatch = ref None, ref None
            let! tempBackward =
                batchesBackward
                |> AsyncSeq.map (fun batch ->
                    match batch with
                    | None, events -> lastBatch := Some events; events
                    | (Some _) as reportedVersion, events -> versionFromStream := reportedVersion; lastBatch := Some events; events
                    |> Array.map (fun e -> e, tryDecode e))
                |> AsyncSeq.concatSeq
                |> AsyncSeq.takeWhileInclusive (function
                    | x, Some e when isOrigin e ->
                        match !lastBatch with
                        | None -> log.Information("GesStop stream={stream} at={eventNumber}", streamName, x.Event.EventNumber)
                        | Some batch ->
                            let used, residual = batch |> partitionPayloadFrom x.Event.EventNumber
                            log.Information("GesStop stream={stream} at={eventNumber} used={used} residual={residual}", streamName, x.Event.EventNumber, used, residual)
                        false
                    | _ -> true) // continue the search
                |> AsyncSeq.toArrayAsync
            let eventsForward = Array.Reverse(tempBackward); tempBackward // sic - relatively cheap, in-place reverse of something we own
            let version = match !versionFromStream with Some version -> version | None -> invalidOp "no version encountered in event batch stream"
            return version, eventsForward }
        let call pos = loggedReadSlice conn streamName Direction.Backward batchSize pos
        let retryingLoggingReadSlice pos = Log.withLoggedRetries retryPolicy "readAttempt" (call pos)
        let log = log |> Log.prop "batchSize" batchSize |> Log.prop "stream" streamName
        let startPosition = int64 StreamPosition.End
        let direction = Direction.Backward
        let readlog = log |> Log.prop "direction" direction
        let batchesBackward : AsyncSeq<int64 option * ResolvedEvent[]> = readBatches readlog retryingLoggingReadSlice maxPermittedBatchReads startPosition
        let! t, (version, events) = mergeFromCompactionPointOrStartFromBackwardsStream log batchesBackward |> Stopwatch.Time
        log |> logBatchRead direction streamName t (Array.map fst events) batchSize version
        return version, events }

module UnionEncoderAdapters =
    let encodedEventOfResolvedEvent (x : ResolvedEvent) : Equinox.Codec.IEvent<byte[]> =
        { new Equinox.Codec.IEvent<_> with
            member __.EventType = x.Event.EventType
            member __.Data = x.Event.Data
            member __.Meta = x.Event.Metadata 
            // Inspecting server code shows both Created and CreatedEpoch are set; taking this as it's less ambiguous than DateTime in the general case
            member __.Timestamp = DateTimeOffset.FromUnixTimeMilliseconds(x.Event.CreatedEpoch) }
    let private eventDataOfEncodedEvent (x : Codec.IEvent<byte[]>) =
        EventData(Guid.NewGuid(), x.EventType, (*isJson*) true, x.Data, x.Meta)
    let encodeEvents (codec : Codec.IUnionEncoder<'event,byte[]>) (xs : 'event seq) : EventData[] =
        xs |> Seq.map (codec.Encode >> eventDataOfEncodedEvent) |> Seq.toArray
    let decodeKnownEvents (codec : Codec.IUnionEncoder<'event, byte[]>) (xs : ResolvedEvent[]) : 'event seq =
        xs |> Seq.map encodedEventOfResolvedEvent |> Seq.choose codec.TryDecode

type Stream = { name: string }
type Position = { streamVersion: int64; compactionEventNumber: int64 option; batchCapacityLimit: int option }
type Token = { stream: Stream; pos: Position }

module Token =
    let private create compactionEventNumber batchCapacityLimit streamName streamVersion : Store.StreamToken =
        { value = box {
            stream = { name = streamName}
            pos = { streamVersion = streamVersion; compactionEventNumber = compactionEventNumber; batchCapacityLimit = batchCapacityLimit } } }
    /// No batching / compaction; we only need to retain the StreamVersion
    let ofNonCompacting streamName streamVersion : Store.StreamToken =
        create None None streamName streamVersion
    // headroom before compaction is necessary given the stated knowledge of the last (if known) `compactionEventNumberOption`
    let private batchCapacityLimit compactedEventNumberOption unstoredEventsPending (batchSize : int) (streamVersion : int64) : int =
        match compactedEventNumberOption with
        | Some (compactionEventNumber : int64) -> (batchSize - unstoredEventsPending) - int (streamVersion - compactionEventNumber + 1L) |> max 0
        | None -> (batchSize - unstoredEventsPending) - (int streamVersion + 1) - 1 |> max 0
    let (*private*) ofCompactionEventNumber compactedEventNumberOption unstoredEventsPending batchSize streamName streamVersion : Store.StreamToken =
        let batchCapacityLimit = batchCapacityLimit compactedEventNumberOption unstoredEventsPending batchSize streamVersion
        create compactedEventNumberOption (Some batchCapacityLimit) streamName streamVersion
    /// Assume we have not seen any compaction events; use the batchSize and version to infer headroom
    let ofUncompactedVersion batchSize streamName streamVersion : Store.StreamToken =
        ofCompactionEventNumber None 0 batchSize streamName streamVersion
    let (|Unpack|) (x : Store.StreamToken) : Token = unbox<Token> x.value
    /// Use previousToken plus the data we are adding and the position we are adding it to infer a headroom
    let ofPreviousTokenAndEventsLength (Unpack previousToken) eventsLength batchSize streamVersion : Store.StreamToken =
        let compactedEventNumber = previousToken.pos.compactionEventNumber
        ofCompactionEventNumber compactedEventNumber eventsLength batchSize previousToken.stream.name streamVersion
    /// Use an event just read from the stream to infer headroom
    let ofCompactionResolvedEventAndVersion (compactionEvent: ResolvedEvent) batchSize streamName streamVersion : Store.StreamToken =
        ofCompactionEventNumber (Some compactionEvent.Event.EventNumber) 0 batchSize streamName streamVersion
    /// Use an event we are about to write to the stream to infer headroom
    let ofPreviousStreamVersionAndCompactionEventDataIndex (Unpack token) compactionEventDataIndex eventsLength batchSize streamVersion' : Store.StreamToken =
        ofCompactionEventNumber (Some (token.pos.streamVersion + 1L + int64 compactionEventDataIndex)) eventsLength batchSize token.stream.name streamVersion'
    let (|StreamPos|) (Unpack token) : Stream * Position = token.stream, token.pos
    let supersedes (Unpack current) (Unpack x) =
        let currentVersion, newVersion = current.pos.streamVersion, x.pos.streamVersion
        newVersion > currentVersion

type GesConnection(readConnection, [<O; D(null)>]?writeConnection, [<O; D(null)>]?readRetryPolicy, [<O; D(null)>]?writeRetryPolicy) =
    member __.ReadConnection = readConnection
    member __.ReadRetryPolicy = readRetryPolicy
    member __.WriteConnection = defaultArg writeConnection readConnection
    member __.WriteRetryPolicy = writeRetryPolicy

type GesBatchingPolicy(getMaxBatchSize : unit -> int, [<O; D(null)>]?batchCountLimit) =
    new (maxBatchSize) = GesBatchingPolicy(fun () -> maxBatchSize)
    member __.BatchSize = getMaxBatchSize()
    member __.MaxBatches = batchCountLimit

[<RequireQualifiedAccess; NoComparison; NoEquality>]
type GatewaySyncResult = Written of Store.StreamToken | Conflict

type GesGateway(conn : GesConnection, batching : GesBatchingPolicy) =
    let isResolvedEventEventType (tryDecode,predicate) (x:ResolvedEvent) = predicate (tryDecode (x.Event.Data))
    let tryIsResolvedEventEventType predicateOption = predicateOption |> Option.map isResolvedEventEventType
    member __.LoadEmpty streamName = Token.ofUncompactedVersion batching.BatchSize streamName -1L
    member __.LoadBatched streamName log (tryDecode,isCompactionEventType): Async<Store.StreamToken * 'event[]> = async {
        let! version, events = Read.loadForwardsFrom log conn.ReadRetryPolicy conn.ReadConnection batching.BatchSize batching.MaxBatches streamName 0L
        match tryIsResolvedEventEventType isCompactionEventType with
        | None -> return Token.ofNonCompacting streamName version, Array.choose tryDecode events
        | Some isCompactionEvent ->
            match events |> Array.tryFindBack isCompactionEvent with
            | None -> return Token.ofUncompactedVersion batching.BatchSize streamName version, Array.choose tryDecode events
            | Some resolvedEvent -> return Token.ofCompactionResolvedEventAndVersion resolvedEvent batching.BatchSize streamName version, Array.choose tryDecode events }
    member __.LoadBackwardsStoppingAtCompactionEvent streamName log (tryDecode,isOrigin): Async<Store.StreamToken * 'event []> = async {
        let! version, events =
            Read.loadBackwardsUntilCompactionOrStart log conn.ReadRetryPolicy conn.ReadConnection batching.BatchSize batching.MaxBatches streamName (tryDecode,isOrigin)
        match Array.tryHead events |> Option.filter (function _, Some e -> isOrigin e | _ -> false) with
        | None -> return Token.ofUncompactedVersion batching.BatchSize streamName version, Array.choose snd events
        | Some (resolvedEvent,_) -> return Token.ofCompactionResolvedEventAndVersion resolvedEvent batching.BatchSize streamName version, Array.choose snd events }
    member __.LoadFromToken useWriteConn streamName log (Token.Unpack token as streamToken) (tryDecode,isCompactionEventType)
        : Async<Store.StreamToken * 'event[]> = async {
        let streamPosition = token.pos.streamVersion + 1L
        let connToUse = if useWriteConn then conn.WriteConnection else conn.ReadConnection
        let! version, events = Read.loadForwardsFrom log conn.ReadRetryPolicy connToUse batching.BatchSize batching.MaxBatches streamName streamPosition
        match isCompactionEventType with
        | None -> return Token.ofNonCompacting streamName version, Array.choose tryDecode events
        | Some isCompactionEvent ->
            match events |> Array.tryFindBack (fun re -> match tryDecode re with Some e -> isCompactionEvent e | _ -> false) with
            | None -> return Token.ofPreviousTokenAndEventsLength streamToken events.Length batching.BatchSize version, Array.choose tryDecode events
            | Some resolvedEvent -> return Token.ofCompactionResolvedEventAndVersion resolvedEvent batching.BatchSize streamName version, Array.choose tryDecode events }
    member __.TrySync log (Token.Unpack token as streamToken) (events, encodedEvents: EventData array) (isCompactionEventType) : Async<GatewaySyncResult> = async {
        let streamVersion = token.pos.streamVersion
        let! wr = Write.writeEvents log conn.WriteRetryPolicy conn.WriteConnection token.stream.name streamVersion encodedEvents
        match wr with
        | EsSyncResult.Conflict -> return GatewaySyncResult.Conflict
        | EsSyncResult.Written wr ->

        let version' = wr.NextExpectedVersion
        let token =
            match isCompactionEventType with
            | None -> Token.ofNonCompacting token.stream.name version'
            | Some isCompactionEvent ->
                match events |> Array.ofList |> Array.tryFindIndexBack isCompactionEvent with
                | None -> Token.ofPreviousTokenAndEventsLength streamToken encodedEvents.Length batching.BatchSize version'
                | Some compactionEventIndex ->
                    Token.ofPreviousStreamVersionAndCompactionEventDataIndex streamToken compactionEventIndex encodedEvents.Length batching.BatchSize version'
        return GatewaySyncResult.Written token }

[<NoComparison; NoEquality; RequireQualifiedAccess>]
type AccessStrategy<'event,'state> =
    | EventsAreState
    | RollingSnapshots of isValid: ('event -> bool) * compact: ('state -> 'event)

type private CompactionContext(eventsLen : int, capacityBeforeCompaction : int) =
 /// Determines whether writing a Compaction event is warranted (based on the existing state and the current `Accumulated` changes)
    member __.IsCompactionDue = eventsLen > capacityBeforeCompaction

type private Category<'event, 'state>(gateway : GesGateway, codec : Codec.IUnionEncoder<'event,byte[]>, ?access : AccessStrategy<'event,'state>) =
    let tryDecode (e: ResolvedEvent) = e |> UnionEncoderAdapters.encodedEventOfResolvedEvent |> codec.TryDecode
    let compactionPredicate =
        match access with
        | None -> None
        | Some AccessStrategy.EventsAreState -> Some (fun _ -> true)
        | Some (AccessStrategy.RollingSnapshots (isValid,_)) -> Some isValid
    let isOrigin =
        match access with
        | None | Some AccessStrategy.EventsAreState -> fun _ -> true
        | Some (AccessStrategy.RollingSnapshots (isValid,_)) -> isValid
    let loadAlgorithm load streamName initial log =
        let batched = load initial (gateway.LoadBatched streamName log (tryDecode,None))
        let compacted = load initial (gateway.LoadBackwardsStoppingAtCompactionEvent streamName log (tryDecode,isOrigin))
        match access with
        | None -> batched
        | Some AccessStrategy.EventsAreState
        | Some (AccessStrategy.RollingSnapshots _) -> compacted
    let load (fold: 'state -> 'event seq -> 'state) initial f = async {
        let! token, events = f
        return token, fold initial events }
    member __.Load (fold: 'state -> 'event seq -> 'state) (initial: 'state) (streamName : string) (log : ILogger) : Async<Store.StreamToken * 'state> =
        loadAlgorithm (load fold) streamName initial log
    member __.LoadFromToken (fold: 'state -> 'event seq -> 'state) (state: 'state) (streamName : string) token (log : ILogger) : Async<Store.StreamToken * 'state> =
        (load fold) state (gateway.LoadFromToken false streamName log token (tryDecode,compactionPredicate))
    member __.TrySync (fold: 'state -> 'event seq -> 'state) (log : ILogger)
            ((Token.StreamPos (stream,pos) as streamToken), state : 'state)
            (events : 'event list) : Async<Store.SyncResult<'state>> = async {
        let events =
            match access with
            | None | Some AccessStrategy.EventsAreState -> events
            | Some (AccessStrategy.RollingSnapshots (_,compact)) ->
                let cc = CompactionContext(List.length events, pos.batchCapacityLimit.Value)
                if cc.IsCompactionDue then events @ [fold state events |> compact] else events

        let encodedEvents : EventData[] = UnionEncoderAdapters.encodeEvents codec events
        let! syncRes = gateway.TrySync log streamToken (events,encodedEvents) compactionPredicate
        match syncRes with
        | GatewaySyncResult.Conflict ->
            return Store.SyncResult.Conflict  (load fold state (gateway.LoadFromToken true stream.name log streamToken (tryDecode,compactionPredicate)))
        | GatewaySyncResult.Written token' ->
            return Store.SyncResult.Written   (token', fold state (Seq.ofList events)) }

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
    type CategoryTee<'event, 'state>(inner: ICategory<'event, 'state, string>, tee : string -> Store.StreamToken * 'state -> unit) =
        let intercept streamName tokenAndState =
            tee streamName tokenAndState
            tokenAndState
        let interceptAsync load streamName = async {
            let! tokenAndState = load
            return intercept streamName tokenAndState }
        interface ICategory<'event, 'state, string> with
            member __.Load (streamName : string) (log : ILogger) : Async<Store.StreamToken * 'state> =
                interceptAsync (inner.Load streamName log) streamName
            member __.TrySync (log : ILogger) ((Token.StreamPos (stream,_) as token), state) (events : 'event list) : Async<Store.SyncResult<'state>> = async {
                let! syncRes = inner.TrySync log (token, state) events
                match syncRes with
                | Store.SyncResult.Conflict resync ->             return Store.SyncResult.Conflict (interceptAsync resync stream.name)
                | Store.SyncResult.Written (token', state') ->    return Store.SyncResult.Written (token', state') }

    let applyCacheUpdatesWithSlidingExpiration
            (cache: Cache)
            (prefix: string)
            (slidingExpiration : TimeSpan)
            (category: ICategory<'event, 'state, string>)
            : ICategory<'event, 'state, string> =
        let policy = new CacheItemPolicy(SlidingExpiration = slidingExpiration)
        let addOrUpdateSlidingExpirationCacheEntry streamName = CacheEntry >> cache.UpdateIfNewer policy (prefix + streamName)
        CategoryTee<'event,'state>(category, addOrUpdateSlidingExpirationCacheEntry) :> _

type private Folder<'event, 'state>(category : Category<'event, 'state>, fold: 'state -> 'event seq -> 'state, initial: 'state, ?readCache) =
    let loadAlgorithm streamName initial log =
        let batched = category.Load fold initial streamName log
        let cached token state = category.LoadFromToken fold state streamName token log
        match readCache with
        | None -> batched
        | Some (cache : Caching.Cache, prefix : string) ->
            match cache.TryGet(prefix + streamName) with
            | None -> batched
            | Some (token, state) -> cached token state
    interface ICategory<'event, 'state, string> with
        member __.Load (streamName : string) (log : ILogger) : Async<Store.StreamToken * 'state> =
            loadAlgorithm streamName initial log
        member __.TrySync (log : ILogger) (token, initialState) (events : 'event list) : Async<Store.SyncResult<'state>> = async {
            let! syncRes = category.TrySync fold log (token, initialState) events
            match syncRes with
            | Store.SyncResult.Conflict resync ->         return Store.SyncResult.Conflict resync
            | Store.SyncResult.Written (token',state') -> return Store.SyncResult.Written (token',state') }

[<NoComparison; NoEquality; RequireQualifiedAccess>]
type CachingStrategy =
    | SlidingWindow of Caching.Cache * window: TimeSpan
    /// Prefix is used to segregate multiple folds per stream when they are stored in the cache
    | SlidingWindowPrefixed of Caching.Cache * window: TimeSpan * prefix: string

type GesResolver<'event,'state>
    (   gateway : GesGateway, codec, fold, initial,
        /// Caching can be overkill for EventStore esp considering the degree to which its intrinsic caching is a first class feature
        /// e.g., A key benefit is that reads of streams more than a few pages long get completed in constant time after the initial load
        [<O; D(null)>]?caching,
        [<O; D(null)>]?access) =
    do  match access with
        | Some (AccessStrategy.EventsAreState) when Option.isSome caching ->
            "Equinox.EventStore does not support (and it would make things _less_ efficient even if it did)"
            + "mixing AccessStrategy.EventsAreState with Caching at present."
            |> invalidOp
        | _ -> ()

    let inner = Category<'event, 'state>(gateway, codec, ?access = access)
    let readCacheOption =
        match caching with
        | None -> None
        | Some (CachingStrategy.SlidingWindow(cache, _)) -> Some(cache, null)
        | Some (CachingStrategy.SlidingWindowPrefixed(cache, _, prefix)) -> Some(cache, prefix)
    let folder = Folder<'event, 'state>(inner, fold, initial, ?readCache = readCacheOption)
    let category : Equinox.Store.ICategory<_,_,_> =
        match caching with
        | None -> folder :> _
        | Some (CachingStrategy.SlidingWindow(cache, window)) ->
            Caching.applyCacheUpdatesWithSlidingExpiration cache null window folder
        | Some (CachingStrategy.SlidingWindowPrefixed(cache, window, prefix)) ->
            Caching.applyCacheUpdatesWithSlidingExpiration cache prefix window folder
    let mkStreamName categoryName streamId = sprintf "%s-%s" categoryName streamId
    let resolve = Stream.create category

    member __.Resolve = function
        | Target.AggregateId (categoryName,streamId) ->
            resolve <| mkStreamName categoryName streamId
        | Target.AggregateIdEmpty (categoryName,streamId) ->
            let streamName = mkStreamName categoryName streamId
            Stream.ofMemento (gateway.LoadEmpty streamName,initial) <| resolve streamName
        | Target.DeprecatedRawName streamName ->
            resolve streamName

    /// Resolve from a Memento being used in a Continuation [based on position and state typically from Stream.CreateMemento]
    member __.FromMemento(Token.Unpack token as streamToken, state) =
        Stream.ofMemento (streamToken,state) <| resolve token.stream.name

type private SerilogAdapter(log : ILogger) =
    interface EventStore.ClientAPI.ILogger with
        member __.Debug(format: string, args: obj []) =           log.Debug(format, args)
        member __.Debug(ex: exn, format: string, args: obj []) =  log.Debug(ex, format, args)
        member __.Info(format: string, args: obj []) =            log.Information(format, args)
        member __.Info(ex: exn, format: string, args: obj []) =   log.Information(ex, format, args)
        member __.Error(format: string, args: obj []) =           log.Error(format, args)
        member __.Error(ex: exn, format: string, args: obj []) =  log.Error(ex, format, args)

[<RequireQualifiedAccess; NoComparison; NoEquality>]
type Logger =
    | SerilogVerbose of ILogger
    | SerilogNormal of ILogger
    | CustomVerbose of EventStore.ClientAPI.ILogger
    | CustomNormal of EventStore.ClientAPI.ILogger
    member log.Configure(b : ConnectionSettingsBuilder) =
        match log with
        | SerilogVerbose logger -> b.EnableVerboseLogging().UseCustomLogger(SerilogAdapter(logger))
        | SerilogNormal logger -> b.UseCustomLogger(SerilogAdapter(logger))
        | CustomVerbose logger -> b.EnableVerboseLogging().UseCustomLogger(logger)
        | CustomNormal logger -> b.UseCustomLogger(logger)

[<RequireQualifiedAccess; NoComparison>]
type Discovery =
    // Allow Uri-based connection definition (discovery://, tcp:// or
    | Uri of Uri
    /// Supply a set of pre-resolved EndPoints instead of letting Gossip resolution derive from the DNS outcome
    | GossipSeeded of seedManagerEndpoints : System.Net.IPEndPoint []
    // Standard Gossip-based discovery based on Dns query and standard manager port
    | GossipDns of clusterDns : string
    // Standard Gossip-based discovery based on Dns query (with manager port overriding default 30778)
    | GossipDnsCustomPort of clusterDns : string * managerPortOverride : int

module private Discovery =
    let buildDns (f : DnsClusterSettingsBuilder -> DnsClusterSettingsBuilder) =
        ClusterSettings.Create().DiscoverClusterViaDns().KeepDiscovering() |> f |> fun s -> s.Build()
    let buildSeeded (f : GossipSeedClusterSettingsBuilder -> GossipSeedClusterSettingsBuilder) =
        ClusterSettings.Create().DiscoverClusterViaGossipSeeds().KeepDiscovering() |> f |> fun s -> s.Build()
    let configureDns clusterDns maybeManagerPort (x : DnsClusterSettingsBuilder) =
        x.SetClusterDns(clusterDns)
        |> fun s -> match maybeManagerPort with Some port -> s.SetClusterGossipPort(port) | None -> s
    let inline configureSeeded (seedEndpoints : System.Net.IPEndPoint []) (x : GossipSeedClusterSettingsBuilder) =
        x.SetGossipSeedEndPoints(seedEndpoints)
    // converts a Discovery mode to a ClusterSettings or a Uri as appropriate
    let (|DiscoverViaUri|DiscoverViaGossip|) : Discovery -> Choice<Uri,ClusterSettings> = function
        | Discovery.Uri uri ->                          DiscoverViaUri    uri
        | Discovery.GossipSeeded seedEndpoints ->       DiscoverViaGossip (buildSeeded  (configureSeeded seedEndpoints))
        | Discovery.GossipDns clusterDns ->             DiscoverViaGossip (buildDns     (configureDns clusterDns None))
        | Discovery.GossipDnsCustomPort (dns, port) ->  DiscoverViaGossip (buildDns     (configureDns dns (Some port)))

[<RequireQualifiedAccess; NoComparison>]
type NodePreference =
    /// Track master via gossip, writes direct, reads should immediately reflect writes, resync without backoff (highest load on master, good write perf)
    | Master
    /// Take the first connection that comes along, ideally a master, but do not track master changes
    | PreferMaster
    /// Prefer slave node, writes normally need forwarding, often can't read writes, resync requires backoff (kindest to master, writes and resyncs expensive)
    | PreferSlave
    /// Take random node, writes may need forwarding, sometimes can't read writes, resync requires backoff (balanced load on master, balanced write perf)
    | Random

// see https://github.com/EventStore/EventStore/issues/1652
[<RequireQualifiedAccess; NoComparison>]
type ConnectionStrategy =
    /// Pair of master and slave connections, writes direct, often can't read writes, resync without backoff (kind to master, writes+resyncs optimal)
    | ClusterTwinPreferSlaveReads
    /// Single connection, with resync backoffs appropriate to the NodePreference
    | ClusterSingle of NodePreference

type GesConnector
    (   username, password, reqTimeout: TimeSpan, reqRetries: int,
        [<O; D(null)>]?log : Logger, [<O; D(null)>]?heartbeatTimeout: TimeSpan, [<O; D(null)>]?concurrentOperationsLimit,
        [<O; D(null)>]?readRetryPolicy, [<O; D(null)>]?writeRetryPolicy,
        /// Additional strings identifying the context of this connection; should provide enough context to disambiguate all potential connections to a cluster
        /// NB as this will enter server and client logs, it should not contain sensitive information
        [<O; D(null)>]?tags : (string*string) seq) =
    let connSettings node =
      ConnectionSettings.Create().SetDefaultUserCredentials(SystemData.UserCredentials(username, password))
        .KeepReconnecting() // ES default: .LimitReconnectionsTo(10)
        .SetQueueTimeoutTo(reqTimeout) // ES default: Zero/unlimited
        .FailOnNoServerResponse() // ES default: DoNotFailOnNoServerResponse() => wait forever; retry and/or log
        .SetOperationTimeoutTo(reqTimeout) // ES default: 7s
        .LimitRetriesForOperationTo(reqRetries) // ES default: 10
        |> fun s ->
            match node with
            | NodePreference.Master -> s.PerformOnMasterOnly()  // explicitly use ES default of requiring master, use default Node preference of Master
            | NodePreference.PreferMaster -> s.PerformOnAnyNode() // override default [implied] PerformOnMasterOnly(), use default Node preference of Master
            | NodePreference.PreferSlave -> s.PerformOnAnyNode().PreferSlaveNode() // override default PerformOnMasterOnly(), override Master Node preference
            | NodePreference.Random -> s.PerformOnAnyNode().PreferRandomNode()  // override default PerformOnMasterOnly(), override Master Node preference
        |> fun s -> match concurrentOperationsLimit with Some col -> s.LimitConcurrentOperationsTo(col) | None -> s // ES default: 5000
        |> fun s -> match heartbeatTimeout with Some v -> s.SetHeartbeatTimeout v | None -> s // default: 1500 ms
        |> fun s -> match log with Some log -> log.Configure s | None -> s
        |> fun s -> s.Build()

    /// Yields an IEventStoreConnection configured and Connect()ed to a node (or the cluster) per the supplied `discovery` and `clusterNodePrefence` preference
    member __.Connect
        (   /// Name should be sufficient to uniquely identify this connection within a single app instance's logs
            name,
            discovery : Discovery, ?clusterNodePreference) : Async<IEventStoreConnection> = async {
        if name = null then nullArg "name"
        let clusterNodePreference = defaultArg clusterNodePreference NodePreference.Master
        let name = String.concat ";" <| seq {
            yield name
            yield string clusterNodePreference
            match tags with None -> () | Some tags -> for key, value in tags do yield sprintf "%s=%s" key value }
        let sanitizedName = name.Replace('\'','_').Replace(':','_') // ES internally uses `:` and `'` as separators in log messages and ... people regex logs
        let conn =
            match discovery with
            | Discovery.DiscoverViaUri uri -> EventStoreConnection.Create(connSettings clusterNodePreference, uri, sanitizedName)
            | Discovery.DiscoverViaGossip clusterSettings -> EventStoreConnection.Create(connSettings clusterNodePreference, clusterSettings, sanitizedName)
        do! conn.ConnectAsync() |> Async.AwaitTaskCorrect
        return conn }

    /// Yields a GesConnection (which may internally be twin connections) configured per the specified strategy
    member __.Establish
        (   /// Name should be sufficient to uniquely identify this (aggregate) connection within a single app instance's logs
            name,
            discovery : Discovery, strategy : ConnectionStrategy) : Async<GesConnection> = async {
        match strategy with
        | ConnectionStrategy.ClusterSingle nodePreference ->
            let! conn = __.Connect(name, discovery, nodePreference)
            return GesConnection(conn, ?readRetryPolicy=readRetryPolicy, ?writeRetryPolicy=writeRetryPolicy)
        | ConnectionStrategy.ClusterTwinPreferSlaveReads ->
            let! masterInParallel = Async.StartChild (__.Connect(name + "-TwinW", discovery, NodePreference.Master))
            let! slave = __.Connect(name + "-TwinR", discovery, NodePreference.PreferSlave)
            let! master = masterInParallel
            return GesConnection(readConnection=slave, writeConnection=master, ?readRetryPolicy=readRetryPolicy, ?writeRetryPolicy=writeRetryPolicy) }