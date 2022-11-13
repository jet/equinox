namespace Equinox.MessageDb

open Equinox.Core
open Equinox.MessageDb.Core
open FSharp.Control
open FsCodec
open System
open System.Diagnostics

type EventBody = ReadOnlyMemory<byte>

[<NoComparison;NoEquality>]
type StreamEventsSlice = { Messages: ITimelineEvent<EventBody> array; IsEnd: bool; LastVersion: int64 }

module private Trace =
    let source = new ActivitySource("Equinox.MessageDb", "4.0.0")

module private Write =
    let private writeEventsAsync (writer: MessageDbWriter) (streamName : string) (version : int64) (events : IEventData<EventBody> array)
        : Async<MdbSyncResult> = async {
            let! wr = writer.WriteMessages(streamName, events, version, Async.DefaultCancellationToken) |> Async.AwaitTaskCorrect
            match wr with
            | MdbSyncResult.ConflictUnknown ->
                let span = Activity.Current
                if span <> null then
                    let tags = ActivityTagsCollection()
                    tags.Add("eqx.event_types", [| for x in events -> x.EventType |])
                    tags.Add("eqx.expected_version", version)
                    span.AddEvent(ActivityEvent("WrongExpectedVersion", tags = tags)) |> ignore
                return wr
            | _ -> return wr }

    let inline len (bytes: EventBody) = bytes.Length
    let eventDataBytes events =
        let eventDataLen (x : IEventData<EventBody>) = len x.Data + len x.Meta
        events |> Array.sumBy eventDataLen
    let private writeEventsLogged (writer: MessageDbWriter) (streamName : string) (version : int64) (events : IEventData<EventBody> array) (attempt: int)
        : Async<MdbSyncResult> = async {
        use span = Trace.source.StartActivity("Write")
        let! result = writeEventsAsync writer streamName version events
        if span <> null then
            let bytes, count = eventDataBytes events, events.Length
            span.AddTag("eqx.bytes", bytes)
                .AddTag("eqx.count", count)
                .AddTag("eqx.stream_name", streamName)
                .AddTag("eqx.expected_version", version)
                .AddTag("eqx.event_types", [| for ev in events -> ev.EventType |]) |> ignore
            if attempt <> 1 then span.AddTag("eqx.attempt", attempt) |> ignore
            match result with
            | MdbSyncResult.ConflictUnknown ->
                span.SetStatus(ActivityStatusCode.Error, "WriteConflict") |> ignore
            | MdbSyncResult.Written x ->
                span.AddTag("eqx.current_position", x) |> ignore
        return result }
    let writeEvents retryPolicy (writer : MessageDbWriter) (streamName : string) (version : int64) (events : IEventData<EventBody> array)
        : Async<MdbSyncResult> =
        let call = writeEventsLogged writer streamName version events
        match retryPolicy with None -> call 1 | Some f -> f call

module private Read =
    let private toSlice (events : ITimelineEvent<EventBody> array) isLast =
        let lastVersion = match Array.tryLast events with Some ev -> ev.Index | None -> -1L
        { Messages = events; IsEnd = isLast; LastVersion = lastVersion }

    let private readSliceAsync (reader : MessageDbReader) (streamName : string) (batchSize : int64) (startPos : int64) (requiresLeader : bool)
        : Async<_> = async {
        let! ct = Async.CancellationToken
        let! page = reader.ReadStream(streamName, startPos, batchSize, requiresLeader, ct) |> Async.AwaitTaskCorrect
        let isLast = int64 page.Length < batchSize
        return toSlice page isLast }

    let private readLastEventAsync (reader : MessageDbReader) (streamName : string) (requiresLeader : bool) : Async<_> = async {
        let! ct = Async.CancellationToken
        let! events = reader.ReadLastEvent(streamName, requiresLeader, ct) |> Async.AwaitTaskCorrect
        return toSlice events false }

    let inline len (bytes: EventBody) = bytes.Length
    let resolvedEventLen (x : ITimelineEvent<EventBody>) = len x.Data + len x.Meta
    let private tracedReadSlice reader streamName batchSize startPos requiresLeader (attempt : int) : Async<_> = async {
        use span = Trace.source.StartActivity("ReadSlice", ActivityKind.Client)
        let! slice = readSliceAsync reader streamName batchSize startPos requiresLeader
        if span <> null then
            let bytes, count = slice.Messages |> Array.sumBy resolvedEventLen, slice.Messages.Length
            span
                .AddTag("eqx.stream_name", streamName)
                .AddTag("eqx.batch_size", batchSize)
                .AddTag("eqx.start_pos", startPos)
                .AddTag("eqx.version", slice.LastVersion)
                .AddTag("eqx.bytes", bytes)
                .AddTag("eqx.count", count) |> ignore
            if requiresLeader then span.AddTag("eqx.requires_leader", requiresLeader) |> ignore
            if attempt <> 1 then span.AddTag("eqx.attempt", attempt) |> ignore

        return slice }

    let private readBatches (readSlice : int64 -> Async<StreamEventsSlice>)
            (maxPermittedBatchReads : int option) (startPosition : int64)
        : AsyncSeq<int64 * ITimelineEvent<EventBody> array> =
        let rec loop batchCount pos : AsyncSeq<int64 * ITimelineEvent<EventBody> array> = asyncSeq {
            let span = Activity.Current
            match maxPermittedBatchReads with
            | Some mpbr when batchCount >= mpbr ->
                if span <> null then span.AddEvent(ActivityEvent("BatchLimitExceeded")).SetStatus(ActivityStatusCode.Error, "Batch limit exceeded") |> ignore
                invalidOp "batch Limit exceeded"
            | _ -> ()

            if span <> null then span.AddTag("eqx.batch_index", batchCount) |> ignore

            let! slice = readSlice pos
            yield slice.LastVersion, slice.Messages
            if not slice.IsEnd then
                yield! loop (batchCount + 1) (slice.LastVersion + 1L) }
        loop 0 startPosition

    let resolvedEventBytes events = events |> Array.sumBy resolvedEventLen

    let loadLastEvent retryPolicy (reader : MessageDbReader) requiresLeader streamName
        : Async<int64 * ITimelineEvent<EventBody> array> = async {
        let read (attempt : int) = async {
            use span = Trace.source.StartActivity("ReadLast", ActivityKind.Client)
            let! slice = readLastEventAsync reader streamName requiresLeader
            if span <> null then
                let bytes, count = resolvedEventBytes slice.Messages, slice.Messages.Length
                span.AddTag("eqx.bytes", bytes)
                    .AddTag("eqx.count", count)
                    .AddTag("eqx.stream_name", streamName)
                    .AddTag("eqx.version", slice.LastVersion) |> ignore
                if requiresLeader then span.AddTag("eqx.requires_leader", requiresLeader) |> ignore
                if attempt <> 1 then span.AddTag("eqx.attempt", attempt) |> ignore
            return slice }

        let! page = match retryPolicy with None -> read 1 | Some f -> f read

        return page.LastVersion, page.Messages }

    let loadForwardsFrom retryPolicy reader batchSize maxPermittedBatchReads streamName startPosition requiresLeader
        : Async<int64 * ITimelineEvent<EventBody> array> = async {
        use span = Trace.source.StartActivity("LoadBatch", ActivityKind.Client)
        let mergeBatches (batches : AsyncSeq<int64 * ITimelineEvent<EventBody> array>) = async {
            let mutable versionFromStream = -1L
            let! (events : ITimelineEvent<EventBody> array) =
                batches
                |> AsyncSeq.map (fun (reportedVersion, events)-> versionFromStream <- max reportedVersion versionFromStream; events)
                |> AsyncSeq.concatSeq
                |> AsyncSeq.toArrayAsync
            let version = versionFromStream
            return version, events }
        let call pos = tracedReadSlice reader streamName batchSize pos requiresLeader
        let retryingLoggingReadSlice pos = match retryPolicy with None -> (call pos 1) | Some f -> f (call pos)
        if span <> null then
            span.AddTag("eqx.batch_size", batchSize).AddTag("eqx.stream_name", streamName) |> ignore
        let batches : AsyncSeq<int64 * ITimelineEvent<EventBody> array> = readBatches retryingLoggingReadSlice maxPermittedBatchReads startPosition
        return! mergeBatches batches }

module private Token =
    // NOTE MessageDB's streamVersion is -1 based, similar to ESDB and SSS
    let create streamVersion : StreamToken =
        { value = box streamVersion
          // The `Version` exposed on the `ISyncContext` is 0-based
          version = streamVersion + 1L
          streamBytes = -1 }
    let inline streamVersion (token: StreamToken) = unbox<int64> token.value

    let supersedes struct (current, x) =
        x.version > current.version

type MessageDbConnection(reader, writer, [<O; D(null)>]?readRetryPolicy, [<O; D(null)>]?writeRetryPolicy) =
    member val Reader = reader
    member val ReadRetryPolicy = readRetryPolicy
    member val Writer = writer
    member val WriteRetryPolicy = writeRetryPolicy

type BatchOptions(getBatchSize : Func<int>, [<O; D(null)>]?batchCountLimit) =
    new (batchSize) = BatchOptions(fun () -> batchSize)
    member _.BatchSize = getBatchSize.Invoke()
    member _.MaxBatches = batchCountLimit

[<RequireQualifiedAccess; NoComparison; NoEquality>]
type GatewaySyncResult = Written of StreamToken | ConflictUnknown

type MessageDbContext(connection : MessageDbConnection, batchOptions : BatchOptions) =
    new (   connection : MessageDbConnection,
            // Max number of Events to retrieve in a single batch. Also affects frequency of RollingSnapshots. Default: 500.
            [<O; D null>] ?batchSize) =
        MessageDbContext(connection, BatchOptions(batchSize = defaultArg batchSize 500))
    member val BatchOptions = batchOptions

    member _.TokenEmpty = Token.create -1L
    member _.LoadBatched(streamName, requireLeader, tryDecode) : Async<StreamToken * 'event array> = async {
        let! version, events = Read.loadForwardsFrom connection.ReadRetryPolicy connection.Reader batchOptions.BatchSize batchOptions.MaxBatches streamName 0L requireLeader
        return Token.create version, Array.chooseV tryDecode events }
    member _.LoadLast(streamName, requireLeader, tryDecode) : Async<StreamToken * 'event array> = async {
        let! version, events = Read.loadLastEvent connection.ReadRetryPolicy connection.Reader requireLeader streamName
        return Token.create version, Array.chooseV tryDecode events }
    member _.Reload(streamName, requireLeader, token, tryDecode)
        : Async<StreamToken * 'event array> = async {
        let streamVersion = Token.streamVersion token
        let startPos = streamVersion + 1L // Reading a stream uses {inclusive} positions, but the streamVersion is `-1`-based
        let! version, events = Read.loadForwardsFrom connection.ReadRetryPolicy connection.Reader batchOptions.BatchSize batchOptions.MaxBatches streamName startPos requireLeader
        return Token.create (max streamVersion version), Array.chooseV tryDecode events }

    member _.TrySync(streamName, token, encodedEvents : IEventData<EventBody> array): Async<GatewaySyncResult> = async {
        let streamVersion = Token.streamVersion token
        match! Write.writeEvents connection.WriteRetryPolicy connection.Writer streamName streamVersion encodedEvents with
        | MdbSyncResult.ConflictUnknown ->
            return GatewaySyncResult.ConflictUnknown
        | MdbSyncResult.Written version' ->
            let token = Token.create version'
            return GatewaySyncResult.Written token }

[<NoComparison; NoEquality; RequireQualifiedAccess>]
type AccessStrategy =
    /// Load only the single most recent event defined in in a stream and trust that it'll be decoded and
    /// doing a <c>fold</c> from any such event will yield a correct and complete state
    /// In other words, the <c>fold</c> function should not need to consider either the preceding <c>'state</c> or <c>'event</c>s.
    | LatestKnownEvent

type private Category<'event, 'state, 'context>(context : MessageDbContext, codec : IEventCodec<_, _, 'context>, ?access) =

    let loadAlgorithm streamName requireLeader =
        match access with
        | None -> context.LoadBatched(streamName, requireLeader, codec.TryDecode)
        | Some AccessStrategy.LatestKnownEvent -> context.LoadLast(streamName, requireLeader, codec.TryDecode)

    let load (fold : 'state -> 'event seq -> 'state) initial f : Async<struct (StreamToken * 'state)> = async {
        let! token, events = f
        return struct (token, fold initial events) }

    member _.Load(fold : 'state -> 'event seq -> 'state, initial : 'state, streamName : string, requireLeader) =
        load fold initial (loadAlgorithm streamName requireLeader)
    member _.Reload(fold : 'state -> 'event seq -> 'state, state : 'state, streamName : string, requireLeader, token) =
        load fold state (context.Reload(streamName, requireLeader, token, codec.TryDecode))

    member x.TrySync<'context>
        (   fold : 'state -> 'event seq -> 'state,
            streamName, token, state : 'state, events : 'event array, ctx : 'context) : Async<SyncResult<'state>> = async {
        let encode e = codec.Encode(ctx, e)
        let encodedEvents : IEventData<EventBody> array = events |> Array.map encode
        match! context.TrySync(streamName, token, encodedEvents) with
        | GatewaySyncResult.ConflictUnknown ->
            return SyncResult.Conflict  (fun ct -> x.Reload(fold, state, streamName, (*requireLeader*)true, token) |> Async.startAsTask ct)
        | GatewaySyncResult.Written token' ->
            return SyncResult.Written   (token', fold state (Seq.ofArray events)) }

type private Folder<'event, 'state, 'context>(category : Category<'event, 'state, 'context>, fold : 'state -> 'event seq -> 'state, initial : 'state, ?readCache) =
    let batched streamName requireLeader ct = category.Load(fold, initial, streamName, requireLeader) |> Async.startAsTask ct
    interface ICategory<'event, 'state, 'context> with
        member _.Load(_log, _categoryName, _streamId, streamName, allowStale, requireLeader, ct) = task {
            match readCache with
            | None -> return! batched streamName requireLeader ct
            | Some (cache : ICache, prefix : string) ->
                match! cache.TryGet(prefix + streamName) with
                | ValueNone -> return! batched streamName requireLeader ct
                | ValueSome tokenAndState when allowStale -> return tokenAndState
                | ValueSome (token, state) -> return! category.Reload(fold, state, streamName, requireLeader, token) }
        member _.TrySync(_log, _categoryName, _streamId, streamName, context, _init, token, originState, events, _ct) = task {
            match! category.TrySync(fold, streamName, token, originState, events, context) with
            | SyncResult.Conflict resync ->          return SyncResult.Conflict resync
            | SyncResult.Written (token', state') -> return SyncResult.Written (token', state') }

/// For MessageDb, caching is less critical than it is for e.g. CosmosDB
/// As such, it can often be omitted, particularly if streams are short, or events are small and/or database latency aligns with request latency requirements
[<NoComparison; NoEquality; RequireQualifiedAccess>]
type CachingStrategy =
    /// Retain a single 'state per streamName.
    /// Each cache hit for a stream renews the retention period for the defined <c>window</c>.
    /// Upon expiration of the defined <c>window</c> from the point at which the cache was entry was last used, a full reload is triggered.
    /// Unless <c>LoadOption.AllowStale</c> is used, each cache hit still incurs a roundtrip to load any subsequently-added events.
    | SlidingWindow of ICache * window : TimeSpan
    /// Retain a single 'state per streamName
    /// Upon expiration of the defined <c>period</c>, a full reload is triggered.
    /// Unless <c>LoadOption.AllowStale</c> is used, each cache hit still incurs a roundtrip to load any subsequently-added events.
    | FixedTimeSpan of ICache * period : TimeSpan
    /// Prefix is used to segregate multiple folds per stream when they are stored in the cache.
    /// Semantics are identical to <c>SlidingWindow</c>.
    | SlidingWindowPrefixed of ICache * window : TimeSpan * prefix : string

type MessageDbCategory<'event, 'state, 'context>(resolveInner, empty) =
    inherit Equinox.Category<'event, 'state, 'context>(resolveInner, empty)
    new (   context : MessageDbContext, codec : IEventCodec<_, _, 'context>, fold, initial,
            [<O; D(null)>]?caching,
            [<O; D(null)>]?access) =
        let inner = Category<'event, 'state, 'context>(context, codec, ?access = access)
        let readCacheOption =
            match caching with
            | None -> None
            | Some (CachingStrategy.SlidingWindow (cache, _))
            | Some (CachingStrategy.FixedTimeSpan (cache, _)) -> Some (cache, null)
            | Some (CachingStrategy.SlidingWindowPrefixed (cache, _, prefix)) -> Some (cache, prefix)
        let folder = Folder<'event, 'state, 'context>(inner, fold, initial, ?readCache = readCacheOption)
        let category : ICategory<_, _, 'context> =
            match caching with
            | None -> folder :> _
            | Some (CachingStrategy.SlidingWindow (cache, window)) ->
                Caching.applyCacheUpdatesWithSlidingExpiration cache null window folder Token.supersedes
            | Some (CachingStrategy.FixedTimeSpan (cache, period)) ->
                Caching.applyCacheUpdatesWithFixedTimeSpan cache null period folder Token.supersedes
            | Some (CachingStrategy.SlidingWindowPrefixed (cache, window, prefix)) ->
                Caching.applyCacheUpdatesWithSlidingExpiration cache prefix window folder Token.supersedes
        let resolveInner streamIds = struct (category, StreamName.Internal.ofCategoryAndStreamId streamIds, ValueNone)
        let empty = struct (context.TokenEmpty, initial)
        MessageDbCategory(resolveInner, empty)

type MessageDbConnector
    (   connectionString : string,
        // Can be used to divert reads to a replica
        // Conflicts detected on write trigger a resync, reading via the `connectionString` to maximize the freshness of the data for the retry
        [<O; D(null)>]?readConnectionString : string,
        [<O; D(null)>]?readRetryPolicy, [<O; D(null)>]?writeRetryPolicy) =
    let readConnectionString = defaultArg readConnectionString connectionString
    member _.Establish() : MessageDbConnection =
        let writer = MessageDbWriter(connectionString)
        let reader = MessageDbReader(readConnectionString, connectionString)
        MessageDbConnection(reader, writer, ?readRetryPolicy = readRetryPolicy, ?writeRetryPolicy = writeRetryPolicy)
