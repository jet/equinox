namespace Equinox.MessageDb

open Equinox.Core
open Equinox.Core.Tracing
open Equinox.MessageDb.Core
open FsCodec
open System
open System.Text.Json
open System.Threading
open System.Threading.Tasks

type EventBody = ReadOnlyMemory<byte>

module Activity =
    let setTags (tags: (string * obj)[]) =
        let act = System.Diagnostics.Activity.Current
        if act <> null then for k,v in tags do act.SetTag(k, v) |> ignore

module Retry =
    let withSpanTag<'t> retryPolicy (f: CancellationToken -> Task<'t>) ct: Task<'t> =
        match retryPolicy with
        | None -> f ct
        | Some retryPolicy ->
            let withRetryTag count =
                Activity.setTags [|Tags.retries, count|]
                f
            retryPolicy withRetryTag

module private Write =

    let inline len (bytes: EventBody) = bytes.Length
    let private eventDataLen (x: IEventData<EventBody>) = len x.Data + len x.Meta
    let private eventDataBytes events = events |> Array.sumBy eventDataLen
    let private writeEventsLogged (writer: MessageDbWriter) streamName version events ct: Task<MdbSyncResult> = task {
        Activity.setTags [|Tags.append_bytes, eventDataBytes events|]
        let! result = writer.WriteMessages(streamName, events, version, ct)
        match result with
        | MdbSyncResult.Written x -> Activity.setTags [|Tags.new_version, x|]
        | MdbSyncResult.ConflictUnknown ->
            let eventTypes =
                if events.Length <= 3
                then [| for x in events -> x.EventType |]
                else [| for x in Seq.take 3 events -> x.EventType |]
            Activity.setTags [|Tags.conflict, true; Tags.append_types, eventTypes|]
        return result }
    let writeEvents retryPolicy writer streamName version events ct: Task<MdbSyncResult> = task {
        let call = writeEventsLogged writer streamName version events
        return! Retry.withSpanTag retryPolicy call ct }

module Read =
    module LoadMethod =
        [<Literal>]
        let last = "Last"
        [<Literal>]
        let batchForward = "BatchForward"

    [<NoComparison;NoEquality>]
    type StreamEventsSlice = { Messages: ITimelineEvent<EventBody>[]; IsEnd: bool; LastVersion: int64 }

    let private toSlice (events: ITimelineEvent<EventBody>[]) isLast: StreamEventsSlice =
        let lastVersion = match Array.tryLast events with Some ev -> ev.Index | None -> -1L
        { Messages = events; IsEnd = isLast; LastVersion = lastVersion }

    let private readSliceAsync (reader: MessageDbReader) (streamName: string) (batchSize: int64) (requiresLeader: bool) (fromVersion: int64) ct = task {
        let! page = reader.ReadStream(streamName, fromVersion, batchSize, requiresLeader, ct)
        let isLast = int64 page.Length < batchSize
        return toSlice page isLast }

    let private readLastEventAsync (reader: MessageDbReader) (streamName: string) (requiresLeader: bool) (eventType: string option) ct = task {
        let! events = reader.ReadLastEvent(streamName, requiresLeader, ct, ?eventType = eventType)
        return toSlice events true }

    let inline len (bytes: EventBody) = bytes.Length
    let private resolvedEventLen (x: ITimelineEvent<EventBody>) = len x.Data + len x.Meta
    let private resolvedEventBytes events = events |> Array.sumBy resolvedEventLen
    let private readBatches fold originState tryDecode (readSlice: int64 -> CancellationToken -> Task<StreamEventsSlice>)
            (maxPermittedBatchReads: int option) (fromVersion: int64) ct
        : Task<int64 * 'state> =
        let mutable batchCount, eventCount, pos = 0, 0, fromVersion
        let mutable version = -1L
        let mutable state = originState
        let rec loop () : Task<unit> = task {
            let! slice = readSlice pos ct
            version <- max version slice.LastVersion
            state <- slice.Messages |> Seq.chooseV tryDecode |> fold state
            batchCount <- batchCount + 1
            eventCount <- eventCount + slice.Messages.Length
            pos <- slice.LastVersion  + 1L
            if not slice.IsEnd then
                match maxPermittedBatchReads with
                | Some mpbr when batchCount >= mpbr -> invalidOp "batch Limit exceeded"
                | _ -> ()
                return! loop () }
        task {
            do! loop ()
            Activity.setTags [|Tags.batches, batchCount; Tags.loaded_count, eventCount; Tags.read_version, version|]
            return version, state }

    let internal loadLastEvent retryPolicy (reader: MessageDbReader) requiresLeader streamName eventType ct
        : Task<int64 * ITimelineEvent<EventBody>[]> = task {
        let read = readLastEventAsync reader streamName requiresLeader eventType
        let! page = Retry.withSpanTag retryPolicy read ct
        Activity.setTags [|Tags.load_method, LoadMethod.last; Tags.loaded_count, page.Messages.Length; Tags.loaded_bytes, resolvedEventBytes page.Messages|]
        return page.LastVersion, page.Messages }

    let internal loadForwardsFrom fold initial tryDecode retryPolicy reader batchSize maxPermittedBatchReads streamName (fromVersion: int64) requiresLeader ct
        : Task<int64 * 'state> = task {
        let call = readSliceAsync reader streamName batchSize requiresLeader
        let retryingReadSlice version = Retry.withSpanTag retryPolicy (call version)
        Activity.setTags [|Tags.batch_size, batchSize; Tags.loaded_from_version, fromVersion; Tags.load_method, LoadMethod.batchForward|]
        return! readBatches fold initial tryDecode retryingReadSlice maxPermittedBatchReads fromVersion ct }

module private Token =

    // NOTE MessageDB's streamVersion is -1 based, similar to ESDB and SSS
    let create streamVersion: StreamToken =
        { value = box streamVersion
          // The `Version` exposed on the `ISyncContext` is 0-based
          version = streamVersion + 1L
          streamBytes = -1 }
    let inline streamVersion (token: StreamToken) = unbox<int64> token.value
    let shouldSnapshot batchSize prev next =
        let previousVersion = prev.version
        let nextVersion = next.version
        let estimatedSnapshotPos = previousVersion - (previousVersion % batchSize)
        nextVersion - estimatedSnapshotPos >= batchSize

    let isStale current candidate = current.version > candidate.version

module private Snapshot =

    let inline snapshotCategory original = original + ":snapshot"
    let inline streamName category (streamId: string) = Equinox.Core.StreamName.render (snapshotCategory category) streamId
    type Meta = {| streamVersion: int64 |} // STJ doesn't want to serialize it unless its anonymous
    let private streamVersion (evt: ITimelineEvent<EventBody>) =
        let meta = evt.Meta // avoid defensive copy
        JsonSerializer.Deserialize<Meta>(meta.Span).streamVersion

    let meta token = {| streamVersion = Token.streamVersion token |} |> JsonSerializer.SerializeToUtf8Bytes |> ReadOnlyMemory
    let decode tryDecode (events: ITimelineEvent<EventBody>[]) =
        match events |> Array.tryFirstV |> ValueOption.bind tryDecode with
        | ValueSome decoded -> ValueSome struct(events[0] |> streamVersion |> Token.create, decoded)
        | ValueNone -> ValueNone

type MessageDbClient internal (reader, writer, ?readRetryPolicy, ?writeRetryPolicy) =
    member val internal Reader = reader
    member val ReadRetryPolicy = readRetryPolicy
    member val internal Writer = writer
    member val WriteRetryPolicy = writeRetryPolicy
    new(connectionString: string,
        // Can be used to divert reads to a replica
        // Conflicts detected on write trigger a resync, reading via the `connectionString` to maximize the freshness of the data for the retry
        [<O; D(null)>]?readConnectionString: string,
        [<O; D(null)>]?readRetryPolicy, [<O; D(null)>]?writeRetryPolicy) =

        let readConnectionString = defaultArg readConnectionString connectionString
        let reader = MessageDbReader(readConnectionString, connectionString)
        let writer = MessageDbWriter(connectionString)
        MessageDbClient(reader, writer, ?readRetryPolicy = readRetryPolicy, ?writeRetryPolicy = writeRetryPolicy)

type BatchOptions(getBatchSize: Func<int>, [<O; D(null)>]?batchCountLimit) =
    new (batchSize) = BatchOptions(fun () -> batchSize)
    member _.BatchSize = getBatchSize.Invoke()
    member val MaxBatches = batchCountLimit

[<RequireQualifiedAccess; NoComparison; NoEquality>]
type internal GatewaySyncResult = Written of StreamToken | ConflictUnknown

type MessageDbContext(client: MessageDbClient, batchOptions: BatchOptions) =
    new(client: MessageDbClient,
        // Max number of Events to retrieve in a single batch. Also affects frequency of Snapshots. Default: 500.
        [<O; D null>] ?batchSize) =
        MessageDbContext(client, BatchOptions(batchSize = defaultArg batchSize 500))
    member val BatchOptions = batchOptions

    member _.TokenEmpty = Token.create -1L
    member _.LoadBatched(streamName, requireLeader, tryDecode, fold, initial, ct): Task<struct(StreamToken * 'state)> = task {
        let! version, state = Read.loadForwardsFrom fold initial tryDecode client.ReadRetryPolicy client.Reader batchOptions.BatchSize batchOptions.MaxBatches streamName 0L requireLeader ct
        return struct(Token.create version, state) }
    member _.LoadLast(streamName, requireLeader, tryDecode, fold, initial, ct): Task<struct(StreamToken * 'state)> = task {
        let! version, events = Read.loadLastEvent client.ReadRetryPolicy client.Reader requireLeader streamName None ct
        return struct(Token.create version, events |> Seq.chooseV tryDecode |> fold initial) }
    member _.LoadSnapshot(category, streamId, requireLeader, tryDecode, eventType, ct) = task {
        let snapshotStream = Snapshot.streamName category streamId
        let! _, events = Read.loadLastEvent client.ReadRetryPolicy client.Reader requireLeader snapshotStream (Some eventType) ct
        let decoded = Snapshot.decode tryDecode events
        let version = match decoded with ValueNone -> -1L | ValueSome (t, _) -> t.version
        Activity.setTags [|Tags.snapshot_version, version|]
        return decoded }

    member _.Reload(streamName, requireLeader, token, tryDecode, fold, initial, ct): Task<struct(StreamToken * 'state)> = task {
        let streamVersion = Token.streamVersion token
        let fromVersion = streamVersion + 1L // Reading a stream uses {inclusive} positions, but the streamVersion is `-1`-based
        let! version, state = Read.loadForwardsFrom fold initial tryDecode client.ReadRetryPolicy client.Reader batchOptions.BatchSize batchOptions.MaxBatches streamName fromVersion requireLeader ct
        return struct(Token.create (max streamVersion version), state) }

    member internal _.TrySync(_category, _streamId, streamName, token, encodedEvents: IEventData<EventBody>[], ct): Task<GatewaySyncResult> = task {
        let streamVersion = Token.streamVersion token
        match! Write.writeEvents client.WriteRetryPolicy client.Writer streamName (StreamVersion streamVersion) encodedEvents ct with
        | MdbSyncResult.Written version' ->
            let token = Token.create version'
            return GatewaySyncResult.Written token
        | MdbSyncResult.ConflictUnknown ->
            return GatewaySyncResult.ConflictUnknown }

    member _.StoreSnapshot(category, streamId, event, ct) = task {
        let snapshotStream = Snapshot.streamName category streamId
        Activity.setTags [|Tags.snapshot_written, true|]
        do! Write.writeEvents None client.Writer snapshotStream Any [| event |] ct :> Task }

[<NoComparison; NoEquality; RequireQualifiedAccess>]
type AccessStrategy<'event, 'state> =
    /// Load only the single most recent event defined in in a stream and trust that it'll be decoded and
    /// doing a <c>fold</c> from any such event will yield a correct and complete state
    /// In other words, the <c>fold</c> function should not need to consider either the preceding <c>'state</c> or <c>'event</c>s.
    | LatestKnownEvent
    /// <summary>
    /// Generates and stores a snapshot event in an adjacent <c>{category}:snapshot-{stream_id}</c> stream
    /// The generation happens every <c>batchSize</c> events.
    /// This means the state of the stream can be reconstructed with exactly 2 round-trips to the database.
    /// The first round-trip fetches the most recent event of type <c>snapshotEventCaseName</c> from the snapshot stream.
    /// The second round-trip fetches <c>batchSize</c> events from the position of the snapshot
    /// The <c>toSnapshot</c> function is used to generate the event to store in the snapshot stream.
    /// It should return the event case whose name matches <c>snapshotEventCaseName</c>
    /// </summary>
    | AdjacentSnapshots of snapshotEventCaseName: string * toSnapshot: ('state -> 'event)

type private Category<'event, 'state, 'context>(context: MessageDbContext, codec: IEventCodec<_, _, 'context>, fold, initial, access) =
    let loadAlgorithm category streamId streamName requireLeader ct =
        match access with
        | None -> context.LoadBatched(streamName, requireLeader, codec.TryDecode, fold, initial, ct)
        | Some AccessStrategy.LatestKnownEvent -> context.LoadLast(streamName, requireLeader, codec.TryDecode, fold, initial, ct)
        | Some (AccessStrategy.AdjacentSnapshots (snapshotType, _)) -> task {
            match! context.LoadSnapshot(category, streamId, requireLeader, codec.TryDecode, snapshotType, ct) with
            | ValueSome (pos, snapshotEvent) ->
                let state = fold initial [| snapshotEvent |]
                let! token, state = context.Reload(streamName, requireLeader, pos, codec.TryDecode, fold, state, ct)
                return struct(token, state)
            | ValueNone -> return! context.LoadBatched(streamName, requireLeader, codec.TryDecode, fold, initial, ct) }
    let reload (sn, leader, token, state) ct = context.Reload(sn, leader, token, codec.TryDecode, fold, state, ct)
    interface ICategory<'event, 'state, 'context> with
        member _.Load(_log, categoryName, streamId, streamName, _maxAge, requireLeader, ct) =
            loadAlgorithm categoryName streamId streamName requireLeader ct
        member x.TrySync(_log, categoryName, streamId, streamName, ctx, _maybeInit, token, state, events, ct) = task {
            let encode e = codec.Encode(ctx, e)
            let encodedEvents: IEventData<EventBody>[] = events |> Array.map encode
            match! context.TrySync(categoryName, streamId, streamName, token, encodedEvents, ct) with
            | GatewaySyncResult.Written token' ->
                let state' = fold state (Seq.ofArray events)
                match access with
                | None | Some AccessStrategy.LatestKnownEvent -> ()
                | Some (AccessStrategy.AdjacentSnapshots(_, toSnap)) ->
                    if Token.shouldSnapshot context.BatchOptions.BatchSize token token' then
                        do! x.StoreSnapshot(categoryName, streamId, ctx, token', toSnap state', ct)
                return SyncResult.Written (token', state')
            | GatewaySyncResult.ConflictUnknown ->
                return SyncResult.Conflict (reload (streamName, (*requireLeader*)true, token, state)) }
    interface Caching.IReloadable<'state> with member _.Reload(_log, sn, leader, token, state, ct) = reload (sn, leader, token, state) ct

    member _.StoreSnapshot(category, streamId, ctx, token, snapshotEvent, ct) =
        let encodedWithMeta =
            let rawEvent = codec.Encode(ctx, snapshotEvent)
            FsCodec.Core.EventData.Create(rawEvent.EventType, rawEvent.Data, meta = Snapshot.meta token)
        context.StoreSnapshot(category, streamId, encodedWithMeta, ct)

type MessageDbCategory<'event, 'state, 'context> internal (resolveInner, empty) =
    inherit Equinox.Category<'event, 'state, 'context>(resolveInner, empty)
    new(context: MessageDbContext, codec: IEventCodec<_, _, 'context>, fold, initial,
        // For MessageDb, caching is less critical than it is for e.g. CosmosDB
        // As such, it can often be omitted, particularly if streams are short, or events are small and/or database latency aligns with request latency requirements
        [<O; D(null)>]?caching,
        [<O; D(null)>]?access) =
        let cat = Category<'event, 'state, 'context>(context, codec, fold, initial, access) |> Caching.apply Token.isStale caching
        let resolveInner categoryName streamId = struct (cat, StreamName.render categoryName streamId, ValueNone)
        let empty = struct (context.TokenEmpty, initial)
        MessageDbCategory(resolveInner, empty)
