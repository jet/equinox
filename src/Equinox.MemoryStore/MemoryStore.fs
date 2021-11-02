/// Implements an in-memory store. This fulfils two goals:
/// 1. Acts as A target for integration testing allowing end-to-end processing of a decision flow in an efficient test
/// 2. Illustrates a minimal implementation of the Storage interface interconnects for the purpose of writing Store connectors
namespace Equinox.MemoryStore

open Equinox
open Equinox.Core
open System.Runtime.InteropServices

/// Equivalent to EventStoreDB's in purpose; signals a conflict has been detected and reprocessing of the decision will be necessary
exception private WrongVersionException of value : obj

/// Internal result used to reflect the outcome of syncing with the entry in the inner ConcurrentDictionary
[<NoEquality; NoComparison>]
type ConcurrentDictionarySyncResult<'Format> = Written of FsCodec.ITimelineEvent<'Format>[] | Conflict

/// Response type for VolatileStore.TrySync to communicate the outcome and updated state of a stream
[<NoEquality; NoComparison>]
type ConcurrentArraySyncResult<'Format> = Written of FsCodec.ITimelineEvent<'Format>[] | Conflict of FsCodec.ITimelineEvent<'Format>[]

/// Maintains a dictionary of ITimelineEvent<'Format>[] per stream-name, allowing one to vary the encoding used to match that of a given concrete store, or optimize test run performance
type VolatileStore<'Format>() =

    let streams = System.Collections.Concurrent.ConcurrentDictionary<string, FsCodec.ITimelineEvent<'Format>[]>()

    // Where TrySync attempts overlap on the same stream, there's a race to raise the Committed event for each 'commit' resulting from a successful Sync
    // If we don't serialize the publishing of the events, its possible for handlers to observe the Events out of order
    let committed = Event<_>()
    // Here we neuter that effect - the BatchingGate can end up with commits submitted out of order, but we serialize the raising of the events per stream
    let publishBatches (commits : (FsCodec.StreamName * FsCodec.ITimelineEvent<'Format>[])[]) = async {
        for streamName, events in commits |> Seq.groupBy fst do
            committed.Trigger(streamName, events |> Seq.collect snd |> Seq.sortBy (fun x -> x.Index) |> Seq.toArray) }
    let publishCommit = AsyncBatchingGate(publishBatches, System.TimeSpan.FromMilliseconds 2.)

    [<CLIEvent>]
    /// Notifies of a batch of events being committed to a given Stream. Guarantees no out of order and/or overlapping raising of the event<br/>
    /// NOTE in some cases, two or more overlapping commits can be coalesced into a single <c>Committed</c> event
    member _.Committed : IEvent<FsCodec.StreamName * FsCodec.ITimelineEvent<'Format>[]> = committed.Publish

    /// Loads state from a given stream
    member _.TryLoad streamName = match streams.TryGetValue streamName with false, _ -> None | true, packed -> Some packed

    /// Attempts a synchronization operation - yields conflicting value if sync function decides there is a conflict
    member _.TrySync
        (   streamName, trySyncValue : FsCodec.ITimelineEvent<'Format>[] -> ConcurrentDictionarySyncResult<'Format>,
            events: FsCodec.ITimelineEvent<'Format>[])
        : Async<ConcurrentArraySyncResult<'Format>> = async {
        let seedStream _streamName = events
        let updateValue _streamName (currentValue : FsCodec.ITimelineEvent<'Format>[]) =
            match trySyncValue currentValue with
            | ConcurrentDictionarySyncResult.Conflict -> raise <| WrongVersionException (box currentValue)
            | ConcurrentDictionarySyncResult.Written value -> value
        try let res = streams.AddOrUpdate(streamName, seedStream, updateValue)
            // we publish the event here, once, as `updateValue` can be invoked multiple times
            do! publishCommit.Execute((FsCodec.StreamName.parse streamName, events))
            return Written res
        with WrongVersionException conflictingValue ->
            return Conflict (unbox conflictingValue) }

type Token = { streamName : string; eventCount : int }

/// Internal implementation detail of MemoryStore
module private Token =

    let private streamTokenOfEventCount streamName (eventCount : int) : StreamToken =
        {   value = box { streamName = streamName; eventCount = eventCount }
            version = int64 eventCount }
    let (|Unpack|) (token: StreamToken) : Token = unbox<Token> token.value
    /// Represent a stream known to be empty
    let ofEmpty streamName = streamTokenOfEventCount streamName 0
    let ofValue streamName (value: 'event array) = streamTokenOfEventCount streamName value.Length

/// Represents the state of a set of streams in a style consistent withe the concrete Store types - no constraints on memory consumption (but also no persistence!).
type Category<'event, 'state, 'context, 'Format>(store : VolatileStore<'Format>, codec : FsCodec.IEventCodec<'event,'Format,'context>, fold, initial) =
    interface ICategory<'event, 'state, string, 'context> with
        member _.Load(_log, streamName, _opt) = async {
            match store.TryLoad streamName with
            | None -> return Token.ofEmpty streamName, initial
            | Some value -> return Token.ofValue streamName value, fold initial (value |> Seq.choose codec.TryDecode) }
        member _.TrySync(_log, Token.Unpack token, state, events : 'event list, context : 'context option) = async {
            let inline map i (e : FsCodec.IEventData<'Format>) =
                FsCodec.Core.TimelineEvent.Create(int64 i, e.EventType, e.Data, e.Meta, e.EventId, e.CorrelationId, e.CausationId, e.Timestamp)
            let encoded = events |> Seq.mapi (fun i e -> map (token.eventCount + i) (codec.Encode(context, e))) |> Array.ofSeq
            let trySyncValue currentValue =
                if Array.length currentValue <> token.eventCount then ConcurrentDictionarySyncResult.Conflict
                else ConcurrentDictionarySyncResult.Written (Seq.append currentValue encoded |> Array.ofSeq)
            match! store.TrySync(token.streamName, trySyncValue, encoded) with
            | ConcurrentArraySyncResult.Written value' ->
                return SyncResult.Written (Token.ofValue token.streamName value', fold state events)
            | ConcurrentArraySyncResult.Conflict conflictingEvents ->
                let resync = async {
                    let token' = Token.ofValue token.streamName conflictingEvents
                    return token', fold state (conflictingEvents |> Seq.skip token.eventCount |> Seq.choose codec.TryDecode) }
                return SyncResult.Conflict resync }

type MemoryStoreCategory<'event, 'state, 'Format, 'context>(store : VolatileStore<'Format>, codec : FsCodec.IEventCodec<'event,'Format,'context>, fold, initial) =
    let category = Category<'event, 'state, 'context, 'Format>(store, codec, fold, initial)
    let resolveStream streamName context = Stream.create category streamName None context

    member _.Resolve(streamName : FsCodec.StreamName, [<Optional; DefaultParameterValue null>] ?option, [<Optional; DefaultParameterValue null>] ?context : 'context) =
        match FsCodec.StreamName.toString streamName, option with
        | sn, (None | Some AllowStale) -> resolveStream sn context
        | sn, Some AssumeEmpty -> Stream.ofMemento (Token.ofEmpty sn, initial) (resolveStream sn context)

    /// Resolve from a Memento being used in a Continuation [based on position and state typically from Stream.CreateMemento]
    member _.FromMemento(Token.Unpack stream as streamToken, state, [<Optional; DefaultParameterValue null>] ?context) =
        Stream.ofMemento (streamToken, state) (resolveStream stream.streamName context)
