// Implements an in-memory store. This fulfils two goals:
// 1. Acts as a target for integration testing allowing end-to-end processing of a decision flow in an efficient test
// 2. Illustrates a minimal implementation of the Storage interface interconnects for the purpose of writing Store connectors
namespace Equinox.MemoryStore

open Equinox.Core
open System.Runtime.InteropServices

/// Maintains a dictionary of ITimelineEvent<'Format>[] per stream-name, allowing one to vary the encoding used to match that of a given concrete store, or optimize test run performance
type VolatileStore<'Format>() =

    let streams = System.Collections.Concurrent.ConcurrentDictionary<string, FsCodec.ITimelineEvent<'Format>[]>()

    let seedStream _streamName struct (_expectedCount, events) = events
    let updateValue _streamName (currentValue : FsCodec.ITimelineEvent<'Format>[]) struct (expectedCount, events) =
        if Array.length currentValue <> expectedCount then currentValue
        // note we don't publish here, as this function can be potentially invoked multiple times where there is a race
        else Array.append currentValue events
    let trySync streamName expectedCount events : bool * FsCodec.ITimelineEvent<'Format>[] =
        let res = streams.AddOrUpdate(streamName, seedStream, updateValue, (expectedCount, events))
        (obj.ReferenceEquals(Array.last res, Array.last events), res)

    // Where TrySync attempts overlap on the same stream, there's a race to raise the Committed event for each 'commit' resulting from a successful Sync
    // If we don't serialize the publishing of the events, its possible for handlers to observe the Events out of order
    let committed = Event<_>()
    // Here we neuter that effect - the BatchingGate can end up with commits submitted out of order, but we serialize the raising of the events per stream
    let publishBatches (commits : (FsCodec.StreamName * FsCodec.ITimelineEvent<'Format>[])[]) = async {
        for streamName, events in commits |> Seq.groupBy fst do
            committed.Trigger(streamName, events |> Seq.collect snd |> Seq.sortBy (fun x -> x.Index) |> Seq.toArray) }
    let publishCommit = AsyncBatchingGate(publishBatches, System.TimeSpan.FromMilliseconds 2.)

    /// Notifies of a batch of events being committed to a given Stream. Guarantees no out of order and/or overlapping raising of the event<br/>
    /// NOTE in some cases, two or more overlapping commits can be coalesced into a single <c>Committed</c> event
    [<CLIEvent>] member _.Committed : IEvent<FsCodec.StreamName * FsCodec.ITimelineEvent<'Format>[]> = committed.Publish

    /// Loads state from a given stream
    member _.TryLoad streamName = match streams.TryGetValue streamName with false, _ -> None | true, packed -> Some packed

    /// Attempts a synchronization operation - yields conflicting value if expectedCount does not match
    member _.TrySync(streamName, expectedCount, events) : Async<bool * FsCodec.ITimelineEvent<'Format>[]> = async {
        let succeeded, _ as outcome = trySync streamName expectedCount events
        if succeeded then do! publishCommit.Execute((FsCodec.StreamName.parse streamName, events))
        return outcome }

type Token = { eventCount : int }

/// Internal implementation detail of MemoryStore
module private Token =

    let private streamTokenOfEventCount (eventCount : int) : StreamToken =
        // TOCONSIDER Could implement streamBytes tracking based on a supplied event size function (store is agnostic to format)
        { value = box { eventCount = eventCount }; version = int64 eventCount; streamBytes = -1 }
    let (|Unpack|) (token : StreamToken) : int = let t = unbox<Token> token.value in t.eventCount
    /// Represent a stream known to be empty
    let ofEmpty = streamTokenOfEventCount 0
    let ofValue (value : 'event array) = streamTokenOfEventCount value.Length

/// Represents the state of a set of streams in a style consistent withe the concrete Store types - no constraints on memory consumption (but also no persistence!).
type Category<'event, 'state, 'context, 'Format>(store : VolatileStore<'Format>, codec : FsCodec.IEventCodec<'event, 'Format, 'context>, fold, initial) =
    interface ICategory<'event, 'state, string, 'context> with
        member _.Load(_log, streamName, _opt) = async {
            match store.TryLoad streamName with
            | None -> return Token.ofEmpty, initial
            | Some value -> return Token.ofValue value, fold initial (value |> Seq.choose codec.TryDecode) }
        member _.TrySync(_log, streamName, Token.Unpack eventCount, state, events : 'event list, context : 'context option) = async {
            let inline map i (e : FsCodec.IEventData<'Format>) =
                FsCodec.Core.TimelineEvent.Create(int64 i, e.EventType, e.Data, e.Meta, e.EventId, e.CorrelationId, e.CausationId, e.Timestamp)
            let encoded = events |> Seq.mapi (fun i e -> map (eventCount + i) (codec.Encode(context, e))) |> Array.ofSeq
            match! store.TrySync(streamName, eventCount, encoded) with
            | true, streamEvents' ->
                return SyncResult.Written (Token.ofValue streamEvents', fold state events)
            | false, conflictingEvents ->
                let resync = async {
                    let token' = Token.ofValue conflictingEvents
                    return token', fold state (conflictingEvents |> Seq.skip eventCount |> Seq.choose codec.TryDecode) }
                return SyncResult.Conflict resync }

type MemoryStoreCategory<'event, 'state, 'Format, 'context>(store : VolatileStore<'Format>, codec : FsCodec.IEventCodec<'event, 'Format, 'context>, fold, initial) =
    let category = Category<'event, 'state, 'context, 'Format>(store, codec, fold, initial)
    let resolve streamName = category :> ICategory<_, _, _, _>, FsCodec.StreamName.toString streamName, (None : (unit -> Async<unit>) option)
    let empty = Token.ofEmpty, initial
    let storeCategory = StoreCategory<'event, 'state, FsCodec.StreamName, 'context>(resolve, empty)
    member _.Resolve(streamName : FsCodec.StreamName, [<Optional; DefaultParameterValue null>] ?context : 'context) = storeCategory.Resolve(streamName, ?context = context)
