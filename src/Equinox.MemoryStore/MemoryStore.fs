// Implements an in-memory store. This fulfils two goals:
// 1. Acts as a target for integration testing allowing end-to-end processing of a decision flow in an efficient test
// 2. Illustrates a minimal implementation of the Storage interface interconnects for the purpose of writing Store connectors
namespace Equinox.MemoryStore

open Equinox.Core

/// Each Sync that results in an append to the store is notified via the Store's `Committed` event
type Commit<'Format> = (struct (FsCodec.StreamName * FsCodec.ITimelineEvent<'Format>[]))

/// Maintains a dictionary of ITimelineEvent<'Format>[] per stream-name, allowing one to vary the encoding used to match
/// that of a given concrete store, or optimize test run performance
type VolatileStore<'Format>() =

    let streams = System.Collections.Concurrent.ConcurrentDictionary<string, FsCodec.ITimelineEvent<'Format>[]>()

    let seedStream _streamName struct (_expectedCount, events) = events
    let updateValue _streamName (currentValue: FsCodec.ITimelineEvent<'Format>[]) struct (expectedCount, events) =
        if currentValue.Length <> expectedCount then currentValue
        // note we don't publish here, as this function can be potentially invoked multiple times where there is a race
        else Array.append currentValue events
    let trySync streamName expectedCount events: struct (bool * FsCodec.ITimelineEvent<'Format>[]) =
        let res = streams.AddOrUpdate(streamName, seedStream, updateValue, (expectedCount, events))
        (obj.ReferenceEquals(Array.last res, Array.last events), res)

    let committed = Event<Commit<'Format>>()
    /// Notifies re batches of events being committed to a given Stream.
    /// Commits are guaranteed to be notified in correct order at stream level under concurrent Equinox Transact calls.
    /// NOTE the caller should inspect and/or copy the event efficiently and immediately
    /// NOTE blocking and/or running reactions synchronously will hamper test performance and/or may result in deadlock
    [<CLIEvent>] member _.Committed: IEvent<Commit<'Format>> = committed.Publish

    /// Loads events from a given stream, null if none yet written
    member _.Load(streamName) =
        let mutable events = Unchecked.defaultof<_>
        streams.TryGetValue(streamName, &events) |> ignore
        events

    /// Attempts a synchronization operation - yields conflicting value if expectedCount does not match
    member _.TrySync(streamName, _categoryName, _streamId, expectedCount, events): struct (bool * FsCodec.ITimelineEvent<'Format>[]) =
        // Where attempts overlap on the same stream, there's a race to raise the Committed event for each 'commit'
        // If we don't serialize the publishing of the events, its possible for handlers to observe the Events out of order
        // NOTE while a Channels based impl might offer better throughput at load, in practical terms serializing all Committed event notifications
        //      works very well as long as the handlers don't do a lot of processing, instead offloading to a private work queue
        // NOTE the lock could be more granular, the guarantee of notification ordering is/needs to be at stream level only
        lock streams <| fun () ->
            let struct (succeeded, _) as outcome = trySync streamName expectedCount events
            if succeeded then committed.Trigger(FsCodec.StreamName.Internal.trust streamName, events)
            outcome

type private StoreCategory<'event, 'state, 'context, 'Format>(store: VolatileStore<'Format>, codec, fold, initial) =
    let res version state events = struct ({ value = null; version = version; streamBytes = -1 }, fold state events)
    let decode events = Array.chooseV (codec : FsCodec.IEventCodec<'event, 'Format, 'context>).TryDecode events
    interface ICategory<'event, 'state, 'context> with
        member _.Empty = res 0 initial Array.empty
        member _.Load(_log, _categoryName, _streamId, streamName, _maxAge, _requireLeader, _ct) = task {
            match store.Load(streamName) with
            | null -> return res 0 initial Array.empty
            | xs -> return res xs.Length initial (decode xs) }
        member _.Sync(_log, categoryName, streamId, streamName, context, token, state, events, _ct) = task {
            let encoded = events |> Array.mapi (fun i e -> FsCodec.Core.TimelineEvent.Create(token.version + int64 i, codec.Encode(context, e)))
            match store.TrySync(streamName, categoryName, streamId, int token.version, encoded) with
            | true, streamEvents' ->
                return SyncResult.Written (res streamEvents'.Length state events)
            | false, conflictingEvents ->
                let eventsSinceExpectedVersion = conflictingEvents |> Seq.skip (int token.version) |> decode
                return SyncResult.Conflict (fun _ct -> task { return res conflictingEvents.Length state eventsSinceExpectedVersion }) }

type MemoryStoreCategory<'event, 'state, 'Format, 'context>(store: VolatileStore<'Format>, name: string, codec, fold, initial) =
    inherit Equinox.Category<'event, 'state, 'context>(name, StoreCategory(store, codec, fold, initial))
