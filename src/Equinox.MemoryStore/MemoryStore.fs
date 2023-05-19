﻿// Implements an in-memory store. This fulfils two goals:
// 1. Acts as a target for integration testing allowing end-to-end processing of a decision flow in an efficient test
// 2. Illustrates a minimal implementation of the Storage interface interconnects for the purpose of writing Store connectors
namespace Equinox.MemoryStore

open Equinox.Core
open System.Threading.Tasks

/// Each Sync that results in an append to the store is notified via the Store's `Committed` event
type Commit<'Format> = (struct (FsCodec.StreamName * FsCodec.ITimelineEvent<'Format>[]))

/// Maintains a dictionary of ITimelineEvent<'Format>[] per stream-name, allowing one to vary the encoding used to match
/// that of a given concrete store, or optimize test run performance
type VolatileStore<'Format> private (lock) =

    let streams = System.Collections.Concurrent.ConcurrentDictionary<string, FsCodec.ITimelineEvent<'Format>[]>()

    let seedStream _streamName struct (_expectedCount, events) = events
    let updateValue _streamName (currentValue : FsCodec.ITimelineEvent<'Format>[]) struct (expectedCount, events) =
        if currentValue.Length <> expectedCount then currentValue
        // note we don't publish here, as this function can be potentially invoked multiple times where there is a race
        else Array.append currentValue events
    let trySync streamName expectedCount events : struct (bool * FsCodec.ITimelineEvent<'Format>[]) =
        let res = streams.AddOrUpdate(streamName, seedStream, updateValue, (expectedCount, events))
        (obj.ReferenceEquals(Array.last res, Array.last events), res)

    let committed = Event<Commit<'Format>>()
    /// Notifies re batches of events being committed to a given Stream.
    /// Commits are guaranteed to be notified in correct order at stream level under concurrent Equinox Transact calls.
    /// NOTE the caller should inspect and/or copy the event efficiently and immediately
    /// NOTE blocking and/or running reactions synchronously will hamper test performance and/or may result in deadlock
    [<CLIEvent>] member _.Committed : IEvent<Commit<'Format>> = committed.Publish

    /// Loads events from a given stream, null if none yet written
    member _.Load(streamName) =
        let mutable events = Unchecked.defaultof<_>
        streams.TryGetValue(streamName, &events) |> ignore
        events

    /// Attempts a synchronization operation - yields conflicting value if expectedCount does not match
    member _.TrySync(streamName, _categoryName, _streamId, expectedCount, events) : struct (bool * FsCodec.ITimelineEvent<'Format>[]) =
        // Where attempts overlap on the same stream, there's a race to raise the Committed event for each 'commit'
        // If we don't serialize the publishing of the events, its possible for handlers to observe the Events out of order
        // NOTE while a Channels based impl might offer better throughput at load, in practical terms serializing all Committed event notifications
        //      works very well as long as the handlers don't do a lot of processing, instead offloading to a private work queue
        // NOTE the lock could be more granular, the guarantee of notification ordering is/needs to be at stream level only
        lock streams <| fun () ->
            let struct (succeeded, _) as outcome = trySync streamName expectedCount events
            if succeeded then committed.Trigger(FSharp.UMX.UMX.tag streamName, events)
            outcome

    /// As per note in TrySync, it's a really bad idea to trigger callbacks (and especially nested ones) in reaction to the Committed event
    /// It's recommended to make use of the constructs in Propulsion.MemoryStore instead of resorting to that
    /// This hack will be removed in V4
    [<System.Obsolete>]
    static member WithOrderingGuaranteesDisabled() = VolatileStore(fun _ f -> f ())
    new () = VolatileStore(lock)

type Token = int

/// Internal implementation detail of MemoryStore
module private Token =

    let private streamTokenOfEventCount (eventCount : int) : StreamToken =
        // TOCONSIDER Could implement streamBytes tracking based on a supplied event size function (store is agnostic to format)
        { value = box eventCount; version = int64 eventCount; streamBytes = -1 }
    let (|Unpack|) (token : StreamToken) : int = unbox<Token> token.value
    /// Represent a stream known to be empty
    let ofEmpty = streamTokenOfEventCount 0
    let ofValue (value : 'event[]) = streamTokenOfEventCount value.Length

/// Represents the state of a set of streams in a style consistent withe the concrete Store types - no constraints on memory consumption (but also no persistence!).
type private Category<'event, 'state, 'context, 'Format>(store: VolatileStore<'Format>, codec: FsCodec.IEventCodec<'event, 'Format, 'context>, fold, initial) =
    interface ICategory<'event, 'state, 'context> with
        member _.Load(_log, _categoryName, _streamId, streamName, _maxAge, _requireLeader, _ct) =
            match store.Load(streamName) with
            | null -> struct (Token.ofEmpty, initial) |> Task.FromResult
            | xs -> struct (Token.ofValue xs, fold initial (Seq.chooseV codec.TryDecode xs)) |> Task.FromResult
        member _.TrySync(_log, categoryName, streamId, streamName, context, _init, Token.Unpack eventCount, state, events, _ct) =
            let inline map i (e : FsCodec.IEventData<'Format>) = FsCodec.Core.TimelineEvent.Create(int64 i, e)
            let encoded = Array.ofSeq events |> Array.mapi (fun i e -> map (eventCount + i) (codec.Encode(context, e)))
            match store.TrySync(streamName, categoryName, streamId, eventCount, encoded) with
            | true, streamEvents' ->
                SyncResult.Written (Token.ofValue streamEvents', fold state events) |> Task.FromResult
            | false, conflictingEvents ->
                let resync _ct =
                    let token' = Token.ofValue conflictingEvents
                    struct (token', fold state (conflictingEvents |> Seq.skip eventCount |> Seq.chooseV codec.TryDecode)) |> Task.FromResult
                SyncResult.Conflict resync |> Task.FromResult

type MemoryStoreCategory<'event, 'state, 'Format, 'context> internal (resolveInner, empty) =
    inherit Equinox.Category<'event, 'state, 'context>(resolveInner, empty)
    new(store: VolatileStore<'Format>, codec: FsCodec.IEventCodec<'event, 'Format, 'context>, fold, initial) =
        let impl = Category<'event, 'state, 'context, 'Format>(store, codec, fold, initial) :> ICategory<_, _, _>
        let resolveInner categoryName streamId = struct (impl, StreamName.render categoryName streamId, ValueNone)
        let empty = struct (Token.ofEmpty, initial)
        MemoryStoreCategory(resolveInner, empty)
