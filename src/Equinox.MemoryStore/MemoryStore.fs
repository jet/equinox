/// Implements an in-memory store. This fulfils two goals:
/// 1. Acts as A target for integration testing allowing end-to-end processing of a decision flow in an efficient test
/// 2. Illustrates a minimal implementation of the Storage interface interconnects for the purpose of writing Store connectors
namespace Equinox.MemoryStore

open Equinox
open Equinox.Core
open System.Runtime.InteropServices

/// Equivalent to GetEventStore's in purpose; signals a conflict has been detected and reprocessing of the decision will be necessary
exception private WrongVersionException of streamName: string * expected: int * value: obj

/// Internal result used to reflect the outcome of syncing with the entry in the inner ConcurrentDictionary
[<NoEquality; NoComparison>]
type ConcurrentDictionarySyncResult<'t> = Written of 't | Conflict of int

/// Response type for VolatileStore.TrySync to communicate the outcome and updated state of a stream
[<NoEquality; NoComparison>]
type ConcurrentArraySyncResult<'t> = Written of 't | Conflict of 't

/// Maintains a dictionary of ITimelineEvent<'Format>[] per stream-name, allowing one to vary the encoding used to match that of a given concrete store, or optimize test run performance
type VolatileStore<'Format>() =
    let streams = System.Collections.Concurrent.ConcurrentDictionary<string,FsCodec.ITimelineEvent<'Format>[]>()
    let committed = Event<_>()

    [<CLIEvent>]
    member __.Committed : IEvent<FsCodec.StreamName * FsCodec.ITimelineEvent<'Format>[]> = committed.Publish

    /// Loads state from a given stream
    member __.TryLoad streamName = match streams.TryGetValue streamName with false, _ -> None | true, packed -> Some packed

    /// Attempts a synchronization operation - yields conflicting value if sync function decides there is a conflict
    member __.TrySync
        (   streamName, trySyncValue : FsCodec.ITimelineEvent<'Format>[] -> ConcurrentDictionarySyncResult<FsCodec.ITimelineEvent<'Format>[]>,
            events: FsCodec.ITimelineEvent<'Format>[])
        : ConcurrentArraySyncResult<FsCodec.ITimelineEvent<'Format>[]> =
        let seedStream _streamName = events
        let updateValue streamName (currentValue : FsCodec.ITimelineEvent<'Format>[]) =
            match trySyncValue currentValue with
            | ConcurrentDictionarySyncResult.Conflict expectedVersion -> raise <| WrongVersionException (streamName, expectedVersion, box currentValue)
            | ConcurrentDictionarySyncResult.Written value -> value
        try let res = streams.AddOrUpdate(streamName, seedStream, updateValue) |> Written
            committed.Trigger((FsCodec.StreamName.parse streamName, events)) // raise here, once, as updateValue can conceptually be invoked multiple times
            res
        with WrongVersionException(_, _, conflictingValue) -> unbox conflictingValue |> Conflict

type Token = { streamVersion: int; streamName: string }

/// Internal implementation detail of MemoryStreamStore
module private Token =

    let private streamTokenOfIndex streamName (streamVersion : int) : StreamToken =
        {   value = box { streamName = streamName; streamVersion = streamVersion }
            version = int64 streamVersion }
    let (|Unpack|) (token: StreamToken) : Token = unbox<Token> token.value
    /// Represent a stream known to be empty
    let ofEmpty streamName initial = streamTokenOfIndex streamName -1, initial
    let tokenOfArray streamName (value: 'event array) = Array.length value - 1 |> streamTokenOfIndex streamName
    let tokenOfSeq streamName (value: 'event seq) = Seq.length value - 1 |> streamTokenOfIndex streamName
    /// Represent a known array of events (without a known folded State)
    let ofEventArray streamName fold initial (events: 'event array) = tokenOfArray streamName events, fold initial (Seq.ofArray events)
    /// Represent a known array of Events together with the associated state
    let ofEventArrayAndKnownState streamName fold (state: 'state) (events: 'event seq) = tokenOfSeq streamName events, fold state events

/// Represents the state of a set of streams in a style consistent withe the concrete Store types - no constraints on memory consumption (but also no persistence!).
type Category<'event, 'state, 'context, 'Format>(store : VolatileStore<'Format>, codec : FsCodec.IEventCodec<'event,'Format,'context>, fold, initial) =
    let (|Decode|) = Array.choose codec.TryDecode
    interface ICategory<'event, 'state, string, 'context> with
        member __.Load(_log, streamName, _opt) = async {
            match store.TryLoad streamName with
            | None -> return Token.ofEmpty streamName initial
            | Some (Decode events) -> return Token.ofEventArray streamName fold initial events }
        member __.TrySync(_log, Token.Unpack token, state, events : 'event list, context : 'context option) = async {
            let inline map i (e : FsCodec.IEventData<'Format>) =
                FsCodec.Core.TimelineEvent.Create(int64 i, e.EventType, e.Data, e.Meta, e.EventId, e.CorrelationId, e.CausationId, e.Timestamp)
            let encoded : FsCodec.ITimelineEvent<_>[] = events |> Seq.mapi (fun i e -> map (token.streamVersion+i+1) (codec.Encode(context,e))) |> Array.ofSeq
            let trySyncValue currentValue =
                if Array.length currentValue <> token.streamVersion + 1 then ConcurrentDictionarySyncResult.Conflict (token.streamVersion)
                else ConcurrentDictionarySyncResult.Written (Seq.append currentValue encoded |> Array.ofSeq)
            match store.TrySync(token.streamName, trySyncValue, encoded) with
            | ConcurrentArraySyncResult.Conflict conflictingEvents ->
                let resync = async {
                    let version = Token.tokenOfArray token.streamName conflictingEvents
                    let successorEvents = conflictingEvents |> Seq.skip (token.streamVersion + 1) |> List.ofSeq
                    return version, fold state (successorEvents |> Seq.choose codec.TryDecode) }
                return SyncResult.Conflict resync
            | ConcurrentArraySyncResult.Written _ -> return SyncResult.Written <| Token.ofEventArrayAndKnownState token.streamName fold state events }

type Resolver<'event, 'state, 'Format, 'context>(store : VolatileStore<'Format>, codec : FsCodec.IEventCodec<'event,'Format,'context>, fold, initial) =
    let category = Category<'event, 'state, 'context, 'Format>(store, codec, fold, initial)
    let resolveStream streamName context = Stream.create category streamName None context
    member __.Resolve(streamName : FsCodec.StreamName, [<Optional; DefaultParameterValue null>] ?option, [<Optional; DefaultParameterValue null>] ?context : 'context) =
        match FsCodec.StreamName.toString streamName, option with
        | sn, (None|Some AllowStale) -> resolveStream sn context
        | sn, Some AssumeEmpty -> Stream.ofMemento (Token.ofEmpty sn initial) (resolveStream sn context)

    /// Resolve from a Memento being used in a Continuation [based on position and state typically from Stream.CreateMemento]
    member __.FromMemento(Token.Unpack stream as streamToken, state, ?context) =
        Stream.ofMemento (streamToken,state) (resolveStream stream.streamName context)
