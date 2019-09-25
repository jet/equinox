/// Implements an in-memory store. This fulfils two goals:
/// 1. Acts as A target for integration testing allowing end-to-end processing of a decision flow in an efficient test
/// 2. Illustrates a minimal implemention of the Storage interface interconnects for the purpose of writing Store connectors
namespace Equinox.MemoryStore

open Equinox
open Equinox.Core
open Serilog
open System.Runtime.InteropServices

/// Equivalent to GetEventStore's in purpose; signals a conflict has been detected and reprocessing of the decision will be necessary
exception private WrongVersionException of streamName: string * expected: int * value: obj

/// Internal result used to reflect the outcome of syncing with the entry in the inner ConcurrentDictionary
[<NoEquality; NoComparison>]
type ConcurrentDictionarySyncResult<'t> = Written of 't | Conflict of int

/// Response type for VolatileStore.TrySync to communicate the outcome and updated state of a stream
[<NoEquality; NoComparison>]
type ConcurrentArraySyncResult<'t> = Written of 't | Conflict of 't

// Maintains a dictionary of boxed typed arrays, raising exceptions if an attempt to extract a value encounters a mismatched type
type VolatileStore() =
    let streams = System.Collections.Concurrent.ConcurrentDictionary<string,obj>()
    let mkBadValueException (log : ILogger) streamName (value : obj) =
        let desc = match value with null -> "null" | v -> v.GetType().FullName
        let ex : exn = invalidOp (sprintf "Could not read stream %s, value was a: %s" streamName desc)
        log.Error<_,_>(ex, "Read Bad Value {StreamName} {Value}", streamName, value)
        ex
    let mkWrongVersionException (log : ILogger) streamName (expected : int) (value: obj) =
        let ex : exn = WrongVersionException (streamName, expected, value)
        log.Warning<_,_,_>(ex, "Unexpected Stored Value {StreamName} {Expected} {Value}", streamName, expected, value)
        ex
    member private __.Unpack<'event> log streamName (x : obj): 'event array =
        match x with
        | :? ('event array) as value -> value
        | value -> raise (mkBadValueException log streamName value)
    member private __.Pack (events : 'event seq) : obj =
        Array.ofSeq events |> box

    /// Loads state from a given stream
    member __.TryLoad streamName log =
        match streams.TryGetValue streamName with
        | false, _ -> None
        | true, packed -> __.Unpack log streamName packed |> Some

    /// Attempts a sychronization operation - yields conflicting value if sync function decides there is a conflict
    member __.TrySync streamName (log : ILogger) (trySyncValue : 'events array -> ConcurrentDictionarySyncResult<'event seq>) (events: 'event seq)
        : ConcurrentArraySyncResult<'event array> =
        let seedStream _streamName = __.Pack events
        let updatePackedValue streamName (packedCurrentValue : obj) =
            let currentValue = __.Unpack log streamName packedCurrentValue
            match trySyncValue currentValue with
            | ConcurrentDictionarySyncResult.Conflict expectedVersion -> raise (mkWrongVersionException log streamName expectedVersion packedCurrentValue)
            | ConcurrentDictionarySyncResult.Written value -> __.Pack value
        try
            let boxedSyncedValue = streams.AddOrUpdate(streamName, seedStream, updatePackedValue)
            ConcurrentArraySyncResult.Written (unbox boxedSyncedValue)
        with WrongVersionException(_, _, conflictingValue) ->
            ConcurrentArraySyncResult.Conflict (unbox conflictingValue)

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
    /// Represent a known array of events (without a known folded State)
    let ofEventArray streamName fold initial (events: 'event array) = tokenOfArray streamName events, fold initial (Seq.ofArray events)
    /// Represent a known array of Events together with the associated state
    let ofEventArrayAndKnownState streamName fold (state: 'state) (events: 'event array) = tokenOfArray streamName events, fold state events

/// Represents the state of a set of streams in a style consistent withe the concrete Store types - no constraints on memory consumption (but also no persistence!).
type Category<'event, 'state>(store : VolatileStore, fold, initial) =
    interface ICategory<'event, 'state, string> with
        member __.Load streamName (log : ILogger) = async {
            match store.TryLoad<'event> streamName log with
            | None -> return Token.ofEmpty streamName initial
            | Some events -> return Token.ofEventArray streamName fold initial events }
        member __.TrySync (log : ILogger) (Token.Unpack token, state) (events : 'event list) = async {
            let trySyncValue currentValue =
                if Array.length currentValue <> token.streamVersion + 1 then ConcurrentDictionarySyncResult.Conflict (token.streamVersion)
                else ConcurrentDictionarySyncResult.Written (Seq.append currentValue events)
            match store.TrySync token.streamName (log : ILogger) trySyncValue events with
            | ConcurrentArraySyncResult.Conflict conflictingEvents ->
                let resync = async {
                    let version = Token.tokenOfArray token.streamName conflictingEvents
                    let successorEvents = conflictingEvents |> Seq.skip (token.streamVersion + 1) |> List.ofSeq
                    return version, fold state (Seq.ofList successorEvents) }
                return SyncResult.Conflict resync
            | ConcurrentArraySyncResult.Written events -> return SyncResult.Written <| Token.ofEventArrayAndKnownState token.streamName fold state events }

type Resolver<'event, 'state>(store : VolatileStore, fold, initial) =
    let category = Category<'event,'state>(store, fold, initial)
    let resolveStream streamName = Stream.create category streamName
    let resolveTarget = function AggregateId (cat,streamId) -> sprintf "%s-%s" cat streamId | StreamName streamName -> streamName
    member __.Resolve(target : Target, [<Optional; DefaultParameterValue null>] ?option) =
        match resolveTarget target, option with
        | sn, None -> resolveStream sn
        | sn, Some AssumeEmpty -> Stream.ofMemento (Token.ofEmpty sn initial) (resolveStream sn)

    /// Resolve from a Memento being used in a Continuation [based on position and state typically from Stream.CreateMemento]
    member __.FromMemento(Token.Unpack stream as streamToken,state) =
        Stream.ofMemento (streamToken,state) (resolveStream stream.streamName)