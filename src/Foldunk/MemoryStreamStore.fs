namespace Foldunk.Stores

open Serilog
open Foldunk

exception private WrongVersionException of streamName: string * expected: int * value: obj

module private EventArraySteamState =
    let tokenOfArray (value: 'event array) = Array.length value - 1
    /// Represent a known array of events (without a known folded State)
    let ofEventArray (events: 'event array) = tokenOfArray events, None, List.ofArray events
    /// Represent a known array of Events together with the associated state
    let ofEventArrayAndKnownState (events: 'event array) (state: 'state) = tokenOfArray events, Some state, []

// Maintains a dictionary of boxed typed arrays, raising exceptions if an attempt to extract a value encounters a mismatched type
type private ConcurrentArrayStore() =
    let streams = System.Collections.Concurrent.ConcurrentDictionary<string,obj>()
    let mkBadValueException (log : ILogger) streamName (value : obj) =
        let desc = match value with null -> "null" | v -> v.GetType().FullName
        let ex : exn = invalidOp (sprintf "Could not read stream %s, value was a: %s" streamName desc)
        log.Error<_,_>(ex, "Read Bad Value {StreamName} {Value}", streamName, value)
        ex
    let mkWrongVersionException (log : ILogger) streamName expected (value: obj) =
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
    member __.TrySync streamName log trySyncValue events =
        let seedStream _streamName = __.Pack events 
        let updatePackedValue streamName (packedCurrentValue : obj) =
            let currentValue = __.Unpack log streamName packedCurrentValue
            match trySyncValue currentValue with
            | Error expected -> raise (mkWrongVersionException log streamName expected packedCurrentValue)
            | Ok value -> __.Pack value
        try
            let boxedSyncedValue = streams.AddOrUpdate(streamName, seedStream, updatePackedValue)
            Ok (unbox boxedSyncedValue)
        with WrongVersionException(_, _, conflictingValue) ->
            Error (unbox conflictingValue)
 
/// In memory implementation of a stream store - no constraints on memory consumption (but also no persistence!).
type MemoryStreamStore<'state,'event>() =
    let store = ConcurrentArrayStore()
    interface IEventStream<int,'state,'event> with
        member __.Load streamName log = async {
            match store.TryLoad streamName log with
            | None -> return StreamState.ofEmpty
            | Some events -> return EventArraySteamState.ofEventArray events }
        member __.TrySync streamName log (token, snapshotState) (events: 'event list, proposedState) = async {
            let trySyncValue currentValue =
                if Array.length currentValue <> token + 1 then Error token
                else Ok (Seq.append currentValue events)
            match store.TrySync streamName log trySyncValue events with
            | Error conflictingEvents ->
                let resync = async {
                    let version = EventArraySteamState.tokenOfArray conflictingEvents
                    let successorEvents = conflictingEvents |> Seq.skip (token+1) |> List.ofSeq
                    return StreamState.ofTokenSnapshotAndEvents version snapshotState successorEvents }
                return Error resync
            | Ok events -> return Ok <| EventArraySteamState.ofEventArrayAndKnownState events proposedState }