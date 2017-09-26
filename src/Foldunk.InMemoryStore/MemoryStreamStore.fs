namespace Foldunk.Stores.InMemoryStore

open Foldunk
open Serilog

exception private WrongVersionException of streamName: string * expected: int * value: obj

[<NoEquality; NoComparison>]
type ImsSyncResultInner<'t> = Written of 't | Conflict of int

[<NoEquality; NoComparison>]
type ImsSyncResult<'t> = Written of 't | Conflict of 't

// Maintains a dictionary of boxed typed arrays, raising exceptions if an attempt to extract a value encounters a mismatched type
type private ConcurrentArrayStore() =
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
    member __.TrySync streamName log (trySyncValue : 'events array -> ImsSyncResultInner<'event seq>) (events: 'event seq) : ImsSyncResult<'event array> =
        let seedStream _streamName = __.Pack events
        let updatePackedValue streamName (packedCurrentValue : obj) =
            let currentValue = __.Unpack log streamName packedCurrentValue
            match trySyncValue currentValue with
            | ImsSyncResultInner.Conflict expectedVersion -> raise (mkWrongVersionException log streamName expectedVersion packedCurrentValue)
            | ImsSyncResultInner.Written value -> __.Pack value
        try
            let boxedSyncedValue = streams.AddOrUpdate(streamName, seedStream, updatePackedValue)
            ImsSyncResult.Written (unbox boxedSyncedValue)
        with WrongVersionException(_, _, conflictingValue) ->
            ImsSyncResult.Conflict (unbox conflictingValue)

/// Internal impl details of MemoryStreamStore
module private MemoryStreamStreamState =
    let private streamTokenOfIndex (streamVersion : int) : Internal.StreamToken =
        { value = box streamVersion }
    /// Represent a stream known to be empty
    let ofEmpty () = streamTokenOfIndex -1, None, []
    let tokenOfArray (value: 'event array) = Array.length value - 1 |> streamTokenOfIndex
    /// Represent a known array of events (without a known folded State)
    let ofEventArray (events: 'event array) = tokenOfArray events, None, List.ofArray events
    /// Represent a known array of Events together with the associated state
    let ofEventArrayAndKnownState (events: 'event array) (state: 'state) = tokenOfArray events, Some state, []

/// In memory implementation of a stream store - no constraints on memory consumption (but also no persistence!).
type MemoryStreamStore<'state, 'event>() =
    let store = ConcurrentArrayStore()
    interface IEventStream<'state, 'event> with
        member __.Load streamName log = async {
            match store.TryLoad streamName log with
            | None -> return MemoryStreamStreamState.ofEmpty ()
            | Some events -> return MemoryStreamStreamState.ofEventArray events }
        member __.TrySync streamName log (token, snapshotState) (events: 'event list, proposedState) = async {
            let trySyncValue currentValue =
                if Array.length currentValue <> unbox token + 1 then ImsSyncResultInner.Conflict (unbox token)
                else ImsSyncResultInner.Written (Seq.append currentValue events)
            match store.TrySync streamName log trySyncValue events with
            | ImsSyncResult.Conflict conflictingEvents ->
                let resync = async {
                    let version = MemoryStreamStreamState.tokenOfArray conflictingEvents
                    let successorEvents = conflictingEvents |> Seq.skip (unbox token + 1) |> List.ofSeq
                    return Internal.StreamState.ofTokenSnapshotAndEvents version snapshotState successorEvents }
                return Internal.SyncResult.Conflict resync
            | ImsSyncResult.Written events -> return Internal.SyncResult.Written <| MemoryStreamStreamState.ofEventArrayAndKnownState events proposedState }