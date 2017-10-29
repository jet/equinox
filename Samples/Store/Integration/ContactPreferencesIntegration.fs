module Samples.Store.Integration.ContactPreferencesIntegration

open Foldunk.EventStore
open Foldunk.MemoryStore
open Swensen.Unquote

#nowarn "1182" // From hereon in, we may have some 'unused' privates (the tests)

let createMemoryStore () =
    new VolatileStore()

let createServiceMem store =
    Backend.ContactPreferences.Service(fun _batchSize _codec _eventTypePredicate -> MemoryStreamBuilder(store).Create)

let createServiceGesWithCompactionSemantics eventStoreConnection =
    Backend.ContactPreferences.Service(fun windowSize predicate -> GesStreamBuilder(eventStoreConnection, windowSize, CompactionStrategy.Predicate predicate).Create)

let createServiceGesWithoutCompactionSemantics eventStoreConnection =
    Backend.ContactPreferences.Service(fun _ignoreWindowSize _ignoreCompactionPredicate -> GesStreamBuilder(eventStoreConnection, defaultBatchSize).Create)

type Tests(testOutputHelper) =
    let testOutput = TestOutputAdapter testOutputHelper
    let createLog () = createLogger (testOutput.Subscribe >> ignore)

    [<AutoData>]
    let ``Can roundtrip in Memory, correctly folding the events`` id value = Async.RunSynchronously <| async {
        let store = createMemoryStore ()
        let log, service = createLog (), createServiceMem store

        let (Domain.ContactPreferences.Id email) = id
        do! service.Update log email value

        let! actual = service.Read log email
        test <@ value = actual @>
    }

    [<AutoData>]
    let ``Can roundtrip against EventStore, correctly folding the events with normal semantics`` id value = Async.RunSynchronously <| async {
        let! eventStoreConnection = connectToLocalEventStoreNode ()
        let log, service = createLog (), createServiceGesWithoutCompactionSemantics eventStoreConnection

        let (Domain.ContactPreferences.Id email) = id
        do! service.Update log email value

        let! actual = service.Read log email
        test <@ value = actual @>
    }

    [<AutoData>]
    let ``Can roundtrip against EventStore, correctly folding the events with compaction semantics`` id value = Async.RunSynchronously <| async {
        let! eventStoreConnection = connectToLocalEventStoreNode ()
        let log, service = createLog (), createServiceGesWithCompactionSemantics eventStoreConnection

        let (Domain.ContactPreferences.Id email) = id
        do! service.Update log email value

        let! actual = service.Read log email
        test <@ value = actual @>
    }