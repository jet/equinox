module Samples.Store.Integration.ContactPreferencesIntegration

open Swensen.Unquote

#nowarn "1182" // From hereon in, we may have some 'unused' privates (the tests)

let createServiceMem () =
    let store = createMemStore ()
    Backend.ContactPreferences.Service(fun _batchSize _codec _eventTypePredicate -> createMemStream store)

let createServiceGesWithCompactionSemantics eventStoreConnection =
    Backend.ContactPreferences.Service(createGesStreamWithCompactionPredicate eventStoreConnection)

let createServiceGesWithoutCompactionSemantics eventStoreConnection =
    Backend.ContactPreferences.Service(fun _ignoreWindowSize _ignoreCompactionPredicate -> createGesStream eventStoreConnection defaultBatchSize)

type Tests(testOutputHelper) =
    let testOutput = TestOutputAdapter testOutputHelper
    let createLog () = createLogger (testOutput.Subscribe >> ignore)

    [<AutoData>]
    let ``Can roundtrip in Memory, correctly folding the events`` id value = Async.RunSynchronously <| async {
        let log, service = createLog (), createServiceMem ()

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