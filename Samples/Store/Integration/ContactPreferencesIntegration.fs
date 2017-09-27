module Samples.Store.Integration.ContactPreferencesIntegration

open Swensen.Unquote

#nowarn "1182" // From hereon in, we may have some 'unused' privates (the tests)

let createServiceMem () =
    let store = createMemStore ()
    Backend.ContactPreferences.Service(fun _codec -> createMemStream store)

let createServiceGes eventStoreConnection =
    let gateway = createGesGateway eventStoreConnection 500
    Backend.ContactPreferences.Service(createGesStream gateway)

type Tests(testOutputHelper) =
    let testOutput = TestOutputAdapter testOutputHelper
    let createLog () = createLogger (testOutput.Subscribe >> ignore)

    [<AutoData>]
    let ``Can roundtrip in Memory, correctly folding the events`` id value = Async.RunSynchronously <| async {
        let log, service = createLog (), createServiceMem ()

        let (Domain.ContactPreferences.Id email) = id
        do! service.Update log email value

        let! actual = service.Load log email
        test <@ value = actual @>
    }

    [<AutoData>]
    let ``Can roundtrip against EventStore, correctly folding the events`` id value = Async.RunSynchronously <| async {
        let! eventStoreConnection = connectToLocalEventStoreNode ()
        let log, service = createLog (), createServiceGes eventStoreConnection

        let (Domain.ContactPreferences.Id email) = id
        do! service.Update log email value

        let! actual = service.Load log email
        test <@ value = actual @>
    }