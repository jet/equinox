module Example.Integration.ContactPreferencesIntegration

open Domain
open Backend // Must shadow Domain
open Swensen.Unquote

#nowarn "1182" // From hereon in, we may have some 'unused' privates (the tests)

let createHandlerWithInMemoryStore id =
    ContactPreferences.Handler(createInMemoryStreamer(), id, maxAttempts = 5)

let createContactPreferencesService eventStoreConnection =
    ContactPreferences.Service(createGesStreamer eventStoreConnection 500)

type Tests(testOutputHelper) =
    let testOutput = TestOutputAdapter testOutputHelper
    let createLog () = createLogger (testOutput.Subscribe >> ignore)

    [<AutoData>]
    let ``Can roundtrip against in memory store, correctly folding the events`` id value = Async.RunSynchronously <| async {
        let log, handler = createLog (), createHandlerWithInMemoryStore id

        let (ContactPreferences.Id email) = id
        let cmd = ContactPreferences.Commands.Update { email = email; preferences = value }
        do! handler.Run log cmd

        let! actual = handler.Load log
        test <@ value = actual @>
    }

    [<AutoData>]
    let ``Can roundtrip against EventStore, correctly folding the events`` id value = Async.RunSynchronously <| async {
        let! eventStoreConnection = connectToLocalEventStoreNode ()
        let log, service = createLog (), createContactPreferencesService eventStoreConnection

        let (ContactPreferences.Id email) = id
        do! service.Update log email value

        let! actual = service.Load log email
        test <@ value = actual @>
    }