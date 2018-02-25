module Samples.Store.Integration.ContactPreferencesIntegration

open Foldunk.EventStore
open Foldunk.MemoryStore
open Foldunk.EventSumCodec
open Swensen.Unquote

#nowarn "1182" // From hereon in, we may have some 'unused' privates (the tests)

let fold, initial= Domain.ContactPreferences.Folds.fold, Domain.ContactPreferences.Folds.initial

let createMemoryStore () =
    new VolatileStore()
let createServiceMem store =
    Backend.ContactPreferences.Service(fun _batchSize _eventTypePredicate -> MemoryStreamBuilder(store, fold, initial).Create)

let codec = generateJsonUtf8SumEncoder<Domain.ContactPreferences.Events.Event>
let createServiceGesWithCompactionSemantics eventStoreConnection =
    let mkStream windowSize predicate =
        GesStreamBuilder(createGesGateway eventStoreConnection windowSize, codec, fold, initial, CompactionStrategy.Predicate predicate).Create
    Backend.ContactPreferences.Service(mkStream)
let createServiceGesWithoutCompactionSemantics eventStoreConnection =
    let gateway = createGesGateway eventStoreConnection defaultBatchSize
    Backend.ContactPreferences.Service(fun _ignoreWindowSize _ignoreCompactionPredicate -> GesStreamBuilder(gateway, codec, fold, initial).Create)

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