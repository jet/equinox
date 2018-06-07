﻿module Samples.Store.Integration.ContactPreferencesIntegration

open Foldunk.EventStore
open Foldunk.MemoryStore
open Swensen.Unquote

#nowarn "1182" // From hereon in, we may have some 'unused' privates (the tests)

let fold, initial= Domain.ContactPreferences.Folds.fold, Domain.ContactPreferences.Folds.initial

let createMemoryStore () =
    new VolatileStore()
let createServiceMem store =
    Backend.ContactPreferences.Service(fun _batchSize _eventTypePredicate -> MemoryStreamBuilder(store, fold, initial).Create)

let codec = genCodec<Domain.ContactPreferences.Events.Event>
let createServiceGesWithCompactionSemantics connection =
    let mkStream windowSize predicate =
        GesStreamBuilder(createGesGateway connection windowSize, codec, fold, initial, CompactionStrategy.Predicate predicate).Create
    Backend.ContactPreferences.Service(mkStream)
let createServiceGesWithoutCompactionSemantics connection =
    let gateway = createGesGateway connection defaultBatchSize
    Backend.ContactPreferences.Service(fun _ignoreWindowSize _ignoreCompactionPredicate -> GesStreamBuilder(gateway, codec, fold, initial).Create)

type Tests(testOutputHelper) =
    let testOutput = TestOutputAdapter testOutputHelper
    let createLog () = createLogger testOutput

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
        let log = createLog ()
        let! conn = connectToLocalEventStoreNode log
        let service = createServiceGesWithoutCompactionSemantics conn

        let (Domain.ContactPreferences.Id email) = id
        do! service.Update log email value

        let! actual = service.Read log email
        test <@ value = actual @>
    }

    [<AutoData>]
    let ``Can roundtrip against EventStore, correctly folding the events with compaction semantics`` id value = Async.RunSynchronously <| async {
        let log = createLog ()
        let! conn = connectToLocalEventStoreNode log
        let service = createServiceGesWithCompactionSemantics conn

        let (Domain.ContactPreferences.Id email) = id
        do! service.Update log email value

        let! actual = service.Read log email
        test <@ value = actual @>
    }