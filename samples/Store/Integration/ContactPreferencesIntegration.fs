﻿module Samples.Store.Integration.ContactPreferencesIntegration

open Equinox.Cosmos
open Equinox.Cosmos.Integration
open Equinox.EventStore
open Equinox.MemoryStore
open Swensen.Unquote
open Xunit

#nowarn "1182" // From hereon in, we may have some 'unused' privates (the tests)

let fold, initial = Domain.ContactPreferences.Folds.fold, Domain.ContactPreferences.Folds.initial

let createMemoryStore () =
    new VolatileStore()
let createServiceMemory log store =
    Backend.ContactPreferences.Service(log, MemoryResolver(store, fold, initial).Resolve)

let codec = genCodec<Domain.ContactPreferences.Events.Event>()
let resolveStreamGesWithOptimizedStorageSemantics gateway =
    GesResolver(gateway 1, codec, fold, initial, AccessStrategy.EventsAreState).Resolve
let resolveStreamGesWithoutAccessStrategy gateway =
    GesResolver(gateway defaultBatchSize, codec, fold, initial).Resolve

let resolveStreamCosmosWithKnownEventTypeSemantics gateway =
    CosmosResolver(gateway 1, codec, fold, initial, AccessStrategy.AnyKnownEventType).Resolve
let resolveStreamCosmosWithoutCustomAccessStrategy gateway =
    CosmosResolver(gateway defaultBatchSize, codec, fold, initial).Resolve

type Tests(testOutputHelper) =
    let testOutput = TestOutputAdapter testOutputHelper
    let createLog () = createLogger testOutput

    let act (service : Backend.ContactPreferences.Service) (id,value) = async {
        let (Domain.ContactPreferences.Id email) = id
        do! service.Update email value

        let! actual = service.Read email
        test <@ value = actual @> }

    [<AutoData>]
    let ``Can roundtrip in Memory, correctly folding the events`` args = Async.RunSynchronously <| async {
        let service = let log, store = createLog (), createMemoryStore () in createServiceMemory log store
        do! act service args
    }

    let arrange connect choose resolveStream = async {
        let log = createLog ()
        let! conn = connect log
        let gateway = choose conn
        return Backend.ContactPreferences.Service(log, resolveStream gateway) }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_EVENTSTORE")>]
    let ``Can roundtrip against EventStore, correctly folding the events with normal semantics`` args = Async.RunSynchronously <| async {
        let! service = arrange connectToLocalEventStoreNode createGesGateway resolveStreamGesWithoutAccessStrategy
        do! act service args
    }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_EVENTSTORE")>]
    let ``Can roundtrip against EventStore, correctly folding the events with compaction semantics`` args = Async.RunSynchronously <| async {
        let! service = arrange connectToLocalEventStoreNode createGesGateway resolveStreamGesWithOptimizedStorageSemantics
        do! act service args
    }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``Can roundtrip against Cosmos, correctly folding the events with normal semantics`` args = Async.RunSynchronously <| async {
        let! service = arrange connectToSpecifiedCosmosOrSimulator createCosmosStore resolveStreamCosmosWithoutCustomAccessStrategy
        do! act service args
    }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``Can roundtrip against Cosmos, correctly folding the events with compaction semantics`` args = Async.RunSynchronously <| async {
        let! service = arrange connectToSpecifiedCosmosOrSimulator createCosmosStore resolveStreamCosmosWithKnownEventTypeSemantics
        do! act service args
    }