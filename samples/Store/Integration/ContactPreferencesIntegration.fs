﻿module Samples.Store.Integration.ContactPreferencesIntegration

open Domain
open Equinox
open Equinox.CosmosStore.Integration.CosmosFixtures
open Swensen.Unquote

let fold, initial = ContactPreferences.Fold.fold, ContactPreferences.Fold.initial

let CategoryName = ContactPreferences.CategoryName
let createMemoryStore () = MemoryStore.VolatileStore<_>()
let createServiceMemory log store =
    MemoryStore.MemoryStoreCategory(store, CategoryName, FsCodec.Box.Codec.Create(), fold, initial)
    |> Decider.forStream log
    |> ContactPreferences.create

let codec = ContactPreferences.Events.codec
let codecJe = ContactPreferences.Events.codecJe
let categoryGesWithOptimizedStorageSemantics context =
    EventStoreDb.EventStoreCategory(context 1, CategoryName, codec, fold, initial, EventStoreDb.AccessStrategy.LatestKnownEvent, CachingStrategy.NoCaching)
let categoryGesWithoutAccessStrategy context =
    EventStoreDb.EventStoreCategory(context defaultBatchSize, CategoryName, codec, fold, initial, EventStoreDb.AccessStrategy.Unoptimized, CachingStrategy.NoCaching)

let categoryCosmosWithLatestKnownEventSemantics context =
    CosmosStore.CosmosStoreCategory(context, CategoryName, codecJe, fold, initial, CosmosStore.AccessStrategy.LatestKnownEvent, CachingStrategy.NoCaching)
let categoryCosmosUnoptimized context =
    CosmosStore.CosmosStoreCategory(context, CategoryName, codecJe, fold, initial, CosmosStore.AccessStrategy.Unoptimized, CachingStrategy.NoCaching)
let categoryCosmosRollingUnfolds context =
    let access = CosmosStore.AccessStrategy.Custom(ContactPreferences.Fold.isOrigin, ContactPreferences.Fold.transmute)
    CosmosStore.CosmosStoreCategory(context, CategoryName, codecJe, fold, initial, access, CachingStrategy.NoCaching)

type Tests(testOutputHelper) =
    let testOutput = TestOutput testOutputHelper
    let log = testOutput.CreateLogger()

    let act (service: ContactPreferences.Service) (id,value) = async {
        do! service.Update(id, value)

        let! actual = service.Read id
        test <@ value = actual @> }

    [<AutoData>]
    let ``Can roundtrip in Memory, correctly folding the events`` args = async {
        let store = createMemoryStore ()
        let service = createServiceMemory log store
        do! act service args }

    let arrangeEs connect choose createCategory = async {
        let client = connect log
        let context = choose client
        return ContactPreferences.create (createCategory context |> Decider.forStream log) }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_EVENTSTORE")>]
    let ``Can roundtrip against EventStore, correctly folding the events with normal semantics`` args = async {
        let! service = arrangeEs connectToLocalEventStoreNode createContext categoryGesWithoutAccessStrategy
        do! act service args }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_EVENTSTORE")>]
    let ``Can roundtrip against EventStore, correctly folding the events with compaction semantics`` args = async {
        let! service = arrangeEs connectToLocalEventStoreNode createContext categoryGesWithOptimizedStorageSemantics
        do! act service args }

    let arrangeCosmos connect createCategory queryMaxItems =
        let context: CosmosStore.CosmosStoreContext = connect log queryMaxItems
        ContactPreferences.create (createCategory context |> Decider.forStream log)

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``Can roundtrip against Cosmos, correctly folding the events with Unoptimized semantics`` args = async {
        let service = arrangeCosmos createPrimaryContext categoryCosmosUnoptimized defaultQueryMaxItems
        do! act service args }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``Can roundtrip against Cosmos, correctly folding the events with LatestKnownEvent semantics`` args = async {
        let service = arrangeCosmos createPrimaryContext categoryCosmosWithLatestKnownEventSemantics 1
        do! act service args }

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``Can roundtrip against Cosmos, correctly folding the events with RollingUnfold semantics`` args = async {
        let service = arrangeCosmos createPrimaryContext categoryCosmosRollingUnfolds defaultQueryMaxItems
        do! act service args }
