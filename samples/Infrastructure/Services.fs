﻿module Samples.Infrastructure.Services

open Domain
open Equinox
open FsCodec.SystemTextJson.Interop // use ToJsonElementCodec because we are doing an overkill example
open Microsoft.Extensions.DependencyInjection
open System

type Store(store) =
    member _.Category
        (   name,
            codec: FsCodec.IEventCodec<'event, ReadOnlyMemory<byte>, unit>,
            fold: Func<'state, 'event[], 'state>,
            initial: 'state,
            snapshot: ('event -> bool) * ('state -> 'event)): Category<'event, 'state, unit> =
        match store with
        | Store.Context.Memory store ->
            MemoryStore.MemoryStoreCategory(store, name, codec, fold, initial)
        | Store.Context.Cosmos (store, caching, unfolds) ->
            let accessStrategy = if unfolds then CosmosStore.AccessStrategy.Snapshot snapshot else CosmosStore.AccessStrategy.Unoptimized
            CosmosStore.CosmosStoreCategory<'event,'state,_>(store, name, codec.ToJsonElementCodec(), fold, initial, accessStrategy, caching)
        | Store.Context.Dynamo (store, caching, unfolds) ->
            let accessStrategy = if unfolds then DynamoStore.AccessStrategy.Snapshot snapshot else DynamoStore.AccessStrategy.Unoptimized
            DynamoStore.DynamoStoreCategory<'event,'state,_>(store, name, FsCodec.Deflate.EncodeTryDeflate codec, fold, initial, accessStrategy, caching)
        | Store.Context.Es (context, caching, unfolds) ->
            let accessStrategy = if unfolds then EventStoreDb.AccessStrategy.RollingSnapshots snapshot else EventStoreDb.AccessStrategy.Unoptimized
            EventStoreDb.EventStoreCategory<'event,'state,_>(context, name, codec, fold, initial, accessStrategy, caching)
        | Store.Context.Sql (context, caching, unfolds) ->
            let accessStrategy = if unfolds then SqlStreamStore.AccessStrategy.RollingSnapshots snapshot else SqlStreamStore.AccessStrategy.Unoptimized
            SqlStreamStore.SqlStreamStoreCategory<'event,'state,_>(context, name, codec, fold, initial, accessStrategy, caching)
        | Store.Context.Mdb (context, caching) ->
            MessageDb.MessageDbCategory<'event,'state,_>(context, name, codec, fold, initial, MessageDb.AccessStrategy.Unoptimized, caching)

type ServiceBuilder(storageConfig, handlerLog) =
     let store = Store storageConfig

     member _.CreateFavoritesService() =
        let fold, initial = Favorites.Fold.fold, Favorites.Fold.initial
        let snapshot = Favorites.Fold.Snapshot.config
        store.Category(Favorites.Stream.Category, Favorites.Events.codec, fold, initial, snapshot)
        |> Decider.forStream handlerLog
        |> Favorites.create

     member _.CreateSaveForLaterService() =
        let fold, initial = SavedForLater.Fold.fold, SavedForLater.Fold.initial
        let snapshot = SavedForLater.Fold.Snapshot.config
        store.Category(SavedForLater.Stream.Category, SavedForLater.Events.codec, fold, initial, snapshot)
        |> Decider.forStream handlerLog
        |> SavedForLater.create 50

     member _.CreateTodosService() =
        let fold, initial = TodoBackend.Fold.fold, TodoBackend.Fold.initial
        let snapshot = TodoBackend.Fold.Snapshot.config
        store.Category(TodoBackend.Stream.Category, TodoBackend.Events.codec, fold, initial, snapshot)
        |> Decider.forStream handlerLog
        |> TodoBackend.create

let register (services : IServiceCollection, storageConfig, handlerLog) =
    let regF (factory : IServiceProvider -> 'T) = services.AddSingleton<'T>(fun (sp: IServiceProvider) -> factory sp) |> ignore

    regF <| fun _sp -> ServiceBuilder(storageConfig, handlerLog)

    regF <| fun sp -> sp.GetService<ServiceBuilder>().CreateFavoritesService()
    regF <| fun sp -> sp.GetService<ServiceBuilder>().CreateSaveForLaterService()
    regF <| fun sp -> sp.GetService<ServiceBuilder>().CreateTodosService()
