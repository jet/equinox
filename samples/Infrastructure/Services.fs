﻿module Samples.Infrastructure.Services

open Domain
open FsCodec.SystemTextJson
open Microsoft.Extensions.DependencyInjection
open System

type StreamResolver(storage) =
    member _.Resolve
        (   codec : FsCodec.IEventCodec<'event,byte[],_>,
            fold: 'state -> 'event seq -> 'state,
            initial : 'state,
            snapshot : ('event -> bool) * ('state -> 'event)) =
        match storage with
        | Storage.StorageConfig.Cosmos (store, caching, unfolds) ->
            let accessStrategy = if unfolds then Equinox.CosmosStore.AccessStrategy.Snapshot snapshot else Equinox.CosmosStore.AccessStrategy.Unoptimized
            Equinox.CosmosStore.CosmosStoreCategory<'event,'state,_>(store, codec.ToJsonElementCodec(), fold, initial, caching, accessStrategy).Resolve
        | Storage.StorageConfig.Es (context, caching, unfolds) ->
            let accessStrategy = if unfolds then Equinox.EventStore.AccessStrategy.RollingSnapshots snapshot |> Some else None
            Equinox.EventStore.EventStoreCategory<'event,'state,_>(context, codec, fold, initial, ?caching = caching, ?access = accessStrategy).Resolve
        | Storage.StorageConfig.Memory store ->
            Equinox.MemoryStore.MemoryStoreCategory(store, codec, fold, initial).Resolve
        | Storage.StorageConfig.Sql (context, caching, unfolds) ->
            let accessStrategy = if unfolds then Equinox.SqlStreamStore.AccessStrategy.RollingSnapshots snapshot |> Some else None
            Equinox.SqlStreamStore.SqlStreamStoreCategory<'event,'state,_>(context, codec, fold, initial, ?caching = caching, ?access = accessStrategy).Resolve

type ServiceBuilder(storageConfig, handlerLog) =
     let cat = StreamResolver(storageConfig)

     member __.CreateFavoritesService() =
        let fold, initial = Favorites.Fold.fold, Favorites.Fold.initial
        let snapshot = Favorites.Fold.isOrigin,Favorites.Fold.snapshot
        Favorites.create handlerLog (cat.Resolve(Favorites.Events.codec,fold,initial,snapshot))

     member __.CreateSaveForLaterService() =
        let fold, initial = SavedForLater.Fold.fold, SavedForLater.Fold.initial
        let snapshot = SavedForLater.Fold.isOrigin,SavedForLater.Fold.compact
        SavedForLater.create 50 handlerLog (cat.Resolve(SavedForLater.Events.codec,fold,initial,snapshot))

     member __.CreateTodosService() =
        let fold, initial = TodoBackend.Fold.fold, TodoBackend.Fold.initial
        let snapshot = TodoBackend.Fold.isOrigin, TodoBackend.Fold.snapshot
        TodoBackend.create handlerLog (cat.Resolve(TodoBackend.Events.codec,fold,initial,snapshot))

let register (services : IServiceCollection, storageConfig, handlerLog) =
    let regF (factory : IServiceProvider -> 'T) = services.AddSingleton<'T>(fun (sp: IServiceProvider) -> factory sp) |> ignore

    regF <| fun _sp -> ServiceBuilder(storageConfig, handlerLog)

    regF <| fun sp -> sp.GetService<ServiceBuilder>().CreateFavoritesService()
    regF <| fun sp -> sp.GetService<ServiceBuilder>().CreateSaveForLaterService()
    regF <| fun sp -> sp.GetService<ServiceBuilder>().CreateTodosService()
