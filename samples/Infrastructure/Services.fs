module Samples.Infrastructure.Services

open Domain
open Equinox
open FsCodec.SystemTextJson.Interop // use ToJsonElementCodec because we are doing an overkill example
open Microsoft.Extensions.DependencyInjection
open System

type Store(store) =
    member _.Category
        (   codec: FsCodec.IEventCodec<'event, ReadOnlyMemory<byte>, unit>,
            fold: 'state -> 'event seq -> 'state,
            initial: 'state,
            snapshot: ('event -> bool) * ('state -> 'event)): Category<'event, 'state, unit> =
        match store with
        | Store.Context.Memory store ->
            Equinox.MemoryStore.MemoryStoreCategory(store, codec, fold, initial)
        | Store.Context.Cosmos (store, caching, unfolds) ->
            let accessStrategy = if unfolds then Equinox.CosmosStore.AccessStrategy.Snapshot snapshot else Equinox.CosmosStore.AccessStrategy.Unoptimized
            Equinox.CosmosStore.CosmosStoreCategory<'event,'state,_>(store, codec.ToJsonElementCodec(), fold, initial, caching, accessStrategy)
        | Store.Context.Dynamo (store, caching, unfolds) ->
            let accessStrategy = if unfolds then Equinox.DynamoStore.AccessStrategy.Snapshot snapshot else Equinox.DynamoStore.AccessStrategy.Unoptimized
            Equinox.DynamoStore.DynamoStoreCategory<'event,'state,_>(store, FsCodec.Deflate.EncodeTryDeflate codec, fold, initial, caching, accessStrategy)
        | Store.Context.Es (context, caching, unfolds) ->
            let accessStrategy = if unfolds then Equinox.EventStoreDb.AccessStrategy.RollingSnapshots snapshot |> Some else None
            Equinox.EventStoreDb.EventStoreCategory<'event,'state,_>(context, codec, fold, initial, ?caching = caching, ?access = accessStrategy)
        | Store.Context.Sql (context, caching, unfolds) ->
            let accessStrategy = if unfolds then Equinox.SqlStreamStore.AccessStrategy.RollingSnapshots snapshot |> Some else None
            Equinox.SqlStreamStore.SqlStreamStoreCategory<'event,'state,_>(context, codec, fold, initial, ?caching = caching, ?access = accessStrategy)
        | Store.Context.Mdb (context, caching) ->
            Equinox.MessageDb.MessageDbCategory<'event,'state,_>(context, codec, fold, initial, ?caching = caching)

type ServiceBuilder(storageConfig, handlerLog) =
     let store = Store storageConfig

     member _.CreateFavoritesService() =
        let fold, initial = Favorites.Fold.fold, Favorites.Fold.initial
        let snapshot = Favorites.Fold.isOrigin, Favorites.Fold.snapshot
        store.Category(Favorites.Events.codec, fold, initial, snapshot)
        |> Decider.resolve handlerLog
        |> Favorites.create

     member _.CreateSaveForLaterService() =
        let fold, initial = SavedForLater.Fold.fold, SavedForLater.Fold.initial
        let snapshot = SavedForLater.Fold.isOrigin, SavedForLater.Fold.compact
        store.Category(SavedForLater.Events.codec, fold, initial, snapshot)
        |> Decider.resolve handlerLog
        |> SavedForLater.create 50

     member _.CreateTodosService() =
        let fold, initial = TodoBackend.Fold.fold, TodoBackend.Fold.initial
        let snapshot = TodoBackend.Fold.isOrigin, TodoBackend.Fold.snapshot
        store.Category(TodoBackend.Events.codec, fold, initial, snapshot)
        |> Decider.resolve handlerLog
        |> TodoBackend.create

let register (services : IServiceCollection, storageConfig, handlerLog) =
    let regF (factory : IServiceProvider -> 'T) = services.AddSingleton<'T>(fun (sp: IServiceProvider) -> factory sp) |> ignore

    regF <| fun _sp -> ServiceBuilder(storageConfig, handlerLog)

    regF <| fun sp -> sp.GetService<ServiceBuilder>().CreateFavoritesService()
    regF <| fun sp -> sp.GetService<ServiceBuilder>().CreateSaveForLaterService()
    regF <| fun sp -> sp.GetService<ServiceBuilder>().CreateTodosService()
