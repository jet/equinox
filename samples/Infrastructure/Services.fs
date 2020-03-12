module Samples.Infrastructure.Services

open Domain
open FsCodec
open Microsoft.Extensions.DependencyInjection
open System
open System.Text.Json

[<NoComparison>]
type StreamCodec<'event, 'context> =
    | JsonElementCodec of IEventCodec<'event, JsonElement, 'context>
    | Utf8ArrayCodec of IEventCodec<'event, byte[], 'context>

type StreamResolver(storage) =
    member __.ResolveWithJsonElementCodec
        (   codec : IEventCodec<'event, JsonElement, _>,
            fold: ('state -> 'event seq -> 'state),
            initial: 'state,
            snapshot: (('event -> bool) * ('state -> 'event))) =
        match storage with
        | Storage.StorageConfig.Cosmos (gateway, caching, unfolds, databaseId, containerId) ->
            let store = Equinox.Cosmos.Context(gateway, databaseId, containerId)
            let accessStrategy = if unfolds then Equinox.Cosmos.AccessStrategy.Snapshot snapshot else Equinox.Cosmos.AccessStrategy.Unoptimized
            Equinox.Cosmos.Resolver<'event,'state,_>(store, codec, fold, initial, caching, accessStrategy).Resolve
        | _ -> failwith "Currently, only Cosmos can be used with a JsonElement codec."

    member __.ResolveWithUtf8ArrayCodec
        (   codec : IEventCodec<'event, byte[], _>,
            fold: ('state -> 'event seq -> 'state),
            initial: 'state,
            snapshot: (('event -> bool) * ('state -> 'event))) =
        match storage with
        | Storage.StorageConfig.Es (context, caching, unfolds) ->
            let accessStrategy = if unfolds then Equinox.EventStore.AccessStrategy.RollingSnapshots snapshot |> Some else None
            Equinox.EventStore.Resolver<'event,'state,_>(context, codec, fold, initial, ?caching = caching, ?access = accessStrategy).Resolve
        | Storage.StorageConfig.Memory store ->
            Equinox.MemoryStore.Resolver(store, codec, fold, initial).Resolve
        | Storage.StorageConfig.Sql (context, caching, unfolds) ->
            let accessStrategy = if unfolds then Equinox.SqlStreamStore.AccessStrategy.RollingSnapshots snapshot |> Some else None
            Equinox.SqlStreamStore.Resolver<'event,'state,_>(context, codec, fold, initial, ?caching = caching, ?access = accessStrategy).Resolve
        | _ -> failwith "Only EventStore, Memory Store, and SQL Store can be used with a byte array codec."

type ServiceBuilder(storageConfig, handlerLog) =
     let resolver = StreamResolver(storageConfig)

     member __.CreateFavoritesService() =
        let fold, initial = Favorites.Fold.fold, Favorites.Fold.initial
        let snapshot = Favorites.Fold.isOrigin,Favorites.Fold.snapshot

        match storageConfig with
        | Storage.StorageConfig.Cosmos _ -> resolver.ResolveWithJsonElementCodec(Favorites.Events.codecStj, fold, initial, snapshot)
        | _ -> resolver.ResolveWithUtf8ArrayCodec(Favorites.Events.codecNewtonsoft, fold, initial, snapshot)
        |> Backend.Favorites.create handlerLog

     member __.CreateSaveForLaterService() =
        let fold, initial = SavedForLater.Fold.fold, SavedForLater.Fold.initial
        let snapshot = SavedForLater.Fold.isOrigin,SavedForLater.Fold.compact

        match storageConfig with
        | Storage.StorageConfig.Cosmos _ -> resolver.ResolveWithJsonElementCodec(SavedForLater.Events.codecStj,fold,initial,snapshot)
        | _ -> resolver.ResolveWithUtf8ArrayCodec(SavedForLater.Events.codecNewtonsoft,fold,initial,snapshot)
        |> Backend.SavedForLater.create 50 handlerLog

     member __.CreateTodosService() =
        let fold, initial = TodoBackend.Fold.fold, TodoBackend.Fold.initial
        let snapshot = TodoBackend.Fold.isOrigin, TodoBackend.Fold.snapshot

        match storageConfig with
        | Storage.StorageConfig.Cosmos _ -> resolver.ResolveWithJsonElementCodec(TodoBackend.Events.codecStj,fold,initial,snapshot)
        | _ -> resolver.ResolveWithUtf8ArrayCodec(TodoBackend.Events.codecNewtonsoft,fold,initial,snapshot)
        |> TodoBackend.create handlerLog

let register (services : IServiceCollection, storageConfig, handlerLog) =
    let regF (factory : IServiceProvider -> 'T) = services.AddSingleton<'T>(fun (sp: IServiceProvider) -> factory sp) |> ignore

    regF <| fun _sp -> ServiceBuilder(storageConfig, handlerLog)

    regF <| fun sp -> sp.GetService<ServiceBuilder>().CreateFavoritesService()
    regF <| fun sp -> sp.GetService<ServiceBuilder>().CreateSaveForLaterService()
    regF <| fun sp -> sp.GetService<ServiceBuilder>().CreateTodosService()
