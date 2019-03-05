module Samples.Infrastructure.Services

open Microsoft.Extensions.DependencyInjection
open System

let serializationSettings = Newtonsoft.Json.Converters.FSharp.Settings.CreateCorrect()
let genCodec<'Union when 'Union :> TypeShape.UnionContract.IUnionContract>() = Equinox.Codec.JsonNet.JsonUtf8.Create<'Union>(serializationSettings)

type StreamResolver(storage) =
    member __.Resolve
        (   codec : Equinox.Codec.IUnionEncoder<'event,byte[]>,
            fold: ('state -> 'event seq -> 'state),
            initial: 'state,
            snapshot: (('event -> bool) * ('state -> 'event))) =
        match storage with
        | Storage.StorageConfig.Memory store ->
            Equinox.MemoryStore.MemoryResolver(store, fold, initial).Resolve
        | Storage.StorageConfig.Es (gateway, caching, unfolds) ->
            let accessStrategy = if unfolds then Equinox.EventStore.AccessStrategy.RollingSnapshots snapshot |> Some else None
            Equinox.EventStore.GesResolver<'event,'state>(gateway, codec, fold, initial, ?caching = caching, ?access = accessStrategy).Resolve
        | Storage.StorageConfig.Cosmos (gateway, caching, unfolds, databaseId, collectionId) ->
            let store = Equinox.Cosmos.CosmosStore(gateway, databaseId, collectionId)
            let accessStrategy = if unfolds then Equinox.Cosmos.AccessStrategy.Snapshot snapshot |> Some else None
            Equinox.Cosmos.CosmosResolver<'event,'state>(store, codec, fold, initial, caching, ?access = accessStrategy).Resolve

type ServiceBuilder(storageConfig, handlerLog) =
     let resolver = StreamResolver(storageConfig)

     member __.CreateFavoritesService() =
        let codec = genCodec<Domain.Favorites.Events.Event>()
        let fold, initial = Domain.Favorites.Folds.fold, Domain.Favorites.Folds.initial
        let snapshot = Domain.Favorites.Folds.isOrigin,Domain.Favorites.Folds.compact
        Backend.Favorites.Service(handlerLog, resolver.Resolve(codec,fold,initial,snapshot))

     member __.CreateSaveForLaterService() =
        let codec = genCodec<Domain.SavedForLater.Events.Event>()
        let fold, initial = Domain.SavedForLater.Folds.fold, Domain.SavedForLater.Folds.initial
        let snapshot = Domain.SavedForLater.Folds.isOrigin,Domain.SavedForLater.Folds.compact
        Backend.SavedForLater.Service(handlerLog, resolver.Resolve(codec,fold,initial,snapshot), maxSavedItems=50)

     member __.CreateTodosService() =
        let codec = genCodec<TodoBackend.Events.Event>()
        let fold, initial = TodoBackend.Folds.fold, TodoBackend.Folds.initial
        let snapshot = TodoBackend.Folds.isOrigin, TodoBackend.Folds.compact
        TodoBackend.Service(handlerLog, resolver.Resolve(codec,fold,initial,snapshot))

let register (services : IServiceCollection, storageConfig, handlerLog) =
    let regF (factory : IServiceProvider -> 'T) = services.AddSingleton<'T>(fun (sp: IServiceProvider) -> factory sp) |> ignore

    regF <| fun _sp -> ServiceBuilder(storageConfig, handlerLog)

    regF <| fun sp -> sp.GetService<ServiceBuilder>().CreateFavoritesService()
    regF <| fun sp -> sp.GetService<ServiceBuilder>().CreateSaveForLaterService()
    regF <| fun sp -> sp.GetService<ServiceBuilder>().CreateTodosService()