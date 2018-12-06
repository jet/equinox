module Samples.Infrastructure.Services

open Microsoft.Extensions.DependencyInjection
open System

let serializationSettings = Newtonsoft.Json.Converters.FSharp.Settings.CreateCorrect()
let genCodec<'Union when 'Union :> TypeShape.UnionContract.IUnionContract>() = Equinox.UnionCodec.JsonUtf8.Create<'Union>(serializationSettings)

type StreamResolver(storage) =
    member __.Resolve
        (   codec : Equinox.UnionCodec.IUnionEncoder<'event,byte[]>,
            fold: ('state -> 'event seq -> 'state),
            initial: 'state,
            snapshot: (('event -> bool) * ('state -> 'event))) =
        match storage with
        | Storage.StorageConfig.Memory store ->
            Equinox.MemoryStore.MemResolver(store, fold, initial).Resolve
        | Storage.StorageConfig.Es (gateway, cache, unfolds) ->
            let accessStrategy = if unfolds then Equinox.EventStore.AccessStrategy.RollingSnapshots snapshot |> Some else None
            Equinox.EventStore.GesResolver<'event,'state>(gateway, codec, fold, initial, ?access = accessStrategy, ?caching = cache).Resolve

type ServiceBuilder(storageConfig, handlerLog) =
     let resolver = StreamResolver(storageConfig)

     member __.CreateFavoritesService() =
        let codec = genCodec<Domain.Favorites.Events.Event>()
        let fold, initial, snapshot = Domain.Favorites.Folds.fold, Domain.Favorites.Folds.initial, Domain.Favorites.Folds.snapshot
        Backend.Favorites.Service(handlerLog, resolver.Resolve(codec,fold,initial,snapshot))

     member __.CreateSaveForLaterService() =
        let codec = genCodec<Domain.SavedForLater.Events.Event>()
        let fold, initial, snapshot = Domain.SavedForLater.Folds.fold, Domain.SavedForLater.Folds.initial, Domain.SavedForLater.Folds.snapshot
        Backend.SavedForLater.Service(handlerLog, resolver.Resolve(codec,fold,initial,snapshot), maxSavedItems=50, maxAttempts=3)

let register (services : IServiceCollection, storageConfig, handlerLog) =
    let regF (factory : IServiceProvider -> 'T) = services.AddSingleton<'T>(fun (sp: IServiceProvider) -> factory sp) |> ignore

    regF <| fun _sp -> ServiceBuilder(storageConfig, handlerLog)

    regF <| fun sp -> sp.GetService<ServiceBuilder>().CreateFavoritesService()
    regF <| fun sp -> sp.GetService<ServiceBuilder>().CreateSaveForLaterService()