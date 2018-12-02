module Samples.Services

open Microsoft.Extensions.DependencyInjection
open Serilog
open System

let registerServices (services : IServiceCollection, storageConfig) =
    let regF (factory : IServiceProvider -> 'T) = services.AddSingleton<'T>(fun (sp: IServiceProvider) -> factory sp) |> ignore

    regF <| fun _sp -> storageConfig : Config.StorageConfig

    regF <| fun sp -> Streams.Resolver(sp.GetService<Config.StorageConfig>())

    let mkFavorites (log: ILogger, resolver: Streams.Resolver) =
        let fold, initial, snapshot = Domain.Favorites.Folds.fold, Domain.Favorites.Folds.initial, Domain.Favorites.Folds.snapshot
        let codec = Streams.genCodec<Domain.Favorites.Events.Event>()
        Backend.Favorites.Service(log, resolver.Resolve(codec,fold,initial,snapshot))
    regF <| fun sp -> mkFavorites (sp.GetService(), sp.GetService())

    let mkSaves (log: ILogger, resolver: Streams.Resolver) =
        let fold, initial, snapshot = Domain.SavedForLater.Folds.fold, Domain.SavedForLater.Folds.initial, Domain.SavedForLater.Folds.snapshot
        let codec = Streams.genCodec<Domain.SavedForLater.Events.Event>()
        Backend.SavedForLater.Service(log, resolver.Resolve(codec,fold,initial,snapshot), maxSavedItems=50, maxAttempts=3)
    regF <| fun sp -> mkSaves (sp.GetService(), sp.GetService())