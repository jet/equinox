module Backend.Favorites

open Domain
open Foldunk

type FavoritesService(streamer : Handler.IEventStream<Favorites.Folds.State, Favorites.Events.Event>, ?maxAttempts) =
    let load log = Handler.load Favorites.Folds.fold Favorites.Folds.initial Favorites.streamName streamer log 
    let execute (ctx : DecisionState<_,_>) = Favorites.Commands.interpret >> ctx.Execute
    let decide cmd ctx = async {
        cmd |> execute ctx
        return ctx.Complete() }

    member __.Run (log : Serilog.ILogger) (clientId : ClientId) cmd = async {
        let! syncState = load log clientId
        return! Handler.run log (defaultArg maxAttempts 1) syncState (decide cmd) }

    member __.Load (log : Serilog.ILogger) (clientId : ClientId) = async {
        let! state = load log clientId
        return state.State }