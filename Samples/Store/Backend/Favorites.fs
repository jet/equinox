module Backend.Favorites

open Domain
open Foldunk

type Handler(streamer : IEventStream<Favorites.Folds.State, Favorites.Events.Event>, log : Serilog.ILogger, clientId : ClientId, maxAttempts) =
    let load log = Handler.load Favorites.Folds.fold Favorites.Folds.initial Favorites.streamName streamer log 
    let execute (ctx : DecisionContext<_,_>) = Favorites.Commands.interpret >> ctx.Execute
    let decide cmd ctx = async {
        cmd |> execute ctx
        return ctx.Complete() }

    member __.Run cmd = async {
        let! syncState = load log clientId
        return! Handler.run log maxAttempts syncState (decide cmd) }

    member __.Load () = async {
        let! state = load log clientId
        return state.State }

type Service(createStreamer, maxAttempts) =
    static let codec = EventSum.generateJsonUtf8SumEncoder<Favorites.Events.Event>
    let createHandler log clientId = Handler(createStreamer codec, log, clientId, maxAttempts = maxAttempts)

    member __.Run (log : Serilog.ILogger) (clientId : ClientId) cmd =
        let handler = createHandler log clientId
        handler.Run cmd

    member __.Load (log : Serilog.ILogger) (clientId : ClientId) =
        let handler = createHandler log clientId
        handler.Load ()