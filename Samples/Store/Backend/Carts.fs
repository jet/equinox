module Backend.Carts

open Domain
open Domain.Cart
open Foldunk

type Handler(streamer : IEventStream<Folds.State, Events.Event>, log : Serilog.ILogger, cartId : CartId, ?maxAttempts) =
    let load log = Handler.load Folds.fold Folds.initial streamName streamer log

    member __.Run decide = async {
        let! syncState = load log cartId
        return! Handler.run log (defaultArg maxAttempts 3) syncState decide }

    member __.Load () = async {
        let! state = load log cartId
        return state.State }

type Service(createStreamer) =
    let codec = Foldunk.EventSum.generateJsonUtf8SumEncoder<Cart.Events.Event>
    let createHandler log id = Handler(createStreamer codec, log, id, maxAttempts = 3)
    member __.Run (log : Serilog.ILogger) (cartId : CartId) decide =
        let handler = createHandler log cartId
        handler.Run decide

    member __.Load (log : Serilog.ILogger) (cartId : CartId) =
        let handler = createHandler log cartId
        handler.Load ()