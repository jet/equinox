module Backend.Carts

open Domain
open Domain.Cart
open Foldunk

type CartService (store : IEventStream<int, Cart.Folds.State, Cart.Events.Event>) =
    let load log = Handler.load Cart.Folds.fold Cart.Folds.initial Cart.Commands.streamName store log 

    member __.Execute (log : Serilog.ILogger) (cartId : CartId) decide =async {
        let maxAttempts = 1
        let! syncState = load log cartId
        return! Handler.run log maxAttempts syncState decide }

    member __.Load (log : Serilog.ILogger) (cartId : CartId) = async {
        let! state = load log cartId
        return state.State }