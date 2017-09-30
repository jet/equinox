module Backend.Cart

open Domain

type Service(createStream) =
    let codec = Foldunk.EventSum.generateJsonUtf8SumEncoder<Cart.Events.Event>
    let streamName (id: CartId) = sprintf "Cart-%s" id.Value
    let handler id = Cart.Handler(streamName id |> createStream codec)

    member __.Flow (log : Serilog.ILogger) (cartId : CartId) flow =
        let handler = handler cartId
        handler.Flow log flow

    member __.Execute (log : Serilog.ILogger) (cartId : CartId) command =
        let handler = handler cartId
        handler.Execute log command

    member __.Read (log : Serilog.ILogger) (cartId : CartId) =
        let handler = handler cartId
        handler.Read log