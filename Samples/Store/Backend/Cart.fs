module Backend.Cart

open Domain

type Service(createStream) =
    static let codec = Foldunk.EventSum.generateJsonUtf8SumEncoder<Cart.Events.Event>
    let stream (id: CartId) =
        sprintf "Cart-%s" id.Value
        |> createStream (Some Cart.Events.Compaction.EventType) codec

    member __.Flow (log : Serilog.ILogger) (cartId : CartId) flow =
        let handler = Cart.Handler(stream cartId)
        handler.Flow log flow

    member __.Execute (log : Serilog.ILogger) (cartId : CartId) command =
        let handler = Cart.Handler(stream cartId)
        handler.Execute log command

    member __.Read (log : Serilog.ILogger) (cartId : CartId) =
        let handler = Cart.Handler(stream cartId)
        handler.Read log