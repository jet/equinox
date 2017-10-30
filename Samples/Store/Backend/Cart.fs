module Backend.Cart

open Domain

type Service(createStream) =
    let stream (id: CartId) =
        sprintf "Cart-%s" id.Value
        |> createStream Cart.Events.Compaction.EventType

    member __.FlowAsync (log : Serilog.ILogger, cartId : CartId, flow, ?prepare) =
        let handler = Cart.Handler(stream cartId)
        handler.FlowAsync(log, flow, ?prepare = prepare)

    member __.Execute (log : Serilog.ILogger) (cartId : CartId) command =
        let handler = Cart.Handler(stream cartId)
        handler.Execute log command

    member __.Read (log : Serilog.ILogger) (cartId : CartId) =
        let handler = Cart.Handler(stream cartId)
        handler.Read log