module Backend.Cart

open Domain

type Service(resolveStream) =
    let stream (id: CartId) =
        sprintf "Cart-%s" id.Value
        |> resolveStream Cart.Events.Compaction.EventType

    member __.FlowAsync (log : Serilog.ILogger, cartId : CartId, flow, ?prepare) =
        Cart.Handler(log, stream cartId)
            .FlowAsync(flow, ?prepare = prepare)

    member __.Execute (log : Serilog.ILogger) (cartId : CartId) command =
        Cart.Handler(log, stream cartId)
            .Execute command

    member __.Read (log : Serilog.ILogger) (cartId : CartId) =
        Cart.Handler(log, stream cartId)
            .Read