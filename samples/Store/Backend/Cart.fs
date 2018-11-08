module Backend.Cart

open Domain

type Service(log, resolveStream) =
    let (|Cart|) (id: CartId) =
        let streamName = sprintf "Cart-%s" id.Value
        Cart.Handler(log, resolveStream Cart.Events.Compaction.EventType streamName)

    member __.FlowAsync (Cart handler, flow, ?prepare) =
        handler.FlowAsync(flow, ?prepare = prepare)

    member __.Execute (Cart handler) command =
        handler.Execute command

    member __.Read (Cart handler) =
        handler.Read