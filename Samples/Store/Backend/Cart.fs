module Backend.Cart

open Domain

type Service(log, resolveStream) =
    let (|Cart|) (id: CartId) =
        let stream = sprintf "Cart-%s" id.Value |> resolveStream Cart.Events.Compaction.EventType
        Cart.Handler(log, stream)

    member __.FlowAsync (Cart cart, flow, ?prepare) =
        cart.FlowAsync(flow, ?prepare = prepare)

    member __.Execute (Cart cart) command =
        cart.Execute command

    member __.Read (Cart cart) =
        cart.Read