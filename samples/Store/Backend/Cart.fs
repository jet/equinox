module Backend.Cart

open Domain

type Service(log, resolveStream) =
    let (|Stream|) (id: CartId) =
        let streamName = sprintf "Cart-%s" id.Value
        Cart.Handler(log, resolveStream streamName)

    member __.FlowAsync (Stream stream, flow, ?prepare) =
        stream.FlowAsync(flow, ?prepare = prepare)

    member __.Execute (Stream stream) command =
        stream.Execute command

    member __.Read (Stream stream) =
        stream.Read