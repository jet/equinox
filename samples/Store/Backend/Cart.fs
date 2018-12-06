module Backend.Cart

open Domain

type Service(log, resolveStream) =
    let (|CatId|) (id: CartId) = Equinox.CatId ("Cart", id.Value)
    let (|Stream|) (CatId id) = Cart.Handler(log, resolveStream id)

    member __.FlowAsync (Stream stream, flow, ?prepare) =
        stream.FlowAsync(flow, ?prepare = prepare)

    member __.Execute (Stream stream) command =
        stream.Execute command

    member __.Read (Stream stream) =
        stream.Read