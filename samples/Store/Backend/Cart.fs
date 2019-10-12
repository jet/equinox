module Backend.Cart

open Domain
open Domain.Cart

type Service(log, resolveStream) =
    let (|AggregateId|) (id: CartId) = Equinox.AggregateId ("Cart", CartId.toStringN id)
    let (|Stream|) (AggregateId id) = Equinox.Stream(log, resolveStream id, maxAttempts = 3)
        
    let flowAsync (Stream stream) (flow, prepare) =
        stream.TransactAsync(fun state -> async {
            match prepare with None -> () | Some prep -> do! prep
            let ctx = Equinox.Accumulator(Folds.fold,state)
            let execute = Commands.interpret >> ctx.Execute
            let res = flow ctx execute
            return res,ctx.Accumulated })
    let read (Stream stream) : Async<Folds.State> =
        stream.Query id
    let execute clientId command =
        flowAsync clientId ((fun _ctx execute -> execute command), None)

    member __.FlowAsync(cartId, flow, ?prepare) =
        flowAsync cartId (flow, prepare)

    member __.Execute(cartId, command) =
        execute cartId command

    member __.Read cartId =
        read cartId