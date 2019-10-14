﻿module Backend.Cart

open Domain
open Domain.Cart

type Service(log, resolveStream) =
    let (|AggregateId|) (id: CartId, opt) = Equinox.AggregateId ("Cart", CartId.toStringN id), opt
    let (|Stream|) (AggregateId (id,opt)) = Equinox.Stream(log, resolveStream (id,opt), maxAttempts = 3)
        
    let flowAsync (Stream stream) (flow, prepare) =
        stream.TransactAsync(fun state -> async {
            match prepare with None -> () | Some prep -> do! prep
            let ctx = Equinox.Accumulator(Folds.fold,state)
            let execute = Commands.interpret >> ctx.Transact
            let res = flow ctx execute
            return res,ctx.Accumulated })
    let read (Stream stream) : Async<Folds.State> =
        stream.Query id
    let execute cartId command =
        flowAsync (cartId,None) ((fun _ctx execute -> execute command), None)

    member __.FlowAsync(cartId, optimistic, flow, ?prepare) =
        flowAsync (cartId,if optimistic then Some Equinox.AllowStale else None) (flow, prepare)

    member __.Execute(cartId, command) =
        execute cartId command

    member __.Read cartId =
        read (cartId,None)
    member __.ReadStale cartId =
        read (cartId,Some Equinox.ResolveOption.AllowStale)