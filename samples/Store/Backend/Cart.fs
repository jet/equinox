module Backend.Cart

open Domain.Cart

let decide command state =
    let events = Commands.interpret command state
    let state' = Fold.fold state events
    state',events

let decideMany decide commands state =
    ((state,[]),commands)
    ||> Seq.fold (fun (state,acc) command ->
        let state',events = decide command state
        state',acc@events)

type Service(log, resolve) =

    let resolve (Events.ForCartId streamId, opt) = Equinox.Stream(log, resolve (streamId,opt), maxAttempts = 3)

    member __.Run(cartId, optimistic, commands : Command seq, ?prepare) : Async<Fold.State> =
        let stream = resolve (cartId,if optimistic then Some Equinox.AllowStale else None)
        stream.TransactAsync(fun state -> async {
            match prepare with None -> () | Some prep -> do! prep
#if false
            let acc = Equinox.Accumulator(Fold.fold,state)
            for cmd in commands do acc.Transact(decide cmd)
            return ctx.State,ctx.Accumulated }
#else
            return decideMany decide commands state })
#endif

    member __.ExecuteManyAsync(cartId, optimistic, commands : Command seq, ?prepare) : Async<unit> =
        __.Run(cartId, optimistic, commands, ?prepare=prepare) |> Async.Ignore

    member __.Execute(cartId, command) =
         __.ExecuteManyAsync(cartId, false, [command])

    member __.Read cartId =
        let stream = resolve (cartId,None)
        stream.Query id
    member __.ReadStale cartId =
        let stream = resolve (cartId,Some Equinox.ResolveOption.AllowStale)
        stream.Query id