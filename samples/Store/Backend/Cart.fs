module Backend.Cart

open Domain
open Domain.Cart

#if ACCUMULATOR
// This was once part of the core Equinox functionality, but was removed in https://github.com/jet/equinox/pull/184
// it remains here solely to serve as an example; the PR details the considerations leading to this conclusion

/// Maintains a rolling folded State while Accumulating Events pended as part of a decision flow
type Accumulator<'event, 'state>(fold : 'state -> 'event seq -> 'state, originState : 'state) =
    let accumulated = ResizeArray<'event>()

    /// The Events that have thus far been pended via the `decide` functions `Execute`/`Decide`d during the course of this flow
    member __.Accumulated : 'event list =
        accumulated |> List.ofSeq

    /// The current folded State, based on the Stream's `originState` + any events that have been Accumulated during the the decision flow
    member __.State : 'state =
        accumulated |> fold originState

    /// Invoke a decision function, gathering the events (if any) that it decides are necessary into the `Accumulated` sequence
    member __.Transact(interpret : 'state -> 'event list) : unit =
        interpret __.State |> accumulated.AddRange
    /// Invoke an Async decision function, gathering the events (if any) that it decides are necessary into the `Accumulated` sequence
    member __.TransactAsync(interpret : 'state -> Async<'event list>) : Async<unit> = async {
        let! events = interpret __.State
        accumulated.AddRange events }
    /// Invoke a decision function, while also propagating a result yielded as the fst of an (result, events) pair
    member __.Transact(decide : 'state -> 'result * 'event list) : 'result =
        let result, newEvents = decide __.State
        accumulated.AddRange newEvents
        result
    /// Invoke a decision function, while also propagating a result yielded as the fst of an (result, events) pair
    member __.TransactAsync(decide : 'state -> Async<'result * 'event list>) : Async<'result> = async {
        let! result, newEvents = decide __.State
        accumulated.AddRange newEvents
        return result }
#else
let interpretMany fold interpreters (state : 'state) : 'state * 'event list =
    ((state,[]),interpreters)
    ||> Seq.fold (fun (state : 'state, acc : 'event list) interpret ->
        let events = interpret state
        let state' = fold state events
        state', acc @ events)
#endif

type Service internal (resolve : CartId * Equinox.ResolveOption option -> Equinox.Stream<Events.Event, Fold.State>) =

    member __.Run(cartId, optimistic, commands : Command seq, ?prepare) : Async<Fold.State> =
        let stream = resolve (cartId,if optimistic then Some Equinox.AllowStale else None)
        stream.TransactAsync(fun state -> async {
            match prepare with None -> () | Some prep -> do! prep
#if ACCUMULATOR
            let acc = Accumulator(Fold.fold, state)
            for cmd in commands do
                acc.Transact(interpret cmd)
            return acc.State, acc.Accumulated })
#else
            return interpretMany Fold.fold (Seq.map interpret commands) state })
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

let create log resolve =
    let resolve (id, opt) =
        let stream = resolve (streamName id, opt)
        Equinox.Stream(log, stream, maxAttempts = 3)
    Service(resolve)
