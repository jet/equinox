namespace Equinox

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