/// This is not part of the Store; it's ported from https://gyyhub.com/gregoryyoung/m-r/tree/master/SimpleCQRS for fun
module Domain.InventoryItem

open System

let streamName (id : InventoryItemId) = FsCodec.StreamName.create "InventoryItem" (InventoryItemId.toString id)

// NOTE - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
module Events =

    type Event =
        | Created of name: string
        | Deactivated
        | Renamed of newName: string
        | Removed of count: int
        | CheckedIn of count: int
        interface TypeShape.UnionContract.IUnionContract

module Fold =
    type State = { active : bool; name: string; quantity: int }
    let initial : State = { active = true; name = null; quantity = 0 }
    let private evolve state = function
        | Events.Created name
        | Events.Renamed name -> { state with name = name }
        | Events.Deactivated -> { state with active = false }
        | Events.Removed count -> { state with quantity = state.quantity - count}
        | Events.CheckedIn count -> { state with quantity = state.quantity + count}
    let fold (state: State) (events: seq<Events.Event>) : State =
        Seq.fold evolve state events

type Command =
    | Create of name: string
    | ChangeName of newName: string
    | Remove of count: int
    | CheckIn of count: int
    | Deactivate

// TODO make commands/event representations idempotent
let interpret command (state : Fold.State) =
    match command with
    | Create name ->
        if String.IsNullOrEmpty name then invalidArg "name" ""
        if state.name = name then [] else
        [ Events.Created name ]
    | ChangeName newName ->
        if String.IsNullOrEmpty newName then invalidArg "newName" ""
        if state.name = newName then [] else
        [ Events.Renamed newName ]
    | Remove count ->
        if count <= 0 then invalidOp "cant remove negative count from inventory"
        [ Events.Removed count]
    | CheckIn count ->
        if count <= 0 then invalidOp "must have a count greater than 0 to add to inventory"
        [ Events.CheckedIn count ]
    | Deactivate ->
        if not state.active then invalidOp "Already deactivated"
        [ Events.Deactivated ]
