module TodoBackend

open Domain

// The TodoBackend spec does not dictate having multiple lists, tenants or clients
// Here, we implement such a discriminator in order to allow each virtual client to maintain independent state
module Stream =
    let [<Literal>] Category = "Todos"
    let id = FsCodec.StreamId.gen ClientId.toString

// NOTE - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
module Events =

    type Todo =         { id: int; order: int; title: string; completed: bool }
    type Deleted =      { id: int }
    type Snapshotted =  { items: Todo[] }
    type Event =
        | Added         of Todo
        | Updated       of Todo
        | Deleted       of Deleted
        | Cleared
        | Snapshotted   of Snapshotted
        interface TypeShape.UnionContract.IUnionContract
    let codec = EventCodec.gen<Event>

module Fold =
    type State = { items: Events.Todo list; nextId: int }
    let initial = { items = []; nextId = 0 }

    module Snapshot =

        let private generate state = Events.Snapshotted { items = Array.ofList state.items }
        let private isOrigin = function Events.Cleared | Events.Snapshotted _ -> true | _ -> false
        let config = isOrigin, generate

    let private evolve s = function
        | Events.Added item ->    { items = item :: s.items; nextId = s.nextId + 1 }
        | Events.Updated value -> { s with items = s.items |> List.map (function { id = id } when id = value.id -> value | item -> item) }
        | Events.Deleted          { id = id } -> { s with items = s.items  |> List.filter (fun x -> x.id <> id) }
        | Events.Cleared ->       { s with items = [] }
        | Events.Snapshotted      { items = items } -> { s with items = List.ofArray items }
    let fold = Array.fold evolve

type Command = Add of Events.Todo | Update of Events.Todo | Delete of id: int | Clear

let decide c (state: Fold.State) = [|
    match c with
    | Add value -> Events.Added { value with id = state.nextId }
    | Update value ->
        match state.items |> List.tryFind (function { id = id } -> id = value.id) with
        | Some current when current <> value -> Events.Updated value
        | _ -> ()
    | Delete id -> if state.items |> List.exists (fun x -> x.id = id) then Events.Deleted { id = id }
    | Clear -> if state.items |> List.isEmpty |> not then Events.Cleared |]

type Service internal (resolve: ClientId -> Equinox.Decider<Events.Event, Fold.State>) =

    member _.List(clientId): Async<Events.Todo seq> =
        let decider = resolve clientId
        decider.Query(fun s -> s.items |> Seq.ofList)

    member _.TryGet(clientId, id) =
        let decider = resolve clientId
        decider.Query(fun s -> s.items |> List.tryFind (fun x -> x.id = id))

    member _.Execute(clientId, command): Async<unit> =
        let decider = resolve clientId
        decider.Transact(decide command)

    member _.Create(clientId, template: Events.Todo): Async<Events.Todo> =
        let decider = resolve clientId
        decider.Transact(decide (Command.Add template), fun s -> s.items |> List.head)

    member _.Patch(clientId, item: Events.Todo): Async<Events.Todo> =
        let decider = resolve clientId
        decider.Transact(decide (Command.Update item), fun s -> s.items |> List.find (fun x -> x.id = item.id))

let create resolve = Service(Stream.id >> resolve)
