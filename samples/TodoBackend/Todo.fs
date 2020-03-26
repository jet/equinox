module TodoBackend

open Domain

// The TodoBackend spec does not dictate having multiple lists, tenants or clients
// Here, we implement such a discriminator in order to allow each virtual client to maintain independent state
let Category = "Todos"
let streamName (id : ClientId) = FsCodec.StreamName.create Category (ClientId.toString id)

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
    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()

module Fold =
    type State = { items : Events.Todo list; nextId : int }
    let initial = { items = []; nextId = 0 }
    let evolve s e =
        match e with
        | Events.Added item ->    { s with items = item :: s.items; nextId = s.nextId + 1 }
        | Events.Updated value -> { s with items = s.items |> List.map (function { id = id } when id = value.id -> value | item -> item) }
        | Events.Deleted          { id=id } -> { s with items = s.items  |> List.filter (fun x -> x.id <> id) }
        | Events.Cleared ->       { s with items = [] }
        | Events.Snapshotted      { items = items } -> { s with items = List.ofArray items }
    let fold : State -> Events.Event seq -> State = Seq.fold evolve
    let isOrigin = function Events.Cleared | Events.Snapshotted _ -> true | _ -> false
    let snapshot state = Events.Snapshotted { items = Array.ofList state.items }

type Command = Add of Events.Todo | Update of Events.Todo | Delete of id: int | Clear

let interpret c (state : Fold.State) =
    match c with
    | Add value -> [Events.Added { value with id = state.nextId }]
    | Update value ->
        match state.items |> List.tryFind (function { id = id } -> id = value.id) with
        | Some current when current <> value -> [Events.Updated value]
        | _ -> []
    | Delete id -> if state.items |> List.exists (fun x -> x.id = id) then [Events.Deleted {id=id}] else []
    | Clear -> if state.items |> List.isEmpty then [] else [Events.Cleared]

type Service internal (resolve : ClientId -> Equinox.Stream<Events.Event, Fold.State>) =

    let execute clientId command =
        let stream = resolve clientId
        stream.Transact(interpret command)
    let query clientId projection =
        let stream = resolve clientId
        stream.Query projection
    let handle clientId command =
        let stream = resolve clientId
        stream.Transact(fun state ->
            let events = interpret command state
            let state' = Fold.fold state events
            state'.items,events)

    member __.List(clientId) : Async<Events.Todo seq> =
        query clientId (fun s -> s.items |> Seq.ofList)

    member __.TryGet(clientId, id) =
        query clientId (fun x -> x.items |> List.tryFind (fun x -> x.id = id))

    member __.Execute(clientId, command) : Async<unit> =
        execute clientId command

    member __.Create(clientId, template: Events.Todo) : Async<Events.Todo> = async {
        let! state' = handle clientId (Command.Add template)
        return List.head state' }

    member __.Patch(clientId, item: Events.Todo) : Async<Events.Todo> = async {
        let! state' = handle clientId (Command.Update item)
        return List.find (fun x -> x.id = item.id) state' }

let create log resolve = Service(fun id -> Equinox.Stream(log, resolve (streamName id), maxAttempts = 3))
