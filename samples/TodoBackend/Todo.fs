namespace TodoBackend

open Domain
open System.Text.Json

// NOTE - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
module Events =

    // The TodoBackend spec does not dictate having multiple lists, tenants or clients
    // Here, we implement such a discriminator in order to allow each virtual client to maintain independent state
    let (|ForClientId|) (id : ClientId) = FsCodec.StreamName.create "Todos" (ClientId.toString id)

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

    module Utf8ArrayCodec =
        let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()

    module JsonElementCodec =
        open FsCodec.SystemTextJson

        let private encode (options: JsonSerializerOptions) =
            fun (evt: Event) ->
                match evt with
                | Added todo -> "Added", JsonSerializer.SerializeToElement(todo, options)
                | Updated todo -> "Updated", JsonSerializer.SerializeToElement(todo, options)
                | Deleted deleted -> "Deleted", JsonSerializer.SerializeToElement(deleted, options)
                | Cleared -> "Cleared", Unchecked.defaultof<JsonElement>
                | Snapshotted snapshotted -> "Snapshotted", JsonSerializer.SerializeToElement(snapshotted, options)
    
        let private tryDecode (options: JsonSerializerOptions) =
            fun (eventType, data: JsonElement) ->
                match eventType with
                | "Added" -> Some (Added <| JsonSerializer.DeserializeElement<Todo>(data, options))
                | "Updated" -> Some (Updated <| JsonSerializer.DeserializeElement<Todo>(data, options))
                | "Deleted" -> Some (Deleted <| JsonSerializer.DeserializeElement<Deleted>(data, options))
                | "Cleared" -> Some Cleared
                | "Snapshotted" -> Some (Snapshotted <| JsonSerializer.DeserializeElement<Snapshotted>(data, options))
                | _ -> None

        let codec options = FsCodec.Codec.Create<Event, JsonElement>(encode options, tryDecode options)

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

module Commands =

    let interpret c (state : Fold.State) =
        match c with
        | Add value -> [Events.Added { value with id = state.nextId }]
        | Update value ->
            match state.items |> List.tryFind (function { id = id } -> id = value.id) with
            | Some current when current <> value -> [Events.Updated value]
            | _ -> []
        | Delete id -> if state.items |> List.exists (fun x -> x.id = id) then [Events.Deleted {id=id}] else []
        | Clear -> if state.items |> List.isEmpty then [] else [Events.Cleared]

type Service(log, resolve, ?maxAttempts) =

    let resolve (Events.ForClientId streamId) = Equinox.Stream(log, resolve streamId, maxAttempts = defaultArg maxAttempts 2)

    let execute clientId command =
        let stream = resolve clientId
        stream.Transact(Commands.interpret command)
    let query clientId projection =
        let stream = resolve clientId
        stream.Query projection
    let handle clientId command =
        let stream = resolve clientId
        stream.Transact(fun state ->
            let events = Commands.interpret command state
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
