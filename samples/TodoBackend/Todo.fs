namespace TodoBackend

open Domain

// NOTE - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
[<AutoOpen>]
module Events =
    type Todo = { id: int; order: int; title: string; completed: bool }
    type DeletedInfo = { id: int }
    type CompactedInfo = { items: Todo[] }
    type Event =
        | Added     of Todo
        | Updated   of Todo
        | Deleted   of DeletedInfo
        | Cleared
        | Compacted of CompactedInfo
        interface TypeShape.UnionContract.IUnionContract
    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()

module Folds =
    type State = { items : Todo list; nextId : int }
    let initial = { items = []; nextId = 0 }
    let evolve s e =
        match e with
        | Added item -> { s with items = item :: s.items; nextId = s.nextId + 1 }
        | Updated value -> { s with items = s.items |> List.map (function { id = id } when id = value.id -> value | item -> item) }
        | Deleted { id=id } -> { s with items = s.items  |> List.filter (fun x -> x.id <> id) }
        | Cleared -> { s with items = [] }
        | Compacted { items = items } -> { s with items = List.ofArray items }
    let fold : State -> Events.Event seq -> State = Seq.fold evolve
    let isOrigin = function Events.Cleared | Events.Compacted _ -> true | _ -> false
    let compact state = Compacted { items = Array.ofList state.items }

type Command = Add of Todo | Update of Todo | Delete of id: int | Clear

module Commands =
    let interpret c (state : Folds.State) =
        match c with
        | Add value -> [Added { value with id = state.nextId }]
        | Update value ->
            match state.items |> List.tryFind (function { id = id } -> id = value.id) with
            | Some current when current <> value -> [Updated value]
            | _ -> []
        | Delete id -> if state.items |> List.exists (fun x -> x.id = id) then [Deleted {id=id}] else []
        | Clear -> if state.items |> List.isEmpty then [] else [Cleared]

type Service(log, resolveStream, ?maxAttempts) =
    // The TodoBackend spec does not dictate having multiple lists, tentants or clients
    // Here, we implement such a discriminator in order to allow each virtual client to maintain independent state
    let (|AggregateId|) (id : ClientId) = Equinox.AggregateId("Todos", ClientId.toStringN id)
    let (|Stream|) (AggregateId id) = Equinox.Stream(log, resolveStream id, maxAttempts = defaultArg maxAttempts 2)
    let execute (Stream stream) command = stream.Transact(Commands.interpret command)
    let query (Stream stream) projection = stream.Query projection
    let handle (Stream stream) command =
        stream.Transact(fun state ->
            let ctx = Equinox.Accumulator(Folds.fold, state)
            ctx.Transact (Commands.interpret command)
            ctx.State.items,ctx.Accumulated)

    member __.List(clientId) : Async<Todo seq> =
        query clientId (fun s -> s.items |> Seq.ofList)

    member __.TryGet(clientId, id) =
        query clientId (fun x -> x.items |> List.tryFind (fun x -> x.id = id))

    member __.Execute(clientId, command) : Async<unit> =
        execute clientId command

    member __.Create(clientId, template: Todo) : Async<Todo> = async {
        let! state' = handle clientId (Command.Add template)
        return List.head state' }

    member __.Patch(clientId, item: Todo) : Async<Todo> = async {
        let! state' = handle clientId (Command.Update item)
        return List.find (fun x -> x.id = item.id) state' }