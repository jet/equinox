namespace TodoBackend

open Domain

// NB - these schemas reflect the actual storage formats and hence need to be versioned with care
[<AutoOpen>]
module Events =
    type Todo = { id: int; order: int; title: string; completed: bool }
    type Event =
        | Added     of Todo
        | Updated   of Todo
        | Deleted   of int
        | Cleared
        | Compacted of Todo[]
        interface TypeShape.UnionContract.IUnionContract

module Folds =
    type State = { items : Todo list; nextId : int }
    let initial = { items = []; nextId = 0 }
    let evolve s e =
        match e with
        | Added item -> { s with items = item :: s.items; nextId = s.nextId + 1 }
        | Updated value -> { s with items = s.items |> List.map (function { id = id } when id = value.id -> value | item -> item) }
        | Deleted id -> { s with items = s.items  |> List.filter (fun x -> x.id <> id) }
        | Cleared -> { s with items = [] }
        | Compacted items -> { s with items = List.ofArray items }
    let fold state = Seq.fold evolve state
    let isOrigin = function Events.Cleared | Events.Compacted _ -> true | _ -> false
    let compact state = Compacted (Array.ofList state.items)

type Command = Add of Todo | Update of Todo | Delete of id: int | Clear

module Commands =
    let interpret c (state : Folds.State) =
        match c with
        | Add value -> [Added { value with id = state.nextId }]
        | Update value ->
            match state.items |> List.tryFind (function { id = id } -> id = value.id) with
            | Some current when current <> value -> [Updated value]
            | _ -> []
        | Delete id -> if state.items |> List.exists (fun x -> x.id = id) then [Deleted id] else []
        | Clear -> if state.items |> List.isEmpty then [] else [Cleared]

type Handler(log, stream, ?maxAttempts) =
    let inner = Equinox.Handler(Folds.fold, log, stream, maxAttempts = defaultArg maxAttempts 2)
    member __.Execute command : Async<unit> =
        inner.Decide <| fun ctx ->
            ctx.Execute (Commands.interpret command)
    member __.Handle command : Async<Todo list> =
        inner.Decide <| fun ctx ->
            ctx.Execute (Commands.interpret command)
            ctx.State.items
    member __.Query(projection : Folds.State -> 't) : Async<'t> =
        inner.Query projection

type Service(handlerLog, resolve) =
    // The TodoBackend spec does not dictate having multiple lists, tentants or clients
    // Here, we implement such a discriminator in order to allow each virtual client to maintain independent state
    let (|AggregateId|) (id : ClientId) = Equinox.AggregateId("Todos", ClientId.toStringN id)
    let (|Stream|) (AggregateId id) = Handler(handlerLog, resolve id)

    member __.List(Stream stream) : Async<Todo seq> =
        stream.Query (fun s -> s.items |> Seq.ofList)

    member __.TryGet(Stream stream, id) =
        stream.Query (fun x -> x.items |> List.tryFind (fun x -> x.id = id))

    member __.Execute(Stream stream, command) : Async<unit> =
        stream.Execute command

    member __.Create(Stream stream, template: Todo) : Async<Todo> = async {
        let! state' = stream.Handle(Command.Add template)
        return List.head state' }

    member __.Patch(Stream stream, item: Todo) : Async<Todo> = async {
        let! state' = stream.Handle(Command.Update item)
        return List.find (fun x -> x.id = item.id) state' }