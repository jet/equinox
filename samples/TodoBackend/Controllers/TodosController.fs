namespace TodoBackend.Controllers

open Microsoft.AspNetCore.Mvc
open TodoBackend

type TodoView =
    {   id: int
        url: string
        order: int; title: string; completed: bool }

type GetByIdArgsTemplate = { id: int }

// Fulfuls contract dictated by https://www.todobackend.com/specs/index.html?https://localhost:5001/todos
// To run, start:
//     & dotnet run -f netcoreapp2.1 -p samples/TodoBackend
//     https://www.todobackend.com/client/index.html?https://localhost:5001/todos
// Stolen from https://github.com/ChristianAlexander/dotnetcore-todo-webapi/blob/master/src/TodoWebApi/Controllers/TodosController.cs
// even more stolen from https://github.com/joeaudette/playground/blob/master/spa-stack/src/FSharp.WebLib/Controllers.fs
[<Route "[controller]">]
type TodosController(service: Service) =
    inherit Controller()

    let toModel (value : TodoView) : Todo = { id = value.id; order = value.order; title = value.title; completed = value.completed }

    member private __.WithUri(x : Todo) : TodoView =
        let url = __.Url.RouteUrl("GetTodo", { id=x.id }, __.Request.Scheme) // Supplying scheme is secret sauce for making it absolute as required by client
        { id = x.id; url = url; order = x.order; title = x.title; completed = x.completed }

    [<HttpGet>]
    member __.Get() : Async<TodoView seq> = async {
        let! xs = service.List
        return seq { for x in xs -> __.WithUri(x) }
    }

    [<HttpGet("{id}", Name="GetTodo")>]
    member __.Get id : Async<IActionResult> = async {
        let! x = service.TryGet id
        return match x with None -> __.NotFound() :> _ | Some x -> ObjectResult(__.WithUri x) :> _
    }

    [<HttpPost>]
    member __.Post([<FromBody>]value : TodoView) : Async<TodoView> = async {
        let! created = service.Create(toModel value)
        return __.WithUri created
    }

    [<HttpPatch "{id}">]
    member __.Patch( id, [<FromBody>]value : TodoView) : Async<TodoView> = async {
        let! updated = service.Patch { toModel value with id = id }
        return __.WithUri updated
    }

    [<HttpDelete "{id}">]
    member __.Delete id : Async<unit> =
        service.Execute <| Delete id

    [<HttpDelete>]
    member __.DeleteAll(): Async<unit> =
        service.Execute <| Clear