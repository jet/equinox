namespace TodoBackend.Controllers

open Domain
open Microsoft.AspNetCore.Mvc
open TodoBackend

type FromClientIdHeaderAttribute() = inherit FromHeaderAttribute(Name="COMPLETELY_INSECURE_CLIENT_ID")

type TodoView =
    {   id: int
        url: string
        order: int; title: string; completed: bool }

type GetByIdArgsTemplate = { id: int }

// Fulfills contract dictated by https://www.todobackend.com
// To run:
//     & dotnet run -f netcoreapp3.1 -p samples/TodoBackend
//     https://www.todobackend.com/client/index.html?https://localhost:5001/todos
// Stolen from https://github.com/ChristianAlexander/dotnetcore-todo-webapi/blob/master/src/TodoWebApi/Controllers/TodosController.cs
// even more stolen from https://github.com/joeaudette/playground/blob/master/spa-stack/src/FSharp.WebLib/Controllers.fs
[<Route "[controller]"; ApiController>]
type TodosController(service: Service) =
    inherit ControllerBase()

    let toModel (value : TodoView) : Events.Todo = { id = value.id; order = value.order; title = value.title; completed = value.completed }

    member private __.WithUri(x : Events.Todo) : TodoView =
        let url = __.Url.RouteUrl("GetTodo", { id=x.id }, __.Request.Scheme) // Supplying scheme is secret sauce for making it absolute as required by client
        { id = x.id; url = url; order = x.order; title = x.title; completed = x.completed }

    [<HttpGet>]
    member __.Get([<FromClientIdHeader>]clientId : ClientId) = async {
        let! xs = service.List(clientId)
        return seq { for x in xs -> __.WithUri(x) }
    }

    [<HttpGet("{id}", Name="GetTodo")>]
    member __.Get([<FromClientIdHeader>]clientId : ClientId, id) : Async<IActionResult> = async {
        let! x = service.TryGet(clientId, id)
        return match x with None -> __.NotFound() :> _ | Some x -> ObjectResult(__.WithUri x) :> _
    }

    [<HttpPost>]
    member __.Post([<FromClientIdHeader>]clientId : ClientId, [<FromBody>]value : TodoView) : Async<TodoView> = async {
        let! created = service.Create(clientId, toModel value)
        return __.WithUri created
    }

    [<HttpPatch "{id}">]
    member __.Patch([<FromClientIdHeader>]clientId : ClientId, id, [<FromBody>]value : TodoView) : Async<TodoView> = async {
        let! updated = service.Patch(clientId, { toModel value with id = id })
        return __.WithUri updated
    }

    [<HttpDelete "{id}">]
    member __.Delete([<FromClientIdHeader>]clientId : ClientId, id): Async<unit> =
        service.Execute(clientId, Delete id)

    [<HttpDelete>]
    member __.DeleteAll([<FromClientIdHeader>]clientId : ClientId): Async<unit> =
        service.Execute(clientId, Clear)
