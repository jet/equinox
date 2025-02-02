namespace TodoBackend.Controllers

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
//     & dotnet run -p samples/TodoBackend
//     https://www.todobackend.com/client/index.html?https://localhost:5001/todos
// Stolen from https://github.com/ChristianAlexander/dotnetcore-todo-webapi/blob/master/src/TodoWebApi/Controllers/TodosController.cs
// even more stolen from https://github.com/joeaudette/playground/blob/master/spa-stack/src/FSharp.WebLib/Controllers.fs
[<Route "[controller]"; ApiController>]
type TodosController(service: Service) =
    inherit ControllerBase()

    let toModel (value : TodoView) : Events.Todo = { id = value.id; order = value.order; title = value.title; completed = value.completed }

    member private this.WithUri(x: Events.Todo) : TodoView =
        let url = this.Url.RouteUrl("GetTodo", { id=x.id }, this.Request.Scheme) // Supplying scheme is secret sauce for making it absolute as required by client
        { id = x.id; url = url; order = x.order; title = x.title; completed = x.completed }

    [<HttpGet>]
    member this.Get([<FromClientIdHeader>]clientId : ClientId) = async {
        let! xs = service.List(clientId)
        return seq { for x in xs -> this.WithUri(x) }
    }

    [<HttpGet("{id}", Name="GetTodo")>]
    member this.Get([<FromClientIdHeader>]clientId : ClientId, id) : Async<IActionResult> = async {
        let! x = service.TryGet(clientId, id)
        return match x with None -> this.NotFound() :> _ | Some x -> ObjectResult(this.WithUri x) :> _
    }

    [<HttpPost>]
    member this.Post([<FromClientIdHeader>]clientId : ClientId, [<FromBody>]value : TodoView) : Async<TodoView> = async {
        let! created = service.Create(clientId, toModel value)
        return this.WithUri created
    }

    [<HttpPatch "{id}">]
    member this.Patch([<FromClientIdHeader>]clientId : ClientId, id, [<FromBody>]value : TodoView) : Async<TodoView> = async {
        let! updated = service.Patch(clientId, { toModel value with id = id })
        return this.WithUri updated
    }

    [<HttpDelete "{id}">]
    member _.Delete([<FromClientIdHeader>]clientId : ClientId, id): Async<unit> =
        service.Delete(clientId, id)

    [<HttpDelete>]
    member _.DeleteAll([<FromClientIdHeader>]clientId : ClientId): Async<unit> =
        service.Clear(clientId)
