module Equinox.Tool.TodoClient

open Domain
open Equinox.Tool.Infrastructure
open System.Net
open System.Net.Http
open System

type Todo = { id: int; url: string; order: int; title: string; completed: bool }

type Session(client: HttpClient, clientId: ClientId) =

    member __.Send(req : HttpRequestMessage) : Async<HttpResponseMessage> =
        let req = req |> HttpReq.withHeader "COMPLETELY_INSECURE_CLIENT_ID" (ClientId.toString clientId)
        client.Send(req)

type TodosClient(session: Session) =

    let basePath = "/todos"

    member __.List() : Async<Todo[]> = async {
        let request = HttpReq.get () |> HttpReq.withPath basePath
        let! response = session.Send request
        return! response |> HttpRes.deserializeOkJsonNet<Todo[]>
    }

    member __.Add(x: Todo) = async {
        let request = HttpReq.post () |> HttpReq.withPath basePath |> HttpReq.withJsonNet x
        let! response = session.Send request
        return! response |> HttpRes.deserializeOkJsonNet<Todo>
    }

    member __.Update(x: Todo)= async {
        let request = HttpReq.patch () |> HttpReq.withUri (Uri x.url) |> HttpReq.withJsonNet x
        let! response = session.Send request
        return! response |> HttpRes.deserializeOkJsonNet<Todo>
    }

    member __.Clear() : Async<unit> = async {
        let request = HttpReq.delete () |> HttpReq.withPath basePath 
        let! response = session.Send request
        return! response.EnsureStatusCode(HttpStatusCode.NoContent)
    }

type Session with

    member session.Todos = TodosClient session
