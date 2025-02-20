﻿module Equinox.Tool.StoreClient

open System
open System.Net
open System.Net.Http

type Session(client: HttpClient, clientId: ClientId) =

    member _.Send(req: HttpRequestMessage) : Async<HttpResponseMessage> =
        let req = req |> HttpReq.withHeader "COMPLETELY_INSECURE_CLIENT_ID" (ClientId.toString clientId)
        client.SendAsync2(req)

type Favorited = { date: DateTimeOffset; skuId: SkuId }

type FavoritesClient(session: Session) =

    member _.Favorite(skus: SkuId[]) = async {
        let request = HttpReq.post () |> HttpReq.withPath "api/favorites" |> HttpReq.withJsonNet skus
        let! response = session.Send request
        return! response.EnsureStatusCode(HttpStatusCode.NoContent)
    }

    member _.List = async {
        let request = HttpReq.get () |> HttpReq.withPath "api/favorites"
        let! response = session.Send request
        return! response |> HttpRes.deserializeOkJsonNet<Favorited[]>
    }

type Saved = { skuId: SkuId; dateSaved: DateTimeOffset }

type SavesClient(session: Session) =

    // this (returning a bool indicating whether it got saved) is fine for now
    // IRL we don't want to be leaning on the fact we get a 400 when we exceed the max items limit as a core API design element
    member _.Save(skus: SkuId[]) : Async<bool> = async {
        let request = HttpReq.post () |> HttpReq.withPath "api/saves" |> HttpReq.withJsonNet skus
        let! response = session.Send request
        if response.StatusCode = HttpStatusCode.BadRequest then
            return false
        else
            do! response.EnsureStatusCode(HttpStatusCode.NoContent)
            return true
    }

    member _.Remove(skus: SkuId[]) : Async<unit> = async {
        let request = HttpReq.delete () |> HttpReq.withPath "api/saves" |> HttpReq.withJsonNet skus
        let! response = session.Send request
        return! response.EnsureStatusCode(HttpStatusCode.NoContent)
    }

    member _.List = async {
        let request = HttpReq.get () |> HttpReq.withPath "api/saves"
        let! response = session.Send request
        return! response |> HttpRes.deserializeOkJsonNet<Saved[]>
    }

type Session with

    member session.Favorites = FavoritesClient session
    member session.Saves = SavesClient session
