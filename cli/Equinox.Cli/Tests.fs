module Equinox.Cli.Tests

open Domain
open Microsoft.Extensions.DependencyInjection
open System
open System.Net.Http

type Test = Favorite | SaveForLater

let executeLocal (container: ServiceProvider) test: ClientId -> Async<unit> =
    match test with
    | Favorite ->
        let service = container.GetRequiredService<Backend.Favorites.Service>()
        fun clientId -> async {
            let sku = Guid.NewGuid() |> SkuId
            do! service.Favorite(clientId,[sku])
            let! items = service.List clientId
            if items |> Array.exists (fun x -> x.skuId = sku) |> not then invalidOp "Added item not found" }
    | SaveForLater ->
        let service = container.GetRequiredService<Backend.SavedForLater.Service>()
        fun clientId -> async {
            let skus = [Guid.NewGuid() |> SkuId; Guid.NewGuid() |> SkuId; Guid.NewGuid() |> SkuId]
            let! saved = service.Save(clientId,skus)
            if saved then
                let! items = service.List clientId
                if skus |> List.forall (fun sku -> items |> Array.exists (fun x -> x.skuId = sku)) |> not then invalidOp "Added item not found"
            else
                let! current = service.List clientId
                let resolveSkus _hasSku = async {
                    return [|for x in current -> x.skuId|] }
                return! service.Remove(clientId, resolveSkus) }

let executeRemote (client: HttpClient) test =
    match test with
    | Favorite ->
        fun clientId ->
            let session = Clients.Session(client, clientId)
            let client = session.Favorites
            async {
                let sku = Guid.NewGuid() |> SkuId
                do! client.Favorite [|sku|]
                let! items = client.List
                if items |> Array.exists (fun x -> x.skuId = sku) |> not then invalidOp "Added item not found" }
    | SaveForLater ->
        fun clientId ->
            let session = Clients.Session(client, clientId)
            let client = session.Saves
            async {
                let skus = [| Guid.NewGuid() |> SkuId; Guid.NewGuid() |> SkuId; Guid.NewGuid() |> SkuId |]
                let! saved = client.Save skus
                if saved then
                    let! items = client.List
                    // NB this can happen if we overload the system and the next operation for this virtual client takes the other leg and removes it
                    if skus |> Array.forall (fun sku -> items |> Array.exists (fun x -> x.skuId = sku)) |> not then invalidOp "Added item not found"
                else
                    let! current = client.List
                    return! client.Remove [|for x in current -> x.skuId|] }