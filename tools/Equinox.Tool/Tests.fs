module Equinox.Tool.Tests

open Domain
open FSharp.UMX
open Infrastructure
open Microsoft.Extensions.DependencyInjection
open System
open System.Net.Http
open System.Text

type Test = Favorite | SaveForLater | Todo of size: int

let [<Literal>] seed = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Donec a diam lectus. Sed sit amet ipsum mauris. Maecenas congue ligula ac quam viverra nec consectetur ante hendrerit. Donec et mollis dolor. Praesent et diam eget libero egestas mattis sit amet vitae augue. Nam tincidunt congue enim, ut porta lorem lacinia consectetur! "
let lipsum len =
    StringBuilder.Build (fun x ->
        while x.Length < len do
            let req = len - x.Length
            x.Append(if req >= seed.Length then seed else seed.Substring(0, req)) |> ignore)
let private guard = obj()
let private rnd = Random()
let rlipsum rlen =
    let actualSize = lock guard (fun () -> rnd.Next(1,rlen))
    lipsum actualSize
type TodoClient.Todo with
    static member Create(size) : TodoClient.Todo =
        { id = 0; url = null; order = 0; title = rlipsum size; completed =  false }
type TodoBackend.Events.Todo with
    static member Create(size) : TodoBackend.Events.Todo =
        { id = 0; order = 0; title = rlipsum size; completed =  false }

let mkSkuId () = Guid.NewGuid() |> SkuId

let executeLocal (container: ServiceProvider) test: ClientId -> Async<unit> =
    match test with
    | Favorite ->
        let service = container.GetRequiredService<Favorites.Service>()
        fun clientId -> async {
            let sku = mkSkuId ()
            do! service.Favorite(clientId,[sku])
            let! items = service.List clientId
            if items |> Array.exists (fun x -> x.skuId = sku) |> not then invalidOp "Added item not found" }
    | SaveForLater ->
        let service = container.GetRequiredService<SavedForLater.Service>()
        fun clientId -> async {
            let skus = [mkSkuId (); mkSkuId (); mkSkuId ()]
            let! saved = service.Save(clientId,skus)
            if saved then
                let! items = service.List clientId
                if skus |> List.forall (fun sku -> items |> Array.exists (fun x -> x.skuId = sku)) |> not then invalidOp "Added item not found"
            else
                let! current = service.List clientId
                let resolveSkus _hasSku = async {
                    return [|for x in current -> x.skuId|] }
                return! service.Remove(clientId, resolveSkus) }
    | Todo size ->
        let service = container.GetRequiredService<TodoBackend.Service>()
        fun clientId -> async {
            let! items = service.List(clientId)
            if Seq.length items > 1000 then
                return! service.Execute(clientId, TodoBackend.Command.Clear)
            else
                let! _ = service.Create(clientId, TodoBackend.Events.Todo.Create size)
                return ()}

let executeRemote (client: HttpClient) test =
    match test with
    | Favorite ->
        fun clientId ->
            let session = StoreClient.Session(client, clientId)
            let client = session.Favorites
            async {
                let sku = mkSkuId ()
                do! client.Favorite [|sku|]
                let! items = client.List
                if items |> Array.exists (fun x -> x.skuId = sku) |> not then invalidOp "Added item not found" }
    | SaveForLater ->
        fun clientId ->
            let session = StoreClient.Session(client, clientId)
            let client = session.Saves
            async {
                let skus = [| mkSkuId (); mkSkuId (); mkSkuId () |]
                let! saved = client.Save skus
                if saved then
                    let! items = client.List
                    // NB this can happen if we overload the system and the next operation for this virtual client takes the other leg and removes it
                    if skus |> Array.forall (fun sku -> items |> Array.exists (fun x -> x.skuId = sku)) |> not then invalidOp "Added item not found"
                else
                    let! current = client.List
                    return! client.Remove [|for x in current -> x.skuId|] }
    | Todo size ->
        fun clientId -> async {
            let session = TodoClient.Session(client, clientId)
            let client = session.Todos
            let! items = client.List()
            if Seq.length items > 1000 then
                return! client.Clear()
            else
                let! _ = client.Add(TodoClient.Todo.Create size)
                return () }
