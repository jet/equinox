namespace Web.Controllers

open Microsoft.AspNetCore.Mvc
open Domain
open Web

[<Route("api/[controller]")>]
[<ApiController>]
type FavoritesController(service : Backend.Favorites.Service) =
    inherit ControllerBase()

    [<HttpGet("{clientId}")>]
    member __.Get(clientId : ClientId, ct) = ctask ct {
        let! res = service.Read(clientId)
        return ActionResult<_> res
    }

    //[<HttpGet("{id}")>]
    //member this.Get(id:int) =
    //    let value = "value"
    //    ActionResult<string>(value)

    //[<HttpPost>]
    //member this.Post([<FromBody>] value:string) =
    //    ()

    //[<HttpPut("{id}")>]
    //member this.Put(id:int, [<FromBody>] value:string ) =
    //    ()

    //[<HttpDelete("{id}")>]
    //member this.Delete(id:int) =
    //    ()
