namespace Web.Controllers

open Domain
open Microsoft.AspNetCore.Mvc

[<Route("api/[controller]")>]
[<ApiController>]
type FavoritesController(service : Backend.Favorites.Service) =
    inherit ControllerBase()

    [<HttpGet("{clientId}")>]
    member __.Get(clientId : ClientId) = async {
        let! res = service.List(clientId)
        return ActionResult<_> res
    }

    [<HttpPost("{clientId}")>]
    member __.Favorite(clientId : ClientId, [<FromBody>]skuIds : SkuId[]) = async {
        return! service.Favorite(clientId,List.ofArray skuIds)
    }

    [<HttpDelete("{clientId}/{skuId}")>]
    member __.Unfavorite(clientId : ClientId, skuId : SkuId) = async {
        return! service.Unfavorite(clientId, skuId)
    }