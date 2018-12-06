namespace Web.Controllers

open Domain
open Microsoft.AspNetCore.Mvc

[<Route "api/[controller]">]
[<ApiController>]
type FavoritesController(service : Backend.Favorites.Service) =
    inherit ControllerBase()

    [<HttpGet>]
    member __.Get
        (   [<FromClientIdHeader>]clientId : ClientId) = async {
        let! res = service.List(clientId)
        return ActionResult<_> res
    }

    [<HttpPost>]
    member __.Favorite
        (   [<FromClientIdHeader>]clientId : ClientId,
            [<FromBody>]skuIds : SkuId[]) = async {
        return! service.Favorite(clientId,List.ofArray skuIds)
    }

    [<HttpDelete "{skuId}">]
    member __.Unfavorite
        (   [<FromClientIdHeader>]clientId,
            skuId : SkuId) = async {
        return! service.Unfavorite(clientId, skuId)
    }