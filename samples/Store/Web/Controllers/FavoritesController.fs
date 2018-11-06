namespace Web.Controllers

open Domain
open Microsoft.AspNetCore.Mvc
open System

[<Route("api/[controller]")>]
[<ApiController>]
type FavoritesController(service : Backend.Favorites.Service) =
    inherit ControllerBase()

    [<HttpGet("{clientId}")>]
    member __.Get(clientId : ClientId) = async {
        let! res = service.Read(clientId)
        return ActionResult<_> res
    }

    [<HttpPost("{clientId}")>]
    member __.Favorite(clientId : ClientId, [<FromBody>]skuIds : SkuId[]) = async {
        let effectiveDate = DateTimeOffset.UtcNow
        return! service.Execute clientId <| Favorites.Command.Favorite(effectiveDate, List.ofArray skuIds)
    }

    [<HttpDelete("{clientId}/{skuId}")>]
    member __.Unfavorite(clientId : ClientId, skuId : SkuId) = async {
        return! service.Execute clientId <| Favorites.Command.Unfavorite skuId
    }