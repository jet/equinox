namespace Web.Controllers

open Domain
open Microsoft.AspNetCore.Mvc

[<Route("api/[controller]")>]
[<ApiController>]
type SavesController(service : Backend.SavedForLater.Service) =
    inherit ControllerBase()

    [<HttpGet("{clientId}")>]
    member __.Get(clientId : ClientId) = async {
        let! res = service.List(clientId)
        return ActionResult<_> res
    }

    [<HttpPost("{clientId}")>]
    member __.Save(clientId : ClientId, [<FromBody>]skuIds : SkuId[]) = async {
        return! service.Save(clientId, List.ofArray skuIds)
    }

    [<HttpDelete("{clientId}")>]
    member __.Remove(clientId : ClientId, [<FromBody>]skuIds : SkuId[]) = async {
        let resolveSkus _hasSavedSku = async { return skuIds }
        return! service.Remove(clientId, resolveSkus)
    }
