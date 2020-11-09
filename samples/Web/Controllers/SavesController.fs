namespace Web.Controllers

open Domain
open Microsoft.AspNetCore.Mvc

type FromClientIdHeaderAttribute() = inherit FromHeaderAttribute(Name="COMPLETELY_INSECURE_CLIENT_ID")

[<Route("api/[controller]")>]
[<ApiController>]
type SavesController(service : SavedForLater.Service) =
    inherit ControllerBase()

    [<HttpGet>]
    member __.Get
        (   [<FromClientIdHeader>]clientId : ClientId) = async {
        let! res = service.List(clientId)
        return ActionResult<_> res
    }

    // Returns 400 if item limit exceeded
    [<HttpPost>]
    member __.Save
        (   [<FromClientIdHeader>]clientId : ClientId,
            [<FromBody>]skuIds : SkuId[]) : Async<ActionResult> = async {
        let! ok = service.Save(clientId, List.ofArray skuIds)
        if ok then return __.NoContent() :> _ else return __.BadRequest("Exceeded maximum number of items in Saved list; please validate before requesting Save.") :> _
    }

    [<HttpDelete>]
    member __.Remove
        (   [<FromClientIdHeader>]clientId : ClientId,
            [<FromBody>]skuIds : SkuId[]) : Async<unit> = async {
        let resolveSkus _hasSavedSku = async { return skuIds }
        return! service.Remove(clientId, resolveSkus)
    }
