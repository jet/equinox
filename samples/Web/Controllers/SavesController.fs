namespace Web.Controllers

open Domain
open Microsoft.AspNetCore.Mvc

type FromClientIdHeaderAttribute() = inherit FromHeaderAttribute(Name="COMPLETELY_INSECURE_CLIENT_ID")

[<Route("api/[controller]")>]
[<ApiController>]
type SavesController(service : SavedForLater.Service) =
    inherit ControllerBase()

    [<HttpGet>]
    member _.Get
        (   [<FromClientIdHeader>]clientId : ClientId) = async {
        let! res = service.List(clientId)
        return ActionResult<_> res
    }

    // Returns 400 if item limit exceeded
    [<HttpPost>]
    member x.Save
        (   [<FromClientIdHeader>]clientId : ClientId,
            [<FromBody>]skuIds : SkuId[]) : Async<ActionResult> = async {
        let! ok = service.Save(clientId, List.ofArray skuIds)
        if ok then return x.NoContent() :> _ else return x.BadRequest("Exceeded maximum number of items in Saved list; please validate before requesting Save.") :> _
    }

    [<HttpDelete>]
    member _.Remove
        (   [<FromClientIdHeader>]clientId : ClientId,
            [<FromBody>]skuIds : SkuId[]) : Async<unit> = async {
        let resolveSkus _hasSavedSku = async { return skuIds }
        return! service.Remove(clientId, resolveSkus)
    }
