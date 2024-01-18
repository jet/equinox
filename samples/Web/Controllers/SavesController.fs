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
        (   [<FromClientIdHeader>]clientId: ClientId) = async {
        let! res = service.List(clientId)
        return ActionResult<_> res }

    // Returns 400 if item limit exceeded
    [<HttpPost>]
    member x.Save
        (   [<FromClientIdHeader>]clientId: ClientId,
            [<FromBody>]skuIds: SkuId[]): Async<ActionResult> = async {
        match! service.Save(clientId, List.ofArray skuIds) with
        | true -> return x.NoContent()
        | false -> return x.BadRequest("Exceeded maximum number of items in Saved list; please validate before requesting Save.") }

    [<HttpDelete>]
    member _.Remove
        (   [<FromClientIdHeader>]clientId: ClientId,
            [<FromBody>]skuIds: SkuId[]): Async<unit> =
        service.Remove(clientId, skuIds)
