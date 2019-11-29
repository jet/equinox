namespace Location

type Wip<'R> = Pending of decide : (Epoch.Folds.Balance -> 'R*Epoch.Events.Event list) | Complete of 'R

/// Manages a Series of Epochs, with a running total being carried forward to the next Epoch when it's Closed
type LocationService internal (zeroBalance, shouldClose, series : Series.Service, epochs : Epoch.Service) =

    let rec execute locationId originEpochId =
        let rec aux epochId balanceToCarryForward wip = async {
            let decide state = match wip with Complete r -> r,[] | Pending decide -> decide state
            match! epochs.Sync(locationId, epochId, balanceToCarryForward, decide, shouldClose) with
            | { balance = bal; result = Some res; isOpen = true } ->
                if originEpochId <> epochId then
                    do! series.ActivateEpoch(locationId, epochId)
                return bal, res
            | { balance = bal; result = Some res } ->
                let successorEpochId = LocationEpochId.next epochId
                return! aux successorEpochId (Some bal) (Wip.Complete res)
            | { balance = bal } ->
                let successorEpochId = LocationEpochId.next epochId
                return! aux successorEpochId (Some bal) wip }
        aux

    member __.Execute(locationId, decide) = async {
        let! activeEpoch = series.Read(locationId)
        let originEpochId, epochId, balanceCarriedForward =
            match activeEpoch with
            | None -> LocationEpochId.parse -1, LocationEpochId.parse 0, Some zeroBalance
            | Some activeEpochId -> activeEpochId, activeEpochId, None
        return! execute locationId originEpochId epochId balanceCarriedForward (Wip.Pending decide)}

[<AutoOpen>]
module Helpers =
    let create (zeroBalance, shouldClose) (series,epochs) =
        LocationService(zeroBalance, shouldClose, series, epochs)

module Cosmos =

    let createService (zeroBalance, shouldClose) (context,cache,maxAttempts) =
        let series = Series.Cosmos.createService (context, cache, maxAttempts)
        let epochs = Epoch.Cosmos.createService (context, cache, maxAttempts)
        create (zeroBalance, shouldClose) (series, epochs)