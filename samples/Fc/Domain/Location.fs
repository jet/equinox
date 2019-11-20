namespace Location

/// Manages a Series of Epochs, with a running total being carried forward to the next Epoch when it's Closed
type LocationService(zeroBalance, shouldClose, series : Location.Series.Service, epoch : Location.Epoch.Service) =

    let rec execute locationId originEpochId interpret =
        let rec aux epochId balanceToCarryForward  = async {
            match! epoch.Sync(locationId,epochId,balanceToCarryForward,interpret,shouldClose) with
            | { balance = bal; isComplete = true } ->
                if originEpochId <> epochId then
                    do! series.ActivateEpoch(locationId,epochId)
                return bal
            | { balance = bal } ->
                let successorEpochId = LocationEpochId.next epochId
                return! aux successorEpochId (Some bal) }
        aux

    member __.Execute(locationId, interpret) = async {
        let! activeEpoch = series.Read(locationId)
        let originEpochId,epochId,balanceCarriedForward =
            match activeEpoch with
            | None -> LocationEpochId.parse -1,LocationEpochId.parse 0,Some zeroBalance
            | Some activeEpochId -> activeEpochId,activeEpochId,None
        return! execute locationId originEpochId interpret epochId balanceCarriedForward }