namespace Fc.Location

open Fc

/// Manages a Chain of Epochs, with a running total being carried forward to the next Epoch when it's Closed
type LocationService(zeroBalance, shouldClose, series : Location.Series.Service, epoch : Location.Epoch.Service) =

    let rec execute locationId (originEpochId,epochId) balanceToCarryForward interpret = async {
        match! epoch.Sync(locationId,epochId,balanceToCarryForward,interpret,shouldClose) with
        | res when res.isComplete ->
            if originEpochId <> epochId then
                do! series.ActivateEpoch(locationId, epochId)
            return res.balance
        | res ->
            let successorEpochId = LocationEpochId.next epochId
            return! execute locationId (originEpochId,successorEpochId) (Some res.balance) interpret }

    member __.Execute(locationId, interpret) = async {
        let! activeEpoch = series.Read(locationId)
        let originEpochId,epochId,balanceCarriedForward =
            match activeEpoch with
            | None -> LocationEpochId.uninitialized,LocationEpochId.zero,Some zeroBalance
            | Some epochId -> epochId,epochId,None
        return! execute (locationId) (originEpochId,epochId) balanceCarriedForward interpret }