namespace Fc.Location

open Fc

/// Manages a Chain of Epochs, with a running total being carried forward to the next Epoch when it's Closed
type LocationService(series : Location.Series.Service, epoch : Location.Epoch.Service) =

    let rec chain locationId (prevEpochId : LocationEpochId) balanceToCarryForward command = async {
        let successorEpochId = LocationEpochId.next prevEpochId
        match! epoch.Execute(locationId, successorEpochId, balanceToCarryForward, command) with
        | Epoch.Open res ->
            do! series.ActivateEpoch(locationId, successorEpochId)
            return res
        | Epoch.Closed bal -> return! chain locationId successorEpochId bal command }

    let execute locationId (epochId : LocationEpochId) command = async {
        match! epoch.Execute(locationId, epochId, command) with
        | _,Epoch.Open res ->
            do! series.ActivateEpoch(locationId, successorEpochId)
            return res
        | true,Epoch.Closed bal ->
            do! series.ActivateEpoch(locationId, successorEpochId)

            return! chain locationId successorEpochId bal command }
        | false,Epoch.Closed bal -> return! chain locationId successorEpochId bal command }

    member __.Read(locationId) : Epoch.EpochState = async {
        let! epochId = series.Read(locationId)
        match! epoch.Read(locationId, epochId) with
        | Epoch.Open res -> return res
        | Epoch.Closed bal -> return! chain locationId epochId bal Epoch.Command.Sync }

    member __.Execute(locationId, decide) = async {
        let! epochId = series.Read(locationId)
        match! epoch.Execute(locationId, epochId, Epoch.Command.Execute decide) with
        | Epoch.Open res -> return res
        | Epoch.Closed bal -> return! chain locationId epochId bal (Epoch.Command.Execute decide) }