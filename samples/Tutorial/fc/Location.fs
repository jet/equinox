namespace Fc.Location

open Fc

type LocationService(series : Location.Series.Service, epoch : Location.Epoch.Service) =

    let rec chain locationId (prevEpochId : LocationEpochId) bal = async {
        let epochId = LocationEpochId.next prevEpochId
        match! epoch.Sync(locationId, epochId, bal) with
        | Epoch.Closed bal -> return! chain locationId epochId bal
        | Epoch.Open res ->
            do! series.MarkActive(locationId, epochId)
            return res
    }

    member __.Read(locationId) = async {
        let! epochId = series.Read(locationId)
        match! epoch.Read(locationId, epochId) with
        | Epoch.Closed bal -> return! chain locationId epochId bal
        | Epoch.Open res -> return res
    }