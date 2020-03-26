module Backend.InventoryItem

open Domain
open Domain.InventoryItem

type Service internal (resolve : InventoryItemId -> Equinox.Stream<Events.Event, Fold.State>) =

    member __.Execute(itemId, command) =
        let stream = resolve itemId
        stream.Transact(interpret command)

    member __.Read(itemId) =
        let stream = resolve itemId
        stream.Query id

let create resolve =
    let resolve id =
        Equinox.Stream(Serilog.Log.ForContext<Service>(), resolve (streamName id), maxAttempts = 3)
    Service(resolve)
