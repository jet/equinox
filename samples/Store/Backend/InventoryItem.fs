module Backend.InventoryItem

open Domain.InventoryItem

type Service(log, resolve, ?maxAttempts) =

    let resolve (Events.ForInventoryItemId id) = Equinox.Stream(log, resolve id, defaultArg maxAttempts 3)

    member __.Execute(itemId, command) =
        let stream = resolve itemId
        stream.Transact(Commands.interpret command)

    member __.Read(itemId) =
        let stream = resolve itemId
        stream.Query id