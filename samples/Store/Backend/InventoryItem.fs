module Backend.InventoryItem

open Domain.InventoryItem

type Service(log, resolve, ?maxAttempts) =

    let (|Stream|) (Events.ForInventoryItemId id) = Equinox.Stream(log, resolve id, defaultArg maxAttempts 3)

    member __.Execute (Stream handler) command =
        handler.Transact(Commands.interpret command)

    member __.Read(Stream handler) =
        handler.Query id 