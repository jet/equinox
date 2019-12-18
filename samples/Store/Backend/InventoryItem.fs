module Backend.InventoryItem

open Domain
open Domain.InventoryItem

type Service(log, resolve, ?maxAttempts) =
    let (|ForInventoryItemId|) (id : InventoryItemId) = Equinox.AggregateId ("InventoryItem", InventoryItemId.toStringN id)
    let (|Stream|) (ForInventoryItemId id) = Equinox.Stream(log, resolve id, defaultArg maxAttempts 3)

    member __.Execute (Stream handler) command =
        handler.Transact(Commands.interpret command)

    member __.Read(Stream handler) =
        handler.Query id 