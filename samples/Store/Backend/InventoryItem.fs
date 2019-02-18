module Backend.InventoryItem

open Domain
open Domain.InventoryItem

type Service(log, resolveStream, ?maxAttempts) =
    let (|AggregateId|) (id : InventoryItemId) = Equinox.AggregateId ("InventoryItem", InventoryItemId.toStringN id)
    let (|Stream|) (AggregateId id) = Equinox.Handler(log, resolveStream id, defaultArg maxAttempts 3)

    member __.Execute (Stream handler) command =
        handler.Transact(Commands.interpret command)

    member __.Read(Stream handler) =
        handler.Query id 