module Backend.Favorites

open Domain
open Domain.Favorites
open System

type Service(log, resolveStream, ?maxAttempts) =
    let (|AggregateId|) (id: ClientId) = Equinox.AggregateId("Favorites", ClientId.toStringN id)
    let (|Stream|) (AggregateId id) = Equinox.Handler(log, resolveStream id, defaultArg maxAttempts 2)
    let execute (Stream stream) command : Async<unit> =
        stream.Transact(Commands.interpret command)
    let read (Stream stream) : Async<Events.Favorited []> =
        stream.Query id

    member __.Execute(clientId, command) =
        execute clientId command

    member __.Favorite(clientId, skus) =
        execute clientId (Command.Favorite(DateTimeOffset.Now, skus))

    member __.Unfavorite(clientId, skus) =
        execute clientId (Command.Unfavorite skus)

    member __.List clientId : Async<Events.Favorited []> =
        read clientId 