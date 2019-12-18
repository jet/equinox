module Backend.Favorites

open Domain
open Domain.Favorites
open System

type Service(log, resolve, ?maxAttempts) =
    let (|Stream|) (Events.ForClientId streamId) = Equinox.Stream(log, resolve streamId, defaultArg maxAttempts 2)
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