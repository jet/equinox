module Backend.Favorites

open Domain
open Domain.Favorites
open System

type Service(log, resolveStream) =
    let (|Stream|) (clientId : ClientId) =
        let streamName = sprintf "Favorites-%s" clientId.Value
        Handler(log, resolveStream streamName)

    member __.Execute(Stream stream, command) =
        stream.Execute(command)

    member __.Favorite(Stream stream, skus) =
        stream.Execute(Command.Favorite(DateTimeOffset.Now, skus))

    member __.Unfavorite(Stream stream, skus) =
        stream.Execute(Command.Unfavorite skus)

    member __.List(Stream stream): Async<Events.Favorited []> =
        stream.Read