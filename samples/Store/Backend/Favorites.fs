module Backend.Favorites

open Domain
open Domain.Favorites
open System

type Service internal (resolve : ClientId -> Equinox.Stream<Events.Event, Fold.State>) =

    let execute clientId command : Async<unit> =
        let stream = resolve clientId
        stream.Transact(interpret command)
    let read clientId : Async<Events.Favorited []> =
        let stream = resolve clientId
        stream.Query id

    member __.Execute(clientId, command) =
        execute clientId command

    member __.Favorite(clientId, skus) =
        execute clientId (Command.Favorite(DateTimeOffset.Now, skus))

    member __.Unfavorite(clientId, sku) =
        execute clientId (Command.Unfavorite sku)

    member __.List clientId : Async<Events.Favorited []> =
        read clientId

let create log resolve =
    let resolve id = Equinox.Stream(log, resolve (streamName id), maxAttempts  = 3)
    Service(resolve)
