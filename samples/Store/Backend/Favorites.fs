module Backend.Favorites

open Domain.Favorites
open System

type Service(log, resolve, ?maxAttempts) =

    let resolve (Events.ForClientId streamId) = Equinox.Stream(log, resolve streamId, defaultArg maxAttempts 2)

    member __.Execute(clientId, command) =
        let stream = resolve clientId
        stream.Transact(Commands.interpret command)

    member __.Favorite(clientId, skus) =
        __.Execute(clientId, Command.Favorite(DateTimeOffset.Now, skus))

    member __.Unfavorite(clientId, sku) =
        __.Execute(clientId, Command.Unfavorite sku)

    member __.List clientId : Async<Events.Favorited []> =
        let stream = resolve clientId
        stream.Query(id)

    member __.ListWithVersion clientId : Async<int64 * Events.Favorited []> =
        let stream = resolve clientId
        stream.QueryEx(fun ctx -> ctx.Version, ctx.State)
