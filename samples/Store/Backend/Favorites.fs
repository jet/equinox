module Backend.Favorites

open Domain

type Service(log, resolveStream) =
    let (|Favorites|) (clientId : ClientId) =
        let streamName = sprintf "Favorites-%s" clientId.Value
        Favorites.Handler(log, resolveStream streamName)

    member __.Execute (Favorites handler) command =
        handler.Execute command

    member __.Read(Favorites handler) =
        handler.Read