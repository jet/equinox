module Backend.Favorites

open Domain

type Service(log, resolveStream) =
    let (|Favorites|) (clientId : ClientId) =
        let streamName = sprintf "Favorites-%s" clientId.Value
        Favorites.Handler(log, resolveStream Domain.Favorites.Events.Compaction.EventType streamName)

    member __.Execute (Favorites favorites) command =
        favorites.Execute command

    member __.Read(Favorites favorites) =
        favorites.Read