module Backend.Favorites

open Domain

type Service(resolveStream) =
    let stream (clientId : ClientId) =
        sprintf "Favorites-%s" clientId.Value
        |> resolveStream Domain.Favorites.Events.Compaction.EventType

    member __.Execute log (clientId : ClientId) command =
        Favorites.Handler(log, stream clientId).Execute command

    member __.Read (log : Serilog.ILogger) (clientId : ClientId) =
        Favorites.Handler(log, stream clientId).Read