module Backend.Favorites

open Domain

type Service(createStream) =
    let stream (clientId : ClientId) =
        sprintf "Favorites-%s" clientId.Value
        |> createStream Domain.Favorites.Events.Compaction.EventType

    member __.Execute (log : Serilog.ILogger) (clientId : ClientId) command =
        let handler = Favorites.Handler(stream clientId)
        handler.Execute log command

    member __.Read (log : Serilog.ILogger) (clientId : ClientId) =
        let handler = Favorites.Handler(stream clientId)
        handler.Read log