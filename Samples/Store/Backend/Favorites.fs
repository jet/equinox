module Backend.Favorites

open Domain

type Service(createStream) =
    static let codec = Foldunk.EventSum.generateJsonUtf8SumEncoder<Favorites.Events.Event>
    let streamName (clientId : ClientId) = sprintf "Favorites-%s" clientId.Value
    let handler id = Favorites.Handler(streamName id |> createStream codec)

    member __.Execute (log : Serilog.ILogger) (clientId : ClientId) command =
        let handler = handler clientId
        handler.Execute log command

    member __.Read (log : Serilog.ILogger) (clientId : ClientId) =
        let handler = handler clientId
        handler.Read log