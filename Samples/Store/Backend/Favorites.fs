module Backend.Favorites

open Domain

type Service(createStream) =
    static let codec = Foldunk.EventSum.generateJsonUtf8SumEncoder<Favorites.Events.Event>
    let streamName (clientId : ClientId) = sprintf "Favorites-%s" clientId.Value
    let handler id = Favorites.Handler(streamName id |> createStream codec)

    member __.Run (log : Serilog.ILogger) (clientId : ClientId) cmd =
        let handler = handler clientId
        handler.Run log cmd

    member __.Load (log : Serilog.ILogger) (clientId : ClientId) =
        let handler = handler clientId
        handler.Load log