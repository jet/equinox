module Backend.Favorites

open Domain

type Service(createStream) =
    static let codec = Foldunk.EventSum.generateJsonUtf8SumEncoder<Favorites.Events.Event>
    let stream (clientId : ClientId) =
        sprintf "Favorites-%s" clientId.Value
        |> createStream codec

    member __.Execute (log : Serilog.ILogger) (clientId : ClientId) command =
        let handler = Favorites.Handler(stream clientId)
        handler.Execute log command

    member __.Read (log : Serilog.ILogger) (clientId : ClientId) =
        let handler = Favorites.Handler(stream clientId)
        handler.Read log