module Backend.Favorites

open Domain

type Service(log, resolveStream) =
    let (|Stream|) (clientId : ClientId) =
        let streamName = sprintf "Favorites-%s" clientId.Value
        Favorites.Handler(log, resolveStream streamName)

    member __.Execute (Stream stream) command =
        stream.Execute command

    member __.Read(Stream stream) =
        stream.Read