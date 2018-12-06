module Backend.Favorites

open Domain
open Domain.Favorites
open System

type Service(log, resolveStream) =
    let (|Stream|) (id: ClientId) = Handler(log, resolveStream (Equinox.CatId("Favorites", id.Value)))

    member __.Execute (Stream stream) command =
        stream.Execute command

    member __.Read(Stream stream) =
        stream.Read