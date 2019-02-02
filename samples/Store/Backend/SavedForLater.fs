module Backend.SavedForLater

open Domain
open Domain.SavedForLater
open Domain.SavedForLater.Commands
open System

type Service(handlerLog, resolveStream, maxSavedItems : int, maxAttempts) =
    do if maxSavedItems < 0 then invalidArg "maxSavedItems" "must be non-negative value."
    let (|AggregateId|) (id: ClientId) = Equinox.AggregateId("SavedForLater", ClientId.toStringN id)
    let (|Stream|) (AggregateId id) = Handler(handlerLog, resolveStream id, maxSavedItems, maxAttempts)

    member __.MaxSavedItems = maxSavedItems

    member __.List(Stream stream) : Async<Events.Item []> =
        stream.Read

    member __.Save(Stream stream, skus : seq<SkuId>) : Async<bool> =
        stream.Execute <| Add (DateTimeOffset.Now, Seq.toArray skus)

    member __.Remove(Stream stream, resolve : (SkuId -> bool) -> Async<SkuId[]>) : Async<unit> =
        let resolve hasSku = async {
            let! skus = resolve hasSku
            return Remove skus }
        stream.Remove resolve

    member __.Merge(Stream source, Stream target) : Async<bool> = async {
        let! state = source.Read
        return! Merge state |> target.Execute }