module Backend.SavedForLater

open Domain
open Domain.SavedForLater
open System

type Service internal (resolve : ClientId -> Equinox.Stream<Events.Event, Fold.State>, maxSavedItems) =

    do if maxSavedItems < 0 then invalidArg "maxSavedItems" "must be non-negative value."

    let execute clientId command : Async<bool> =
        let stream = resolve clientId
        stream.Transact(decide maxSavedItems command)
    let read clientId : Async<Events.Item[]> =
        let stream = resolve clientId
        stream.Query id
    let remove clientId (resolveCommand : ((SkuId->bool) -> Async<Command>)) : Async<unit> =
        let stream = resolve clientId
        stream.TransactAsync(fun (state : Fold.State) -> async {
            let contents = seq { for item in state -> item.skuId } |> set
            let! cmd = resolveCommand contents.Contains
            let _, events = decide maxSavedItems cmd state
            return (),events } )

    member __.MaxSavedItems = maxSavedItems

    member __.List clientId : Async<Events.Item []> = read clientId

    member __.Save(clientId, skus : seq<SkuId>) : Async<bool> =
        execute clientId <| Add (DateTimeOffset.Now, Seq.toArray skus)

    member __.Remove(clientId, resolve : (SkuId -> bool) -> Async<SkuId[]>) : Async<unit> =
        let resolve hasSku = async {
            let! skus = resolve hasSku
            return Remove skus }
        remove clientId resolve

    member __.Merge(clientId, targetId) : Async<bool> = async {
        let! state = read clientId
        return! execute targetId (Merge state) }

let create maxSavedItems log resolve =
    let resolve id = Equinox.Stream(log, resolve (streamName id), maxAttempts = 3)
    Service(resolve, maxSavedItems)
