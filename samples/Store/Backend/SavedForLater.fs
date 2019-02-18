module Backend.SavedForLater

open Domain
open Domain.SavedForLater
open System

type Service(handlerLog, resolveStream, maxSavedItems : int, ?maxAttempts) =
    do if maxSavedItems < 0 then invalidArg "maxSavedItems" "must be non-negative value."
    let (|AggregateId|) (id: ClientId) = Equinox.AggregateId("SavedForLater", ClientId.toStringN id)
    let (|Stream|) (AggregateId id) = Equinox.Stream(handlerLog, resolveStream id, defaultArg maxAttempts 3)
    let read (Stream stream) : Async<Events.Item[]> =
        stream.Query id
    let execute (Stream stream) command : Async<bool> =
        stream.Transact(Commands.decide maxSavedItems command)
    let remove (Stream stream) (resolve : ((SkuId->bool) -> Async<Command>)) : Async<unit> =
        stream.TransactAsync(fun (state : Folds.State) -> async {
            let contents = seq { for item in state -> item.skuId } |> set
            let! cmd = resolve contents.Contains
            let _, events = Commands.decide maxSavedItems cmd state
            return (),events } )

    member __.MaxSavedItems = maxSavedItems

    member __.List clientId : Async<Events.Item []> =
        read clientId

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