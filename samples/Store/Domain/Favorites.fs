module Domain.Favorites

module Stream =
    let [<Literal>] Category = "Favorites"
    let id = FsCodec.StreamId.gen ClientId.toString
    let name = id >> FsCodec.StreamName.create Category

// NOTE - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
module Events =

    type Favorited =                            { date: System.DateTimeOffset; skuId: SkuId }
    type Unfavorited =                          { skuId: SkuId }
    type Snapshotted =                          { net: Favorited[] }

    type Event =
        | Snapshotted                           of Snapshotted
        | Favorited                             of Favorited
        | Unfavorited                           of Unfavorited
        interface TypeShape.UnionContract.IUnionContract
    let codec = EventCodec.gen<Event>
    let codecJe = EventCodec.genJsonElement<Event>

module Fold =

    type State = Events.Favorited []
    let initial: State = [||]

    module Snapshot =

        let generate state = Events.Snapshotted { net = state }
        let private isOrigin = function Events.Snapshotted _ -> true | _ -> false
        let config = isOrigin, generate

    type private InternalState(input: State) =
        let dict = System.Collections.Generic.Dictionary<SkuId, Events.Favorited>()
        let favorite (e: Events.Favorited) =   dict.[e.skuId] <- e
        let favoriteAll (xs: Events.Favorited seq) = for x in xs do favorite x
        do favoriteAll input
        member _.ReplaceAllWith xs =           dict.Clear(); favoriteAll xs
        member _.Favorite(e: Events.Favorited) = favorite e
        member _.Unfavorite id =               dict.Remove id |> ignore
        member _.AsState() =                   Seq.toArray dict.Values

    let private evolve (s: InternalState) = function
        | Events.Snapshotted { net = net } ->   s.ReplaceAllWith net
        | Events.Favorited e ->                 s.Favorite e
        | Events.Unfavorited { skuId = id } ->  s.Unfavorite id
    let fold (state: State) (events: seq<Events.Event>): State =
        let s = InternalState state
        for e in events do evolve s e
        s.AsState()

let has skuId (state: Fold.State) = state |> Array.exists (fun x -> x.skuId = skuId)

let decideFavorite date skuIds state = [|
    for skuId in Seq.distinct skuIds do
        if state |> has skuId |> not then
            Events.Favorited { date = date; skuId = skuId } |]

let decideUnfavorite skuId state = [|
    if state |> has skuId then
        Events.Unfavorited { skuId = skuId } |]

type Service internal (resolve: ClientId -> Equinox.Decider<Events.Event, Fold.State>) =

    member _.Favorite(clientId, skus, ?at) =
        let decider = resolve clientId
        decider.Transact(decideFavorite (defaultArg at System.DateTimeOffset.Now) skus)

    member _.Unfavorite(clientId, sku) =
        let decider = resolve clientId
        decider.Transact(decideUnfavorite sku)

    member _.List clientId: Async<Events.Favorited []> =
        let decider = resolve clientId
        decider.Query(id)

    member _.ListWithVersion clientId: Async<int64 * Events.Favorited []> =
        let decider = resolve clientId
        decider.QueryEx(fun ctx -> ctx.Version, ctx.State)

    // NOTE not a real world example - used for an integration test; TODO get a better example where it's actually relevant
    member _.UnfavoriteWithPostVersion(clientId, sku) =
        let decider = resolve clientId
        let mapResult () (c: Equinox.ISyncContext<_>) = c.Version
        decider.TransactEx((fun c -> (), decideUnfavorite sku c.State), mapResult)

let create resolve =
    Service(Stream.id >> resolve)
