module Domain.Favorites

// NB - these schemas reflect the actual storage formats and hence need to be versioned with care
module Events =
    type Favorited =                            { date: System.DateTimeOffset; skuId: SkuId }
    type Unfavorited =                          { skuId: SkuId }

    type Event =
        | Favorited                             of Favorited
        | Unfavorited                           of Unfavorited

module Folds =
    type State = Events.Favorited []

    type private InternalState(input: State) =
        let dict = new System.Collections.Generic.Dictionary<SkuId, Events.Favorited>()
        let favorite (e : Events.Favorited) =   dict.[e.skuId] <- e
        let favoriteAll (xs: Events.Favorited seq) = for x in xs do favorite x
        do favoriteAll input
        member __.Favorite(e : Events.Favorited) =  favorite e
        member __.Unfavorite id =               dict.Remove id |> ignore
        member __.AsState() =                   Seq.toArray dict.Values

    let initial : State = [||]
    let private evolve (s: InternalState) = function
        | Events.Favorited e ->                 s.Favorite e
        | Events.Unfavorited { skuId = id } ->  s.Unfavorite id
    let fold (state: State) (events: seq<Events.Event>) : State =
        let s = InternalState state
        for e in events do evolve s e
        s.AsState()

type Command =
    | Favorite      of date : System.DateTimeOffset * skuIds : SkuId list
    | Unfavorite    of skuId : SkuId

module Commands =
    let interpret command (state : Folds.State) =
        let doesntHave skuId = state |> Array.exists (fun x -> x.skuId = skuId) |> not
        match command with
        | Favorite (date = date; skuIds = skuIds) ->
            [ for skuId in Seq.distinct skuIds do
                if doesntHave skuId then
                    yield Events.Favorited { date = date; skuId = skuId } ]
        | Unfavorite skuId ->
            if doesntHave skuId then [] else
            [ Events.Unfavorited { skuId = skuId } ]

type Handler(stream) =
    let handler = Foldunk.Handler(Folds.fold, Folds.initial)
    member __.Execute log command : Async<unit> =
        handler.Run stream log ignore <| fun ctx ->
            (Commands.interpret >> ctx.Execute) command
    member __.Read log : Async<Folds.State> =
        handler.Query stream log id