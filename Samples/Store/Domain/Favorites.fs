module Domain.Favorites

// NB - these schemas reflect the actual storage formats and hence need to be versioned with care
module Events =
    type Favorited =                            { date: System.DateTimeOffset; skuId: SkuId }
    type Unfavorited =                          { skuId: SkuId }
    module Compaction =
        let [<Literal>] EventType = "compacted/1"
        type Compacted =                        { net: Favorited[] }

    type Event =
        | [<System.Runtime.Serialization.DataMember(Name = Compaction.EventType)>]
          Compacted                             of Compaction.Compacted
        | Favorited                             of Favorited
        | Unfavorited                           of Unfavorited

module Folds =
    type State = Events.Favorited []

    type private InternalState(input: State) =
        let dict = new System.Collections.Generic.Dictionary<SkuId, Events.Favorited>()
        let favorite (e : Events.Favorited) =   dict.[e.skuId] <- e
        let favoriteAll (xs: Events.Favorited seq) = for x in xs do favorite x
        do favoriteAll input
        member __.ReplaceAllWith xs =           dict.Clear(); favoriteAll xs
        member __.Favorite(e : Events.Favorited) =  favorite e
        member __.Unfavorite id =               dict.Remove id |> ignore
        member __.AsState() =                   Seq.toArray dict.Values

    let initial : State = [||]
    let private evolve (s: InternalState) = function
        | Events.Compacted { net = net } ->     s.ReplaceAllWith net
        | Events.Favorited e ->                 s.Favorite e
        | Events.Unfavorited { skuId = id } ->  s.Unfavorite id
    let fold (state: State) (events: seq<Events.Event>) : State =
        let s = InternalState state
        for e in events do evolve s e
        s.AsState()

type Command =
    | Compact
    | Favorite      of date : System.DateTimeOffset * skuIds : SkuId list
    | Unfavorite    of skuId : SkuId

module Commands =
    let interpret command (state : Folds.State) =
        let doesntHave skuId = state |> Array.exists (fun x -> x.skuId = skuId) |> not
        match command with
        | Compact ->
            [ Events.Compacted { net = state } ]
        | Favorite (date = date; skuIds = skuIds) ->
            [ for skuId in Seq.distinct skuIds do
                if doesntHave skuId then
                    yield Events.Favorited { date = date; skuId = skuId } ]
        | Unfavorite skuId ->
            if doesntHave skuId then [] else
            [ Events.Unfavorited { skuId = skuId } ]

type Handler(stream, ?maxAttempts) =
    let handler = Foldunk.Handler(Folds.fold, maxAttempts = defaultArg maxAttempts 2)
    member __.Execute log command : Async<unit> =
        handler.Decide stream log <| fun ctx ->
            let execute = Commands.interpret >> ctx.Execute
            execute command
            if ctx.IsCompactionDue then
                execute Command.Compact
    member __.Read log : Async<Folds.State> =
        handler.Query stream log id