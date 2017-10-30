module Domain.ContactPreferences

type Id = Id of email: string

// NB - these schemas reflect the actual storage formats and hence need to be versioned with care
module Events =
    type Preferences = { manyPromotions : bool; littlePromotions : bool; productReview : bool; quickSurveys : bool }
    type Value = { email : string; preferences : Preferences }

    type Event =
    | [<System.Runtime.Serialization.DataMember(Name = "contactPreferencesChanged")>]Updated of Value

module Folds =
    type State = Events.Preferences

    let initial : State = { manyPromotions = false; littlePromotions = false; productReview = false; quickSurveys = false }
    let private evolve _ignoreState = function
        | Events.Updated { preferences = value } -> value
    let fold (state: State) (events: seq<Events.Event>) : State =
        Seq.tryLast events |> Option.fold evolve state

type Command =
    | Update of Events.Value

module Commands =
    let interpret command (state : Folds.State) =
        match command with
        | Update ({ preferences = preferences } as value) ->
            if state = preferences then [] else
            [ Events.Updated value ]

type Handler(stream) =
    let handler = Foldunk.Handler(Folds.fold, maxAttempts = 3)
    member __.Update log email value : Async<unit> =
        handler.Decide stream log <| fun ctx ->
            let execute = Commands.interpret >> ctx.Execute
            execute <| Update { email = email; preferences = value }
    member __.Read log : Async<Folds.State> =
        handler.Query stream log id