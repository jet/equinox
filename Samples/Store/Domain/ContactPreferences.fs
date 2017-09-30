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
    let private evolve = function
        | Events.Updated { preferences = value } -> value
    let fold (_state: State) (events: seq<Events.Event>) : State =
        match Seq.tryLast events |> Option.map evolve with
        | None -> initial
        | Some value-> value

type Command =
    | Update of Events.Value

module Commands =
    let interpret command (state : Folds.State) =
        match command with
        | Update ({ preferences = preferences } as value) ->
            if state = preferences then [] else
            [ Events.Updated value ]

type Handler(stream) =
    let handler = Foldunk.Handler(Folds.fold, Folds.initial)
    member __.Update log email value : Async<unit> =
        handler.Run stream log <| fun ctx ->
            let execute = Commands.interpret >> ctx.Execute
            execute <| Update { email = email; preferences = value }
    member __.Read log : Async<Folds.State> =
        handler.Query stream log id