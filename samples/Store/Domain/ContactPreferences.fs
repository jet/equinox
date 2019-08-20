module Domain.ContactPreferences

type Id = Id of email: string

// NB - these schemas reflect the actual storage formats and hence need to be versioned with care
module Events =
    type Preferences = { manyPromotions : bool; littlePromotions : bool; productReview : bool; quickSurveys : bool }
    type Value = { email : string; preferences : Preferences }

    type Event =
        | [<System.Runtime.Serialization.DataMember(Name = "contactPreferencesChanged")>]Updated of Value
        interface TypeShape.UnionContract.IUnionContract

module Folds =
    type State = Events.Preferences

    let initial : State = { manyPromotions = false; littlePromotions = false; productReview = false; quickSurveys = false }
    let private evolve _ignoreState = function
        | Events.Updated { preferences = value } -> value
    let fold (state: State) (events: seq<Events.Event>) : State =
        Seq.tryLast events |> Option.fold evolve state
    let isOrigin _ = true
    /// When using AccessStrategy.RollingUnfolds, the events become unfolds, but the logic remains identical
    let transmute events _state =
        [],events

type Command =
    | Update of Events.Value

module Commands =
    let interpret command (state : Folds.State) =
        match command with
        | Update ({ preferences = preferences } as value) ->
            if state = preferences then [] else
            [ Events.Updated value ] 