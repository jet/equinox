﻿module Domain.ContactPreferences

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

type Command =
    | Update of Events.Value

module Commands =
    let interpret command (state : Folds.State) =
        match command with
        | Update ({ preferences = preferences } as value) ->
            if state = preferences then [] else
            [ Events.Updated value ]

type Handler(log, stream) =
    let inner = Equinox.Handler(log, stream, maxAttempts = 3)
    member __.Update email value : Async<unit> =
        let command = Update { email = email; preferences = value }
        inner.Transact(Commands.interpret command)
    member __.Read : Async<Folds.State> =
        inner.Query id