module Samples.Store.Domain.Tests.ContactPreferencesTests

open Domain.ContactPreferences
open Domain.ContactPreferences.Events
open Domain.ContactPreferences.Fold
open Swensen.Unquote

/// Put the aggregate into the state where the command should trigger an event; verify correct events are yielded
let verifyCorrectEventGenerationWhenAppropriate variant prefs (originState: State) =
    let initialEvents =
        match prefs, variant with
        // Variant 1: Initial state
        | _, Choice1Of3 () -> []
        // Variant 2: Same state
        | value, Choice2Of3 () -> [Updated value]
        // Variant 2: Force something to change
        | ({ preferences = { quickSurveys = qs } as preferences } as value), Choice3Of3 () ->
            [Updated { value with preferences = { preferences with quickSurveys = not qs}}]
    let state = fold originState initialEvents
    let events = interpretUpdate prefs state
    let state' = fold state events

    match prefs, events with
    | cValue, [| Updated eValue |] ->
        test <@ eValue.preferences = cValue.preferences
                && cValue.preferences = state' @>
    | cValue, [||] ->
        test <@ state = cValue.preferences
                && state' = state @>
    | c, e -> failwith $"Invalid result - Update %A{c} yielded Events %A{e} in State %A{state}"

/// Processing should allow for any given Command to be retried at will
let verifyIdempotency prefs (originState: State) =
    // Put the aggregate into the state where the command should not trigger an event
    let establish: Event list = [ Updated prefs]
    let state = fold originState establish
    let events = interpretUpdate prefs state
    // Assert we decided nothing needs to happen
    test <@ Array.isEmpty events @>

[<DomainProperty(MaxTest = 1000)>]
let ``interpret yields correct events, idempotently`` variant prefs (originState: State) =
    verifyCorrectEventGenerationWhenAppropriate variant prefs originState
    verifyIdempotency prefs originState
