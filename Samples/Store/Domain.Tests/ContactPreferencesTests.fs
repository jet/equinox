module Example.Domain.Tests.ContactPreferencesTests

open Domain.ContactPreferences.Commands
open Domain.ContactPreferences.Events
open Domain.ContactPreferences.Folds
open Swensen.Unquote

/// As a basic sanity check, verify the basic behavior we'd expect per command if we were to run it on an empty stream
let verifyCanProcessInInitialState cmd =
    let events = interpret cmd initial
    match cmd with
    | Update value when value.preferences = initial ->
        test <@ List.isEmpty events @>
    | Update _ ->
        test <@ (not << List.isEmpty) events @>

/// Put the aggregate into the state where the command should trigger an event; verify correct events are yielded
let verifyCorrectEventGenerationWhenAppropriate cmd = 
    let generateEventsTriggeringNeedForChange: Command -> Event list = function
        | Update ({ preferences = { quickSurveys = qs } as preferences } as value) ->
            [Updated { value with preferences = { preferences with quickSurveys = not qs}}]
    let verifyResultingEventsAreCorrect state (command, events, state') =
        match command, events with
        | Update cvalue, [Updated evalue] ->
            test <@ evalue.preferences = cvalue.preferences
                    && cvalue.preferences = state' @>
        | c,e -> failwithf "Invalid result - Command %A yielded Events %A in State %A" c e state
    let initialEvents = cmd |> generateEventsTriggeringNeedForChange
    let state = fold initial initialEvents
    let events = interpret cmd state
    let state' = fold state events
    (cmd, events, state') |> verifyResultingEventsAreCorrect state

/// Processing should allow for any given Command to be retried at will
let verifyIdempotency (cmd: Command) =
    // Put the aggregate into the state where the command should not trigger an event
    let establish: Event list = cmd |> function
        | Update value ->
            [ Updated value]
    let state = fold initial establish
    let events = interpret cmd state
    // Assert we decided nothing needs to happen
    test <@ List.isEmpty events @>

[<Property(MaxTest = 1000)>]
let ``interpret yields correct events, idempotently`` (cmd: Command) =
    cmd |> verifyCanProcessInInitialState
    cmd |> verifyCorrectEventGenerationWhenAppropriate
    cmd |> verifyIdempotency