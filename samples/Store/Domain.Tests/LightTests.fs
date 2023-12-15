module Domain.Tests.LightTests

open Domain.Light
open Swensen.Unquote

type Case = Off | On | After3Cycles
let establish = function
    | Off ->          initial
    | On ->           fold initial [| SwitchedOn |]
    | After3Cycles -> [| for _ in 1..3 do SwitchedOn; SwitchedOff |] |> fold initial

let run cmd state =
    let events = decideSwitch cmd state
    events, fold state events

let [<FsCheck.Xunit.Property>] props case cmd =
    let state = establish case
    let events, state = run cmd state
    match case, cmd with
    | Off,          true ->  events =! [| SwitchedOn |]
    | Off,          false -> events =! [||]
    | On,           true ->  events =! [||]
    | On,           false -> events =! [| SwitchedOff |]
    | After3Cycles, true ->  events =! [| Broke |]
    | After3Cycles, false -> events =! [||]

    [||] =! decideSwitch cmd state // all commands are idempotent
