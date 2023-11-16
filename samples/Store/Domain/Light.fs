module Domain.Light

// By Jérémie Chassaing / @thinkb4coding
// https://github.com/dddeu/dddeu20-talks-jeremie-chassaing-functional-event-sourcing/blob/main/EventSourcing.fsx#L52-L84

type Event =
    | SwitchedOn
    | SwitchedOff
    | Broke
type State =
    | Working of CurrentState
    | Broken
and CurrentState = { on: bool; remainingUses: int }
let initial = Working { on = false; remainingUses = 3 }
let evolve s e =
    match s with
    | Broken -> s
    | Working s ->
        match e with
        | SwitchedOn -> Working { on = true; remainingUses = s.remainingUses - 1 }
        | SwitchedOff -> Working { s with on = false }
        | Broke -> Broken
let fold = Array.fold evolve

let decideSwitch (on: bool) s = [|
    match s with
    | Broken -> ()
    | Working { on = true } ->
        if not on then
            SwitchedOff
    | Working { on = false; remainingUses = r } ->
        if on then
            if r = 0 then Broke
            else SwitchedOn |]
