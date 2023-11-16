module Domain.Light

// By Jérémie Chassaing / @thinkb4coding
// https://github.com/dddeu/dddeu20-talks-jeremie-chassaing-functional-event-sourcing/blob/main/EventSourcing.fsx#L52-L84
type Command =

    | SwitchOn
    | SwitchOff
type Event =
    | SwitchedOn
    | SwitchedOff
    | Broke

type Status =
    | On
    | Off

type Working =
    { Status : Status
      RemainingUses: int}

type State =
    | Working of Working
    | Broken
let initialState = Working { Status = Off; RemainingUses = 3}

let decide (command: Command) (state: State) : Event list =
    match state, command with
    | Working { Status = Off; RemainingUses = 0 }, SwitchOn ->
        [ Broke]
    | Working { Status = Off}, SwitchOn -> [ SwitchedOn]
    |  Working { Status = On }, SwitchOff -> [SwitchedOff]
    | _ -> []

let evolve (state: State) (event: Event) : State =
    match state, event with
    | _, Broke -> Broken
    | Working s, SwitchedOn ->
        Working { Status = On;
                  RemainingUses = s.RemainingUses - 1 }
    | Working s, SwitchedOff ->
        Working { s with Status = Off}
    | _ -> state
