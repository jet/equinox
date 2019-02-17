// Compile Tutorial.fsproj by either a) right-clicking or b) typing
// dotnet build samples/Tutorial before attempting to send this to FSI with Alt-Enter
#r "bin/Debug/netstandard2.0/Serilog.dll"
#r "bin/Debug/netstandard2.0/Serilog.Sinks.Console.dll"
#r "bin/Debug/netstandard2.0/Equinox.dll"
#r "bin/Debug/netstandard2.0/Equinox.MemoryStore.dll"

// Contributed by @voronoipotato

(* Events are things that have already happened, 
   they always exist in the past, and should always be past tense verbs*)

type Event = 
    | Incremented
    | Decremented
    | Cleared of int 
(* A counter going up might clear to 0, 
   but a counter going down might clear to 100. *)

type State = State of int
(*Evolve takes the present state and one event and figures out the next state*)
let evolve state event =
    match event, state with
    | Incremented, State s -> State(s + 1)
    | Decremented, State s -> State(s - 1)
    | Cleared x , _ -> State x

(*fold is just folding the evolve function over all events to get the current state
  It's equivalent to Linq's Aggregate function *)
let fold state events = Seq.fold evolve state events 

(*Commands are the things we intend to happen, though they may not*)
type Command = 
    | Increment
    | Decrement
    | Clear of int

(* Decide consumes a command and the current state to decide what events actually happened.
   This particular counter allows numbers from 0 to 100.*)
let decide command state = 
    match command with
    | Increment -> 
        if state > 100 then [] else [Incremented]
    | Decrement -> 
        if state <= 0 then [] else [Decremented]
    | Clear i -> 
        if state = i then [] else [Cleared i]

type Handler(log, stream, ?maxAttempts) =
    let inner = Equinox.Handler(log, stream, defaultArg maxAttempts 3)
    member __.Execute command : Async<unit> =
        inner.Transact(decide command)
    member __.Read : Async<int> =
        inner.Query id

type Service(log, resolveStream) =
    let (|AggregateId|) (id : string) = Equinox.AggregateId("Counter", id)
    let (|Stream|) (AggregateId id) = Handler(log, resolveStream id)
    member __.Execute(Stream stream, command) : Async<unit> =
        stream.Execute command
    member __.Read(Stream stream) : Async<int> =
        stream.Read 