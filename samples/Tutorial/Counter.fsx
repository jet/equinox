// Compile Tutorial.fsproj by either a) right-clicking or b) typing
// dotnet build samples/Tutorial before attempting to send this to FSI with Alt-Enter
#I "bin/Debug/netstandard2.0/"
#r "Serilog.dll"
#r "Serilog.Sinks.Console.dll"
#r "Equinox.dll"
#r "Equinox.MemoryStore.dll"

// Contributed by @voronoipotato

(* Events are things that have already happened, 
   they always exist in the past, and should always be past tense verbs*)

(* A counter going up might clear to 0, but a counter going down might clear to 100. *)
type Cleared = { value : int }
type Event = 
    | Incremented
    | Decremented
    | Cleared of Cleared
    interface TypeShape.UnionContract.IUnionContract

type State = State of int
let initial : State = State 0
(*Evolve takes the present state and one event and figures out the next state*)
let evolve state event =
    match event, state with
    | Incremented, State s -> State (s + 1)
    | Decremented, State s -> State (s - 1)
    | Cleared { value = x }, _ -> State x

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
let decide command (State state) = 
    match command with
    | Increment -> 
        if state > 100 then [] else [Incremented]
    | Decrement -> 
        if state <= 0 then [] else [Decremented]
    | Clear i -> 
        if state = i then [] else [Cleared {value = i}]

type Service(log, resolveStream, ?maxAttempts) =
    let (|AggregateId|) (id : string) = Equinox.AggregateId("Counter", id)
    let (|Stream|) (AggregateId id) = Equinox.Stream(log, resolveStream id, defaultArg maxAttempts 3)

    let execute (Stream stream) command : Async<unit> =
        stream.Transact(decide command)
    let read (Stream stream) : Async<int> =
        stream.Query(fun (State value) -> value)

    member __.Execute(instanceId, command) : Async<unit> =
        execute instanceId command
    member __.Reset(instanceId, value) : Async<unit> =
        execute instanceId (Clear value)

    member __.Read instanceId : Async<int> =
        read instanceId 

let store = Equinox.MemoryStore.VolatileStore()
let resolve = Equinox.MemoryStore.Resolver(store, FsCodec.Box.Codec.Create(), fold, initial).Resolve
open Serilog
let log = LoggerConfiguration().WriteTo.Console().CreateLogger()
let service = Service(log, resolve, maxAttempts=3)
let clientId = "ClientA"
service.Read(clientId) |> Async.RunSynchronously
service.Execute(clientId, Increment) |> Async.RunSynchronously
service.Read(clientId) |> Async.RunSynchronously
service.Reset(clientId, 5) |> Async.RunSynchronously 
service.Read(clientId) |> Async.RunSynchronously