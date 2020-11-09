// Compile Tutorial.fsproj before attempting to send this to FSI with Alt-Enter by either:
// a) right-clicking or 
// b) typing dotnet build samples/Tutorial 
#r "nuget:Serilog"
#r "nuget:Serilog.Sinks.Console"
#r "nuget:Equinox"
#r "nuget:Equinox.MemoryStore"
#r "nuget:TypeShape"
#r "nuget:Fsharp.UMX"
#r "nuget:FsCodec"
#r "nuget:FsCodec.Box"
#r "nuget:FsCodec.NewtonsoftJson"

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
(* Kind of DDD aggregate ID *)
let streamName (id : string) = FsCodec.StreamName.create "Counter" id

type State = State of int
let initial : State = State 0
(* Evolve takes the present state and one event and figures out the next state*)
let evolve state event =
    match event, state with
    | Incremented, State s -> State (s + 1)
    | Decremented, State s -> State (s - 1)
    | Cleared { value = x }, _ -> State x

(* Fold is folding the evolve function over all events to get the current state
   It's equivalent to LINQ's Aggregate function *)
let fold state events = Seq.fold evolve state events

(* Commands are the things we intend to happen, though they may not*)
type Command = 
    | Increment
    | Decrement
    | Clear of int

(* Decide consumes a command and the current state to decide what events actually happened.
   This particular counter allows numbers from 0 to 100. *)

let decide command (State state) =
    match command with
    | Increment -> 
        if state > 100 then [] else [Incremented]
    | Decrement -> 
        if state <= 0 then [] else [Decremented]
    | Clear i -> 
        if state = i then [] else [Cleared {value = i}]

type Service internal (resolve : string -> Equinox.Decider<Event, State>) =

    member __.Execute(instanceId, command) : Async<unit> =
        let decider = resolve instanceId
        decider.Transact(decide command)

    member __.Reset(instanceId, value) : Async<unit> =
        __.Execute(instanceId, Clear value)

    member __.Read instanceId : Async<int> =
        let decider = resolve instanceId
        decider.Query(fun (State value) -> value)

(* Out of the box, logging is via Serilog (can be wired to anything imaginable).
   We wire up logging for demo purposes using MemoryStore.VolatileStore's Committed event
   MemoryStore itself, by design, has no intrinsic logging
   (other store bindings have rich relevant logging about roundtrips to physical stores etc) *)

open Serilog
let log = LoggerConfiguration().WriteTo.Console().CreateLogger()
let logEvents stream (events : FsCodec.ITimelineEvent<_>[]) =
    log.Information("Committed to {stream}, events: {@events}", stream, seq { for x in events -> x.EventType })

(* We can integration test using an in-memory store
   See other examples such as Cosmos.fsx to see how we integrate with CosmosDB and/or other concrete stores *)

let store = Equinox.MemoryStore.VolatileStore()
let _ = store.Committed.Subscribe(fun (s, xs) -> logEvents s xs)
let codec = FsCodec.Box.Codec.Create()
let cat = Equinox.MemoryStore.MemoryStoreCategory(store, codec, fold, initial)
let resolve instanceId = Equinox.Decider(log, streamName instanceId |> cat.Resolve, maxAttempts = 3)
let service = Service(resolve)

let clientId = "ClientA"
service.Read(clientId) |> Async.RunSynchronously
service.Execute(clientId, Increment) |> Async.RunSynchronously
service.Read(clientId) |> Async.RunSynchronously
service.Reset(clientId, 5) |> Async.RunSynchronously 
service.Read(clientId) |> Async.RunSynchronously
