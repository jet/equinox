#if LOCAL
// Compile Tutorial.fsproj before attempting to send this to FSI with Alt-Enter by either:
// a) right-clicking or
// b) typing dotnet build samples/Tutorial
#I "bin/Debug/netstandard2.1/"
#r "Serilog.dll"
#r "Serilog.Sinks.Console.dll"
#r "Equinox.dll"
#r "Equinox.MemoryStore.dll"
#r "TypeShape.dll"
#r "FSharp.UMX.dll"
#r "FsCodec.dll"
#r "FsCodec.NewtonsoftJson.dll"
#else
#r "nuget:Equinox.MemoryStore, *-*"
#r "nuget:FsCodec.Box"
#r "nuget:Serilog.Sinks.Console"
#endif

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

// Events for a given DDD aggregate are considered to be in the same 'Category' for indexing purposes
// When reacting to events (using Propulsion), the Category will be a key thing to filter events based on
let [<Literal>] Category = "Counter"
// Maps from an app-level counter name (perhaps a strongly typed id), to a well-formed StreamId that can be stored in the Event Store
// For this sample, we let callers just pass a string, and we trust it's suitable for use as a StreamId directly
let streamId = Equinox.StreamId.gen id

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

    member _.Execute(instanceId, command) : Async<unit> =
        let decider = resolve instanceId
        decider.Transact(decide command)

    member x.Reset(instanceId, value) : Async<unit> =
        x.Execute(instanceId, Clear value)

    member _.Read instanceId : Async<int> =
        let decider = resolve instanceId
        decider.Query(fun (State value) -> value)

(* Out of the box, logging is via Serilog (can be wired to anything imaginable).
   We wire up logging for demo purposes using MemoryStore.VolatileStore's Committed event
   MemoryStore itself, by design, has no intrinsic logging
   (other store bindings have rich relevant logging about roundtrips to physical stores etc) *)

open Serilog
let log = LoggerConfiguration().WriteTo.Console().CreateLogger()
let logEvents c s (events : FsCodec.ITimelineEvent<_>[]) =
    log.Information("Committed to {categoryName}-{streamId}, events: {@events}", c, s, seq { for x in events -> x.EventType })

(* We can integration test using an in-memory store
   See other examples such as Cosmos.fsx to see how we integrate with CosmosDB and/or other concrete stores *)

let store = Equinox.MemoryStore.VolatileStore()
let _ = store.Committed.Subscribe(fun struct (c, s, xs) -> logEvents c s xs)
let codec = FsCodec.Box.Codec.Create()
let resolve =
    Equinox.MemoryStore.MemoryStoreCategory(store, codec, fold, initial)
    |> Equinox.Decider.resolveWithContext log
let service = Service(streamId >> resolve Category ())

let clientId = "ClientA"
service.Read(clientId) |> Async.RunSynchronously
service.Execute(clientId, Increment) |> Async.RunSynchronously
service.Read(clientId) |> Async.RunSynchronously
service.Reset(clientId, 5) |> Async.RunSynchronously
service.Read(clientId) |> Async.RunSynchronously
