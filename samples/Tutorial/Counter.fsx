#if LOCAL
// Compile Tutorial.fsproj before attempting to send this to FSI with Alt-Enter by either:
// a) right-clicking or
// b) typing dotnet build samples/Tutorial
#I "bin/Debug/net6.0/"
#r "Serilog.dll"
#r "Serilog.Sinks.Console.dll"
#r "Equinox.dll"
#r "Equinox.MemoryStore.dll"
#r "TypeShape.dll"
#r "FSharp.UMX.dll"
#r "FsCodec.dll"
#r "FsCodec.Box.dll"
#else
#r "nuget:Equinox.MemoryStore, *-*"
#r "nuget:FsCodec.Box, *-*"
#r "nuget:Serilog.Sinks.Console"
#endif

// Contributed by @voronoipotato

(* Events are things that have already happened,
   they always exist in the past, and should always be past tense verbs*)

(* A counter going up might clear to 0, but a counter going down might clear to 100. *)
type Cleared = { value: int }
type Event =
    | Incremented
    | Decremented
    | Cleared of Cleared
    interface TypeShape.UnionContract.IUnionContract

// Events for a given DDD aggregate are considered to be in the same 'Category' for indexing purposes
// When reacting to events (using Propulsion), the CategoryName will be a key thing to filter events based on
let [<Literal>] private CategoryName = "Counter"
// Maps from an app-level counter name (perhaps a strongly typed id), to a well-formed StreamId that can be stored in the Event Store
// For this sample, we let callers just pass a string, and we trust it's suitable for use as a StreamId directly
let private streamId = FsCodec.StreamId.gen id

(* NOTE the State is never stored directly, so it can be as simple and direct as necessary
   Typically it's immutable, which enables it to be cached and/or safely have concurrent readers and writers etc. *)
type State = int
let initial: State = 0
(* Evolve takes the present state and one event and figures out the next state
   NOTE the logic should always be simple, with no decisions - it's just gathering/tracking facts that may be relevant to making a decision later
   If you ever want to make it log or print outputs, that's a bad sign *)
let evolve state event: State =
    match event with
    | Incremented -> state + 1
    | Decremented -> state - 1
    | Cleared { value = x } -> x

(* Fold is folding the evolve function over all events to get the current state
   It's equivalent to LINQ's Aggregate function *)
let fold state = Array.fold evolve state

(* NOTE There's no Command DU (the history does show how it once worked using that)
   Instead we have decision functions, and the Service passes one (together with any relevant inputs and helpers) to Decider.Transact
   Each decision function gets to represent the outcome of the decision as zero, one or more events
   One implication of that is that each decision can return a relevant result (though in many cases, returning unit is sufficient)
   Equally importantly, for a real app, unit testing the decision logic is simple and direct, with extraneous boilerplate *)

let increment state = [| if state < 100 then Incremented |]
let decrement state = [| if state > 0 then Decremented |]
let reset value state = [| if state <> value then Cleared { value = value } |]

type Service internal (resolve: string -> Equinox.Decider<Event, State>) =

    member _.Decrement(instanceId) : Async<unit> =
        let decider = resolve instanceId
        decider.Transact decrement
        
    member _.Increment(instanceId) : Async<unit> =
        let decider = resolve instanceId
        decider.Transact increment

    member x.Reset(instanceId, value) : Async<unit> =
        let decider = resolve instanceId
        decider.Transact(reset value)

    member _.Read instanceId: Async<int> =
        let decider = resolve instanceId
        // id is the identity function, returning the full state. For anything real, you'd make probably project to a DTO
        decider.Query id

(* Out of the box, logging is via Serilog (can be wired to anything imaginable).
   We wire up logging for demo purposes using MemoryStore.VolatileStore's Committed event
   MemoryStore itself, by design, has no intrinsic logging
   (other store bindings have rich relevant logging about roundtrips to physical stores etc.) *)

open Serilog
let log = LoggerConfiguration().WriteTo.Console().CreateLogger()
let logEvents sn (events: FsCodec.ITimelineEvent<_>[]) =
    log.Information("Committed to {streamName}, events: {@events}", sn, seq { for x in events -> x.EventType })

(* We can integration test using an in-memory store
   See other examples such as Cosmos.fsx to see how we integrate with CosmosDB and/or other concrete stores *)

let store = Equinox.MemoryStore.VolatileStore()
let _ = store.Committed.Subscribe(fun struct (sn, xs) -> logEvents sn xs)
let codec = FsCodec.Box.Codec.Create()
let cat = Equinox.MemoryStore.MemoryStoreCategory(store, CategoryName, codec, fold, initial)
let service = Service(streamId >> Equinox.Decider.forStream log cat)

let instanceId = "ClientA"
service.Read(instanceId) |> Async.RunSynchronously
service.Increment(instanceId) |> Async.RunSynchronously
service.Decrement(instanceId) |> Async.RunSynchronously
service.Read(instanceId) |> Async.RunSynchronously
service.Reset(instanceId, 5) |> Async.RunSynchronously
service.Read(instanceId) |> Async.RunSynchronously
