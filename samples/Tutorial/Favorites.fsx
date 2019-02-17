// Compile Tutorial.fsproj by either a) right-clicking or b) typing
// dotnet build samples/Tutorial before attempting to send this to FSI with Alt-Enter
#r "bin/Debug/netstandard2.0/Serilog.dll"
#r "bin/Debug/netstandard2.0/Serilog.Sinks.Console.dll"
#r "bin/Debug/netstandard2.0/Equinox.dll"
#r "bin/Debug/netstandard2.0/Equinox.MemoryStore.dll"

module List =
    let contains x = List.exists ((=) x)

(*
 * EVENTS
 *)

(* Define the events that will be saved in the Stream *)

type Event =
    | Added of string
    | Removed of string

let initial : string list = []
let evolve state = function
    | Added sku -> sku :: state
    | Removed sku -> state |> List.filter (fun x -> x <> sku)
let fold s xs = Seq.fold evolve s xs

(* With the basic Events and `fold` defined, we have enough to build the state from the Events:- *)

let initialState = initial
//val initialState : string list = []

let favesCba = fold initialState [Added "a"; Added "b"; Added "c"]
//val favesCba : string list = ["c"; "b"; "a"]

(*
 * COMMANDS
 *)

(* Now we can build a State from the Events, we can interpret a Command in terms of how we'd represent that in the stream *)

type Command =
    | Add of string
    | Remove of string
let interpret command state =
    match command with
    | Add sku -> if state |> List.contains sku then [] else [Added sku]
    | Remove sku -> if state |> List.contains sku |> not then [] else [Removed sku]

(* Note we don't yield events if they won't have a relevant effect - the interpret function makes the processing idempotent
    if a retry of a command happens, it should not make a difference *)

let removeBEffect = interpret (Remove "b") favesCba
//val removeBEffect : Event list = [Removed "b"]

let favesCa = fold favesCba removeBEffect
// val favesCa : string list = ["c"; "a"] 

let _removeBAgainEffect = interpret (Remove "b") favesCa
//val _removeBAgainEffect : Event list = []

(*
 * HANDLER API
 *)

(* Equinox.Handler provides low level functions against a Stream given
    a) the fold function so it can maintain the state as we did above
    b) a log to send metrics and store roundtrip info to
    c) a maximum number of attempts to make if we clash with a conflicting write *)

type Handler(log, stream, ?maxAttempts) =
    let inner = Equinox.Handler(log, stream, maxAttempts = defaultArg maxAttempts 2)
    member __.Execute command : Async<unit> =
        inner.Transact(interpret command)
    member __.Read : Async<string list> =
        inner.Query id

(* When we Execute a command, Equinox.Handler will use `fold` and `interpret` to Decide whether Events need to be written
    Normally, we'll let failures percolate via exceptions, but not return a result (i.e. we don't say "your command caused 1 event") *)

// For now, write logs to the Console (in practice we'd connect it to a concrete log sink)
open Serilog
let log = LoggerConfiguration().WriteTo.Console(Serilog.Events.LogEventLevel.Debug).CreateLogger()

// related streams are termed a Category; Each client will have it's own Stream.
let categoryName = "Favorites"
let clientAFavoritesStreamId = Equinox.AggregateId(categoryName,"ClientA")

// For test purposes, we use the in-memory store
let store = Equinox.MemoryStore.VolatileStore()

// Each store has a Resolver which provides an IStream instance which binds to a specific stream in a specific store
// ... because the nature of the contract with the handler is such that the store hands over State, we also pass the `initial` and `fold` as we used above
let stream streamName = Equinox.MemoryStore.MemResolver(store, fold, initial).Resolve(streamName)

// We hand the streamId to the resolver
let clientAStream = stream clientAFavoritesStreamId
// ... and pass the stream to the Handler
let handler = Handler(log, clientAStream)

(* Run some commands *)

handler.Execute (Add "a") |> Async.RunSynchronously
handler.Execute (Add "b") |> Async.RunSynchronously
// Idempotency comes into play if we run it twice:
handler.Execute (Add "b") |> Async.RunSynchronously

(* Read the current state *)

handler.Read |> Async.RunSynchronously
// val it : string list = ["b"; "a"]

(*
* SERVICES
*)

(* Building a service to package Command Handling and related functions
    No, this is not doing CQRS! *)

type Service(log, resolveStream) =
    let streamHandlerFor (clientId: string) =
        let aggregateId = Equinox.AggregateId("Favorites", clientId)
        let stream = resolveStream aggregateId
        Handler(log, stream)

    member __.Favorite(clientId, sku) =
        let stream = streamHandlerFor clientId
        stream.Execute(Add sku)

    member __.Unfavorite(clientId, skus) =
        let stream = streamHandlerFor clientId
        stream.Execute(Remove skus)

    member __.List(clientId): Async<string list> =
        let stream = streamHandlerFor clientId
        stream.Read

let resolveStream = Equinox.MemoryStore.MemResolver(store, fold, initial).Resolve

let service = Service(log, resolveStream)

let client = "ClientB"
service.Favorite(client, "a") |> Async.RunSynchronously
service.Favorite(client, "b") |> Async.RunSynchronously 
service.List(client) |> Async.RunSynchronously 
// val it : string list = ["b"; "a"]

service.Unfavorite(client, "b") |> Async.RunSynchronously 
service.List(client) |> Async.RunSynchronously 
//val it : string list = ["a"]