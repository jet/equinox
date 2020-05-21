// Compile Tutorial.fsproj by either a) right-clicking or b) typing
// dotnet build samples/Tutorial before attempting to send this to FSI with Alt-Enter
#I "bin/Debug/netstandard2.1/"
#r "Serilog.dll"
#r "Serilog.Sinks.Console.dll"
#r "Equinox.dll"
#r "Equinox.MemoryStore.dll"
#r "FSharp.UMX.dll"
#r "FSCodec.dll"

(*
 * EVENTS
 *)

(* Define the events that will be saved in the Stream *)

// Note using strings and DateTimes etc as Event payloads is not supported for .Cosmos or .EventStore using the UnionCodec support
// i.e. typically records are used for the Event Payloads even in cases where you feel you'll only ever have a single primitive value

type Event =
    | Added of string  
    | Removed of string
// No IUnionContract or Codec required as we're using a custom encoder in this example
//    interface TypeShape.UnionContract.IUnionContract

type State = string list
let initial : State = []
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
 * STREAM API
 *)

(* Equinox.Stream provides low level functions against an IStream given
    a) a log to send metrics and store roundtrip info to
    b) a maximum number of attempts to make if we clash with a conflicting write *)

// Example of wrapping Stream to encapsulate stream access patterns (see DOCUMENTATION.md for reasons why this is not advised in real apps)
type Handler(stream : Equinox.Stream<Event, State>) =
    member __.Execute command : Async<unit> =
        stream.Transact(interpret command)
    member __.Read : Async<string list> =
        stream.Query id

(* When we Execute a command, Equinox.Stream will use `fold` and `interpret` to Decide whether Events need to be written
    Normally, we'll let failures percolate via exceptions, but not return a result (i.e. we don't say "your command caused 1 event") *)

// For now, write logs to the Console (in practice we'd connect it to a concrete log sink)
open Serilog
let log = LoggerConfiguration().WriteTo.Console().CreateLogger()

// related streams are termed a Category; Each client will have it's own Stream.
let Category = "Favorites"
let clientAFavoritesStreamName = FsCodec.StreamName.create Category "ClientA"

// For test purposes, we use the in-memory store
let store = Equinox.MemoryStore.VolatileStore()
// MemoryStore (as with most Event Stores) provides a way to observe events that have been persisted to a stream
// For demo purposes we emit those to the log (which emits to the console)
let logEvents stream (events : FsCodec.ITimelineEvent<_>[]) =
    log.Information("Committed to {stream}, events: {@events}", stream, seq { for x in events -> x.EventType })
let _ = store.Committed.Subscribe(fun (s, xs) -> logEvents s xs)

let codec =
    // For this example, we hand-code; normally one uses one of the FsCodec auto codecs, which codegen something similar
    let encode = function
        | Added x -> "Add",box x
        | Removed x -> "Remove",box x
    let tryDecode : string*obj -> Event option = function
        | "Add", (:? string as x) -> Added x |> Some
        | "Remove", (:? string as x) -> Removed x |> Some
        | _ -> None
    FsCodec.Codec.Create(encode,tryDecode)
// Each store has a Resolver that generates IStream instances binding to a specific stream in a specific store
// ... because the nature of the contract with the handler is such that the store hands over State, we also pass the `initial` and `fold` as we used above
let resolver = Equinox.MemoryStore.Resolver(store, codec, fold, initial)
let stream streamName = Equinox.Stream(log, resolver.Resolve streamName, maxAttempts = 2)

// We hand the streamId to the resolver
let clientAStream = stream clientAFavoritesStreamName
// ... and pass the stream to the Handler
let handler = Handler(clientAStream)

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

type Service(streamFor : string -> Handler) =

    member __.Favorite(clientId, sku) =
        let stream = streamFor clientId
        stream.Execute(Add sku)

    member __.Unfavorite(clientId, skus) =
        let stream = streamFor clientId
        stream.Execute(Remove skus)

    member __.List(clientId): Async<string list> =
        let stream = streamFor clientId
        stream.Read

(* See Counter.fsx and Cosmos.fsx for a more compact representation which makes the Handler wiring less obtrusive *)
let streamFor (clientId: string) =
    let streamName = FsCodec.StreamName.create "Favorites" clientId
    let stream = Equinox.Stream(log, resolver.Resolve streamName, maxAttempts = 3)
    Handler(stream)

let service = Service(streamFor)

let client = "ClientB"
service.Favorite(client, "a") |> Async.RunSynchronously
service.Favorite(client, "b") |> Async.RunSynchronously 
service.List(client) |> Async.RunSynchronously 
// val it : string list = ["b"; "a"]

service.Unfavorite(client, "b") |> Async.RunSynchronously 
service.List(client) |> Async.RunSynchronously 
//val it : string list = ["a"]