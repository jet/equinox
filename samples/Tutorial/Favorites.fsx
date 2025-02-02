#if LOCAL
// Compile Tutorial.fsproj by either a) right-clicking or b) typing
// dotnet build samples/Tutorial before attempting to send this to FSI with Alt-Enter
#I "bin/Debug/net6.0/"
#r "Serilog.dll"
#r "Serilog.Sinks.Console.dll"
#r "Equinox.dll"
#r "Equinox.MemoryStore.dll"
#r "FSharp.UMX.dll"
#r "FSCodec.dll"
#else
#r "nuget:Equinox, *-*"
#r "nuget:Equinox.MemoryStore, *-*"
#r "nuget:Serilog.Sinks.Console"
#endif

(*
 * EVENTS
 *)

(* Define the events that will be saved in the stream *)

// Note using strings and DateTimes etc. as Event payloads is not supported for .CosmosStore or .EventStore using the
// TypeShape IUnionContract based Codec support
// i.e. typically records are used for the Event Payloads even in cases where you feel you'll only ever have a single primitive value

type Event =
    | Added of string
    | Removed of string
// No IUnionContract or Codec required as we're using a custom encoder in this example
//    interface TypeShape.UnionContract.IUnionContract

type State = string list
let initial: State = []
let evolve state = function
    | Added sku -> sku :: state
    | Removed sku -> state |> List.filter (fun x -> x <> sku)
let fold s xs = Array.fold evolve s xs

(* With the basic Events and `fold` defined, we have enough to build the state from the Events:- *)

let initialState = initial
//val initialState : string list = []

let favesCba = fold initialState [| Added "a"; Added "b"; Added "c" |]
//val favesCba : string list = ["c"; "b"; "a"]

(*
 * DECISIONS / COMMANDS
 *)

(* Now we can build a State from the Events, we can map from an external need/intent to how we'd represent any changes
   needed to out current state to reflect that *)

module Decisions =
    
    let add sku state = [|
        if not (state |> List.contains sku) then
            Added sku |]
    let remove sku state = [|
        if state |> List.contains sku then
            Removed sku |]

(* ^ Note we don't yield events if there is no state change required
   This is referred to by the term idempotency https://en.wikipedia.org/wiki/Idempotence
   In summary, re-running the same request immediately should tend to have zero effect and not do any write trip to the store *) 

let removeBEffect = Decisions.remove "b" favesCba
//val removeBEffect : Event list = [Removed "b"]

let favesCa = fold favesCba removeBEffect
// val favesCa : string list = ["c"; "a"]

let _removeBAgainEffect = Decisions.remove "b" favesCa
//val _removeBAgainEffect : Event list = []

// related streams are termed a Category; Each client will have its own Stream.
let [<Literal>] private CategoryName = "Favorites"
let clientAFavoritesStreamId = FsCodec.StreamId.gen id "ClientA"

// For test purposes, we use the in-memory store
let store = Equinox.MemoryStore.VolatileStore()
// For now, write logs to the Console (in practice we'd connect it to a concrete log sink)
open Serilog
let log = LoggerConfiguration().WriteTo.Console().CreateLogger()
// MemoryStore (as with most Event Stores) provides a way to observe events that have been persisted to a stream
// For demo purposes we emit those to the log (which emits to the console)
let logEvents sn (events: FsCodec.ITimelineEvent<_>[]) =
    log.Information("Committed to {sn}, events: {@events}", FsCodec.StreamName.toString sn, seq { for x in events -> x.EventType })
let _ = store.Committed.Subscribe(fun struct (sn, xs) -> logEvents sn xs)

let codec =
    // For this example, we hand-code; normally one uses one of the FsCodec auto codecs, which codegen something similar
    let encode = function
        | Added x -> struct ("Add", box x)
        | Removed x -> "Remove", box x
    let tryDecode name (e: obj): Event voption =
        match name, e with
        | "Add", (:? string as x) -> Added x |> ValueSome
        | "Remove", (:? string as x) -> Removed x |> ValueSome
        | _ -> ValueNone
    FsCodec.Codec.Create(encode, tryDecode)
(*
 * CATEGORY
 *)

// Each store has a <Store>Category that is used to resolve IStream instances binding to a specific stream in a specific store
// ... because the nature of the contract with the Decider is such that the store hands over State, we also pass the `initial` and `fold` as we used above
let memoryStoreCategory = Equinox.MemoryStore.MemoryStoreCategory(store, CategoryName, codec, fold, initial)
let deciderForStreamInMemoryStoreCategory = Equinox.Decider.forStream log memoryStoreCategory

(*
 * DECIDER
 *)

// ... Given a Category, we can get a Decider for a given StreamId within it
// A Decider lets you Transact or Query based on a state

let clientADecider = deciderForStreamInMemoryStoreCategory clientAFavoritesStreamId

// That Decider is then used as a building block

// each time you want to do something against the stream, you do that via the Decider that's been established over it: 

clientADecider.Transact(Decisions.add "a") |> Async.RunSynchronously
clientADecider.Transact(Decisions.add "b") |> Async.RunSynchronously
// Idempotency comes into play if we run it twice:
clientADecider.Transact(Decisions.add "b") |> Async.RunSynchronously

(* Read the current state *)

clientADecider.Query(fun state -> state) |> Async.RunSynchronously

(*
* SERVICES
*)

// In practice, you don't do a chain of actions against a given decider - you build a Service method that
// - obtains the Decider
// - applies the relevant decision or render function to execute the command and/or read the state 

type Service(resolve: string -> Equinox.Decider<Event, State>) =

    member _.Favorite(clientId, sku): Async<unit> =
        let decider = resolve clientId
        decider.Transact(Decisions.add sku)

    member _.Unfavorite(clientId, sku): Async<unit> =
        let decider = resolve clientId
        decider.Transact(Decisions.remove sku)

    member _.List(clientId): Async<string list> =
        let decider = resolve clientId
        decider.Query(id)

(* See Counter.fsx and Cosmos.fsx for a more compact representation which makes the Service wiring less obtrusive *)
let resolve (cat: Equinox.Category<_, _, _>) (clientId: string) =
    let streamId = FsCodec.StreamId.gen id clientId
    Equinox.Decider.forStream log cat streamId 

let service = Service(resolve memoryStoreCategory)

let client = "ClientB"
service.Favorite(client, "a") |> Async.RunSynchronously
service.Favorite(client, "b") |> Async.RunSynchronously
service.List(client) |> Async.RunSynchronously
// val it : string list = ["b"; "a"]

service.Unfavorite(client, "b") |> Async.RunSynchronously
service.List(client) |> Async.RunSynchronously
//val it : string list = ["a"]
