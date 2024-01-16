#if !LOCAL
// Compile Tutorial.fsproj by either a) right-clicking or b) typing
// dotnet build samples/Tutorial before attempting to send this to FSI with Alt-Enter
#I "bin/Debug/net6.0/"
#r "System.Runtime.Caching.dll"
#r "Serilog.dll"
#r "Serilog.Sinks.Console.dll"
#r "Newtonsoft.Json.dll"
#r "TypeShape.dll"
#r "Equinox.dll"
#r "FSharp.UMX.dll"
#r "FsCodec.dll"
#r "FsCodec.SystemTextJson.dll"
#r "FSharp.Control.TaskSeq.dll"
#r "Microsoft.Azure.Cosmos.Client.dll"
#r "Equinox.CosmosStore.dll"
#else
#r "nuget:Equinox.CosmosStore, *-*"
#r "nuget:FsCodec.SystemTextJson, *-*"
#r "nuget:Serilog.Sinks.Console"
#r "nuget:Serilog.Sinks.Seq"
#endif

open System

(* NB It's recommended to look at Favorites.fsx first as it establishes the groundwork
   This tutorial stresses different aspects *)

module Stream =
    let Category = "Todos"
    let id = FsCodec.StreamId.gen id

type Todo =             { id: int; order: int; title: string; completed: bool }
type DeletedInfo =      { id: int }
type Snapshotted =      { items: Todo[] }
type Event =
    | Added             of Todo
    | Updated           of Todo
    | Deleted           of DeletedInfo
    | Cleared
    | Snapshotted       of Snapshotted
    interface TypeShape.UnionContract.IUnionContract
let codec = FsCodec.SystemTextJson.CodecJsonElement.Create<Event>()

type State = { items : Todo list; nextId : int }
let initial = { items = []; nextId = 0 }

module Snapshot =
    
    let private generate state = Snapshotted { items = Array.ofList state.items }
    let private isOrigin = function Cleared | Snapshotted _ -> true | _ -> false
    let config = isOrigin, generate
    
let private evolve s = function
    | Added item -> { s with items = item :: s.items; nextId = s.nextId + 1 }
    | Updated value -> { s with items = s.items |> List.map (function { id = id } when id = value.id -> value | item -> item) }
    | Deleted { id=id } -> { s with items = s.items |> List.filter (fun x -> x.id <> id) }
    | Cleared -> { s with items = [] }
    | Snapshotted { items=items } -> { s with items = List.ofArray items }
let fold = Array.fold evolve

type Command = Add of Todo | Update of Todo | Delete of id: int | Clear
let interpret c (state : State) = [|
    match c with
    | Add value -> Added { value with id = state.nextId }
    | Update value ->
        match state.items |> List.tryFind (function { id = id } -> id = value.id) with
        | Some current when current <> value -> Updated value
        | _ -> ()
    | Delete id -> if state.items |> List.exists (fun x -> x.id = id) then Deleted { id = id }
    | Clear -> if state.items |> List.isEmpty |> not then Cleared |]

type Service internal (resolve : string -> Equinox.Decider<Event, State>) =

    member _.List clientId : Async<Todo seq> =
        let decider = resolve clientId
        decider.Query(fun s -> s.items |> Seq.ofList)
    member _.TryGet(clientId, id) =
        let decider = resolve clientId
        decider.Query(fun s -> s.items |> List.tryFind (fun x -> x.id = id))
    member _.Execute(clientId, command) : Async<unit> =
        let decider = resolve clientId
        decider.Transact(interpret command)
    member _.Create(clientId, template: Todo): Async<Todo> =
        let decider = resolve clientId
        decider.Transact(interpret (Add template), fun s -> s.items |> List.head)
    member _.Patch(clientId, item: Todo): Async<Todo> =
        let decider = resolve clientId
        decider.Transact(interpret (Update item), fun s -> s.items |> List.find (fun x -> x.id = item.id))
    member _.Clear clientId : Async<unit> =
        let decider = resolve clientId
        decider.Transact(interpret Clear)

(*
 * EXERCISE THE SERVICE
 *)

let initialState = initial
//val initialState : State = {items = [];
//                            nextId = 0;}

let oneItem = fold initialState [| Added { id = 0; order = 0; title = "Feed cat"; completed = false } |]
//val oneItem : State = {items = [{id = 0;
//                                 order = 0;
//                                 title = "Feed cat";
//                                 completed = false;}];
//                       nextId = 1;}

fold oneItem [| Cleared |]
//val it : State = {items = [];
//                  nextId = 1;}

open Serilog
let log = LoggerConfiguration().WriteTo.Console().CreateLogger()

let [<Literal>] appName = "equinox-tutorial"
let cache = Equinox.Cache(appName, 20)

open Equinox.CosmosStore

module Store =

    let read key = Environment.GetEnvironmentVariable key |> Option.ofObj |> Option.get
    let discovery = Discovery.ConnectionString (read "EQUINOX_COSMOS_CONNECTION")
    let connector = CosmosStoreConnector(discovery, TimeSpan.FromSeconds 5., 2, TimeSpan.FromSeconds 5.)
    let databaseId, containerId = read "EQUINOX_COSMOS_DATABASE", read "EQUINOX_COSMOS_CONTAINER"
    let client = connector.Connect(databaseId, [| containerId |]) |> Async.RunSynchronously
    let context = CosmosStoreContext(client, databaseId, containerId, tipMaxEvents = 100)
    let cacheStrategy = Equinox.CachingStrategy.SlidingWindow (cache, TimeSpan.FromMinutes 20.)

    let access = AccessStrategy.Snapshot Snapshot.config
    let category = CosmosStoreCategory(context, Stream.Category, codec, fold, initial, access, cacheStrategy)

let service = Service(Stream.id >> Equinox.Decider.forStream log Store.category)

let client = "ClientJ"
let item = { id = 0; order = 0; title = "Feed cat"; completed = false }
service.Create(client, item) |> Async.RunSynchronously
//val it : Todo = {id = 0;
//                 order = 0;
//                 title = "Feed cat";
//                 completed = false;}
service.List(client) |> Async.RunSynchronously
//val it : seq<Todo> = [{id = 0;
//                       order = 0;
//                       title = "Feed cat";
//                       completed = false;}]
service.Execute(client, Clear) |> Async.RunSynchronously
//val it : unit = ()

service.TryGet(client,42) |> Async.RunSynchronously
//val it : Todo option = None

let item2 = { id = 3; order = 0; title = "Feed dog"; completed = false }
service.Create(client, item2) |> Async.RunSynchronously
service.TryGet(client, 3) |> Async.RunSynchronously
//val it : Todo option = Some {id = 3;
//                             order = 0;
//                             title = "Feed dog";
//                             completed = false;}

let itemH = { id = 1; order = 0; title = "Feed horse"; completed = false }
service.Patch(client, itemH) |> Async.RunSynchronously
//[05:49:33 INF] EqxCosmos Tip 304 116ms rc=1
//Updated {id = 1;
//         order = 0;
//         title = "Feed horse";
//         completed = false;}Updated {id = 1;
//         order = 0;
//         title = "Feed horse";
//         completed = false;}[05:49:33 INF] EqxCosmos Sync 1+1 534ms rc=14.1
//val it : Todo = {id = 1;
//                 order = 0;
//                 title = "Feed horse";
//                 completed = false;}
service.Execute(client, Delete 1) |> Async.RunSynchronously
//[05:47:18 INF] EqxCosmos Tip 304 224ms rc=1
//Deleted 1[05:47:19 INF] EqxCosmos Sync 1+1 230ms rc=13.91
//val it : unit = ()
service.List(client) |> Async.RunSynchronously
//[05:47:22 INF] EqxCosmos Tip 304 119ms rc=1
//val it : seq<Todo> = []
