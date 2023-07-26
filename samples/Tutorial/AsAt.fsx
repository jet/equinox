// Example of using the FsCodec up/down conversion mechanism to access the underlying `Index` of the event in the stream
//   in order to be able to query to obtain an as-at balance
// For a more realistic and detailed example, see https://andrewcmeier.com/bi-temporal-event-sourcing

// NOTES
// - relying on the Index in a model in this manner is in no way common practice (the above tutorial uses first class
//   identifiers inside the events to label points in time)
// - exposing the Index and/or version externally as part of one's API is rarely a good solution either; you ideally want
//   incoming commands to embody intent expressed in terms of a Domain Model rather than having it be coupled to details
//   of the underlying storage and/or versioning thereof.
// - the same general point applies to over-using querying of streams for read purposes as we do here;
//   applying CQRS principles can often lead to a better model regardless of raw necessity

#if !LOCAL
// Compile Tutorial.fsproj by either a) right-clicking or b) typing
// dotnet build samples/Tutorial before attempting to send this to FSI with Alt-Enter
#if VISUALSTUDIO
#r "netstandard"
#endif
#I "bin/Debug/net6.0/"
#r "System.Configuration.ConfigurationManager.dll"
#r "System.Runtime.Caching.dll"
#r "Serilog.dll"
#r "Serilog.Sinks.Console.dll"
#r "Serilog.Sinks.Seq.dll"
#r "Equinox.Core.dll"
#r "Newtonsoft.Json.dll"
#r "FSharp.UMX.dll"
#r "FsCodec.dll"
#r "Equinox.dll"
#r "TypeShape.dll"
#r "FsCodec.SystemTextJson.dll"
#r "EventStore.Client.dll"
#r "EventStore.Client.Streams.dll"
#r "Equinox.EventStoreDb.dll"
#r "Microsoft.Azure.Cosmos.Client.dll"
#r "Equinox.CosmosStore.dll"
#else
#r "nuget:Serilog.Sinks.Console"
#r "nuget:Serilog.Sinks.Seq"
#r "nuget:Equinox.CosmosStore, *-*"
#r "nuget:Equinox.EventStoreDb, *-*"
#r "nuget:FsCodec.SystemTextJson, *-*"
#endif

open System

let [<Literal>] Category = "Account"
let streamId = Equinox.StreamId.gen id

module Events =

    type Delta = { count : int }
    type SnapshotInfo = { balanceLog : int[] }
    type Contract =
        | Added of Delta
        | Removed of Delta
        | Snapshot of SnapshotInfo
        interface TypeShape.UnionContract.IUnionContract

    // unlike most Aggregates, knowing the Event's index is critical - for this reason, we always propagate that index alongside the event body
    type Event = int64 * Contract

    // our upconversion function doesn't actually fit the term - it just tuples the underlying event
    let up (evt : FsCodec.ITimelineEvent<_>) e : Event =
        evt.Index, e
    // as per the `up`, the downConverter needs to drop the index (which is only there for symmetry), add null metadata
    let down (_index, e) : struct (Contract * _ voption * DateTimeOffset voption) =
        e, ValueNone, ValueNone

    // unlike most normal codecs, we have a mapping to supply as we want the Index to be added to each event so we can track it in the State as we fold
    let codecJe = FsCodec.SystemTextJson.CodecJsonElement.Create(up, down)
    let codec = FsCodec.SystemTextJson.Codec.Create(up, down)

module Fold =

    type State = int[]
    module State =
        let balance (x: State) = x |> Array.tryLast |> Option.defaultValue 0
    let initial : State = [||]
    // Rather than composing a `fold` from an `evolve` function as one normally does, it makes sense for us to do it as
    // a loop as we are appending each time but can't mutate the incoming state
    let fold state (xs : Events.Event seq) =
        let mutable bal = state |> Array.tryLast |> Option.defaultValue 0
        let bals = ResizeArray(state)
        let record ver delta =
            let ver = int ver
            // If we're ignoring some events, the balance should remain the same, but we need indexes to be correct
            while bals.Count < ver do
                bals.Add bal
            bal <- bal + delta
            bals.Add bal
        for x in xs do
            match x with
            | ver,Events.Added e -> record ver +e.count
            | ver,Events.Removed e -> record ver -e.count
            | _ver,Events.Snapshot e -> bals.Clear(); bals.AddRange e.balanceLog
        bals.ToArray()
    // generate a snapshot when requested
    let snapshot state : Events.Event = -1L,Events.Snapshot { balanceLog = state }
    // Recognize a relevant snapshot when we meet one in the chain
    let isValid : Events.Event -> bool = function _, Events.Snapshot _ -> true | _ -> false

type Command =
    | Add of int
    | Remove of int
let interpret command state =
    match command with
    | Add delta -> [-1L,Events.Added { count = delta}]
    | Remove delta ->
        let bal = state |> Fold.State.balance
        if bal < delta then invalidArg "delta" $"delta %d{delta} exceeds balance %d{bal}"
        else [-1L,Events.Removed {count = delta}]

type Service internal (resolve : string -> Equinox.Decider<Events.Event, Fold.State>) =

    let execute clientId command : Async<unit> =
        let decider = resolve clientId
        decider.Transact(interpret command)

    let query clientId projection : Async<int> =
        let decider = resolve clientId
        decider.Query projection

    member _.Add(clientId, count) = execute clientId (Add count)
    member _.Remove(clientId, count) = execute clientId (Remove count)
    member _.Read(clientId) = query clientId Fold.State.balance
    member _.AsAt(clientId,index) = query clientId (fun state -> state[index])

module Log =
    open Serilog
    open Serilog.Events
    let verbose = true // false will remove lots of noise
    let log =
        let c = LoggerConfiguration()
        let c = if verbose then c.MinimumLevel.Debug() else c
        let c = c.WriteTo.Sink(Equinox.EventStoreDb.Log.InternalMetrics.Stats.LogSink()) // to power Log.InternalMetrics.dump
        let c = c.WriteTo.Sink(Equinox.CosmosStore.Core.Log.InternalMetrics.Stats.LogSink()) // to power Log.InternalMetrics.dump
        let c = c.WriteTo.Seq("http://localhost:5341") // https://getseq.net
        let c = c.WriteTo.Console(if verbose then LogEventLevel.Debug else LogEventLevel.Information)
        c.CreateLogger()
    let dumpMetrics () =
        Equinox.CosmosStore.Core.Log.InternalMetrics.dump log
        Equinox.EventStoreDb.Log.InternalMetrics.dump log

let [<Literal>] AppName = "equinox-tutorial"
let cache = Equinox.Cache(AppName, 20)

module EventStore =

    open Equinox.EventStoreDb

    let snapshotWindow = 500
    // NOTE: use `docker compose up` to establish the standard 3 node config at ports 1113/2113
    let connector = EventStoreConnector(reqTimeout = TimeSpan.FromSeconds 5., reqRetries = 3)
    let esc = connector.Connect(AppName, Discovery.ConnectionString "esdb://localhost:2111,localhost:2112,localhost:2113?tls=true&tlsVerifyCert=false")
    let connection = EventStoreConnection(esc)
    let context = EventStoreContext(connection, batchSize = snapshotWindow)
    // cache so normal read pattern is to read from whatever we've built in memory
    let cacheStrategy = Equinox.CachingStrategy.SlidingWindow (cache, TimeSpan.FromMinutes 20.) // OR CachingStrategy.NoCaching
    // rig snapshots to be injected as events into the stream every `snapshotWindow` events
    let accessStrategy = AccessStrategy.RollingSnapshots (Fold.isValid,Fold.snapshot)
    let cat = EventStoreCategory(context, Category, Events.codec, Fold.fold, Fold.initial, cacheStrategy, accessStrategy)
    let resolve = Equinox.Decider.forStream Log.log cat

module Cosmos =

    open Equinox.CosmosStore

    let read key = Environment.GetEnvironmentVariable key |> Option.ofObj |> Option.get
    let discovery = Discovery.ConnectionString (read "EQUINOX_COSMOS_CONNECTION")
    let connector = CosmosStoreConnector(discovery, TimeSpan.FromSeconds 5., 2, TimeSpan.FromSeconds 5., Microsoft.Azure.Cosmos.ConnectionMode.Gateway)
    let storeClient = CosmosStoreClient.Connect(connector.CreateAndInitialize, read "EQUINOX_COSMOS_DATABASE", read "EQUINOX_COSMOS_CONTAINER") |> Async.RunSynchronously
    let context = CosmosStoreContext(storeClient, tipMaxEvents = 10)
    let cacheStrategy = CachingStrategy.SlidingWindow (cache, TimeSpan.FromMinutes 20.) // OR CachingStrategy.NoCaching
    let accessStrategy = AccessStrategy.Snapshot (Fold.isValid,Fold.snapshot)
    let cat = CosmosStoreCategory(context, Category, Events.codecJe, Fold.fold, Fold.initial, cacheStrategy, accessStrategy)
    let resolve = Equinox.Decider.forStream Log.log cat

let service = Service(streamId >> EventStore.resolve)
//let service= Service(streamId >> Cosmos.resolve)

let client = "ClientA"
service.Add(client, 1) |> Async.RunSynchronously
service.Add(client, 3) |> Async.RunSynchronously
service.Remove(client, 1) |> Async.RunSynchronously
service.Read(client) |> Async.RunSynchronously |> printf "%A"

Log.dumpMetrics ()
