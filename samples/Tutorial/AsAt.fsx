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

// Compile Tutorial.fsproj by either a) right-clicking or b) typing
// dotnet build samples/Tutorial before attempting to send this to FSI with Alt-Enter
#if VISUALSTUDIO
#r "netstandard"
#endif
#I "bin/Debug/netstandard2.1/"
#r "Serilog.dll"
#r "Serilog.Sinks.Console.dll"
#r "Newtonsoft.Json.dll"
#r "FSharp.UMX.dll"
#r "FsCodec.dll"
#r "Equinox.Core.dll"
#r "Equinox.dll"
#r "TypeShape.dll"
#r "FsCodec.NewtonsoftJson.dll"
#r "FSharp.Control.AsyncSeq.dll"
#r "System.Net.Http"
#r "Serilog.Sinks.Seq.dll"
#r "Eventstore.ClientAPI.dll"
#r "Equinox.EventStore.dll"
#r "Microsoft.Azure.Cosmos.Direct.dll"
#r "Microsoft.Azure.Cosmos.Client.dll"
#r "Equinox.Cosmos.dll"

open System

let streamName clientId = FsCodec.StreamName.create "Account" clientId

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

    // unlike most normal codecs, we have a mapping to supply as we want the Index to be added to each event so we can track it in the State as we fold
    let codec =
        // our upconversion function doesn't actually fit the term - it just tuples the underlying event
        let up (evt : FsCodec.ITimelineEvent<_>,e) : Event =
            evt.Index,e
        // as per the `up`, the downConverter needs to drop the index (which is only there for symmetry), add null metadata
        let down (_index,e) : Contract * _ option * DateTimeOffset option =
            e,None,None
        FsCodec.NewtonsoftJson.Codec.Create(up,down)

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
    let isValid : Events.Event -> bool = function (_,Events.Snapshot _) -> true | _ -> false

type Command =
    | Add of int
    | Remove of int
let interpret command state =
    match command with
    | Add delta -> [-1L,Events.Added { count = delta}]
    | Remove delta ->
        let bal = state |> Fold.State.balance
        if bal < delta then invalidArg "delta" (sprintf "delta %d exceeds balance %d" delta bal)
        else [-1L,Events.Removed {count = delta}]

type Service internal (resolve : string -> Equinox.Stream<Events.Event, Fold.State>) =

    let execute clientId command : Async<unit> =
        let stream = resolve clientId
        stream.Transact(interpret command)
    let query clientId projection : Async<int> =
        let stream = resolve clientId
        stream.Query projection

    member __.Add(clientId, count) = execute clientId (Add count)
    member __.Remove(clientId, count) = execute clientId (Remove count)
    member __.Read(clientId) = query clientId (Fold.State.balance)
    member __.AsAt(clientId,index) = query clientId (fun state -> state.[index])

module Log =
    open Serilog
    open Serilog.Events
    let verbose = true // false will remove lots of noise
    let log =
        let c = LoggerConfiguration()
        let c = if verbose then c.MinimumLevel.Debug() else c
        let c = c.WriteTo.Sink(Equinox.EventStore.Log.InternalMetrics.Stats.LogSink()) // to power Log.InternalMetrics.dump
        let c = c.WriteTo.Sink(Equinox.Cosmos.Store.Log.InternalMetrics.Stats.LogSink()) // to power Log.InternalMetrics.dump
        let c = c.WriteTo.Seq("http://localhost:5341") // https://getseq.net
        let c = c.WriteTo.Console(if verbose then LogEventLevel.Debug else LogEventLevel.Information)
        c.CreateLogger()
    let dumpMetrics () =
        Equinox.Cosmos.Store.Log.InternalMetrics.dump log
        Equinox.EventStore.Log.InternalMetrics.dump log

let [<Literal>] appName = "equinox-tutorial"
let cache = Equinox.Cache(appName, 20)

module EventStore =
    open Equinox.EventStore
    let snapshotWindow = 500
    // see QuickStart for how to run a local instance in a mode that emulates the behavior of a cluster
    let (host,username,password) = "localhost", "admin", "changeit"
    let connector = Connector(username,password,TimeSpan.FromSeconds 5., reqRetries=3, log=Logger.SerilogNormal Log.log)
    let esc = connector.Connect(appName, Discovery.GossipDns host) |> Async.RunSynchronously
    let log = Logger.SerilogNormal (Log.log)
    let conn = Connection(esc)
    let context = Context(conn, BatchingPolicy(maxBatchSize=snapshotWindow))
    // cache so normal read pattern is to read from whatever we've built in memory
    let cacheStrategy = CachingStrategy.SlidingWindow (cache, TimeSpan.FromMinutes 20.) // OR CachingStrategy.NoCaching
    // rig snapshots to be injected as events into the stream every `snapshotWindow` events
    let accessStrategy = AccessStrategy.RollingSnapshots (Fold.isValid,Fold.snapshot)
    let resolver = Resolver(context, Events.codec, Fold.fold, Fold.initial, cacheStrategy, accessStrategy)
    let resolve id = Equinox.Stream(Log.log, resolver.Resolve(streamName id), maxAttempts = 3)

module Cosmos =
    open Equinox.Cosmos
    let read key = System.Environment.GetEnvironmentVariable key |> Option.ofObj |> Option.get

    let connector = Connector(TimeSpan.FromSeconds 5., 2, TimeSpan.FromSeconds 5., log=Log.log, mode=Microsoft.Azure.Cosmos.ConnectionMode.Gateway)
    let conn = connector.Connect(appName, Discovery.FromConnectionString (read "EQUINOX_COSMOS_CONNECTION")) |> Async.RunSynchronously
    let context = Context(conn, read "EQUINOX_COSMOS_DATABASE", read "EQUINOX_COSMOS_CONTAINER")
    let cacheStrategy = CachingStrategy.SlidingWindow (cache, TimeSpan.FromMinutes 20.) // OR CachingStrategy.NoCaching
    let accessStrategy = AccessStrategy.Snapshot (Fold.isValid,Fold.snapshot)
    let resolver = Resolver(context, Events.codec, Fold.fold, Fold.initial, cacheStrategy, accessStrategy)
    let resolve id = Equinox.Stream(Log.log, resolver.Resolve(streamName id), maxAttempts = 3)

let serviceES = Service(EventStore.resolve)
let serviceCosmos = Service(Cosmos.resolve)

let client = "ClientA"
serviceES.Add(client, 1) |> Async.RunSynchronously
serviceES.Add(client, 3) |> Async.RunSynchronously
serviceES.Remove(client, 1) |> Async.RunSynchronously
serviceES.Read(client) |> Async.RunSynchronously |> printf "%A"

Log.dumpMetrics ()