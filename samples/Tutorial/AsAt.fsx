// Compile Tutorial.fsproj by either a) right-clicking or b) typing
// dotnet build samples/Tutorial before attempting to send this to FSI with Alt-Enter
#if VISUALSTUDIO
#r "netstandard"
#endif
#I "bin/Debug/netstandard2.0/"
#r "Serilog.dll"
#r "Serilog.Sinks.Console.dll"
#r "Newtonsoft.Json.dll"
#r "FsCodec.dll"
#r "Equinox.dll"
#r "TypeShape.dll"
#r "FsCodec.NewtonsoftJson.dll"
#r "FSharp.Control.AsyncSeq.dll"
#r "System.Net.Http"
#r "Serilog.Sinks.Seq.dll"
#r "Eventstore.ClientAPI.dll"
#r "Equinox.EventStore.dll"
#r "Microsoft.Azure.DocumentDb.Core.dll"
#r "Equinox.Cosmos.dll"

open System

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
        let up (evt : FsCodec.IIndexedEvent<_>,e) : Event =
            evt.Index,e
        // as per the `up`, the downcoverter needs to drop the index (which is only there for symmetry), add null metadata
        let down (_,e) : Contract * _ option * DateTimeOffset option =
            e,None,None
        FsCodec.NewtonsoftJson.Codec.Create(up,down)

module Folds =
    open Events

    type State = int[]
    module State =
        let balance (x: State) = x |> Array.tryLast |> Option.defaultValue 0
    let initial : State = [||]
    // Rather than composing a `fold` from an `evolve` function as one normally does, it makes sense for us to do it as
    // a loop as we are appending each time but can't mutate the incoming state
    let fold state (xs : Event seq) =
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
            | ver,Added e -> record ver +e.count
            | ver,Removed e -> record ver -e.count
            | _ver,Snapshot e -> bals.Clear(); bals.AddRange e.balanceLog
        bals.ToArray()
    // generate a snapshot when requested
    let snapshot state : Event = -1L,Snapshot { balanceLog = state }
    // Recognize a relevant snapshot when we meet one in the chain
    let isValid : Event -> bool = function (_,Snapshot _) -> true | _ -> false

module Commands =
    open Events
    open Folds

    type Command =
        | Add of int
        | Remove of int
    let interpret command state =
        match command with
        | Add delta -> [-1L,Added { count = delta}]
        | Remove delta ->
            let bal = state |> State.balance
            if bal < delta then invalidArg "delta" (sprintf "delta %d exceeds balance %d" delta bal)
            else [-1L,Removed {count = delta}]

type Service(log, resolveStream, ?maxAttempts) =
    let (|AggregateId|) clientId = Equinox.AggregateId("Account", clientId)
    let (|Stream|) (AggregateId aggregateId) = Equinox.Stream(log, resolveStream aggregateId, defaultArg maxAttempts 3)

    let execute (Stream stream) command : Async<unit> = stream.Transact(Commands.interpret command)
    let query (Stream stream) projection : Async<int> = stream.Query projection

    member __.Add(clientId, count) = execute clientId (Commands.Add count)
    member __.Remove(clientId, count) = execute clientId (Commands.Remove count)
    member __.Read(clientId) = query clientId (Folds.State.balance)
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

module EventStore =
    open Equinox.EventStore
    let snapshotWindow = 500
    // see QuickStart for how to run a local instance in a mode that emulates the behavior of a cluster
    let (appName,host,username,password) = "equinox-tutorial","localhost", "admin", "changeit"
    // cache so normal read pattern is to read from whatever we've built in memory
    let cache = Caching.Cache(appName, 20)
    let connector = Connector(username,password,TimeSpan.FromSeconds 5., reqRetries=3, log=Logger.SerilogNormal Log.log)
    let esc = connector.Connect(appName, Discovery.GossipDns host) |> Async.RunSynchronously
    let log = Logger.SerilogNormal (Log.log)
    let conn = Connection(esc)
    let context = Context(conn, BatchingPolicy(maxBatchSize=snapshotWindow))
    let cacheStrategy = CachingStrategy.SlidingWindow (cache, TimeSpan.FromMinutes 20.) // OR CachingStrategy.NoCaching
    // rig snapshots to be injected as events into the stream every `snapshotWindow` events
    let accessStrategy = AccessStrategy.RollingSnapshots (Folds.isValid,Folds.snapshot)
    let resolve = Resolver(context, Events.codec, Folds.fold, Folds.initial, cacheStrategy, accessStrategy).Resolve

module Cosmos =
    open Equinox.Cosmos
    let read key = System.Environment.GetEnvironmentVariable key |> Option.ofObj |> Option.get

    let connector = Connector(requestTimeout=TimeSpan.FromSeconds 5., maxRetryAttemptsOnThrottledRequests=2, maxRetryWaitTimeInSeconds=5, log=Log.log, mode=ConnectionMode.Gateway)
    let conn = connector.Connect("equinox-tutorial", Discovery.FromConnectionString (read "EQUINOX_COSMOS_CONNECTION")) |> Async.RunSynchronously
    let context = Context(conn, read "EQUINOX_COSMOS_DATABASE", read "EQUINOX_COSMOS_CONTAINER")
    let cache = Caching.Cache("equinox-tutorial", 20)
    let cacheStrategy = CachingStrategy.SlidingWindow (cache, TimeSpan.FromMinutes 20.) // OR CachingStrategy.NoCaching
    let accessStrategy = AccessStrategy.Snapshot (Folds.isValid,Folds.snapshot)
    let resolve = Resolver(context, Events.codec, Folds.fold, Folds.initial, cacheStrategy, accessStrategy).Resolve

let serviceES = Service(Log.log, EventStore.resolve)
let serviceCosmos = Service(Log.log, Cosmos.resolve)

let client = "ClientA"
serviceES.Add(client, 1) |> Async.RunSynchronously
serviceES.Add(client, 3) |> Async.RunSynchronously
serviceES.Remove(client, 1) |> Async.RunSynchronously
serviceES.Read(client) |> Async.RunSynchronously

Log.dumpMetrics ()