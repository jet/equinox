// Compile Tutorial.fsproj by either a) right-clicking or b) typing
// dotnet build samples/Tutorial before attempting to send this to FSI with Alt-Enter
#r "netstandard"
#I "bin/Debug/netstandard2.0/"
#r "Serilog.dll"
#r "Serilog.Sinks.Console.dll"
#r "Newtonsoft.Json.dll"
#r "TypeShape.dll"
#r "Equinox.dll"
#r "Equinox.Codec.dll"
#r "FSharp.Control.AsyncSeq.dll"
#r "Microsoft.Azure.DocumentDb.Core.dll"
#r "System.Net.Http"
#r "Serilog.Sinks.Seq.dll"
#r "Equinox.Cosmos.dll"
#load "../Infrastructure/Log.fs"

open Equinox.Cosmos
open System

module Favorites =
    type Event =
        | Added of string
        | Removed of string
        interface TypeShape.UnionContract.IUnionContract
    let initial : string list = []
    let evolve state = function
        | Added sku -> sku :: state
        | Removed sku -> state |> List.filter (fun x -> x <> sku)
    let fold s xs = Seq.fold evolve s xs

    type Command =
        | Add of string
        | Remove of string
    let interpret command state =
        match command with
        | Add sku -> if state |> List.contains sku then [] else [Added sku]
        | Remove sku -> if state |> List.contains sku then [Removed sku] else []

    type Service(log, resolveStream, ?maxAttempts) =
        let (|AggregateId|) clientId = Equinox.AggregateId("Favorites", clientId)
        let (|Stream|) (AggregateId aggregateId) = Equinox.Stream(log, resolveStream aggregateId, defaultArg maxAttempts 3)

        let execute (Stream stream) command : Async<unit> = stream.Transact(interpret command)
        let read (Stream stream) : Async<string list> = stream.Query id

        member __.Favorite(clientId, sku) = execute clientId (Add sku)
        member __.Unfavorite(clientId, skus) = execute clientId (Remove skus)
        member __.List clientId: Async<string list> = read clientId

module Log =
    open Serilog
    open Serilog.Events
    open Samples.Infrastructure.Log
    let verbose = true // false will remove lots of noise
    let log =
        let c = LoggerConfiguration()
        let c = if verbose then c.MinimumLevel.Debug() else c
        let c = c.WriteTo.Sink(RuCounterSink())
        let c = c.WriteTo.Seq("http://localhost:5341") // https://getseq.net
        let c = c.WriteTo.Console(if verbose then LogEventLevel.Debug else LogEventLevel.Information)
        c.CreateLogger()

    let dumpStats () =
        let stats =
          [ "Read", RuCounterSink.Read
            "Write", RuCounterSink.Write
            "Resync", RuCounterSink.Resync ]
        let mutable totalCount, totalRc, totalMs = 0L, 0., 0L
        let logActivity name count rc lat =
            log.Information("{name}: {count:n0} requests costing {ru:n0} RU (average: {avg:n2}); Average latency: {lat:n0}ms",
                name, count, rc, (if count = 0L then Double.NaN else rc/float count), (if count = 0L then Double.NaN else float lat/float count))
        for name, stat in stats do
            let ru = float stat.rux100 / 100.
            totalCount <- totalCount + stat.count
            totalRc <- totalRc + ru
            totalMs <- totalMs + stat.ms
            logActivity name stat.count ru stat.ms
        logActivity "TOTAL" totalCount totalRc totalMs

module Store =
    let read key = System.Environment.GetEnvironmentVariable key |> Option.ofObj |> Option.get

    let connector = CosmosConnector(requestTimeout=TimeSpan.FromSeconds 5., maxRetryAttemptsOnThrottledRequests=2, maxRetryWaitTimeInSeconds=5, log=Log.log)
    let conn = connector.Connect("equinox-tutorial", Discovery.FromConnectionString (read "EQUINOX_COSMOS_CONNECTION")) |> Async.RunSynchronously
    let gateway = CosmosGateway(conn, CosmosBatchingPolicy())

    let store = CosmosStore(gateway, read "EQUINOX_COSMOS_DATABASE", read "EQUINOX_COSMOS_COLLECTION")
    let cache = Caching.Cache("equinox-tutorial", 20)
    let cacheStrategy = CachingStrategy.SlidingWindow (cache, TimeSpan.FromMinutes 20.) // OR CachingStrategy.NoCaching

module FavoritesCategory = 
    let codec = Equinox.Codec.NewtonsoftJson.Json.Create<Favorites.Event>(Newtonsoft.Json.JsonSerializerSettings())
    let resolve = CosmosResolver(Store.store, codec, Favorites.fold, Favorites.initial, Store.cacheStrategy).Resolve

let service = Favorites.Service(Log.log, FavoritesCategory.resolve)

let client = "ClientJ"
service.Favorite(client, "a") |> Async.RunSynchronously
service.Favorite(client, "b") |> Async.RunSynchronously 
service.List(client) |> Async.RunSynchronously 

service.Unfavorite(client, "b") |> Async.RunSynchronously 
service.List(client) |> Async.RunSynchronously 

Log.dumpStats()