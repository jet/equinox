﻿// Compile Tutorial.fsproj by either a) right-clicking or b) typing
// dotnet build samples/Tutorial before attempting to send this to FSI with Alt-Enter
#if VISUALSTUDIO
#r "netstandard"
#endif
#I "bin/Debug/netstandard2.0/"
#r "Serilog.dll"
#r "Serilog.Sinks.Console.dll"
#r "Newtonsoft.Json.dll"
#r "TypeShape.dll"
#r "Equinox.dll"
#r "Equinox.Core.dll"
#r "FSharp.UMX.dll"
#r "FsCodec.dll"
#r "FsCodec.NewtonsoftJson.dll"
#r "FSharp.Control.AsyncSeq.dll"
#r "Microsoft.Azure.DocumentDb.Core.dll"
#r "System.Net.Http"
#r "Serilog.Sinks.Seq.dll"
#r "Equinox.Cosmos.dll"

let Category = "Favorites"
let streamName clientId = FsCodec.StreamName.create Category clientId

module Favorites =

    type Item = { sku : string }
    type Event =
        | Added of Item
        | Removed of Item
        interface TypeShape.UnionContract.IUnionContract
    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()
    let initial : string list = []
    let evolve state = function
        | Added {sku = sku } -> sku :: state
        | Removed {sku = sku } -> state |> List.filter (fun x -> x <> sku)
    let fold s xs = Seq.fold evolve s xs

    type Command =
        | Add of string
        | Remove of string
    let interpret command state =
        match command with
        | Add sku -> if state |> List.contains sku then [] else [Added {sku = sku}]
        | Remove sku -> if state |> List.contains sku then [Removed {sku = sku}] else []

    type Service internal (resolve : ClientId -> Equinox.Stream<Events.Event, Fold.State>) =

        let execute clientId command : Async<unit> =
            let stream = resolve clientId
            stream.Transact(interpret command)

        member __.Favorite(clientId, sku) = execute clientId (Add sku)
        member __.Unfavorite(clientId, skus) = execute clientId (Remove skus)
        member __.List clientId: Async<string list> =
            let stream = resolve clientId
            stream.Query id

module Log =
    open Serilog
    open Serilog.Events
    let verbose = true // false will remove lots of noise
    let log =
        let c = LoggerConfiguration()
        let c = if verbose then c.MinimumLevel.Debug() else c
        let c = c.WriteTo.Sink(Store.Log.InternalMetrics.Stats.LogSink()) // to power Log.InternalMetrics.dump
        let c = c.WriteTo.Seq("http://localhost:5341") // https://getseq.net
        let c = c.WriteTo.Console(if verbose then LogEventLevel.Debug else LogEventLevel.Information)
        c.CreateLogger()
    let dumpMetrics () = Store.Log.InternalMetrics.dump log

let [<Literal>] appName = "equinox-tutorial"
let cache = Equinox.Cache(appName, 20)

module Store =
    let read key = System.Environment.GetEnvironmentVariable key |> Option.ofObj |> Option.get

    let connector = Connector(TimeSpan.FromSeconds 5., 2, TimeSpan.FromSeconds 5., log=Log.log)
    let conn = connector.Connect(appName, Discovery.FromConnectionString (read "EQUINOX_COSMOS_CONNECTION")) |> Async.RunSynchronously
    let gateway = Gateway(conn, BatchingPolicy())

    let context = Context(gateway, read "EQUINOX_COSMOS_DATABASE", read "EQUINOX_COSMOS_CONTAINER")
    let cacheStrategy = CachingStrategy.SlidingWindow (cache, TimeSpan.FromMinutes 20.) // OR CachingStrategy.NoCaching

module FavoritesCategory = 
    let resolver = Resolver(Store.context, Favorites.codec, Favorites.fold, Favorites.initial, Store.cacheStrategy, AccessStrategy.Unoptimized)

let service resolve =
    let resolve clientId =
        let streamName = streamName clientId
        Equinox.Stream(Log.log, FavoritesCategory.resolver.Resolve streamName, maxAttempts = 3)
    Favorites.Service resolve

let client = "ClientJ"
service.Favorite(client, "a") |> Async.RunSynchronously
service.Favorite(client, "b") |> Async.RunSynchronously 
service.List(client) |> Async.RunSynchronously 

service.Unfavorite(client, "b") |> Async.RunSynchronously 
service.List(client) |> Async.RunSynchronously 

Log.dumpMetrics ()