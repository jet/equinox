module Web.Program

open Argu
open Microsoft.AspNetCore.Builder
open Microsoft.Extensions.Hosting
open Serilog

let writeToStatsSinks (c: LoggerConfiguration) =
    c.WriteTo.Sink(Equinox.CosmosStore.Core.Log.InternalMetrics.Stats.LogSink())
     .WriteTo.Sink(Equinox.DynamoStore.Core.Log.InternalMetrics.Stats.LogSink())
     .WriteTo.Sink(Equinox.EventStoreDb.Log.InternalMetrics.Stats.LogSink())
     .WriteTo.Sink(Equinox.SqlStreamStore.Log.InternalMetrics.Stats.LogSink())

let createWebHost (args: string[], parsed) : IHost =
    let wab = WebApplication.CreateBuilder(args)
    Startup.ConfigureServices(wab.Services, parsed)
    wab.Host.UseSerilog() |> ignore
    let wa = wab.Build()
    Startup.Configure(wa)
    wa

[<EntryPoint>]
let main argv =
    try printfn "Running at pid %d" (System.Diagnostics.Process.GetCurrentProcess().Id)
        let p = Arguments.Parse argv
        // Replace logger chain with https://github.com/serilog/serilog-aspnetcore
        let c =
            LoggerConfiguration()
                .MinimumLevel.Debug()
                .MinimumLevel.Override("Microsoft", Serilog.Events.LogEventLevel.Warning)
                .WriteTo.Console()
                // TOCONSIDER log and reset every minute or something ?
                |> writeToStatsSinks
        let c =
            let maybeSeq = if p.Contains LocalSeq then Some "http://localhost:5341" else None
            match maybeSeq with None -> c | Some endpoint -> c.WriteTo.Seq(endpoint)
        try Log.Logger <- c.CreateLogger()
            createWebHost(argv, p).Run()
            0
        finally Log.CloseAndFlush()
    with :? ArguParseException as e -> eprintfn $"%s{e.Message}"; 1
        | e -> eprintfn $"EXCEPTION: %O{e}"; 1
