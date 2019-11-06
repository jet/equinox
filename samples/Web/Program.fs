namespace Web

open Argu
open Microsoft.AspNetCore
open Microsoft.AspNetCore.Hosting
open Microsoft.Extensions.DependencyInjection
open Serilog

module Program =
    let createWebHostBuilder (args,parsed) : IWebHostBuilder =
        WebHost
            .CreateDefaultBuilder(args)
            .ConfigureServices(fun services -> Startup.ConfigureServices(services, parsed))
            .Configure(fun app -> Startup.Configure(app, app.ApplicationServices.GetService<IHostingEnvironment>()))
            .UseSerilog()

    [<EntryPoint>]
    let main argv =
        try
            let programName = System.Reflection.Assembly.GetEntryAssembly().GetName().Name
            printfn "Running at pid %d" (System.Diagnostics.Process.GetCurrentProcess().Id)
            let args = ArgumentParser.Create<Arguments>(programName = programName).ParseCommandLine(argv)
            // Replace logger chain with https://github.com/serilog/serilog-aspnetcore
            let c =
                LoggerConfiguration()
                    .MinimumLevel.Debug()
                    .MinimumLevel.Override("Microsoft", Serilog.Events.LogEventLevel.Warning)
                    .Enrich.FromLogContext()
                    .WriteTo.Console()
                    // TOCONSIDER log and reset every minute or something ?
                    .WriteTo.Sink(Equinox.Cosmos.Store.Log.InternalMetrics.Stats.LogSink())
                    .WriteTo.Sink(Equinox.EventStore.Log.InternalMetrics.Stats.LogSink())
                    .WriteTo.Sink(Equinox.SqlStreamStore.Log.InternalMetrics.Stats.LogSink())
            let c =
                let maybeSeq = if args.Contains LocalSeq then Some "http://localhost:5341" else None
                match maybeSeq with None -> c | Some endpoint -> c.WriteTo.Seq(endpoint)
            let log : ILogger = c.CreateLogger() :> _
            Log.Logger <- log
            createWebHostBuilder(argv, args).Build().Run()
            0
        with e ->
            eprintfn "%s" e.Message
            1