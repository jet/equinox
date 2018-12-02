namespace Web

open Microsoft.AspNetCore
open Microsoft.AspNetCore.Hosting
open Serilog

module Program =
    let createWebHostBuilder args : IWebHostBuilder =
        WebHost
            .CreateDefaultBuilder(args)
            .UseStartup<Startup>()
            .UseSerilog(fun hostingContext (loggerConfiguration : LoggerConfiguration) ->
                loggerConfiguration
                    .ReadFrom.Configuration(hostingContext.Configuration)
                    .Enrich.FromLogContext()
                    .WriteTo.Console() |> ignore)

    [<EntryPoint>]
    let main args =
        try
            createWebHostBuilder(args).Build().Run()
            0
        with e ->
            eprintfn "%s" e.Message
            1