namespace Web

open Microsoft.AspNetCore
open Microsoft.AspNetCore.Hosting
open Serilog

module Program =
    let exitCode = 0

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
        createWebHostBuilder(args).Build().Run()

        exitCode
