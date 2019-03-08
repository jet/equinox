namespace Web

open Argu
open Microsoft.AspNetCore.Builder
open Microsoft.AspNetCore.Hosting
open Microsoft.AspNetCore.Mvc
open Microsoft.Extensions.DependencyInjection
open Samples.Infrastructure
open Serilog
open Serilog.Events

[<NoComparison>]
type Arguments =
    | [<AltCommandLine("-vc")>] VerboseConsole
    | [<AltCommandLine("-S")>] LocalSeq
    | [<AltCommandLine("-C")>] Cached
    | [<AltCommandLine("-U")>] Unfolds
    | [<CliPrefix(CliPrefix.None); Last; Unique>] Memory of ParseResults<Storage.MemoryStore.Arguments>
    | [<CliPrefix(CliPrefix.None); Last; Unique>] Es of ParseResults<Storage.EventStore.Arguments>
    | [<CliPrefix(CliPrefix.None); Last; Unique>] Cosmos of ParseResults<Storage.Cosmos.Arguments>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | VerboseConsole -> "Include low level Domain and Store logging in screen output."
            | LocalSeq -> "configures writing to a local Seq endpoint at http://localhost:5341, see https://getseq.net"
            | Cached -> "employ a 50MB cache."
            | Unfolds -> "employ a store-appropriate Rolling Snapshots and/or Unfolding strategy."
            | Memory _ -> "specify In-Memory Volatile Store (Default store)."
            | Es _ -> "specify storage in EventStore (--help for options)."
            | Cosmos _ -> "specify storage in CosmosDb (--help for options)."

type App = class end

// :shame: This should be a class used via UseStartup, but I couldnt figure out how to pass the parsed args in as MS have changed stuff around too much to make it googleable within my boredom threshold
type Startup() =
    // This method gets called by the runtime. Use this method to add services to the container.
    static member ConfigureServices(services: IServiceCollection, args: ParseResults<Arguments>) : unit =
        services.AddMvc().SetCompatibilityVersion(CompatibilityVersion.Version_2_1) |> ignore

        let verboseConsole = args.Contains VerboseConsole
        let maybeSeq = if args.Contains LocalSeq then Some "http://localhost:5341" else None
        let createStoreLog verboseStore =
            let c = LoggerConfiguration().Destructure.FSharpTypes()
            let c = if verboseStore then c.MinimumLevel.Debug() else c
            let c = c.WriteTo.Console((if verboseStore && verboseConsole then LogEventLevel.Debug else LogEventLevel.Warning), theme = Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code)
            let c = match maybeSeq with None -> c | Some endpoint -> c.WriteTo.Seq(endpoint)
            c.CreateLogger() :> ILogger

        let storeConfig, storeLog : Storage.StorageConfig * ILogger =
            let options = args.GetResults Cached @ args.GetResults Unfolds
            let cache, unfolds = options |> List.contains Cached, options |> List.contains Unfolds
            let defaultBatchSize = 500
            let log = Log.ForContext<App>()

            match args.TryGetSubCommand() with
            | Some (Es sargs) ->
                let storeLog = createStoreLog <| sargs.Contains Storage.EventStore.Arguments.VerboseStore
                log.Information("EventStore Storage options: {options:l}", options)
                Storage.EventStore.config (log,storeLog) (cache, unfolds, defaultBatchSize) sargs, storeLog
            | Some (Cosmos sargs) ->
                let storeLog = createStoreLog <| sargs.Contains Storage.Cosmos.Arguments.VerboseStore
                log.Information("CosmosDb Storage options: {options:l}", options)
                Storage.Cosmos.config (log,storeLog) (cache, unfolds, defaultBatchSize) (Storage.Cosmos.Info sargs), storeLog
            | _  | Some (Memory _) ->
                log.Fatal("Web App is using Volatile Store; Storage options: {options:l}", options)
                Storage.MemoryStore.config (), log
        Services.register(services, storeConfig, storeLog)

    // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
    static member Configure(app: IApplicationBuilder, env: IHostingEnvironment) : unit =
        if env.IsDevelopment() then app.UseDeveloperExceptionPage() |> ignore
        else app.UseHsts() |> ignore

        app.UseHttpsRedirection()
            .UseCors(fun x -> x.WithOrigins([|"https://www.todobackend.com"|]).AllowAnyHeader().AllowAnyMethod() |> ignore)
            .UseMvc() |> ignore