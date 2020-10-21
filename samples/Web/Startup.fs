namespace Web

open Argu
open Microsoft.AspNetCore.Builder
open Microsoft.AspNetCore.Mvc
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Hosting
open Samples.Infrastructure
open Serilog
open Serilog.Events

[<NoComparison>]
type Arguments =
    | [<AltCommandLine "-V">]                                 Verbose
    | [<AltCommandLine "-S">]                                 LocalSeq
    | [<AltCommandLine "-C">]                                 Cached
    | [<AltCommandLine "-U">]                                 Unfolds
    | [<CliPrefix(CliPrefix.None); Last>]                       Cosmos   of ParseResults<Storage.Cosmos.Arguments>
    | [<CliPrefix(CliPrefix.None); Last>]                       Es       of ParseResults<Storage.EventStore.Arguments>
    | [<CliPrefix(CliPrefix.None); Last>]                       Memory   of ParseResults<Storage.MemoryStore.Arguments>
    | [<CliPrefix(CliPrefix.None); Last; AltCommandLine "ms">]  MsSql    of ParseResults<Storage.Sql.Ms.Arguments>
    | [<CliPrefix(CliPrefix.None); Last; AltCommandLine "my">]  MySql    of ParseResults<Storage.Sql.My.Arguments>
    | [<CliPrefix(CliPrefix.None); Last; AltCommandLine "pg">]  Postgres of ParseResults<Storage.Sql.Pg.Arguments>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | Verbose ->                    "Include low level Domain and Store logging in screen output."
            | LocalSeq ->                   "configures writing to a local Seq endpoint at http://localhost:5341, see https://getseq.net"
            | Cached ->                     "employ a 50MB cache."
            | Unfolds ->                    "employ a store-appropriate Rolling Snapshots and/or Unfolding strategy."
            | Cosmos _ ->                   "specify storage in CosmosDB (--help for options)."
            | Es _ ->                       "specify storage in EventStore (--help for options)."
            | Memory _ ->                   "specify In-Memory Volatile Store (Default store)."
            | MsSql _ ->                    "specify storage in Sql Server (--help for options)."
            | MySql _ ->                    "specify storage in MySql (--help for options)."
            | Postgres _ ->                 "specify storage in Postgres (--help for options)."

type App = class end

// :shame: This should be a class used via UseStartup, but I couldnt figure out how to pass the parsed args in as MS have changed stuff around too much to make it googleable within my boredom threshold
type Startup() =
    // This method gets called by the runtime. Use this method to add services to the container.
    static member ConfigureServices(services: IServiceCollection, args: ParseResults<Arguments>) : unit =
        services
            .AddMvc()
            .AddJsonOptions(fun o ->
                // NOTE this is technically superfluous as we don't use `option`s in the models at present
                // The key side-effect we want to guarantee is that we reference `FsCodec.SystemTextJson`,
                // which will trigger a dependency on `System.Text.Json` >= `5.0.0-preview.3`
                // This makes F# records roundtrip (System.Text.Json v4 required parameterless constructors on records)
                o.JsonSerializerOptions.Converters.Add(FsCodec.SystemTextJson.Converters.JsonOptionConverter()))
            .SetCompatibilityVersion(CompatibilityVersion.Latest) |> ignore

        let verbose = args.Contains Verbose
        let maybeSeq = if args.Contains LocalSeq then Some "http://localhost:5341" else None
        let createStoreLog verboseStore =
            let c = LoggerConfiguration().Destructure.FSharpTypes()
            let c = if verboseStore then c.MinimumLevel.Debug() else c
            let c = c.WriteTo.Console((if verboseStore && verbose then LogEventLevel.Debug else LogEventLevel.Warning), theme = Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code)
            let c = match maybeSeq with None -> c | Some endpoint -> c.WriteTo.Seq(endpoint)
            c.CreateLogger() :> ILogger

        let storeConfig, storeLog : Storage.StorageConfig * ILogger =
            let options = args.GetResults Cached @ args.GetResults Unfolds
            let unfolds = options |> List.exists (function Unfolds -> true | _ -> false)
            let log = Log.ForContext<App>()

            let cache = if options |> List.exists (function Cached -> true | _ -> false) then Equinox.Cache(Storage.appName, sizeMb = 50) |> Some else None
            match args.TryGetSubCommand() with
            | Some (Cosmos sargs) ->
                let storeLog = createStoreLog <| sargs.Contains Storage.Cosmos.Arguments.VerboseStore
                log.Information("CosmosDB Storage options: {options:l}", options)
                Storage.Cosmos.config log (cache, unfolds) (Storage.Cosmos.Info sargs), storeLog
            | Some (Es sargs) ->
                let storeLog = createStoreLog <| sargs.Contains Storage.EventStore.Arguments.VerboseStore
                log.Information("EventStoreDB Storage options: {options:l}", options)
                Storage.EventStore.config (log,storeLog) (cache, unfolds) sargs, storeLog
            | Some (MsSql sargs) ->
                log.Information("SqlStreamStore MsSql Storage options: {options:l}", options)
                Storage.Sql.Ms.config log (cache, unfolds) sargs, log
            | Some (MySql sargs) ->
                log.Information("SqlStreamStore MySql Storage options: {options:l}", options)
                Storage.Sql.My.config log (cache, unfolds) sargs, log
            | Some (Postgres sargs) ->
                log.Information("SqlStreamStore Postgres Storage options: {options:l}", options)
                Storage.Sql.Pg.config log (cache, unfolds) sargs, log
            | _  | Some (Memory _) ->
                log.Fatal("Web App is using Volatile Store; Storage options: {options:l}", options)
                Storage.MemoryStore.config (), log
        Services.register(services, storeConfig, storeLog)

    // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
    static member Configure(app: IApplicationBuilder, env: IHostEnvironment) : unit =
        if env.IsDevelopment() then app.UseDeveloperExceptionPage() |> ignore
        else app.UseHsts() |> ignore

        app
            .UseHttpsRedirection()
            .UseCors(fun x -> x.WithOrigins("https://www.todobackend.com").AllowAnyHeader().AllowAnyMethod() |> ignore)
            .UseRouting()
            .UseEndpoints(fun endpoints -> endpoints.MapControllers() |> ignore) |> ignore
