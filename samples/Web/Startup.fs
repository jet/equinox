namespace Web

open Argu
open Microsoft.AspNetCore.Builder
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Hosting
open Samples.Infrastructure
open Serilog
open Serilog.Events

[<NoEquality; NoComparison>]
type Arguments =
    | [<AltCommandLine "-V">]                                   Verbose
    | [<AltCommandLine "-S">]                                   LocalSeq
    | [<AltCommandLine "-C">]                                   Cached
    | [<AltCommandLine "-U">]                                   Unfolds
    | [<CliPrefix(CliPrefix.None); Last>]                       Memory   of ParseResults<Storage.MemoryStore.Parameters>
    | [<CliPrefix(CliPrefix.None); Last>]                       Cosmos   of ParseResults<Storage.Cosmos.Parameters>
    | [<CliPrefix(CliPrefix.None); Last>]                       Dynamo   of ParseResults<Storage.Dynamo.Parameters>
    | [<CliPrefix(CliPrefix.None); Last>]                       Es       of ParseResults<Storage.EventStore.Parameters>
    | [<CliPrefix(CliPrefix.None); Last; AltCommandLine "ms">]  MsSql    of ParseResults<Storage.Sql.Ms.Parameters>
    | [<CliPrefix(CliPrefix.None); Last; AltCommandLine "my">]  MySql    of ParseResults<Storage.Sql.My.Parameters>
    | [<CliPrefix(CliPrefix.None); Last; AltCommandLine "pg">]  Postgres of ParseResults<Storage.Sql.Pg.Parameters>
    | [<CliPrefix(CliPrefix.None); Last>]                       Mdb      of ParseResults<Storage.MessageDb.Parameters>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | Verbose ->                    "Include low level Domain and Store logging in screen output."
            | LocalSeq ->                   "configures writing to a local Seq endpoint at http://localhost:5341, see https://getseq.net"
            | Cached ->                     "employ a 50MB cache."
            | Unfolds ->                    "employ a store-appropriate Rolling Snapshots and/or Unfolding strategy."
            | Cosmos _ ->                   "specify storage in CosmosDB (--help for options)."
            | Dynamo _ ->                   "specify storage in DynamoDB (--help for options)."
            | Es _ ->                       "specify storage in EventStore (--help for options)."
            | Memory _ ->                   "specify In-Memory Volatile Store (Default store)."
            | MsSql _ ->                    "specify storage in Sql Server (--help for options)."
            | MySql _ ->                    "specify storage in MySql (--help for options)."
            | Postgres _ ->                 "specify storage in Postgres (--help for options)."
            | Mdb _ ->                      "specify storage in MessageDB (--help for options)."
module Arguments =
    let parse argv =
        let programName = System.Reflection.Assembly.GetEntryAssembly().GetName().Name
        ArgumentParser.Create<Arguments>(programName = programName).ParseCommandLine(argv)

type App = class end

// :shame: This should be a class used via UseStartup, but I couldn't figure out how to pass the parsed args in as MS have changed stuff around too much to make it googleable within my boredom threshold
type Startup() =
    // This method gets called by the runtime. Use this method to add services to the container.
    static member ConfigureServices(services: IServiceCollection, p : ParseResults<Arguments>) : unit =
        services
            .AddMvc()
            .AddJsonOptions(fun o ->
                FsCodec.SystemTextJson.Options.Default.Converters
                // NOTE this is technically superfluous as we don't use any constructs that require custom behavior in the models at present
                // The key side-effect we want to guarantee is that we reference `FsCodec.SystemTextJson`,
                // which will trigger a dependency on `System.Text.Json` >= `6.0.1`
                |> Seq.iter o.JsonSerializerOptions.Converters.Add) |> ignore

        let verbose = p.Contains Verbose
        let maybeSeq = if p.Contains LocalSeq then Some "http://localhost:5341" else None
        let createStoreLog storeVerbose =
            let c = LoggerConfiguration()
            let c = if storeVerbose then c.MinimumLevel.Debug() else c
            let c = c.WriteTo.Console((if storeVerbose && verbose then LogEventLevel.Debug else LogEventLevel.Warning), theme = Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code)
            let c = match maybeSeq with None -> c | Some endpoint -> c.WriteTo.Seq(endpoint)
            c.CreateLogger() :> ILogger

        let storeConfig, storeLog : Storage.StorageConfig * ILogger =
            let options = p.GetResults Cached @ p.GetResults Unfolds
            let unfolds = options |> List.exists (function Unfolds -> true | _ -> false)
            let log = Log.ForContext<App>()

            let cache = if options |> List.exists (function Cached -> true | _ -> false) then Equinox.Cache(Storage.appName, sizeMb = 50) |> Some else None
            match p.GetSubCommand() with
            | Cosmos sp ->
                let storeLog = createStoreLog <| sp.Contains Storage.Cosmos.Parameters.StoreVerbose
                log.Information("CosmosDB Storage options: {options:l}", options)
                Storage.Cosmos.config log (cache, unfolds) (Storage.Cosmos.Arguments sp), storeLog
            | Dynamo sp ->
                let storeLog = createStoreLog <| sp.Contains Storage.Dynamo.Parameters.StoreVerbose
                log.Information("DynamoDB Storage options: {options:l}", options)
                Storage.Dynamo.config log (cache, unfolds) (Storage.Dynamo.Arguments sp), storeLog
            | Es sp ->
                let storeLog = createStoreLog <| sp.Contains Storage.EventStore.Parameters.StoreVerbose
                log.Information("EventStoreDB Storage options: {options:l}", options)
                Storage.EventStore.config log (cache, unfolds) sp, storeLog
            | MsSql sp ->
                log.Information("SqlStreamStore MsSql Storage options: {options:l}", options)
                Storage.Sql.Ms.config log (cache, unfolds) sp, log
            | MySql sp ->
                log.Information("SqlStreamStore MySql Storage options: {options:l}", options)
                Storage.Sql.My.config log (cache, unfolds) sp, log
            | Postgres sp ->
                log.Information("SqlStreamStore Postgres Storage options: {options:l}", options)
                Storage.Sql.Pg.config log (cache, unfolds) sp, log
            | Memory _ ->
                log.Fatal("Web App is using Volatile Store; Storage options: {options:l}", options)
                Storage.MemoryStore.config (), log
            | Mdb sp ->
                log.Information("MessageDB Storage options: {options:l}", options)
                Storage.MessageDb.config log cache sp, log
            | x -> Storage.missingArg (sprintf "unexpected subcommand %A" x)
        Services.register(services, storeConfig, storeLog)

    // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
    static member Configure(app: IApplicationBuilder, env: IHostEnvironment) : unit =
        if env.IsDevelopment() then app.UseDeveloperExceptionPage() |> ignore
        else app.UseHsts().UseHttpsRedirection() |> ignore

        app
            .UseCors(fun x -> x.WithOrigins("https://www.todobackend.com").AllowAnyHeader().AllowAnyMethod() |> ignore)
            .UseRouting()
            .UseEndpoints(fun endpoints -> endpoints.MapControllers() |> ignore) |> ignore
