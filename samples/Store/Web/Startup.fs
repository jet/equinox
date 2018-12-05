namespace Web

open Argu
open Microsoft.AspNetCore.Builder
open Microsoft.AspNetCore.Hosting
open Microsoft.AspNetCore.Mvc
open Microsoft.Extensions.DependencyInjection
open Samples
open Samples.Config
open Serilog
open Serilog.Events

[<NoComparison>]
type Arguments =
    | [<AltCommandLine("-vc")>] VerboseConsole
    | [<AltCommandLine("-S")>] LocalSeq
    | [<AltCommandLine("-C")>] Cached
    | [<AltCommandLine("-U")>] Unfolds
    | [<CliPrefix(CliPrefix.None); Last; Unique>] Memory of ParseResults<MemArguments>
    | [<CliPrefix(CliPrefix.None); Last; Unique>] Es of ParseResults<EsArguments>
    | [<CliPrefix(CliPrefix.None); Last; Unique>] Cosmos of ParseResults<CosmosArguments>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | VerboseConsole -> "Include low level Domain and Store logging in screen output."
            | LocalSeq -> "Configures writing to a local Seq endpoint at http://localhost:5341, see https://getseq.net"
            | Memory _ -> "specify In-Memory Volatile Store (Default store)"
            | Es _ -> "specify EventStore actions"
            | Cosmos _ -> "specify CosmosDb actions"
            | Cached -> "Employ a 50MB cache"
            | Unfolds -> "Employ a store-appropriate Rolling Snapshots and/or Unfolding strategy"

type App = class end

// :shame: This should be a class uaed via UseStartup, but I couldnt figure out how to pass the parsed args in as MS have changed stuff around too much to make it googleable within my boredom threshold
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

        let storeConfig : StorageConfig =
            let options = args.GetResults Cached @ args.GetResults Unfolds
            let cache, unfolds = options |> List.contains Cached, options |> List.contains Unfolds
            let log = Log.ForContext<App>()

            match args.TryGetSubCommand() with
            | Some (Es sargs) ->
                let storeLog = createStoreLog <| sargs.Contains EsArguments.VerboseStore
                log.Information("EventStore Storage options: {options:l}", options)
                EventStore.config (log,storeLog) (cache, unfolds) sargs
            | Some (Cosmos sargs) ->
                let storeLog = createStoreLog <| sargs.Contains CosmosArguments.VerboseStore
                log.Information("CosmosDb Storage options: {options:l}", options)
                Cosmos.config (log,storeLog) (cache, unfolds) sargs
            | _  | Some (Memory _) ->
                log.Fatal("Web App is using Volatile Store; Storage options: {options:l}", options)
                MemoryStore.config ()
        Services.registerServices(services, storeConfig)

    // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
    static member Configure(app: IApplicationBuilder, env: IHostingEnvironment) : unit =
        if env.IsDevelopment() then app.UseDeveloperExceptionPage() |> ignore
        else app.UseHsts() |> ignore

        app.UseHttpsRedirection()
            .UseMvc() |> ignore