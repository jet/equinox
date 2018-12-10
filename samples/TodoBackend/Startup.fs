namespace TodoBackend

open Argu
open Microsoft.AspNetCore.Builder
open Microsoft.AspNetCore.Hosting
open Microsoft.AspNetCore.Mvc
open Microsoft.Extensions.DependencyInjection
open Samples.Infrastructure.Services
open Samples.Infrastructure.Storage
open Serilog
open Serilog.Events
open System

[<NoComparison>]
type Arguments =
    | [<AltCommandLine("-vc")>] VerboseConsole
    | [<AltCommandLine("-S")>] LocalSeq
    | [<AltCommandLine("-C")>] Cached
    | [<AltCommandLine("-U")>] Unfolds
    | [<CliPrefix(CliPrefix.None); Last; Unique>] Memory of ParseResults<MemArguments>
    | [<CliPrefix(CliPrefix.None); Last; Unique>] Es of ParseResults<EsArguments>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | VerboseConsole -> "Include low level Domain and Store logging in screen output."
            | LocalSeq -> "configures writing to a local Seq endpoint at http://localhost:5341, see https://getseq.net"
            | Cached -> "employ a 50MB cache."
            | Unfolds -> "employ a store-appropriate Rolling Snapshots and/or Unfolding strategy."
            | Memory _ -> "specify In-Memory Volatile Store (Default store)."
            | Es _ -> "specify storage in EventStore (--help for options)."

type App = class end

// :shame: This should be a class used via UseStartup, but I couldnt figure out how to pass the parsed args in as MS have changed stuff around too much to make it googleable within my boredom threshold
type Startup() =
    // This method gets called by the runtime. Use this method to add services to the container.
    static member ConfigureServices(services: IServiceCollection, args: ParseResults<Arguments>) : unit =
        services.AddCors().AddMvc().SetCompatibilityVersion(CompatibilityVersion.Version_2_1) |> ignore

        let verboseConsole = args.Contains VerboseConsole
        let maybeSeq = if args.Contains LocalSeq then Some "http://localhost:5341" else None
        let createStoreLog verboseStore =
            let c = LoggerConfiguration().Destructure.FSharpTypes()
            let c = if verboseStore then c.MinimumLevel.Debug() else c
            let c = c.WriteTo.Console((if verboseStore && verboseConsole then LogEventLevel.Debug else LogEventLevel.Warning), theme = Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code)
            let c = match maybeSeq with None -> c | Some endpoint -> c.WriteTo.Seq(endpoint)
            c.CreateLogger() :> ILogger

        let storeConfig, storeLog : StorageConfig * ILogger =
            let options = args.GetResults Cached @ args.GetResults Unfolds
            let cache, unfolds = options |> List.contains Cached, options |> List.contains Unfolds
            let log = Log.ForContext<App>()

            match args.TryGetSubCommand() with
            | Some (Es sargs) ->
                let storeLog = createStoreLog <| sargs.Contains EsArguments.VerboseStore
                log.Information("EventStore Storage options: {options:l}", options)
                EventStore.config (log,storeLog) (cache, unfolds) sargs, storeLog
            | _  | Some (Memory _) ->
                log.Fatal("Web App is using Volatile Store; Storage options: {options:l}", options)
                MemoryStore.config (), log

        let regF (factory : IServiceProvider -> 'T) = services.AddSingleton<'T>(fun (sp: IServiceProvider) -> factory sp) |> ignore

        let resolver = StreamResolver(storeConfig)

        regF <| fun _sp ->
            let codec = genCodec<Events.Event>()
            let fold, initial, snapshot = Folds.fold, Folds.initial, Folds.snapshot
            Service(storeLog, resolver.Resolve(codec,fold,initial,snapshot))

    // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
    static member Configure(app: IApplicationBuilder, env: IHostingEnvironment) : unit =
        if env.IsDevelopment() then app.UseDeveloperExceptionPage() |> ignore
        else app.UseHsts() |> ignore

        app.UseHttpsRedirection()
            .UseCors(fun x -> x.WithOrigins([|"https://www.todobackend.com"|]).AllowAnyHeader().AllowAnyMethod() |> ignore)
            .UseMvc() |> ignore