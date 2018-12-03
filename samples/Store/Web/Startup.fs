namespace Web

open Argu
open Microsoft.AspNetCore.Builder
open Microsoft.AspNetCore.Hosting
open Microsoft.AspNetCore.Mvc
open Microsoft.Extensions.Configuration
open Microsoft.Extensions.DependencyInjection
open Samples
open Samples.Config
open System

[<NoComparison>]
type Arguments =
    | [<AltCommandLine("-v")>] Verbose
    | [<AltCommandLine("-vc")>] VerboseConsole
    | [<AltCommandLine("-S")>] LocalSeq
    | [<AltCommandLine("-C")>] Cached
    | [<AltCommandLine("-U")>] Unfolds
    | [<CliPrefix(CliPrefix.None); Last; Unique>] Memory of ParseResults<MemArguments>
    | [<CliPrefix(CliPrefix.None); Last; Unique>] Es of ParseResults<EsArguments>
    | [<CliPrefix(CliPrefix.None); Last; Unique>] Cosmos of ParseResults<CosmosArguments>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | Verbose -> "Include low level Domain logging."
            | VerboseConsole -> "Include low level Domain and Store logging in screen output."
            | LocalSeq -> "Configures writing to a local Seq endpoint at http://localhost:5341, see https://getseq.net"
            | Memory _ -> "specify In-Memory Volatile Store (Default store)"
            | Es _ -> "specify EventStore actions"
            | Cosmos _ -> "specify CosmosDb actions"
            | Cached -> "Employ a 50MB cache"
            | Unfolds -> "Employ a store-appropriate Rolling Snapshots and/or Unfolding strategy"

type Startup(_configuration: IConfiguration) =
    let configureMvc (services : IServiceCollection) =
        services.AddMvc().SetCompatibilityVersion(CompatibilityVersion.Version_2_1) |> ignore

    // This method gets called by the runtime. Use this method to add services to the container.
    member __.ConfigureServices(services: IServiceCollection) : unit =
        configureMvc services

        let programName = System.Reflection.Assembly.GetEntryAssembly().GetName().Name
        let args = ArgumentParser.Create<Arguments>(programName = programName).ParseCommandLine(Environment.GetCommandLineArgs() |> Array.tail)

        let verboseConsole = args.Contains VerboseConsole
        let maybeSeq = if args.Contains LocalSeq then Some "http://localhost:5341" else None
        let createStoreLog verboseStore = Log.createStoreLog verboseStore verboseConsole maybeSeq

        let storeConfig : StorageConfig =
            let options = args.GetResults Cached @ args.GetResults Unfolds
            let cache, unfolds = options |> List.contains Cached, options |> List.contains Unfolds
            let verbose = args.Contains Verbose
            let log = Log.createDomainLog verbose verboseConsole maybeSeq

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
                log.Information("Volatile Store; Storage options: {options:l}", options)
                MemoryStore.config ()
        Services.registerServices(services, storeConfig)

    // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
    member __.Configure(app: IApplicationBuilder, env: IHostingEnvironment) : unit =
        if env.IsDevelopment() then app.UseDeveloperExceptionPage() |> ignore
        else app.UseHsts() |> ignore

        app.UseHttpsRedirection()
            .UseMvc() |> ignore