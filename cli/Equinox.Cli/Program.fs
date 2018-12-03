module Equinox.Cli.Program

open Argu
open Domain.Infrastructure
open Equinox.Cli.Infrastructure
open Microsoft.Extensions.DependencyInjection
open Samples
open Samples.Config
open Samples.Log
open Serilog
open System
open System.Threading

[<NoEquality; NoComparison>]
type Arguments =
    | [<AltCommandLine("-v")>] Verbose
    | [<AltCommandLine("-vc")>] VerboseConsole
    | [<AltCommandLine("-S")>] LocalSeq
    | [<AltCommandLine("-l")>] LogFile of string
    | [<CliPrefix(CliPrefix.None); Last; Unique; AltCommandLine("init")>] Initialize of ParseResults<InitArguments>
    | [<CliPrefix(CliPrefix.None); Last; Unique; AltCommandLine>] Run of ParseResults<TestArguments>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | Verbose -> "Include low level Domain logging."
            | VerboseConsole -> "Include low level Domain and Store logging in screen output."
            | LocalSeq -> "Configures writing to a local Seq endpoint at http://localhost:5341, see https://getseq.net"
            | LogFile _ -> "specify a log file to write the result breakdown (default: Equinox.Cli.log)."
            | Run _ -> "Run a load test"
            | Initialize _ -> "Initialize a store"
and [<NoComparison>]InitArguments =
    | [<AltCommandLine("-ru"); Mandatory>] Rus of int
    | [<CliPrefix(CliPrefix.None)>] Cosmos of ParseResults<CosmosArguments>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | Rus _ -> "Specify RUs to Allocate for the Application Collection."
            | Cosmos _ -> "Cosmos connection parameters."
and [<NoComparison>]WebArguments =
    | [<AltCommandLine("-u")>] Endpoint of string
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | Endpoint _ -> "Target address. Default: https://localhost:5001/api"
and [<NoComparison>]
    TestArguments =
    | [<AltCommandLine("-t"); First; Unique>] Name of Test
    | [<AltCommandLine("-C")>] Cached
    | [<AltCommandLine("-U")>] Unfolds
    | [<AltCommandLine("-f")>] TestsPerSecond of int
    | [<AltCommandLine("-d")>] DurationM of float
    | [<AltCommandLine("-e")>] ErrorCutoff of int64
    | [<AltCommandLine("-i")>] ReportIntervalS of int
    | [<CliPrefix(CliPrefix.None); Last; Unique>] Memory of ParseResults<MemArguments>
    | [<CliPrefix(CliPrefix.None); Last; Unique>] Es of ParseResults<EsArguments>
    | [<CliPrefix(CliPrefix.None); Last; Unique>] Cosmos of ParseResults<CosmosArguments>
    | [<CliPrefix(CliPrefix.None); Last; Unique>] Web of ParseResults<WebArguments>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | Name _ -> "Specify which test to run. (default: Favorites)"
            | Cached -> "Employ a 50MB cache"
            | Unfolds -> "Employ a store-appropriate Rolling Snapshots and/or Unfolding strategy"
            | TestsPerSecond _ -> "specify a target number of requests per second (default: 1000)."
            | DurationM _ -> "specify a run duration in minutes (default: 30)."
            | ErrorCutoff _ -> "specify an error cutoff (default: 10000)."
            | ReportIntervalS _ -> "specify reporting intervals in seconds (default: 10)."
            | Memory _ -> "Target in-process Transient Memory Store (Default if not other target specified)"
            | Es _ -> "Run transaction in-process against EventStore"
            | Cosmos _ -> "Run transaction in-process against CosmosDb"
            | Web _ -> "Run transaction against Web endpoint"
and Test = Favorites | SaveForLater

module Test =
    let run log testsPerSecond duration errorCutoff reportingIntervals (clients : ClientId[]) runSingleTest =
        let mutable idx = -1L
        let selectClient () =
            let clientIndex = Interlocked.Increment(&idx) |> int
            clients.[clientIndex % clients.Length]
        let selectClient = async { return async { return selectClient() } }
        Local.runLoadTest log reportingIntervals testsPerSecond errorCutoff duration selectClient runSingleTest

    let createTest (container: ServiceProvider) test =
        match test with
        | Favorites ->
            let service = container.GetRequiredService<Backend.Favorites.Service>()
            fun clientId -> async {
                let sku = Guid.NewGuid() |> SkuId
                do! service.Favorite(clientId,[sku])
                let! items = service.List clientId
                if items |> Array.exists (fun x -> x.skuId = sku) |> not then invalidOp "Added item not found" }
        | SaveForLater ->
            let service = container.GetRequiredService<Backend.SavedForLater.Service>()
            fun clientId -> async {
                let skus = [Guid.NewGuid() |> SkuId; Guid.NewGuid() |> SkuId; Guid.NewGuid() |> SkuId]
                let! saved = service.Save(clientId,skus)
                if saved then
                    let! items = service.List clientId
                    if skus |> List.forall (fun sku -> items |> Array.exists (fun x -> x.skuId = sku)) |> not then invalidOp "Added item not found"
                else
                    let! current = service.List clientId
                    let resolveSkus _hasSku = async {
                        return [|for x in current -> x.skuId|] }
                    let! removed = service.Remove(clientId, resolveSkus)
                    if not removed then invalidOp "Remove failed" }
    let createRunner (domainLog : ILogger, verbose) container (targs: ParseResults<TestArguments>) =
        let test = targs.GetResult(Name,Favorites)
        let run = createTest container test
        let execute clientId =
            if not verbose then run clientId
            else async {
                domainLog.Information("Executing for client {sessionId}", clientId)
                try return! run clientId
                with e -> domainLog.Warning(e, "Test threw an exception"); e.Reraise () }
        test,execute

let createResultLog fileName =
    LoggerConfiguration()
        .Destructure.FSharpTypes()
        .WriteTo.File(fileName)
        .CreateLogger()

let runTest (log: ILogger) (verbose,verboseConsole,maybeSeq) reportFilename (args: ParseResults<TestArguments>) =
    let storage = args.TryGetSubCommand()

    let createStoreLog verboseStore = Log.createStoreLog verboseStore verboseConsole maybeSeq
    let storeLog, storeConfig: ILogger * StorageConfig option =
        let options = args.GetResults Cached @ args.GetResults Unfolds
        let cache, unfolds = options |> List.contains Cached, options |> List.contains Unfolds

        match storage with
        | Some (Es sargs) ->
            let storeLog = createStoreLog <| sargs.Contains EsArguments.VerboseStore
            log.Information("EventStore Storage options: {options:l}", options)
            storeLog, EventStore.config (log,storeLog) (cache, unfolds) sargs |> Some
        | Some (Cosmos sargs) ->
            let storeLog = createStoreLog <| sargs.Contains CosmosArguments.VerboseStore
            log.Information("CosmosDb Storage options: {options:l}", options)
            storeLog, Cosmos.config (log,storeLog) (cache, unfolds) sargs |> Some
        | Some (Web wargs) ->
            createStoreLog false, None
        | _  | Some (Memory _) ->
            log.Information("Volatile Store; Storage options: {options:l}", options)
            createStoreLog false, MemoryStore.config () |> Some
    match storeConfig with
    | None -> failwith "TODO web"
    | Some storeConfig ->
        let services = ServiceCollection()
        services.AddSingleton<ILogger>(storeLog) |> ignore
        Services.registerServices(services, storeConfig)
        let container = services.BuildServiceProvider()

        let test, runTest = Test.createRunner (log, verbose) container args

        let errorCutoff = args.GetResult(ErrorCutoff,10000L)
        let testsPerSecond = args.GetResult(TestsPerSecond,1000)
        let duration = args.GetResult(DurationM,30.) |> TimeSpan.FromMinutes
        let reportingIntervals =
            match args.GetResults(ReportIntervalS) with
            | [] -> TimeSpan.FromSeconds 10.|> Seq.singleton
            | intervals -> seq { for i in intervals -> TimeSpan.FromSeconds(float i) }
            |> fun intervals -> [| yield duration; yield! intervals |]
        let clients = Array.init (testsPerSecond * 2) (fun _ -> Guid.NewGuid () |> ClientId)

        log.Information( "Running {test} for {duration} @ {tps} hits/s across {clients} clients; Max errors: {errorCutOff}, reporting intervals: {ri}, report file: {report}",
            test, duration, testsPerSecond, clients.Length, errorCutoff, reportingIntervals, reportFilename)
        let results = Test.run log testsPerSecond (duration.Add(TimeSpan.FromSeconds 5.)) errorCutoff reportingIntervals clients runTest |> Async.RunSynchronously

        let resultFile = createResultLog reportFilename
        for r in results do
            resultFile.Information("Aggregate: {aggregate}", r)
        log.Information("Run completed; Current memory allocation: {bytes:n2} MiB", (GC.GetTotalMemory(true) |> float) / 1024./1024.)

        match storeConfig with
        | StorageConfig.Cosmos _ ->
            let stats =
              [ "Read", RuCounterSink.Read
                "Write", RuCounterSink.Write
                "Resync", RuCounterSink.Resync ]
            let mutable totalCount, totalRc, totalMs = 0L, 0., 0L
            let logActivity name count rc lat =
                log.Information("{name}: {count:n0} requests costing {ru:n0} RU (average: {avg:n2}); Average latency: {lat:n0}ms",
                    name, count, rc, (if count = 0L then Double.NaN else rc/float count), (if count = 0L then Double.NaN else float lat/float count))
            for name, stat in stats do
                let ru = float stat.rux100 / 100.
                totalCount <- totalCount + stat.count
                totalRc <- totalRc + ru
                totalMs <- totalMs + stat.ms
                logActivity name stat.count ru stat.ms
            logActivity "TOTAL" totalCount totalRc totalMs
            let measures : (string * (TimeSpan -> float)) list =
              [ "s", fun x -> x.TotalSeconds
                "m", fun x -> x.TotalMinutes
                "h", fun x -> x.TotalHours ]
            let logPeriodicRate name count ru = log.Information("rp{name} {count:n0} = ~{ru:n0} RU", name, count, ru)
            let duration = args.GetResult(DurationM,1.) |> TimeSpan.FromMinutes
            for uom, f in measures do let d = f duration in if d <> 0. then logPeriodicRate uom (float totalCount/d |> int64) (totalRc/d)
        | _ -> ()

[<EntryPoint>]
let main argv =
    let programName = System.Reflection.Assembly.GetEntryAssembly().GetName().Name
    let parser = ArgumentParser.Create<Arguments>(programName = programName)
    try
        let args = parser.ParseCommandLine argv
        let verboseConsole = args.Contains VerboseConsole
        let maybeSeq = if args.Contains LocalSeq then Some "http://localhost:5341" else None
        let verbose = args.Contains Verbose
        let log = Log.createDomainLog verbose verboseConsole maybeSeq
        match args.GetSubCommand() with
        | Initialize iargs ->
            let rus = iargs.GetResult(Rus)
            match iargs.TryGetSubCommand() with
            | Some (InitArguments.Cosmos sargs) ->
                let storeLog = Log.createStoreLog (sargs.Contains CosmosArguments.VerboseStore) verboseConsole maybeSeq
                let dbName, collName, (_pageSize: int), conn = Cosmos.conn (log,storeLog) sargs
                log.Information("Configuring CosmosDb Collection with Throughput Provision: {rus:n0} RU/s", rus)
                Equinox.Cosmos.Store.Sync.Initialization.initialize log conn.Client dbName collName rus |> Async.RunSynchronously
            | _ -> failwith "please specify a cosmos endpoint"
        | Run rargs ->
            let reportFilename = args.GetResult(LogFile,programName+".log") |> fun n -> System.IO.FileInfo(n).FullName
            runTest log (verbose,verboseConsole,maybeSeq) reportFilename rargs
        | _ -> failwith "Please specify a valid subcommand :- init or run"
        0
    with e ->
        printfn "%s" e.Message
        1