module Equinox.Tool.Program

open Argu
open Equinox.Tool.Infrastructure
open FSharp.AWS.DynamoDB // Throughput
open FSharp.Control
open FSharp.UMX
open Microsoft.Extensions.DependencyInjection
open Samples.Infrastructure
open Serilog
open Serilog.Events
open System
open System.Net.Http
open System.Threading

module CosmosInit = Equinox.CosmosStore.Core.Initialization

let [<Literal>] appName = "equinox-tool"

[<NoEquality; NoComparison>]
type Arguments =
    | [<AltCommandLine "-V"; Unique>]       Verbose
    | [<AltCommandLine "-C"; Unique>]       VerboseConsole
    | [<AltCommandLine "-S"; Unique>]       LocalSeq
    | [<AltCommandLine "-l"; Unique>]       LogFile of string
    | [<CliPrefix(CliPrefix.None); Last>]   Run of ParseResults<TestParameters>
    | [<CliPrefix(CliPrefix.None); Last>]   Init of ParseResults<InitParameters>
    | [<CliPrefix(CliPrefix.None); Last>]   InitAws of ParseResults<TableParameters>
    | [<CliPrefix(CliPrefix.None); Last>]   Config of ParseResults<ConfigParameters>
    | [<CliPrefix(CliPrefix.None); Last>]   Stats of ParseResults<StatsParameters>
    | [<CliPrefix(CliPrefix.None); Last>]   Query of ParseResults<QueryParameters>
    | [<CliPrefix(CliPrefix.None); Last>]   Dump of ParseResults<DumpParameters>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | Verbose ->                    "Include low level logging regarding specific test runs."
            | VerboseConsole ->             "Include low level test and store actions logging in on-screen output to console."
            | LocalSeq ->                   "Configures writing to a local Seq endpoint at http://localhost:5341, see https://getseq.net"
            | LogFile _ ->                  "specify a log file to write the result breakdown into (default: eqx.log)."
            | Run _ ->                      "Run a load test"
            | Init _ ->                     "Initialize Store/Container (supports `cosmos` stores; also handles RU/s provisioning adjustment)."
            | InitAws _ ->                  "Initialize DynamoDB Table (supports `dynamo` stores; also handles RU/s provisioning adjustment)."
            | Config _ ->                   "Initialize Database Schema (supports `mssql`/`mysql`/`postgres` SqlStreamStore stores)."
            | Stats _ ->                    "inspect store to determine numbers of streams/documents/events and/or config (supports `cosmos` and `dynamo` stores)."
            | Query _ ->                    "Load/Summarise streams based on Cosmos SQL Queries (supports `cosmos` only)."
            | Dump _ ->                     "Load and show events in a specified stream (supports all stores)."
and [<NoComparison; NoEquality; RequireSubcommand>] InitParameters =
    | [<AltCommandLine "-ru"; Unique>]      Rus of int
    | [<AltCommandLine "-A"; Unique>]       Autoscale
    | [<AltCommandLine "-m"; Unique>]       Mode of CosmosModeType
    | [<AltCommandLine "-P"; Unique>]       SkipStoredProc
    | [<AltCommandLine "-U"; Unique>]       IndexUnfolds
    | [<CliPrefix(CliPrefix.None)>]         Cosmos of ParseResults<Store.Cosmos.Parameters>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | Rus _ ->                      "Specify RU/s level to provision (Not applicable for Serverless Mode; Default: 400 RU/s for Container/Database; Default: Max 4000 RU/s for Container/Database when Autoscale specified)."
            | Autoscale ->                  "Autoscale provisioned throughput. Use --rus to specify the maximum RU/s."
            | Mode _ ->                     "Configure RU mode to use Container-level RU, Database-level RU, or Serverless allocations (Default: Use Container-level allocation)."
            | SkipStoredProc ->             "Inhibit creation of stored procedure in specified Container."
            | IndexUnfolds ->               "Index `c` and `d` fields within the `u` field of Tip items. Default: Don't index"
            | Cosmos _ ->                   "Cosmos Connection parameters."
and CosmosModeType = Container | Db | Serverless
and CosmosInitArguments(p : ParseResults<InitParameters>) =
    let rusOrDefault (value: int) = p.GetResult(Rus, value)
    let throughput auto = if auto then CosmosInit.Throughput.Autoscale (rusOrDefault 4000) else CosmosInit.Throughput.Manual (rusOrDefault 400)
    member val ProvisioningMode =
        match p.GetResult(InitParameters.Mode, CosmosModeType.Container), p.Contains Autoscale with
        | CosmosModeType.Container, auto -> CosmosInit.Provisioning.Container (throughput auto)
        | CosmosModeType.Db, auto ->        CosmosInit.Provisioning.Database (throughput auto)
        | CosmosModeType.Serverless, auto when auto || p.Contains Rus -> p.Raise "Cannot specify RU/s or Autoscale in Serverless mode"
        | CosmosModeType.Serverless, _ ->   CosmosInit.Provisioning.Serverless
    member val SkipStoredProc =             p.Contains InitParameters.SkipStoredProc
    member val IndexUnfolds =               p.Contains InitParameters.IndexUnfolds
and [<NoComparison; NoEquality; RequireSubcommand>] TableParameters =
    | [<AltCommandLine "-D"; Unique>]       OnDemand
    | [<AltCommandLine "-s"; Mandatory>]    Streaming of Equinox.DynamoStore.Core.Initialization.StreamingMode
    | [<AltCommandLine "-r"; Unique>]       ReadCu of int64
    | [<AltCommandLine "-w"; Unique>]       WriteCu of int64
    | [<CliPrefix(CliPrefix.None); Last>]   Dynamo of ParseResults<Store.Dynamo.Parameters>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | OnDemand ->                   "Specify On-Demand Capacity Mode. (Default: Provisioned mode)"
            | ReadCu _ ->                   "Specify Read Capacity Units to provision for the Table. (Not applicable in On-Demand mode)"
            | WriteCu _ ->                  "Specify Write Capacity Units to provision for the Table. (Not applicable in On-Demand mode)"
            | Streaming _ ->                "Specify Streaming Mode: New=NEW_IMAGE, NewAndOld=NEW_AND_OLD_IMAGES, Off=Disabled."
            | Dynamo _ ->                   "DynamoDB Connection parameters."
and DynamoInitArguments(p : ParseResults<TableParameters>) =
    let onDemand =                          p.Contains OnDemand
    member val StreamingMode =              p.GetResult Streaming
    member val Throughput =                 if not onDemand then Throughput.Provisioned (ProvisionedThroughput(p.GetResult ReadCu, p.GetResult WriteCu))
                                            elif onDemand && (p.Contains ReadCu || p.Contains WriteCu) then p.Raise "CUs are not applicable in On-Demand mode"
                                            else Throughput.OnDemand
and [<NoComparison; NoEquality; RequireSubcommand>] ConfigParameters =
    | [<CliPrefix(CliPrefix.None); Last; AltCommandLine "ms">] MsSql    of ParseResults<Store.Sql.Ms.Parameters>
    | [<CliPrefix(CliPrefix.None); Last; AltCommandLine "my">] MySql    of ParseResults<Store.Sql.My.Parameters>
    | [<CliPrefix(CliPrefix.None); Last; AltCommandLine "pg">] Postgres of ParseResults<Store.Sql.Pg.Parameters>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | MsSql _ ->                    "Configure Sql Server Store."
            | MySql _ ->                    "Configure MySql Store."
            | Postgres _ ->                 "Configure Postgres Store."
and [<NoComparison; NoEquality; RequireSubcommand>] StatsParameters =
    | [<AltCommandLine "-E"; Unique>]       Events
    | [<AltCommandLine "-S"; Unique>]       Streams
    | [<AltCommandLine "-D"; Unique>]       Documents
    | [<AltCommandLine "-A"; Unique>]       All
    | [<AltCommandLine "-P"; Unique>]       Parallel
    | [<CliPrefix(CliPrefix.None)>]         Cosmos of ParseResults<Store.Cosmos.Parameters>
    | [<CliPrefix(CliPrefix.None)>]         Dynamo of ParseResults<Store.Dynamo.Parameters>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | Events ->                     "Count the number of Events in the store."
            | Streams ->                    "Count the number of Streams in the store. (Default action if no others supplied)"
            | Documents ->                  "Count the number of Documents in the store."
            | All ->                        "Request all available stats (equivalent to -ESD)"
            | Parallel ->                   "Run in Parallel (CAREFUL! can overwhelm RU allocations)."
            | Cosmos _ ->                   "Cosmos Connection parameters."
            | Dynamo _ ->                   "Dynamo Connection parameters."
and [<NoComparison; NoEquality; RequireSubcommand>] QueryParameters =
    | [<AltCommandLine "-sn"; Unique>]      StreamName of string
    | [<AltCommandLine "-cn"; Unique>]      CategoryName of string
    | [<AltCommandLine "-cl"; Unique>]      CategoryLike of string
    | [<AltCommandLine "-un"; Unique>]      UnfoldName of string
    | [<AltCommandLine "-uc"; Unique>]      UnfoldCriteria of string
    | [<AltCommandLine "-m"; Unique>]       Mode of Mode
    | [<AltCommandLine "-o"; Unique>]       File of string
    | [<AltCommandLine "-P"; Unique>]       Pretty
    | [<AltCommandLine "-C"; Unique>]       Console
    | [<CliPrefix(CliPrefix.None)>]         Cosmos of ParseResults<Store.Cosmos.Parameters>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | StreamName _ ->               "Specify stream name to match against `p`, e.g. `$UserServices-f7c1ce63389a45bdbea1cccebb1b3c8a`."
            | CategoryName _ ->             "Specify category name to match against `p`, e.g. `$UserServices`."
            | CategoryLike _ ->             "Specify category name to match against `p` as a Cosmos LIKE expression (with `%` as wildcard, e.g. `$UserServices-%`."
            | UnfoldName _ ->               "Specify unfold Name to match against `u.c`, e.g. `Snapshotted`"
            | UnfoldCriteria _ ->           "Specify constraints on Unfold (reference unfold fields via `u.d.`, top level fields via `c.`), e.g. `u.d.name = \"TenantName1\"`."
            | Mode _ ->                     "readOnly: Only read `u`nfolds, not `_etag`.\n" +
                                            "readWithStream: Read `u`nfolds and `p` (stream name), but not `_etag`.\n" +
                                            "default: Retrieve full data (p, u, _etag).\n" +
                                            "raw: Read all Items(documents) in full.\n"
            | File _ ->                     "Export retrieved JSON to file"
            | Pretty ->                     "Render the JSON indented over multiple lines"
            | Console ->                    "Also emit the JSON to the console. Default: Gather statistics (and, optionally, write to specified file)"
            | Cosmos _ ->                   "Parameters for CosmosDB."
and [<RequireQualifiedAccess>] Mode = ReadOnly | ReadWithStream | Default | Raw
and [<RequireQualifiedAccess>] Criteria = SingleStream of string | CatName of string | CatLike of string | Unfiltered
and QueryArguments(p: ParseResults<QueryParameters>) =
    member val Mode = p.GetResult(Mode, Mode.Default)
    member val Pretty = p.Contains QueryParameters.Pretty
    member val TeeConsole = p.Contains Console
    member val Criteria =
        match p.TryGetResult StreamName, p.TryGetResult CategoryName, p.TryGetResult CategoryLike with
        | Some sn, None, None -> Criteria.SingleStream sn
        | Some _, Some _, _
        | Some _, _, Some _ -> p.Raise "StreamName and CategoryLike/CategoryName mutually exclusive"
        | None, Some cn, None -> Criteria.CatName cn
        | None, None, Some cl -> Criteria.CatLike cl
        | None, None, None -> Criteria.Unfiltered
        | None, Some _, Some _ -> p.Raise "CategoryLike and CategoryName are mutually exclusive"
    member val Filepath = p.TryGetResult File
    member val UnfoldName = p.TryGetResult UnfoldName
    member val UnfoldCriteria = p.TryGetResult UnfoldCriteria
    member val CosmosArgs =
        match p.GetSubCommand() with
        | QueryParameters.Cosmos p -> Store.Cosmos.Arguments p
        | x -> p.Raise $"unexpected subcommand %A{x}"
    member x.ConfigureStore(log: ILogger) =
        let storeConfig = None, true
        Store.Cosmos.config log storeConfig x.CosmosArgs
and [<NoComparison; NoEquality; RequireSubcommand>] DumpParameters =
    | [<AltCommandLine "-s"; MainCommand>]  Stream of FsCodec.StreamName
    | [<AltCommandLine "-C"; Unique>]       Correlation
    | [<AltCommandLine "-B"; Unique>]       Blobs
    | [<AltCommandLine "-J"; Unique>]       JsonSkip
    | [<AltCommandLine "-P"; Unique>]       Pretty
    | [<AltCommandLine "-F"; Unique>]       FlattenUnfolds
    | [<AltCommandLine "-T"; Unique>]       TimeRegular
    | [<AltCommandLine "-U"; Unique>]       UnfoldsOnly
    | [<AltCommandLine "-E"; Unique >]      EventsOnly
    | [<CliPrefix(CliPrefix.None)>]                            Cosmos   of ParseResults<Store.Cosmos.Parameters>
    | [<CliPrefix(CliPrefix.None)>]                            Dynamo   of ParseResults<Store.Dynamo.Parameters>
    | [<CliPrefix(CliPrefix.None); Last>]                      Es       of ParseResults<Store.EventStore.Parameters>
    | [<CliPrefix(CliPrefix.None); Last>]                      Mdb      of ParseResults<Store.MessageDb.Parameters>
    | [<CliPrefix(CliPrefix.None); Last; AltCommandLine "ms">] MsSql    of ParseResults<Store.Sql.Ms.Parameters>
    | [<CliPrefix(CliPrefix.None); Last; AltCommandLine "my">] MySql    of ParseResults<Store.Sql.My.Parameters>
    | [<CliPrefix(CliPrefix.None); Last; AltCommandLine "pg">] Postgres of ParseResults<Store.Sql.Pg.Parameters>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | Stream _ ->                   "Specify stream(s) to dump."
            | Correlation ->                "Include Correlation/Causation identifiers"
            | Blobs ->                      "Don't assume Data/Metadata is UTF-8 text"
            | JsonSkip ->                   "Don't assume Data/Metadata is JSON"
            | Pretty ->                     "Pretty print the JSON over multiple lines"
            | FlattenUnfolds ->             "Don't pretty print the JSON over multiple lines for Unfolds"
            | TimeRegular ->                "Don't humanize time intervals between events"
            | UnfoldsOnly ->                "Exclude Events. Default: show both Events and Unfolds"
            | EventsOnly ->                 "Exclude Unfolds/Snapshots. Default: show both Events and Unfolds."
            | Es _ ->                       "Parameters for EventStore."
            | Cosmos _ ->                   "Parameters for CosmosDB."
            | Dynamo _ ->                   "Parameters for DynamoDB."
            | MsSql _ ->                    "Parameters for Sql Server."
            | MySql _ ->                    "Parameters for MySql."
            | Postgres _ ->                 "Parameters for Postgres."
            | Mdb _ ->                      "Parameters for MessageDB."
and DumpArguments(p: ParseResults<DumpParameters>) =
    member _.ConfigureStore(log : ILogger, createStoreLog) =
        let storeConfig = None, true
        match p.GetSubCommand() with
        | DumpParameters.Cosmos p ->
            let storeLog = createStoreLog <| p.Contains Store.Cosmos.Parameters.StoreVerbose
            storeLog, Store.Cosmos.config log storeConfig (Store.Cosmos.Arguments p)
        | DumpParameters.Dynamo p ->
            let storeLog = createStoreLog <| p.Contains Store.Dynamo.Parameters.StoreVerbose
            storeLog, Store.Dynamo.config log storeConfig (Store.Dynamo.Arguments p)
        | DumpParameters.Es p ->
            let storeLog = createStoreLog <| p.Contains Store.EventStore.Parameters.StoreVerbose
            storeLog, Store.EventStore.config log storeConfig p
        | DumpParameters.MsSql p ->
            let storeLog = createStoreLog false
            storeLog, Store.Sql.Ms.config log storeConfig p
        | DumpParameters.MySql p ->
            let storeLog = createStoreLog false
            storeLog, Store.Sql.My.config log storeConfig p
        | DumpParameters.Postgres p ->
            let storeLog = createStoreLog false
            storeLog, Store.Sql.Pg.config log storeConfig p
        | DumpParameters.Mdb p ->
            let storeLog = createStoreLog false
            storeLog, Store.MessageDb.config log None p
        | x -> p.Raise $"unexpected subcommand %A{x}"
and [<NoComparison>] WebParameters =
    | [<AltCommandLine "-u">]               Endpoint of string
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | Endpoint _ ->                 "Target address. Default: https://localhost:5001"
and [<NoComparison; NoEquality; RequireSubcommand>] TestParameters =
    | [<AltCommandLine "-t"; Unique>]       Name of Test
    | [<AltCommandLine "-s">]               Size of int
    | [<AltCommandLine "-C">]               Cached
    | [<AltCommandLine "-U">]               Unfolds
    | [<AltCommandLine "-f">]               TestsPerSecond of int
    | [<AltCommandLine "-d">]               DurationM of float
    | [<AltCommandLine "-e">]               ErrorCutoff of int64
    | [<AltCommandLine "-i">]               ReportIntervalS of int
    | [<CliPrefix(CliPrefix.None); Last>]                      Cosmos    of ParseResults<Store.Cosmos.Parameters>
    | [<CliPrefix(CliPrefix.None); Last>]                      Dynamo    of ParseResults<Store.Dynamo.Parameters>
    | [<CliPrefix(CliPrefix.None); Last>]                      Es        of ParseResults<Store.EventStore.Parameters>
    | [<CliPrefix(CliPrefix.None); Last>]                      Memory    of ParseResults<Store.MemoryStore.Parameters>
    | [<CliPrefix(CliPrefix.None); Last; AltCommandLine "ms">] MsSql     of ParseResults<Store.Sql.Ms.Parameters>
    | [<CliPrefix(CliPrefix.None); Last; AltCommandLine "my">] MySql     of ParseResults<Store.Sql.My.Parameters>
    | [<CliPrefix(CliPrefix.None); Last; AltCommandLine "pg">] Postgres  of ParseResults<Store.Sql.Pg.Parameters>
    | [<CliPrefix(CliPrefix.None); Last>]                      Mdb       of ParseResults<Store.MessageDb.Parameters>
    | [<CliPrefix(CliPrefix.None); Last>]                      Web       of ParseResults<WebParameters>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | Name _ ->                     "specify which test to run. (default: Favorite)."
            | Size _ ->                     "For `-t Todo`: specify random title length max size to use (default 100)."
            | Cached ->                     "employ a 50MB cache, wire in to Stream configuration."
            | Unfolds ->                    "employ a store-appropriate Rolling Snapshots and/or Unfolding strategy."
            | TestsPerSecond _ ->           "specify a target number of requests per second (default: 1000)."
            | DurationM _ ->                "specify a run duration in minutes (default: 30)."
            | ErrorCutoff _ ->              "specify an error cutoff; test ends when exceeded (default: 10000)."
            | ReportIntervalS _ ->          "specify reporting intervals in seconds (default: 10)."
            | Es _ ->                       "Run transactions in-process against EventStore."
            | Cosmos _ ->                   "Run transactions in-process against CosmosDB."
            | Dynamo _ ->                   "Run transactions in-process against DynamoDB."
            | Memory _ ->                   "target in-process Transient Memory Store (Default if not other target specified)."
            | MsSql _ ->                    "Run transactions in-process against Sql Server."
            | MySql _ ->                    "Run transactions in-process against MySql."
            | Postgres _ ->                 "Run transactions in-process against Postgres."
            | Mdb _ ->                      "Run transactions in-process against MessageDB."
            | Web _ ->                      "Run transactions against a Web endpoint."
and Test = Favorite | SaveForLater | Todo
and TestArguments(p : ParseResults<TestParameters>) =
    member val Options =                    p.GetResults Cached @ p.GetResults Unfolds
    member x.Cache =                        x.Options |> List.exists (function Cached ->  true | _ -> false)
    member x.Unfolds =                      x.Options |> List.exists (function Unfolds -> true | _ -> false)
    member val Test =                       p.GetResult(Name, Test.Favorite)
    member val ErrorCutoff =                p.GetResult(ErrorCutoff, 10000L)
    member val TestsPerSecond =             p.GetResult(TestsPerSecond, 1000)
    member val Duration =                   p.GetResult(DurationM, 30.) |> TimeSpan.FromMinutes
    member x.ReportingIntervals =           match p.GetResults(ReportIntervalS) with
                                            | [] -> TimeSpan.FromSeconds 10.|> Seq.singleton
                                            | intervals -> seq { for i in intervals -> TimeSpan.FromSeconds(float i) }
                                            |> fun intervals -> [| yield x.Duration; yield! intervals |]
    member x.ConfigureStore(log : ILogger, createStoreLog) =
        let cache = if x.Cache then Equinox.Cache(appName, sizeMb = 50) |> Some else None
        match p.GetSubCommand() with
        | Cosmos p ->   let storeLog = createStoreLog <| p.Contains Store.Cosmos.Parameters.StoreVerbose
                        log.Information("Running transactions in-process against CosmosDB with storage options: {options:l}", x.Options)
                        storeLog, Store.Cosmos.config log (cache, x.Unfolds) (Store.Cosmos.Arguments p)
        | Dynamo p ->   let storeLog = createStoreLog <| p.Contains Store.Dynamo.Parameters.StoreVerbose
                        log.Information("Running transactions in-process against DynamoDB with storage options: {options:l}", x.Options)
                        storeLog, Store.Dynamo.config log (cache, x.Unfolds) (Store.Dynamo.Arguments p)
        | Es p ->       let storeLog = createStoreLog <| p.Contains Store.EventStore.Parameters.StoreVerbose
                        log.Information("Running transactions in-process against EventStore with storage options: {options:l}", x.Options)
                        storeLog, Store.EventStore.config log (cache, x.Unfolds) p
        | MsSql p ->    let storeLog = createStoreLog false
                        log.Information("Running transactions in-process against MsSql with storage options: {options:l}", x.Options)
                        storeLog, Store.Sql.Ms.config log (cache, x.Unfolds) p
        | MySql p ->    let storeLog = createStoreLog false
                        log.Information("Running transactions in-process against MySql with storage options: {options:l}", x.Options)
                        storeLog, Store.Sql.My.config log (cache, x.Unfolds) p
        | Postgres p -> let storeLog = createStoreLog false
                        log.Information("Running transactions in-process against Postgres with storage options: {options:l}", x.Options)
                        storeLog, Store.Sql.Pg.config log (cache, x.Unfolds) p
        | Mdb p ->      let storeLog = createStoreLog false
                        log.Information("Running transactions in-process against MessageDb with storage options: {options:l}", x.Options)
                        storeLog, Store.MessageDb.config log cache p
        | Memory _ ->   log.Warning("Running transactions in-process against Volatile Store with storage options: {options:l}", x.Options)
                        createStoreLog false, Store.MemoryStore.config ()
        | x ->          p.Raise $"unexpected subcommand %A{x}"
    member _.Tests =    match p.GetResult(Name, Favorite) with
                        | Favorite ->     Tests.Favorite
                        | SaveForLater -> Tests.SaveForLater
                        | Todo ->         Tests.Todo (p.GetResult(Size, 100))
module Arguments =
    let programName () = System.Reflection.Assembly.GetEntryAssembly().GetName().Name
    let parse argv =
        ArgumentParser.Create<Arguments>(programName = programName ()).ParseCommandLine(argv)

let writeToStatsSinks (c : LoggerConfiguration) =
    c.WriteTo.Sink(Equinox.CosmosStore.Core.Log.InternalMetrics.Stats.LogSink())
     .WriteTo.Sink(Equinox.DynamoStore.Core.Log.InternalMetrics.Stats.LogSink())
     .WriteTo.Sink(Equinox.EventStoreDb.Log.InternalMetrics.Stats.LogSink())
     .WriteTo.Sink(Equinox.SqlStreamStore.Log.InternalMetrics.Stats.LogSink())

let createStoreLog verbose verboseConsole maybeSeqEndpoint =
    let c = LoggerConfiguration()
    let c = if verbose then c.MinimumLevel.Debug() else c
    let c = writeToStatsSinks c
    let level =
        match verbose, verboseConsole with
        | true, true -> LogEventLevel.Debug
        | false, true -> LogEventLevel.Information
        | _ -> LogEventLevel.Warning
    let outputTemplate = "{Timestamp:T} {Level:u1} {Message:l} {Properties}{NewLine}{Exception}"
    let c = c.WriteTo.Console(level, outputTemplate, theme = Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code)
    let c = match maybeSeqEndpoint with None -> c | Some endpoint -> c.WriteTo.Seq(endpoint)
    c.CreateLogger() :> ILogger

// Reset the start time based on which the shared global metrics will be computed
let resetStats () =
    let _ = Equinox.CosmosStore.Core.Log.InternalMetrics.Stats.LogSink.Restart()
    let _ = Equinox.DynamoStore.Core.Log.InternalMetrics.Stats.LogSink.Restart()
    let _ = Equinox.EventStoreDb.Log.InternalMetrics.Stats.LogSink.Restart()
    let _ = Equinox.SqlStreamStore.Log.InternalMetrics.Stats.LogSink.Restart() in ()

let dumpStats log = function
    | Store.Config.Cosmos _ -> Equinox.CosmosStore.Core.Log.InternalMetrics.dump log
    | Store.Config.Dynamo _ -> Equinox.DynamoStore.Core.Log.InternalMetrics.dump log
    | Store.Config.Es _ ->     Equinox.EventStoreDb.Log.InternalMetrics.dump log
    | Store.Config.Sql _ ->    Equinox.SqlStreamStore.Log.InternalMetrics.dump log
    | Store.Config.Mdb _ ->    Equinox.MessageDb.Log.InternalMetrics.dump log
    | Store.Config.Memory _ -> ()

module LoadTest =

    open Equinox.Tools.TestHarness

    let private runLoadTest log testsPerSecond duration errorCutoff reportingIntervals (clients : ClientId[]) runSingleTest =
        let mutable idx = -1L
        let selectClient () =
            let clientIndex = Interlocked.Increment(&idx) |> int
            clients[clientIndex % clients.Length]
        let selectClient = async { return async { return selectClient() } }
        Local.runLoadTest log reportingIntervals testsPerSecond errorCutoff duration selectClient runSingleTest
    let private decorateWithLogger (domainLog : ILogger, verbose) (run: 't -> Async<unit>) =
        let execute clientId =
            if not verbose then run clientId
            else async {
                domainLog.Information("Executing for client {sessionId}", clientId)
                try return! run clientId
                with e -> domainLog.Warning(e, "Test threw an exception"); e.Reraise () }
        execute
    let private createResultLog fileName = LoggerConfiguration().WriteTo.File(fileName).CreateLogger()
    let run (log : ILogger) (verbose, verboseConsole, maybeSeq) reportFilename (p : ParseResults<TestParameters>) =
        let createStoreLog storeVerbose = createStoreLog storeVerbose verboseConsole maybeSeq
        let a = TestArguments p
        let storeLog, storeConfig, httpClient: ILogger * Store.Config option * HttpClient option =
            match p.GetSubCommand() with
            | Web p ->
                let uri = p.GetResult(WebParameters.Endpoint,"https://localhost:5001") |> Uri
                log.Information("Running web test targeting: {url}", uri)
                createStoreLog false, None, new HttpClient(BaseAddress = uri) |> Some
            | _ ->
                let storeLog, storeConfig = a.ConfigureStore(log, createStoreLog)
                storeLog, Some storeConfig, None
        let test, duration = a.Tests, a.Duration
        let runSingleTest : ClientId -> Async<unit> =
            match storeConfig, httpClient with
            | None, Some client ->
                let execForClient = Tests.executeRemote client test
                decorateWithLogger (log, verbose) execForClient
            | Some storeConfig, _ ->
                let services = ServiceCollection()
                Services.register(services, storeConfig, storeLog)
                let container = services.BuildServiceProvider()
                let execForClient = Tests.executeLocal container test
                decorateWithLogger (log, verbose) execForClient
            | None, None -> invalidOp "impossible None, None"
        let clients = Array.init (a.TestsPerSecond * 2) (fun _ -> % Guid.NewGuid())

        let renderedIds = clients |> Seq.map ClientId.toString |> if verboseConsole then id else Seq.truncate 5
        log.ForContext((if verboseConsole then "clientIds" else "clientIdsExcerpt"),renderedIds)
            .Information("Running {test} for {duration} @ {tps} hits/s across {clients} clients; Max errors: {errorCutOff}, reporting intervals: {ri}, report file: {report}",
            test, a.Duration, a.TestsPerSecond, clients.Length, a.ErrorCutoff, a.ReportingIntervals, reportFilename)

        resetStats ()

        let results = runLoadTest log a.TestsPerSecond (duration.Add(TimeSpan.FromSeconds 5.)) a.ErrorCutoff a.ReportingIntervals clients runSingleTest |> Async.RunSynchronously

        let resultFile = createResultLog reportFilename
        for r in results do
            resultFile.Information("Aggregate: {aggregate}", r)
        log.Information("Run completed; Current memory allocation: {bytes:n2} MiB", (GC.GetTotalMemory(true) |> float) / 1024./1024.)

        storeConfig |> Option.iter (dumpStats log)

let createDomainLog verbose verboseConsole maybeSeqEndpoint =
    let c = LoggerConfiguration()
    let c = if verbose then c.MinimumLevel.Debug() else c
    let c = writeToStatsSinks c
    let c = let outputTemplate = "{Timestamp:T} {Level:u1} {Message:l} {Properties}{NewLine}{Exception}"
            let consoleLevel = if verboseConsole then LogEventLevel.Debug else LogEventLevel.Information
            c.WriteTo.Console(consoleLevel, outputTemplate, theme = Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code)
    let c = match maybeSeqEndpoint with None -> c | Some endpoint -> c.WriteTo.Seq(endpoint)
    c.CreateLogger()

module CosmosInit =

    let connect log (p : ParseResults<Store.Cosmos.Parameters>) =
        Store.Cosmos.connect log (Store.Cosmos.Arguments p) |> fst

    let containerAndOrDb (log: ILogger) (p: ParseResults<InitParameters>) =
        let a = CosmosInitArguments p
        match p.GetSubCommand() with
        | InitParameters.Cosmos cp ->
            let connector, dName, cName = connect log cp
            let l = log.ForContext("IndexUnfolds", a.IndexUnfolds)
            match a.ProvisioningMode with
            | CosmosInit.Provisioning.Container throughput ->
                l.Information("CosmosStore provisioning at {mode:l} level for {rus:n0} RU/s", "Container", throughput)
            | CosmosInit.Provisioning.Database throughput ->
                l.Information("CosmosStore provisioning at {mode:l} level for {rus:n0} RU/s", "Database", throughput)
            | CosmosInit.Provisioning.Serverless ->
                l.Information("CosmosStore provisioning in {mode:l} mode with automatic RU/s as configured in account", "Serverless")
            CosmosInit.init log (connector.CreateUninitialized()) (dName, cName) a.ProvisioningMode a.IndexUnfolds a.SkipStoredProc
        | x -> p.Raise $"unexpected subcommand %A{x}"

module DynamoInit =

    open Equinox.DynamoStore
    open Store.Dynamo

    let table (log : ILogger) (p : ParseResults<TableParameters>) = async {
        let a = DynamoInitArguments p
        match p.GetSubCommand() with
        | TableParameters.Dynamo sp ->
            let sa = Store.Dynamo.Arguments sp
            sa.Connector.LogConfiguration(log)
            let t = a.Throughput
            match t with
            | Throughput.Provisioned t ->
                log.Information("DynamoStore Provisioning Table {table} with {read}R/{write}WCU Provisioned capacity; streaming {streaming}",
                                sa.Table, t.ReadCapacityUnits, t.WriteCapacityUnits, a.StreamingMode)
            | Throughput.OnDemand ->
                log.Information("DynamoStore Provisioning Table {table} with On-Demand capacity management; streaming {streaming}",
                                sa.Table, a.StreamingMode)
            let client = sa.Connector.CreateDynamoDbClient()
            let! t = Core.Initialization.provision client sa.Table (t, a.StreamingMode)
            log.Information("DynamoStore DynamoDB Streams ARN {streamArn}", Core.Initialization.tryGetActiveStreamsArn t)
        | x -> p.Raise $"unexpected subcommand %A{x}" }

module SqlInit =

    let databaseOrSchema (log: ILogger) (p : ParseResults<ConfigParameters>) =
        match p.GetSubCommand() with
        | ConfigParameters.MsSql p ->
            let a = Store.Sql.Ms.Arguments(p)
            Store.Sql.Ms.connect log (a.ConnectionString, a.Credentials, a.Schema, true) |> Async.Ignore
        | ConfigParameters.MySql p ->
            let a = Store.Sql.My.Arguments(p)
            Store.Sql.My.connect log (a.ConnectionString, a.Credentials, true) |> Async.Ignore
        | ConfigParameters.Postgres p ->
            let a = Store.Sql.Pg.Arguments(p)
            Store.Sql.Pg.connect log (a.ConnectionString, a.Credentials, a.Schema, true) |> Async.Ignore

module CosmosStats =

    open Store.Dynamo

    type Microsoft.Azure.Cosmos.Container with // NB DO NOT CONSIDER PROMULGATING THIS HACK
        member container.QueryValue<'T>(sqlQuery : string) =
            let query : Microsoft.Azure.Cosmos.FeedResponse<'T> = container.GetItemQueryIterator<'T>(sqlQuery).ReadNextAsync() |> Async.AwaitTaskCorrect |> Async.RunSynchronously
            query |> Seq.exactlyOne
    let run (log : ILogger, _verboseConsole, _maybeSeq) (p : ParseResults<StatsParameters>) =
        match p.GetSubCommand() with
        | StatsParameters.Cosmos sp ->
            let doS, doD, doE =
                let all, s, d, e = p.Contains All, p.Contains StatsParameters.Streams, p.Contains Documents, p.Contains StatsParameters.Events
                all || s, all || d, all || e
            let doS = doS || (not doD && not doE) // default to counting streams only unless otherwise specified
            let inParallel = p.Contains Parallel
            let connector, dName, cName = CosmosInit.connect log sp
            let container = connector.CreateUninitialized().GetContainer(dName, cName)
            let ops = [| if doS then "Streams",   """SELECT VALUE COUNT(1) FROM c WHERE c.id="-1" """
                         if doD then "Documents", """SELECT VALUE COUNT(1) FROM c"""
                         if doE then "Events",    """SELECT VALUE SUM(c.n) FROM c WHERE c.id="-1" """ |]
            log.Information("Computing {measures} ({mode})", Seq.map fst ops, (if inParallel then "in parallel" else "serially"))
            ops |> Seq.map (fun (name, sql) -> async {
                    log.Debug("Running query: {sql}", sql)
                    let res = container.QueryValue<int>(sql)
                    log.Information("{stat}: {result:N0}", name, res)})
                |> if inParallel then Async.Parallel else Async.Sequential
                |> Async.Ignore<unit[]>
        | StatsParameters.Dynamo sp -> async {
            let sa = Store.Dynamo.Arguments sp
            sa.Connector.LogConfiguration(log)
            let client = sa.Connector.CreateDynamoDbClient()
            let! t = Equinox.DynamoStore.Core.Initialization.describe client sa.Table
            match t.BillingModeSummary, t.ProvisionedThroughput, Equinox.DynamoStore.Core.Initialization.tryGetActiveStreamsArn t with
            | null, p, streamsArn when p <> null ->
                log.Information("DynamoStore Table {table} Provisioned with {read}R/{write}WCU Provisioned capacity; Streams ARN {streaming}",
                                sa.Table, p.ReadCapacityUnits, p.WriteCapacityUnits, streamsArn)
            | bms, _, streamsArn when bms.BillingMode = Amazon.DynamoDBv2.BillingMode.PAY_PER_REQUEST ->
                log.Information("DynamoStore Table {table} Provisioned with On-Demand capacity management; Streams ARN {streaming}", sa.Table, streamsArn)
            | _, _, streamsArn ->
                log.Information("DynamoStore Table {table} Provisioning Unknown; Streams ARN {streaming}", sa.Table, streamsArn) }
        | x -> p.Raise $"unexpected subcommand %A{x}"

module Dump =

    let prettySerdes = lazy FsCodec.SystemTextJson.Serdes(FsCodec.SystemTextJson.Options.Create(indent = true))
    let private prettifyJson (json: string) =
        use parsed = System.Text.Json.JsonDocument.Parse json
        prettySerdes.Value.Serialize parsed
    let run (log : ILogger, verboseConsole, maybeSeq) (p : ParseResults<DumpParameters>) =
        let a = DumpArguments p
        let createStoreLog storeVerbose = createStoreLog storeVerbose verboseConsole maybeSeq
        let storeLog, storeConfig = a.ConfigureStore(log, createStoreLog)
        let doU, doE = not (p.Contains EventsOnly), not (p.Contains UnfoldsOnly)
        let doC, doJ, doS, doT = p.Contains Correlation, not (p.Contains JsonSkip), not (p.Contains Blobs), not (p.Contains TimeRegular)
        let store = Services.Store(storeConfig)

        let initial = List.empty
        let fold state events = (events, state) ||> Seq.foldBack (fun e l -> e :: l)
        let tryDecode (x : FsCodec.ITimelineEvent<ReadOnlyMemory<byte>>) = ValueSome x
        let idCodec = FsCodec.Codec.Create((fun _ -> failwith "No encoding required"), tryDecode, (fun _ _ -> failwith "No mapCausation"))
        let isOriginAndSnapshot = (fun (event : FsCodec.ITimelineEvent<_>) -> not doE && event.IsUnfold), fun _state -> failwith "no snapshot required"
        let formatUnfolds, formatEvents =
            if p.Contains FlattenUnfolds then id else prettifyJson
            , if p.Contains Pretty then prettifyJson else id
        let mutable payloadBytes = 0
        let render format (data : ReadOnlyMemory<byte>) =
            payloadBytes <- payloadBytes + data.Length
            if data.IsEmpty then null
            elif not doS then $"%6d{data.Length}b"
            else try let s = System.Text.Encoding.UTF8.GetString data.Span
                     if doJ then try format s
                                 with e -> log.ForContext("str", s).Warning(e, "JSON Parse failure - use --JsonSkip option to inhibit"); reraise()
                     else $"(%d{s.Length} chars)"
                 with e -> log.Warning(e, "UTF-8 Parse failure - use --Blobs option to inhibit"); reraise()
        let dumpEvents (streamName: FsCodec.StreamName) = async {
            let struct (categoryName, sid) = FsCodec.StreamName.split streamName
            let cat = store.Category(categoryName, idCodec, fold, initial, isOriginAndSnapshot)
            let decider = Equinox.Decider.forStream storeLog cat sid
            let! streamBytes, events = decider.QueryEx(fun c -> c.StreamEventBytes, c.State)
            let mutable prevTs = None
            for x in events |> Seq.filter (fun e -> (e.IsUnfold && doU) || (not e.IsUnfold && doE)) do
                let ty, render = if x.IsUnfold then "U", render formatUnfolds else "E", render formatEvents
                let interval =
                    match prevTs with Some p when not x.IsUnfold -> Some (x.Timestamp - p) | _ -> None
                    |> function
                    | None -> if doT then "n/a" else "0"
                    | Some (i : TimeSpan) when not doT -> i.ToString()
                    | Some (i : TimeSpan) when i.TotalDays >= 1. -> i.ToString "d\dhh\hmm\m"
                    | Some i when i.TotalHours >= 1. -> i.ToString "h\hmm\mss\s"
                    | Some i when i.TotalMinutes >= 1. -> i.ToString "m\mss\.ff\s"
                    | Some i -> i.ToString("s\.fff\s")
                prevTs <- Some x.Timestamp
                if not doC then log.Information("{i,4}@{t:u}+{d,9} {u:l} {e:l} {data:l} {meta:l}",
                                    x.Index, x.Timestamp, interval, ty, x.EventType, render x.Data, render x.Meta)
                else log.Information("{i,4}@{t:u}+{d,9} Corr {corr} Cause {cause} {u:l} {e:l} {data:l} {meta:l}",
                         x.Index, x.Timestamp, interval, x.CorrelationId, x.CausationId, ty, x.EventType, render x.Data, render x.Meta)
            match streamBytes with ValueNone -> () | ValueSome x -> log.Information("ISyncContext.StreamEventBytes {kib:n1}KiB", float x / 1024.) }
        resetStats ()
        let streams = p.GetResults DumpParameters.Stream
        log.ForContext("streams",streams).Information("Reading...")
        streams
        |> Seq.map dumpEvents
        |> Async.Parallel
        |> Async.Ignore<unit[]>
        |> Async.RunSynchronously

        log.Information("Total Event Bodies Payload {kib:n1}KiB", float payloadBytes / 1024.)
        if verboseConsole then
            dumpStats log storeConfig

module Query =

    let inline miB x = float x / 1024. / 1024.
    let private unixEpoch = DateTime.UnixEpoch
    type System.Text.Json.JsonElement with
        member x.Size = if x.ValueKind = System.Text.Json.JsonValueKind.Null then 0 else x.GetRawText().Length
    type System.Text.Json.JsonDocument with
        member x.Cast<'T>() = System.Text.Json.JsonSerializer.Deserialize<'T>(x.RootElement)
        member x.Timestamp =
            let ok, p = x.RootElement.TryGetProperty("_ts")
            if ok then p.GetDouble() |> unixEpoch.AddSeconds |> Some else None
    let composeQuery (a: QueryArguments) =
        let partitionKeyCriteria =
            match a.Criteria with
            | Criteria.SingleStream sn -> $"c.p = \"{sn}\""
            | Criteria.CatName n -> $"c.p LIKE \"{n}-%%\""
            | Criteria.CatLike pat -> $"c.p LIKE \"{pat}\""
            | Criteria.Unfiltered -> Log.Warning "No StreamName or CategoryName/CategoryLike specified - Unfold Criteria better be unambiguous"; "1=1"
        let selectedFields =
            match a.Mode with
            | Mode.ReadOnly -> "c.u"
            | Mode.ReadWithStream -> "c.p, c.u"
            | Mode.Default -> "c.p, c.u, c._etag"
            | Mode.Raw -> "*"
        let unfoldFilter =
            let exists cond = $"EXISTS (SELECT VALUE u FROM u IN c.u WHERE {cond})"
            match [| match a.UnfoldName with None -> () | Some un -> $"u.c = \"{un}\""
                     match a.UnfoldCriteria with None -> () | Some uc -> uc |] with
            | [||] -> "1=1"
            | [| x |] -> x |> exists
            | xs -> String.Join(" AND ", xs) |> exists
        $"SELECT {selectedFields} FROM c WHERE {partitionKeyCriteria} AND {unfoldFilter}"
    let inline arrayLen x = if isNull x then 0 else Array.length x
    let run (log: ILogger) (a: QueryArguments) = async {
        let sql = composeQuery a
        Log.Information("Querying {q}", sql)
        let storeConfig = a.ConfigureStore(log)
        let container = match storeConfig with Store.Config.Cosmos (cc, _, _) -> cc.Container | _ -> failwith "Query requires Cosmos"
        let opts = Microsoft.Azure.Cosmos.QueryRequestOptions(MaxItemCount = a.CosmosArgs.QueryMaxItems)
        use query = container.GetItemQueryIterator<System.Text.Json.JsonDocument>(sql, requestOptions = opts)

        let serdes = if a.Pretty then Dump.prettySerdes.Value else FsCodec.SystemTextJson.Serdes.Default
        let maybeFileStream = a.Filepath |> Option.map (fun p ->
            Log.Information("Dumping content to {path}", System.IO.FileInfo(p).FullName)
            System.IO.File.Open(p, System.IO.FileMode.Create))

        let sw, sw2 = System.Diagnostics.Stopwatch(), System.Diagnostics.Stopwatch.StartNew()
        let mutable accCount, accRus, accBytesRead = 0L, 0., 0L
        let cats = System.Collections.Generic.HashSet()
        try while query.HasMoreResults do
                sw.Restart()
                let! res = query.ReadNextAsync(CancellationToken.None) |> Async.AwaitTaskCorrect
                let pageSize = res.Resource |> Seq.sumBy _.RootElement.Size
                let newestAge = res.Resource |> Seq.choose _.Timestamp |> Seq.tryLast |> Option.map (fun ts -> ts - DateTime.UtcNow)
                let items = [| for x in res.Resource -> x.Cast<Equinox.CosmosStore.Core.Tip>() |]
                let count, us, es = items.Length, items |> Seq.sumBy (_.u >> arrayLen), items |> Seq.sumBy (_.e >> arrayLen)
                Log.Information("Page {count}s, {us}u, {es}e {ru}RU {s:N1}s {mib:N1}MiB age {age:dddd\.hh\:mm\:ss}",
                                count, us, es, res.RequestCharge, sw.Elapsed.TotalSeconds, miB pageSize, Option.toNullable newestAge)
                accBytesRead <- accBytesRead + int64 pageSize
                accCount <- accCount + int64 count
                accRus <- accRus + res.RequestCharge
                for x in items do
                    if not (isNull x.p) then
                        let struct (categoryName, _sid) = x.p |> FsCodec.StreamName.parse |> FsCodec.StreamName.split
                        cats.Add categoryName |> ignore
                maybeFileStream |> Option.iter (fun stream ->
                    for x in res.Resource do
                        serdes.SerializeToStream(x, stream)
                        stream.WriteByte(byte '\n'))
                if a.TeeConsole then
                    res.Resource |> Seq.iter (serdes.Serialize >> Console.WriteLine)
        finally
            let fileSize = maybeFileStream |> Option.map _.Position |> Option.defaultValue 0
            maybeFileStream |> Option.iter _.Close() // Before we log so time includes flush time and no confusion
            Log.Information("TOTALS {cats}c, {count:N0}s, {ru:N2}RU R/W {mib:N1}/{mib:N1}MiB {s:N1}s",
                            cats.Count, accCount, accRus, miB accBytesRead, miB fileSize, sw2.Elapsed.TotalSeconds) }

[<EntryPoint>]
let main argv =
    try let p = Arguments.parse argv
        let verboseConsole = p.Contains VerboseConsole
        let maybeSeq = if p.Contains LocalSeq then Some "http://localhost:5341" else None
        let verbose = p.Contains Verbose
        Log.Logger <- createDomainLog verbose verboseConsole maybeSeq
        try let log = Log.Logger
            try match p.GetSubCommand() with
                | Init a ->     (CosmosInit.containerAndOrDb log a CancellationToken.None).Wait()
                | InitAws a ->  DynamoInit.table log a |> Async.RunSynchronously
                | Config a ->   SqlInit.databaseOrSchema log a |> Async.RunSynchronously
                | Query a ->    Query.run log (QueryArguments a) |> Async.RunSynchronously
                | Dump a ->     Dump.run (log, verboseConsole, maybeSeq) a
                | Stats a ->    CosmosStats.run (log, verboseConsole, maybeSeq) a |> Async.RunSynchronously
                | Run a ->      let n = p.GetResult(LogFile, Arguments.programName () + ".log")
                                let reportFilename = System.IO.FileInfo(n).FullName
                                LoadTest.run log (verbose, verboseConsole, maybeSeq) reportFilename a
                | x ->          p.Raise $"unexpected subcommand %A{x}"
                0
            with e when not (e :? ArguParseException) -> Log.Fatal(e, "Exiting"); 2
        finally Log.CloseAndFlush()
    with :? ArguParseException as e -> eprintfn $"%s{e.Message}"; 1
        | e -> eprintfn $"EXCEPTION: {e}"; 1
