module Equinox.Tool.Program

#if DEBUG // as of V8, compiling in Debug fails but release succeeds
#nowarn "3511"
#endif

open Argu
open FSharp.AWS.DynamoDB // Throughput
open Samples.Infrastructure
open Serilog
open Serilog.Events
open System
open System.Threading

module CosmosInit = Equinox.CosmosStore.Core.Initialization

let [<Literal>] appName = "equinox-tool"

[<NoComparison; NoEquality>]
type Parameters =
    | [<AltCommandLine "-Q"; Unique>]       Quiet
    | [<AltCommandLine "-V"; Unique>]       Verbose
    | [<AltCommandLine "-C"; Unique>]       VerboseConsole
    | [<AltCommandLine "-S"; Unique>]       LocalSeq
    | [<AltCommandLine "-l"; Unique>]       LogFile of string
    | [<CliPrefix(CliPrefix.None); Last>]   Dump of ParseResults<DumpParameters>
    | [<CliPrefix(CliPrefix.None); Last>]   LoadTest of ParseResults<Tests.LoadTest.LoadTestParameters>
    | [<CliPrefix(CliPrefix.None); Last>]   Init of ParseResults<InitParameters>
    | [<CliPrefix(CliPrefix.None); Last>]   InitAws of ParseResults<TableParameters>
    | [<CliPrefix(CliPrefix.None); Last>]   InitSql of ParseResults<InitSqlParameters>
    | [<CliPrefix(CliPrefix.None); Last>]   Stats of ParseResults<StatsParameters>
    | [<CliPrefix(CliPrefix.None); Last>]   Query of ParseResults<QueryParameters>
    | [<CliPrefix(CliPrefix.None); Last>]   Top of ParseResults<TopParameters>
    | [<CliPrefix(CliPrefix.None); Last>]   Destroy of ParseResults<DestroyParameters>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | Quiet ->                      "Omit timestamps from log output"
            | Verbose ->                    "Include low level logging regarding specific test runs."
            | VerboseConsole ->             "Include low level test and store actions logging in on-screen output to console."
            | LocalSeq ->                   "Configures writing to a local Seq endpoint at http://localhost:5341, see https://getseq.net"
            | LogFile _ ->                  "specify a log file to write the result breakdown into (default: eqx.log)."
            | Dump _ ->                     "Load and show events in a specified stream (supports all stores)."
            | LoadTest _ ->                 "Run a load test"
            | Init _ ->                     "Initialize Store/Container (supports `cosmos` stores; also handles RU/s provisioning adjustment)."
            | InitAws _ ->                  "Initialize DynamoDB Table (supports `dynamo` stores; also handles RU/s provisioning adjustment)."
            | InitSql _ ->                  "Initialize Database Schema (supports `mssql`/`mysql`/`postgres` SqlStreamStore stores)."
            | Stats _ ->                    "inspect store to determine numbers of streams/documents/events and/or config (supports `cosmos` and `dynamo` stores)."
            | Query _ ->                    "Load/Summarise streams based on Cosmos SQL Queries (supports `cosmos` only)."
            | Top _ ->                      "Scan to determine top categories and streams (supports `cosmos` only)."
            | Destroy _ ->                  "DELETE documents for a nominated category and/or stream (includes a dry-run mode). (supports `cosmos` only)."
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
and [<NoComparison; NoEquality; RequireSubcommand>] InitSqlParameters =
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
    | [<AltCommandLine "-O"; Unique>]       Oldest
    | [<AltCommandLine "-N"; Unique>]       Newest
    | [<AltCommandLine "-P"; Unique>]       Parallel
    | [<CliPrefix(CliPrefix.None)>]         Cosmos of ParseResults<Store.Cosmos.Parameters>
    | [<CliPrefix(CliPrefix.None)>]         Dynamo of ParseResults<Store.Dynamo.Parameters>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | Events ->                     "Count the number of Events in the store."
            | Streams ->                    "Count the number of Streams in the store."
            | Documents ->                  "Count the number of Documents in the store."
            | Oldest ->                     "Oldest document, based on the _ts field"
            | Newest ->                     "Newest document, based on the _ts field"
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
            | CategoryLike _ ->             "Specify category name to match against `p` as a Cosmos LIKE expression (with `%` as wildcard, e.g. `$UserServices-%`)."
            | UnfoldName _ ->               "Specify unfold Name to match against `u.c`, e.g. `Snapshotted`"
            | UnfoldCriteria _ ->           "Specify constraints on Unfold (reference unfold fields via `u.d.`, top level fields via `c.`), e.g. `u.d.name = \"TenantName1\"`."
            | Mode _ ->                     "default: `_etag` plus snapwithStream (_etag, p, u[0].d). <- Default for normal queries\n" +
                                            "snaponly: Only read `u[0].d`\n" +
                                            "snapwithstream: Read `u[0].d` and `p` (stream name), but not `_etag`.\n" +
                                            "readonly: Only read `u`nfolds, not `_etag`.\n" +
                                            "readwithstream: Read `u`nfolds and `p` (stream name), but not `_etag`.\n" +
                                            "raw: Read all Items(documents) in full. <- Default when Output File specified\n"
            | File _ ->                     "Export full retrieved JSON to file. NOTE this switches the default mode to `Raw`"
            | Pretty ->                     "Render the JSON indented over multiple lines"
            | Console ->                    "Also emit the JSON to the console. Default: Gather statistics (but only write to a File if specified)"
            | Cosmos _ ->                   "Parameters for CosmosDB."
and [<RequireQualifiedAccess>] Mode = Default | SnapOnly | SnapWithStream | ReadOnly | ReadWithStream | Raw
and [<RequireQualifiedAccess>] Criteria =
    | SingleStream of string | CatName of string | CatLike of string | Custom of sql: string | Unfiltered
    member x.Sql = x |> function
        | Criteria.SingleStream sn ->   $"c.p = \"{sn}\""
        | Criteria.CatName n ->         $"c.p LIKE \"{n}-%%\""
        | Criteria.CatLike pat ->       $"c.p LIKE \"{pat}\""
        | Criteria.Custom filter ->     filter
        | Criteria.Unfiltered ->        "1=1"
and QueryArguments(p: ParseResults<QueryParameters>) =
    member val Mode =                       p.GetResult(QueryParameters.Mode, if p.Contains QueryParameters.File then Mode.Raw else Mode.Default)
    member val Pretty =                     p.Contains QueryParameters.Pretty
    member val TeeConsole =                 p.Contains QueryParameters.Console
    member val Criteria =
        match p.TryGetResult QueryParameters.StreamName, p.TryGetResult QueryParameters.CategoryName, p.TryGetResult QueryParameters.CategoryLike with
        | Some sn, None, None ->            Criteria.SingleStream sn
        | Some _, Some _, _
        | Some _, _, Some _ ->              p.Raise "StreamName and CategoryLike/CategoryName are mutually exclusive"
        | None, Some cn, None ->            Criteria.CatName cn
        | None, None, Some cl ->            Criteria.CatLike cl
        | None, None, None ->               Criteria.Unfiltered
        | None, Some _, Some _ ->           p.Raise "CategoryLike and CategoryName are mutually exclusive"
    member val Filepath =                   p.TryGetResult QueryParameters.File
    member val UnfoldName =                 p.TryGetResult QueryParameters.UnfoldName
    member val UnfoldCriteria =             p.TryGetResult QueryParameters.UnfoldCriteria
    member val CosmosArgs =                 p.GetResult QueryParameters.Cosmos |> Store.Cosmos.Arguments
    member x.Connect() =                    match Store.Cosmos.config Log.Logger (None, true) x.CosmosArgs with
                                            | Store.Config.Cosmos (cc, _, _) -> cc.Container
                                            | _ -> p.Raise "Query requires Cosmos"
and [<NoComparison; NoEquality; RequireSubcommand>] TopParameters =
    | [<AltCommandLine "-sn"; Unique>]      StreamName of string
    | [<AltCommandLine "-cn"; Unique>]      CategoryName of string
    | [<AltCommandLine "-cl"; Unique>]      CategoryLike of string
    | [<AltCommandLine "-S"; Unique>]       Streams
    | [<AltCommandLine "-T"; Unique>]       TsOrder
    | [<AltCommandLine "-c">]               Limit of int
    | [<MainCommand; AltCommandLine "-s"; Unique>] Sort of Order
    | [<CliPrefix(CliPrefix.None)>]         Cosmos of ParseResults<Store.Cosmos.Parameters>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | StreamName _ ->               "Specify stream name to match against `p`, e.g. `$UserServices-f7c1ce63389a45bdbea1cccebb1b3c8a`."
            | CategoryName _ ->             "Specify category name to match against `p`, e.g. `$UserServices`."
            | CategoryLike _ ->             "Specify category name to match against `p` as a Cosmos LIKE expression (with `%` as wildcard, e.g. `$UserServices-%`."
            | Streams ->                    "Stream level stats"
            | TsOrder ->                    "Retrieve data in `_ts` ORDER (generally has significant RU impact). Default: Use continuation tokens"
            | Sort _ ->                     "Sort order for results"
            | Limit _ ->                    "Number of categories to limit output to (Streams limit is 10x the category limit). Default: 100"
            | Cosmos _ ->                   "Parameters for CosmosDB."
and Order = Name | Items | Events | Unfolds | Size | EventSize | UnfoldSize | InflateSize | CorrCauseSize
and TopArguments(p: ParseResults<TopParameters>) =
    member val Criteria =
        match p.TryGetResult TopParameters.StreamName, p.TryGetResult TopParameters.CategoryName, p.TryGetResult TopParameters.CategoryLike with
        | Some sn, None, None ->            Criteria.SingleStream sn
        | Some _, Some _, _
        | Some _, _, Some _ ->              p.Raise "StreamName and CategoryLike/CategoryName are mutually exclusive"
        | None, Some cn, None ->            Criteria.CatName cn
        | None, None, Some cl ->            Criteria.CatLike cl
        | None, None, None ->               Criteria.Unfiltered
        | None, Some _, Some _ ->           p.Raise "CategoryLike and CategoryName are mutually exclusive"
    member val CosmosArgs =                 p.GetResult TopParameters.Cosmos |> Store.Cosmos.Arguments
    member val StreamLevel =                p.Contains Streams
    member val Count =                      p.GetResult(Limit, 100)
    member val TsOrder =                    p.Contains TsOrder
    member val Order =                      p.GetResult(Sort, Order.Size)
    member x.StreamCount =                  p.GetResult(Limit, x.Count * 10)
    member x.Connect() =                    match Store.Cosmos.config Log.Logger (None, true) x.CosmosArgs with
                                            | Store.Config.Cosmos (cc, _, _) -> cc.Container
                                            | _ -> failwith "Top requires Cosmos"
    member x.Execute(sql) =                 let container = x.Connect()
                                            let qd = Microsoft.Azure.Cosmos.QueryDefinition sql
                                            let qo = Microsoft.Azure.Cosmos.QueryRequestOptions(MaxItemCount = x.CosmosArgs.QueryMaxItems)
                                            container.GetItemQueryIterator<System.Text.Json.JsonElement>(qd, requestOptions = qo)
and [<NoComparison; NoEquality; RequireSubcommand>] DestroyParameters =
    | [<AltCommandLine "-sn"; Unique>]      StreamName of string
    | [<AltCommandLine "-cn"; Unique>]      CategoryName of string
    | [<AltCommandLine "-cl"; Unique>]      CategoryLike of string
    | [<AltCommandLine "-cs"; Unique>]      CustomFilter of sql: string
    | [<AltCommandLine "-f"; Unique>]       Force
    | [<CliPrefix(CliPrefix.None)>]         Cosmos of ParseResults<Store.Cosmos.Parameters>
    interface IArgParserTemplate with
        member a.Usage = a |> function
            | StreamName _ ->               "Specify stream name to match against `p`, e.g. `$UserServices-f7c1ce63389a45bdbea1cccebb1b3c8a`."
            | CategoryName _ ->             "Specify category name to match against `p`, e.g. `$UserServices`."
            | CategoryLike _ ->             "Specify category name to match against `p` as a Cosmos LIKE expression (with `%` as wildcard, e.g. `$UserServices-%`."
            | CustomFilter _ ->             "Specify a custom filter, referencing the document as `c.` (e.g. `'c.p LIKE \"test-%\" AND c._ts < 1717138092'`)"
            | Force ->                      "Actually delete the documents (default is a dry run, reporting what would be deleted)"
            | Cosmos _ ->                   "Parameters for CosmosDB."
and DestroyArguments(p: ParseResults<DestroyParameters>) =
    member val Criteria =
        match p.TryGetResult StreamName, p.TryGetResult CategoryName, p.TryGetResult CategoryLike, p.TryGetResult CustomFilter with
        | Some sn, None, None, None ->      Criteria.SingleStream sn
        | Some _, Some _, _, None
        | Some _, _, Some _, None ->        p.Raise "StreamName and CategoryLike/CategoryName are mutually exclusive"
        | None, Some cn, None, None ->      Criteria.CatName cn
        | None, None, Some cl, None ->      Criteria.CatLike cl
        | None, None, None, Some filter ->  Criteria.Custom filter
        | _, _, _, Some _ ->                p.Raise "Custom SQL and Category/Stream name settings are mutually exclusive"
        | None, None, None, None ->         failwith "Category, stream name, or custom SQL must be supplied"
        | None, Some _, Some _, None ->     p.Raise "CategoryLike and CategoryName are mutually exclusive"
    member val CosmosArgs =                 p.GetResult DestroyParameters.Cosmos |> Store.Cosmos.Arguments
    member val DryRun =                     p.Contains Force |> not
    member val Dop =                        4
    member val StatsInterval =              TimeSpan.FromSeconds 30
    member x.Connect() =                    match Store.Cosmos.config Log.Logger (None, true) x.CosmosArgs with
                                            | Store.Config.Cosmos (cc, _, _) -> cc.Container
                                            | _ -> failwith "Destroy requires Cosmos"
and SnEventsUnfolds = { p: string; id: string; es: int; us: int }
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

let createDomainLog quiet verbose verboseConsole maybeSeqEndpoint =
    let c = LoggerConfiguration()
    let c = if verbose then c.MinimumLevel.Debug() else c
    let c = writeToStatsSinks c
    let c = let outputTemplate = "{Timestamp:T} {Level:u1} {Message:l} {Properties}{NewLine}{Exception}"
            let outputTemplate = if quiet then outputTemplate.Substring(outputTemplate.IndexOf ' ' + 1) else outputTemplate
            let consoleLevel = if verboseConsole then LogEventLevel.Debug else LogEventLevel.Information
            c.WriteTo.Console(consoleLevel, outputTemplate, theme = Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code)
    let c = match maybeSeqEndpoint with None -> c | Some endpoint -> c.WriteTo.Seq(endpoint)
    c.CreateLogger()

module Dynamo =

    open Store.Dynamo
    let logConfig log (sa: Arguments) = sa.Connector.LogConfiguration(log)

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
                l.Information("CosmosStore provisioning in {mode:l} mode with throughput as defined at account level", "Serverless")
            CosmosInit.init log (connector.CreateUninitialized()) (dName, cName) a.ProvisioningMode a.IndexUnfolds a.SkipStoredProc
        | x -> p.Raise $"unexpected subcommand %A{x}"

module CosmosStats =

    open Equinox.CosmosStore.Linq.Internal
    open FSharp.Control

    let run (log : ILogger, _verboseConsole, _maybeSeq) (p : ParseResults<StatsParameters>) =
        match p.GetSubCommand() with
        | StatsParameters.Cosmos sp ->
            let doS, doD, doE, doO, doN =
                let s, d, e, o, n = p.Contains StatsParameters.Streams, p.Contains Documents, p.Contains StatsParameters.Events, p.Contains Oldest, p.Contains Newest
                let all = not (s || d || e || o || n)
                all || s, all || d, all || e, all || o, all || n
            let doS = doS || (not doD && not doE) // default to counting streams only unless otherwise specified
            let inParallel = p.Contains Parallel
            let connector, dName, cName = CosmosInit.connect log sp
            let container = connector.CreateUninitialized().GetContainer(dName, cName)
            let ops = [| if doS then "Streams",   """SELECT VALUE COUNT(1) FROM c WHERE c.id="-1" """
                         if doD then "Documents", """SELECT VALUE COUNT(1) FROM c"""
                         if doE then "Events",    """SELECT VALUE SUM(c.n) FROM c WHERE c.id="-1" """
                         if doO then "Oldest",    """SELECT VALUE MIN(c._ts) FROM c"""
                         if doN then "Newest",    """SELECT VALUE MAX(c._ts) FROM c""" |]
            let render = if log.IsEnabled LogEventLevel.Debug then snd else fst
            log.Information("Computing {measures} ({mode})", Seq.map render ops, (if inParallel then "in parallel" else "serially"))
            ops |> Seq.map (fun (name, sql) -> async {
                    let! res = Microsoft.Azure.Cosmos.QueryDefinition sql
                               |> container.GetItemQueryIterator<int>
                               |> Query.enum_ log container "Stat" null LogEventLevel.Debug |> TaskSeq.head |> Async.AwaitTaskCorrect
                    match name with
                    | "Oldest" | "Newest" -> log.Information("{stat,-10}: {result,13} ({d:u})", name, res, DateTime.UnixEpoch.AddSeconds(float res))
                    | _ -> log.Information("{stat,-10}: {result,13:N0}", name, res) })
                |> if inParallel then Async.Parallel else Async.Sequential
                |> Async.Ignore<unit[]>
        | StatsParameters.Dynamo sp -> async {
            let sa = Store.Dynamo.Arguments sp
            Dynamo.logConfig log sa
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

let prettySerdes = lazy FsCodec.SystemTextJson.Serdes(FsCodec.SystemTextJson.Options.Create(indent = true))

type System.Text.Json.JsonElement with
    member x.Timestamp = x.GetProperty("_ts").GetDouble() |> DateTime.UnixEpoch.AddSeconds
    member x.TryProp(name: string) = let mutable p = Unchecked.defaultof<_> in if x.TryGetProperty(name, &p) then ValueSome p else ValueNone

module StreamName =
    let categoryName = FsCodec.StreamName.parse >> FsCodec.StreamName.split >> fun struct (cn, _sid) -> cn

module CosmosQuery =

    open Equinox.CosmosStore.Linq.Internal
    open FSharp.Control
    let inline miB x = Equinox.CosmosStore.Linq.Internal.miB x
    type System.Text.Json.JsonElement with
        member x.Utf8ByteCount = if x.ValueKind = System.Text.Json.JsonValueKind.Null then 0 else x.GetRawText() |> System.Text.Encoding.UTF8.GetByteCount
    type System.Text.Json.JsonDocument with
        member x.Cast<'T>() = System.Text.Json.JsonSerializer.Deserialize<'T>(x.RootElement)
        member x.Timestamp =
            let ok, p = x.RootElement.TryGetProperty("_ts")
            if ok then p.GetDouble() |> DateTime.UnixEpoch.AddSeconds |> Some else None
    let private composeSql (a: QueryArguments) =
        match a.Criteria with
        | Criteria.Unfiltered ->
            let lel = if a.Mode = Mode.Raw then LogEventLevel.Debug elif a.Filepath = None then LogEventLevel.Warning else LogEventLevel.Information
            Log.Write(lel, "No StreamName or CategoryName/CategoryLike specified - Unfold Criteria better be unambiguous")
        | _ -> ()
        let selectedFields =
            match a.Mode with
            | Mode.Default ->               "c._etag, c.p, c.u[0].d"
            | Mode.SnapOnly ->              "c.u[0].d"
            | Mode.SnapWithStream ->        "c.p, c.u[0].d"
            | Mode.ReadOnly ->              "c.u" // TOCONSIDER remove; adjust TryLoad/TryHydrateTip
            | Mode.ReadWithStream ->        "c.p, c.u" // TOCONSIDER remove; adjust TryLoad/TryHydrateTip
            | Mode.Raw ->                   "*"
        let unfoldFilter =
            let exists cond = $"EXISTS (SELECT VALUE u FROM u IN c.u WHERE {cond})"
            match [| match a.UnfoldName with None -> () | Some un -> $"u.c = \"{un}\""
                     match a.UnfoldCriteria with None -> () | Some uc -> uc |] with
            | [||] -> "1=1"
            | [| x |] -> x |> exists
            | xs -> String.Join(" AND ", xs) |> exists
        $"SELECT {selectedFields} FROM c WHERE {a.Criteria.Sql} AND {unfoldFilter}"
    let private queryDef (a: QueryArguments) =
        let sql = composeSql a
        Log.Information("Querying {mode}: {q}", a.Mode, sql)
        Microsoft.Azure.Cosmos.QueryDefinition sql
    let run (a: QueryArguments) = task {
        let sw = System.Diagnostics.Stopwatch.StartNew()
        let serdes = if a.Pretty then prettySerdes.Value else FsCodec.SystemTextJson.Serdes.Default
        let maybeFileStream = a.Filepath |> Option.map (fun p ->
            Log.Information("Dumping {mode} content to {path}", a.Mode, System.IO.FileInfo(p).FullName)
            System.IO.File.Create p) // Silently truncate if it exists, makes sense for typical usage
        let qo = Microsoft.Azure.Cosmos.QueryRequestOptions(MaxItemCount = a.CosmosArgs.QueryMaxItems)
        let container = a.Connect()
        let pageStreams, accStreams = System.Collections.Generic.HashSet(), System.Collections.Generic.HashSet()
        let mutable accI, accE, accU, accRus, accBytesRead = 0L, 0L, 0L, 0., 0L
        let it = container.GetItemQueryIterator<System.Text.Json.JsonDocument>(queryDef a, requestOptions = qo)
        try for rtt, rc, items, rdc, rds, ods in it |> Query.enum__ do
                let mutable newestTs = DateTime.MinValue
                let items = [| for x in items -> newestTs <- max newestTs x.RootElement.Timestamp
                                                 System.Text.Json.JsonSerializer.Deserialize<Equinox.CosmosStore.Core.Tip>(x.RootElement) |]
                let inline arrayLen x = if isNull x then 0 else Array.length x
                pageStreams.Clear(); for x in items do if x.p <> null && pageStreams.Add x.p then accStreams.Add x.p |> ignore
                let pageI, pageE, pageU = items.Length, items |> Seq.sumBy (_.e >> arrayLen), items |> Seq.sumBy (_.u >> arrayLen)
                Log.Information("Page{rdc,5}>{count,4}i{streams,5}s{es,5}e{us,5}u{rds,5:f2}>{ods,4:f2}MiB{rc,7:f2}RU{s,5:N1}s age {age:dddd\.hh\:mm\:ss}",
                                rdc, pageI, pageStreams.Count, pageE, pageU, miB rds, miB ods, rc, rtt.TotalSeconds, DateTime.UtcNow - newestTs)
                maybeFileStream |> Option.iter (fun stream ->
                    for x in items do
                        serdes.SerializeToStream(x, stream)
                        stream.WriteByte(byte '\n'))
                if a.TeeConsole then
                    items |> Seq.iter (serdes.Serialize >> Console.WriteLine)
                accI <- accI + int64 pageI; accE <- accE + int64 pageE; accU <- accU + int64 pageU
                accRus <- accRus + rc; accBytesRead <- accBytesRead + int64 ods
        finally
            let fileSize = maybeFileStream |> Option.map _.Position |> Option.defaultValue 0
            maybeFileStream |> Option.iter _.Close() // Before we log so time includes flush time and no confusion
            let accCategories = System.Collections.Generic.HashSet(accStreams |> Seq.map StreamName.categoryName).Count
            Log.Information("TOTALS {count:N0}i {cats}c {streams:N0}s {es:N0}e {us:N0}u R/W {rmib:N1}/{wmib:N1}MiB {ru:N2}RU {s:N1}s",
                            accI, accCategories, accStreams.Count, accE, accU, miB accBytesRead, miB fileSize, accRus, sw.Elapsed.TotalSeconds) }

module CosmosTop =

    open Equinox.CosmosStore.Linq.Internal
    open FSharp.Control
    open System.Text.Json

    let _t = Unchecked.defaultof<Equinox.CosmosStore.Core.Tip>
    let inline tryEquinoxStreamName (x: JsonElement) =
        match x.TryProp(nameof _t.p) with
        | ValueSome (je: JsonElement) when je.ValueKind = JsonValueKind.String ->
            je.GetString() |> FsCodec.StreamName.parse |> FsCodec.StreamName.toString |> ValueSome
        | _ -> ValueNone
    let inline parseEquinoxStreamName (x: JsonElement) =
        match tryEquinoxStreamName x with
        | ValueNone -> failwith $"Could not parse document:\n{prettySerdes.Value.Serialize x}"
        | ValueSome sn -> sn

    module private Parser =
        let scratch = new System.IO.MemoryStream()
        let utf8Size (x: JsonElement) =
            scratch.Position <- 0L
            JsonSerializer.Serialize(scratch, x)
            scratch.Position
        let inflatedUtf8Size x =
            scratch.Position <- 0L
            if Equinox.CosmosStore.Core.JsonElement.tryInflateTo scratch x then scratch.Position
            else utf8Size x
        let infSize = function ValueSome x -> inflatedUtf8Size x | ValueNone -> 0
        // using the length as a decent proxy for UTF-8 length of corr/causation; if you have messy data in there, you'll have bigger problems to worry about
        let inline stringLen x = match x with ValueSome (x: JsonElement) when x.ValueKind <> JsonValueKind.Null -> x.GetString().Length | _ -> 0
        let _e = Unchecked.defaultof<Equinox.CosmosStore.Core.Event> // Or Unfold - both share field names
        let dmcSize (x: JsonElement) =
            (struct (0, 0L), x.EnumerateArray())
            ||> Seq.fold (fun struct (c, i) x ->
                struct (c + (x.TryProp(nameof _e.correlationId) |> stringLen) + (x.TryProp(nameof _e.causationId) |> stringLen),
                        i + (x.TryProp(nameof _e.d) |> infSize) + (x.TryProp(nameof _e.m) |> infSize)))
        let private tryParseEventOrUnfold = function
            | ValueNone -> struct (0, 0L, struct (0, 0L))
            | ValueSome (x: JsonElement) -> x.GetArrayLength(), utf8Size x, dmcSize x
        let _t = Unchecked.defaultof<Equinox.CosmosStore.Core.Tip>
        [<Struct; CustomEquality; NoComparison>]
        type Stat =
            { key: string; count: int; events: int; unfolds: int; bytes: int64; eBytes: int64; uBytes: int64; cBytes: int64; iBytes: int64 }
            member x.Merge y =
                {   key = x.key; count = x.count + y.count; events = x.events + y.events; unfolds = x.unfolds + y.unfolds; bytes = x.bytes + y.bytes
                    eBytes = x.eBytes + y.eBytes; uBytes = x.uBytes + y.uBytes; cBytes = x.cBytes + y.cBytes; iBytes = x.iBytes + y.iBytes }
            override x.GetHashCode() = StringComparer.Ordinal.GetHashCode x.key
            override x.Equals y = match y with :? Stat as y -> StringComparer.Ordinal.Equals(x.key, y.key) | _ -> false
            static member Create(key, x: JsonElement) =
                let struct (e, eb, struct (ec, ei)) = x.TryProp(nameof _t.e) |> tryParseEventOrUnfold
                let struct (u, ub, struct (uc, ui)) = x.TryProp(nameof _t.u) |> tryParseEventOrUnfold
                {   key = key; count = 1; events = e; unfolds = u
                    bytes = utf8Size x; eBytes = eb; uBytes = ub; cBytes = int64 (ec + uc); iBytes = ei + ui }
    let [<Literal>] OrderByTs = " ORDER BY c._ts"
    let private sql (a: TopArguments) = $"SELECT * FROM c WHERE {a.Criteria.Sql}{if a.TsOrder then OrderByTs else null}"
    let run (a: TopArguments) = task {
        let sw = System.Diagnostics.Stopwatch.StartNew()
        let pageStreams, accStreams = System.Collections.Generic.HashSet(), System.Collections.Generic.HashSet()
        let mutable accI, accE, accU, accRus, accRds, accOds, accBytes = 0L, 0L, 0L, 0., 0L, 0L, 0L
        let s = System.Collections.Generic.HashSet()
        let group = if a.StreamLevel then id else StreamName.categoryName
        try for rtt, rc, items, rdc, rds, ods in a.Execute(sql a) |> Query.enum__ do
                let mutable pageI, pageE, pageU, pageB, pageCc, pageDm, newestTs, sw = 0, 0, 0, 0L, 0L, 0L, DateTime.MinValue, System.Diagnostics.Stopwatch.StartNew()
                for x in items do
                    newestTs <- max newestTs x.Timestamp
                    let sn = parseEquinoxStreamName x
                    if pageStreams.Add sn && not a.StreamLevel then accStreams.Add sn |> ignore
                    let x = Parser.Stat.Create(group sn, x)
                    let mutable v = Unchecked.defaultof<_>
                    s.Add(if s.TryGetValue(x, &v) then s.Remove x |> ignore; v.Merge x else x) |> ignore
                    pageI <- pageI + 1; pageE <- pageE + x.events; pageU <- pageU + x.unfolds
                    pageB <- pageB + x.bytes; pageCc <- pageCc + x.cBytes; pageDm <- pageDm + x.iBytes
                Log.Information("Page{rdc,5}>{count,4}i{streams,5}s{es,5}e{us,5}u{rds,5:f2}>{ods,4:f2}<{jds,4:f2}MiB{rc,7:f2}RU{s,5:N1}s D+M{im,4:f1} C+C{cm,5:f2} {ms,3}ms age {age:dddd\.hh\:mm\:ss}",
                                rdc, pageI, pageStreams.Count, pageE, pageU, miB rds, miB ods, miB pageB, rc, rtt.TotalSeconds, miB pageDm, miB pageCc, sw.ElapsedMilliseconds, DateTime.UtcNow - newestTs)
                pageStreams.Clear()
                accI <- accI + int64 pageI; accE <- accE + int64 pageE; accU <- accU + int64 pageU
                accRus <- accRus + rc; accRds <- accRds + int64 rds; accOds <- accOds + int64 ods; accBytes <- accBytes + pageB
        finally

        let accCats = (if a.StreamLevel then s |> Seq.map _.key else accStreams) |> Seq.map group |> System.Collections.Generic.HashSet |> _.Count
        let accStreams = if a.StreamLevel then s.Count else accStreams.Count
        let iBytes, cBytes = s |> Seq.sumBy _.iBytes, s |> Seq.sumBy _.cBytes
        let giB x = miB x / 1024.
        Log.Information("TOTALS {count:N0}i {cats:N0}c {streams:N0}s {es:N0}e {us:N0}u read {rg:f1}GiB output {og:f1}GiB JSON {tg:f1}GiB D+M(inflated) {ig:f1}GiB C+C {cm:f2}MiB {ru:N2}RU {s:N1}s",
                        accI, accCats, accStreams, accE, accU, giB accRds, giB accOds, giB accBytes, giB iBytes, miB cBytes, accRus, sw.Elapsed.TotalSeconds)

        let sort: Parser.Stat seq -> Parser.Stat seq = a.Order |> function
            | Order.Name -> Seq.sortBy _.key
            | Order.Size -> Seq.sortByDescending _.bytes
            | Order.Items -> Seq.sortByDescending _.count
            | Order.Events -> Seq.sortByDescending _.events
            | Order.Unfolds -> Seq.sortByDescending _.unfolds
            | Order.EventSize -> Seq.sortByDescending _.eBytes
            | Order.UnfoldSize -> Seq.sortByDescending _.uBytes
            | Order.InflateSize -> Seq.sortByDescending _.iBytes
            | Order.CorrCauseSize -> Seq.sortByDescending _.cBytes
        let render (x: Parser.Stat) =
            Log.Information("{count,7}i {tm,6:N2}MiB E{events,7} {em,7:N1} U{unfolds,7} {um,6:N1} D+M{dm,6:N1} C+C{cm,5:N1} {key}",
                            x.count, miB x.bytes, x.events, miB x.eBytes, x.unfolds, miB x.uBytes, miB x.iBytes, miB x.cBytes, x.key)
        if a.StreamLevel then
            let collapsed = s |> Seq.groupBy (_.key >> StreamName.categoryName) |> Seq.map (fun (cat, xs) -> { (xs |> Seq.reduce _.Merge) with key = cat })
            sort collapsed |> Seq.truncate a.Count |> Seq.iter render
        sort s |> Seq.truncate (if a.StreamLevel then a.StreamCount else a.Count) |> Seq.iter render }

module CosmosDestroy =

    open Equinox.CosmosStore.Linq.Internal
    open FSharp.Control

    type Sem(max) =
        let inner = new SemaphoreSlim(max)
        member _.IsEmpty = inner.CurrentCount = max
        member _.TryWait(ms: int) = inner.WaitAsync ms
        member _.Release() = inner.Release() |> ignore

    module Channel =

        open System.Threading.Channels
        let unboundedSr<'t> = Channel.CreateUnbounded<'t>(UnboundedChannelOptions(SingleReader = true))
        let write (w: ChannelWriter<_>) = w.TryWrite >> ignore
        let inline readAll (r: ChannelReader<_>) () = seq {
            let mutable msg = Unchecked.defaultof<_>
            while r.TryRead(&msg) do
                yield msg }

    let run (a: DestroyArguments) = task {
        let tsw = System.Diagnostics.Stopwatch.StartNew()
        let sql = $"SELECT c.p, c.id, ARRAYLENGTH(c.e) AS es, ARRAYLENGTH(c.u) AS us FROM c WHERE {a.Criteria.Sql}"
        if a.DryRun then Log.Warning("Dry-run of deleting items based on {sql}", sql)
        else Log.Warning("DESTROYING all Items WHERE {sql}", a.Criteria.Sql)
        let container = a.Connect()
        let query =
            let qd = Microsoft.Azure.Cosmos.QueryDefinition sql
            let qo = Microsoft.Azure.Cosmos.QueryRequestOptions(MaxItemCount = a.CosmosArgs.QueryMaxItems)
            container.GetItemQueryIterator<SnEventsUnfolds>(qd, requestOptions = qo)
        let pageStreams, accStreams = System.Collections.Generic.HashSet(), System.Collections.Generic.HashSet()
        let mutable accI, accE, accU, accRus, accDelRu, accRds, accOds = 0L, 0L, 0L, 0., 0., 0L, 0L
        let deletionDop = Sem a.Dop
        let writeResult, readResults = let c = Channel.unboundedSr<struct (float * string)> in Channel.write c.Writer, Channel.readAll c.Reader
        try for rtt, rc, items, rdc, rds, ods in query |> Query.enum__ do
                let mutable pageI, pageE, pageU, pRu, iRu = 0, 0, 0, 0., 0.
                let pageSw, intervalSw = System.Diagnostics.Stopwatch.StartNew(), System.Diagnostics.Stopwatch.StartNew()
                let drainResults () =
                    let mutable failMessage = null
                    for ru, exn in readResults () do
                        iRu <- iRu + ru; pRu <- pRu + ru
                        if exn <> null && failMessage <> null then failMessage <- exn
                    if intervalSw.Elapsed > a.StatsInterval then
                        Log.Information(".. Deleted {count,5}i {streams,7}s{es,7}e{us,7}u {rus,7:N2}WRU/s {s,6:N1}s",
                                        pageI, pageStreams.Count, pageE, pageU, iRu / intervalSw.Elapsed.TotalSeconds, pageSw.Elapsed.TotalSeconds)
                        intervalSw.Restart()
                        iRu <- 0
                    if failMessage <> null then failwith failMessage
                    (a.StatsInterval - intervalSw.Elapsed).TotalMilliseconds |> int
                let awaitState check = task {
                    let mutable reserved = false
                    while not reserved do
                        match drainResults () with
                        | wait when wait <= 0 -> ()
                        | timeoutAtNextLogInterval ->
                            match! check timeoutAtNextLogInterval with
                            | false -> ()
                            | true -> reserved <- true }
                let checkEmpty () = task {
                    if deletionDop.IsEmpty then return true else
                    do! System.Threading.Tasks.Task.Delay 1
                    return deletionDop.IsEmpty }
                let awaitCapacity () = awaitState deletionDop.TryWait
                let releaseCapacity () = deletionDop.Release()
                let awaitCompletion () = awaitState (fun _timeout -> checkEmpty ())
                for i in items do
                    if pageStreams.Add i.p then accStreams.Add i.p |> ignore
                    pageI <- pageI + 1; pageE <- pageE + i.es; pageU <- pageU + i.us
                    if not a.DryRun then
                        do! awaitCapacity ()
                        ignore <| task { // we could do a Task.Run dance, but kicking it off inline without waiting suits us fine as results processed above
                            let! res = container.DeleteItemStreamAsync(i.id, Microsoft.Azure.Cosmos.PartitionKey i.p)
                            releaseCapacity ()
                            let exn =
                                if res.IsSuccessStatusCode || res.StatusCode = System.Net.HttpStatusCode.NotFound then null
                                else $"Deletion of {i.p}/{i.id} failed with Code: {res.StatusCode} Message: {res.ErrorMessage}\n{res.Diagnostics}"
                            writeResult (res.Headers.RequestCharge, exn) }
                do! awaitCompletion () // we want stats output and/or failure exceptions to align with Pages
                let ps = pageSw.Elapsed.TotalSeconds
                Log.Information("Page{rdc,6}>{count,5}i {streams,7}s{es,7}e{us,7}u{rds,8:f2}>{ods,4:f2} {prc,8:f2}RRU {rs,5:N1}s {rus:N2}WRU/s {ps,5:N1}s",
                                rdc, pageI, pageStreams.Count, pageE, pageU, miB rds, miB ods, rc, rtt.TotalSeconds, pRu / ps, ps)
                pageStreams.Clear()
                accI <- accI + int64 pageI; accE <- accE + int64 pageE; accU <- accU + int64 pageU
                accRus <- accRus + rc; accDelRu <- accDelRu + pRu; accRds <- accRds + int64 rds; accOds <- accOds + int64 ods
         finally

         let accCats = accStreams |> Seq.map StreamName.categoryName |> System.Collections.Generic.HashSet |> _.Count
         Log.Information("TOTALS {count:N0}i {cats:N0}c {streams:N0}s {es:N0}e {us:N0}u read {rmib:f1}MiB output {omib:f1}MiB {rru:N2}RRU Avg {aru:N2}WRU/s Delete {dru:N2}WRU Total {s:N1}s",
                         accI, accCats, accStreams.Count, accE, accU, miB accRds, miB accOds, accRus, accDelRu / tsw.Elapsed.TotalSeconds, accDelRu, tsw.Elapsed.TotalSeconds) }

module DynamoInit =

    open Equinox.DynamoStore

    let table (log : ILogger) (p : ParseResults<TableParameters>) = async {
        let a = DynamoInitArguments p
        match p.GetSubCommand() with
        | TableParameters.Dynamo sp ->
            let sa = Store.Dynamo.Arguments sp
            Dynamo.logConfig log sa
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

    let databaseOrSchema (log: ILogger) (p : ParseResults<InitSqlParameters>) =
        match p.GetSubCommand() with
        | InitSqlParameters.MsSql p ->
            let a = Store.Sql.Ms.Arguments(p)
            Store.Sql.Ms.connect log (a.ConnectionString, a.Credentials, a.Schema, true) |> Async.Ignore
        | InitSqlParameters.MySql p ->
            let a = Store.Sql.My.Arguments(p)
            Store.Sql.My.connect log (a.ConnectionString, a.Credentials, true) |> Async.Ignore
        | InitSqlParameters.Postgres p ->
            let a = Store.Sql.Pg.Arguments(p)
            Store.Sql.Pg.connect log (a.ConnectionString, a.Credentials, a.Schema, true) |> Async.Ignore

module Dump =

    let private prettifyJson (json: string) =
        use parsed = System.Text.Json.JsonDocument.Parse json
        prettySerdes.Value.Serialize parsed
    let run (log : ILogger, verboseConsole, maybeSeq) (p : ParseResults<DumpParameters>) = async {
        let a = DumpArguments p
        let createStoreLog storeVerbose = createStoreLog storeVerbose verboseConsole maybeSeq
        let storeLog, storeConfig = a.ConfigureStore(log, createStoreLog)
        let doU, doE = not (p.Contains EventsOnly), not (p.Contains UnfoldsOnly)
        let doC, doJ, doS, doT = p.Contains Correlation, not (p.Contains JsonSkip), not (p.Contains Blobs), not (p.Contains TimeRegular)
        let store = Services.Store(storeConfig)

        let initial = List.empty
        let fold state events = (events, state) ||> Seq.foldBack (fun e l -> e :: l)
        let tryDecode (x: FsCodec.ITimelineEvent<ReadOnlyMemory<byte>>) = ValueSome x
        let idCodec = FsCodec.Codec.Create((fun _ -> failwith "No encoding required"), tryDecode, (fun _ _ -> failwith "No mapCausation"))
        let isOriginAndSnapshot = (fun (event : FsCodec.ITimelineEvent<_>) -> not doE && event.IsUnfold), fun _state -> failwith "no snapshot required"
        let formatUnfolds, formatEvents =
            if p.Contains FlattenUnfolds then id else prettifyJson
            , if p.Contains Pretty then prettifyJson else id
        let mutable payloadBytes = 0
        let render format (data: ReadOnlyMemory<byte>) =
            payloadBytes <- payloadBytes + data.Length
            if data.IsEmpty then null
            elif not doS then $"%6d{data.Length}b"
            else try let s = System.Text.Encoding.UTF8.GetString data.Span
                     if doJ then try format s
                                 with e -> log.ForContext("str", s).Warning(e, "JSON Parse failure - use --JsonSkip option to inhibit"); reraise()
                     else $"(%d{s.Length} chars)"
                 with e -> log.Warning(e, "UTF-8 Parse failure - use --Blobs option to inhibit"); reraise()
        let humanize: TimeSpan -> string = function
            | x when x.TotalDays >= 1. -> x.ToString "d\dhh\hmm\m"
            | x when x.TotalHours >= 1. -> x.ToString "h\hmm\mss\s"
            | x when x.TotalMinutes >= 1. -> x.ToString "m\mss\.ff\s"
            | x -> x.ToString("s\.fff\s")
        let dumpEvents (streamName: FsCodec.StreamName) = async {
            let struct (categoryName, sid) = FsCodec.StreamName.split streamName
            let cat = store.Category(categoryName, idCodec, fold, initial, isOriginAndSnapshot)
            let decider = Equinox.Decider.forStream storeLog cat sid
            let! streamBytes, events = decider.QueryEx(fun c -> c.StreamEventBytes, c.State)
            let mutable prevTs = None
            for x in events |> Seq.filter (fun e -> (e.IsUnfold && doU) || (not e.IsUnfold && doE)) do
                let ty, render = if x.IsUnfold then "U", render formatUnfolds else "E", render formatEvents
                let interval =
                    match prevTs with
                    | Some p when not x.IsUnfold -> let ts = x.Timestamp - p in if doT then humanize ts else ts.ToString()
                    | _ -> if doT then "n/a" else "0"
                prevTs <- Some x.Timestamp
                if not doC then log.Information("{i,4}@{t:u}+{d,9} {u:l} {e:l} {data:l} {meta:l}",
                                                x.Index, x.Timestamp, interval, ty, x.EventType, render x.Data, render x.Meta)
                else log.Information("{i,4}@{t:u}+{d,9} Corr {corr} Cause {cause} {u:l} {e:l} {data:l} {meta:l}",
                                     x.Index, x.Timestamp, interval, x.CorrelationId, x.CausationId, ty, x.EventType, render x.Data, render x.Meta)
            match streamBytes with ValueNone -> () | ValueSome x -> log.Information("ISyncContext.StreamEventBytes {kib:n1}KiB", float x / 1024.) }
        resetStats ()
        let streams = p.GetResults DumpParameters.Stream
        log.ForContext("streams",streams).Information("Reading...")
        do! streams
            |> Seq.map dumpEvents
            |> Async.Parallel
            |> Async.Ignore<unit[]>

        log.Information("Total Event Bodies Payload {kib:n1}KiB", float payloadBytes / 1024.)
        if verboseConsole then
            dumpStats log storeConfig }

type Arguments(p: ParseResults<Parameters>) =
    let maybeSeq = if p.Contains LocalSeq then Some "http://localhost:5341" else None
    let quiet, verbose, verboseConsole = p.Contains Quiet, p.Contains Verbose, p.Contains VerboseConsole
    member _.CreateDomainLog() = createDomainLog quiet verbose verboseConsole maybeSeq
    member _.ExecuteSubCommand() = async {
        match p.GetSubCommand() with
        | Init a ->     do! CosmosInit.containerAndOrDb Log.Logger a CancellationToken.None |> Async.AwaitTaskCorrect
        | InitAws a ->  do! DynamoInit.table Log.Logger a
        | InitSql a ->  do! SqlInit.databaseOrSchema Log.Logger a
        | Dump a ->     do! Dump.run (Log.Logger, verboseConsole, maybeSeq) a
        | Query a ->    do! CosmosQuery.run (QueryArguments a) |> Async.AwaitTaskCorrect
        | Top a ->      do! CosmosTop.run (TopArguments a) |> Async.AwaitTaskCorrect
        | Destroy a ->  do! CosmosDestroy.run (DestroyArguments a) |> Async.AwaitTaskCorrect
        | Stats a ->    do! CosmosStats.run (Log.Logger, verboseConsole, maybeSeq) a
        | LoadTest a -> let n = p.GetResult(LogFile, fun () -> p.ProgramName + ".log")
                        let reportFilename = System.IO.FileInfo(n).FullName
                        let createStoreLog storeVerbose = createStoreLog storeVerbose verboseConsole maybeSeq
                        Tests.LoadTest.run Log.Logger (createStoreLog, verbose, verboseConsole) (resetStats, dumpStats) reportFilename a
        | x ->          p.Raise $"unexpected subcommand %A{x}" }
    static member Parse argv = ArgumentParser.Create().ParseCommandLine(argv) |> Arguments

[<EntryPoint>]
let main argv =
    try let a = Arguments.Parse argv
        try Log.Logger <- a.CreateDomainLog()
            try a.ExecuteSubCommand() |> Async.RunSynchronously; 0
            with e when not (e :? ArguParseException) -> Log.Fatal(e, "Exiting"); 2
        finally Log.CloseAndFlush()
    with :? ArguParseException as e -> eprintfn $"%s{e.Message}"; 1
        | e -> eprintfn $"EXCEPTION: %O{e}"; 1
