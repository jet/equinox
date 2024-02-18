module Equinox.Tool.Tests

open System.Threading
open Domain
open Microsoft.Extensions.DependencyInjection
open System
open System.Net.Http
open System.Text
open Serilog

type TestKind = Favorite | SaveForLater | Todo of size: int

let [<Literal>] seed = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Donec a diam lectus. Sed sit amet ipsum mauris. Maecenas congue ligula ac quam viverra nec consectetur ante hendrerit. Donec et mollis dolor. Praesent et diam eget libero egestas mattis sit amet vitae augue. Nam tincidunt congue enim, ut porta lorem lacinia consectetur! "
let lipsum len =
    StringBuilder.Build (fun x ->
        while x.Length < len do
            let req = len - x.Length
            x.Append(if req >= seed.Length then seed else seed.Substring(0, req)) |> ignore)
let private guard = obj()
let private rnd = Random()
let rlipsum rlen =
    let actualSize = lock guard (fun () -> rnd.Next(1,rlen))
    lipsum actualSize
type TodoClient.Todo with
    static member Create(size) : TodoClient.Todo =
        { id = 0; url = null; order = 0; title = rlipsum size; completed =  false }
type TodoBackend.Events.Todo with
    static member Create(size) : TodoBackend.Events.Todo =
        { id = 0; order = 0; title = rlipsum size; completed =  false }

let mkSkuId () = Guid.NewGuid() |> SkuId

let executeLocal (container: ServiceProvider) test: ClientId -> Async<unit> =
    match test with
    | Favorite ->
        let service = container.GetRequiredService<Favorites.Service>()
        fun clientId -> async {
            let sku = mkSkuId ()
            do! service.Favorite(clientId,[sku])
            let! items = service.List clientId
            if items |> Array.exists (fun x -> x.skuId = sku) |> not then invalidOp "Added item not found" }
    | SaveForLater ->
        let service = container.GetRequiredService<SavedForLater.Service>()
        fun clientId -> async {
            let skus = [mkSkuId (); mkSkuId (); mkSkuId ()]
            match! service.Save(clientId,skus) with
            | true ->
                let! items = service.List clientId
                if skus |> List.forall (fun sku -> items |> Array.exists (fun x -> x.skuId = sku)) |> not then invalidOp "Added item not found"
            | false ->
                return! service.RemoveAll(clientId) }
    | Todo size ->
        let service = container.GetRequiredService<TodoBackend.Service>()
        fun clientId -> async {
            match! service.List(clientId) with
            | items when Seq.length items > 1000 ->
                return! service.Clear(clientId)
            | _ ->
                let! _ = service.Create(clientId, TodoBackend.Events.Todo.Create size)
                return ()}

let executeRemote (client: HttpClient) test =
    match test with
    | Favorite ->
        fun clientId ->
            let session = StoreClient.Session(client, clientId)
            let client = session.Favorites
            async {
                let sku = mkSkuId ()
                do! client.Favorite [|sku|]
                let! items = client.List
                if items |> Array.exists (fun x -> x.skuId = sku) |> not then invalidOp "Added item not found" }
    | SaveForLater ->
        fun clientId ->
            let session = StoreClient.Session(client, clientId)
            let client = session.Saves
            async {
                let skus = [| mkSkuId (); mkSkuId (); mkSkuId () |]
                match! client.Save skus with
                | true ->
                    let! items = client.List
                    // NB this can happen if we overload the system and the next operation for this virtual client takes the other leg and removes it
                    if skus |> Array.forall (fun sku -> items |> Array.exists (fun x -> x.skuId = sku)) |> not then invalidOp "Added item not found"
                | false ->
                    let! current = client.List
                    return! client.Remove [|for x in current -> x.skuId|] }
    | Todo size ->
        fun clientId -> async {
            let session = TodoClient.Session(client, clientId)
            let client = session.Todos
            let! items = client.List()
            if Seq.length items > 1000 then
                return! client.Clear()
            else
                let! _ = client.Add(TodoClient.Todo.Create size)
                return () }

module LoadTest =

    open Argu
    open Samples.Infrastructure

    type [<NoComparison; NoEquality; RequireSubcommand>] LoadTestParameters =
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
    and [<NoComparison>] WebParameters =
        | [<AltCommandLine "-u">]               Endpoint of string
        interface IArgParserTemplate with
            member a.Usage = a |> function
                | Endpoint _ ->                 "Target address. Default: https://localhost:5001"
    and Test = Favorite | SaveForLater | Todo
    and TestArguments(p : ParseResults<LoadTestParameters>) =
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
            let cache = if x.Cache then Equinox.Cache("dummy", sizeMb = 50) |> Some else None
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
        member _.TestKind = match p.GetResult(Name, Favorite) with
                            | Favorite ->     TestKind.Favorite
                            | SaveForLater -> TestKind.SaveForLater
                            | Todo ->         TestKind.Todo (p.GetResult(Size, 100))


    let private runLoadTest log testsPerSecond duration errorCutoff reportingIntervals (clients : ClientId[]) runSingleTest =
        let mutable idx = -1L
        let selectClient () =
            let clientIndex = Interlocked.Increment(&idx) |> int
            clients[clientIndex % clients.Length]
        let selectClient = async { return async { return selectClient() } }
        Equinox.Tools.TestHarness.Local.runLoadTest log reportingIntervals testsPerSecond errorCutoff duration selectClient runSingleTest
    let private decorateWithLogger (domainLog : ILogger, verbose) (run: 't -> Async<unit>) =
        let execute clientId =
            if not verbose then run clientId
            else async {
                domainLog.Information("Executing for client {sessionId}", clientId)
                try return! run clientId
                with e -> domainLog.Warning(e, "Test threw an exception"); e.Reraise () }
        execute
    let private createResultLog fileName = LoggerConfiguration().WriteTo.File(fileName).CreateLogger()
    let run (log : ILogger) (createStoreLog, verbose, verboseConsole) (resetStats, dumpStats) reportFilename (p : ParseResults<LoadTestParameters>) =
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
        let test, duration = a.TestKind, a.Duration
        let runSingleTest : ClientId -> Async<unit> =
            match storeConfig, httpClient with
            | None, Some client ->
                let execForClient = executeRemote client test
                decorateWithLogger (log, verbose) execForClient
            | Some storeConfig, _ ->
                let services = ServiceCollection()
                Services.register(services, storeConfig, storeLog)
                let container = services.BuildServiceProvider()
                let execForClient = executeLocal container test
                decorateWithLogger (log, verbose) execForClient
            | None, None -> invalidOp "impossible None, None"
        let clients = Array.init (a.TestsPerSecond * 2) (fun _ -> ClientId.gen())

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
