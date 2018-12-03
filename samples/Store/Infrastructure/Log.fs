module Samples.Log

open Serilog
open Serilog.Events

[<AutoOpen>]
module SerilogHelpers =
    open Equinox.Cosmos.Store
    let inline (|Stats|) ({ interval = i; ru = ru }: Log.Measurement) = ru, let e = i.Elapsed in int64 e.TotalMilliseconds

    let (|CosmosReadRc|CosmosWriteRc|CosmosResyncRc|CosmosResponseRc|) = function
        | Log.Tip (Stats s)
        | Log.TipNotFound (Stats s)
        | Log.TipNotModified (Stats s)
        | Log.Query (_,_, (Stats s)) -> CosmosReadRc s
        // slices are rolled up into batches so be sure not to double-count
        | Log.Response (_,(Stats s)) -> CosmosResponseRc s
        | Log.SyncSuccess (Stats s)
        | Log.SyncConflict (Stats s) -> CosmosWriteRc s
        | Log.SyncResync (Stats s) -> CosmosResyncRc s
    let (|SerilogScalar|_|) : LogEventPropertyValue -> obj option = function
        | (:? ScalarValue as x) -> Some x.Value
        | _ -> None
    let (|CosmosMetric|_|) (logEvent : LogEvent) : Log.Event option =
        match logEvent.Properties.TryGetValue("cosmosEvt") with
        | true, SerilogScalar (:? Log.Event as e) -> Some e
        | _ -> None
    type RuCounter =
        { mutable rux100: int64; mutable count: int64; mutable ms: int64 }
        static member Create() = { rux100 = 0L; count = 0L; ms = 0L }
        member __.Ingest (ru, ms) =
            System.Threading.Interlocked.Increment(&__.count) |> ignore
            System.Threading.Interlocked.Add(&__.rux100, int64 (ru*100.)) |> ignore
            System.Threading.Interlocked.Add(&__.ms, ms) |> ignore
    type RuCounterSink() =
        static member val Read = RuCounter.Create()
        static member val Write = RuCounter.Create()
        static member val Resync = RuCounter.Create()
        interface Serilog.Core.ILogEventSink with
            member __.Emit logEvent = logEvent |> function
                | CosmosMetric (CosmosReadRc stats) -> RuCounterSink.Read.Ingest stats
                | CosmosMetric (CosmosWriteRc stats) -> RuCounterSink.Write.Ingest stats
                | CosmosMetric (CosmosResyncRc stats) -> RuCounterSink.Resync.Ingest stats
                | _ -> ()

let createDomainLog verbose verboseConsole maybeSeqEndpoint =
    let c = LoggerConfiguration().Destructure.FSharpTypes().Enrich.FromLogContext()
    let c = if verbose then c.MinimumLevel.Debug() else c
    let c = c.WriteTo.Sink(RuCounterSink())
    let c = c.WriteTo.Console((if verboseConsole then LogEventLevel.Debug else LogEventLevel.Information), theme = Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code)
    let c = match maybeSeqEndpoint with None -> c | Some endpoint -> c.WriteTo.Seq(endpoint)
    c.CreateLogger()

let createStoreLog verbose verboseConsole maybeSeqEndpoint =
    let c = LoggerConfiguration().Destructure.FSharpTypes()
    let c = if verbose then c.MinimumLevel.Debug() else c
    let c = c.WriteTo.Sink(RuCounterSink())
    let c = c.WriteTo.Console((if verbose && verboseConsole then LogEventLevel.Debug else LogEventLevel.Warning), theme = Sinks.SystemConsole.Themes.AnsiConsoleTheme.Code)
    let c = match maybeSeqEndpoint with None -> c | Some endpoint -> c.WriteTo.Seq(endpoint)
    c.CreateLogger() :> ILogger