[<AutoOpen>]
module Equinox.Cosmos.Integration.Infrastructure

open Domain
open FsCheck
open Serilog
open System
open Serilog.Core

type FsCheckGenerators =
    static member SkuId = Arb.generate |> Gen.map SkuId |> Arb.fromGen
    static member ContactPreferencesId =
        Arb.generate<Guid>
        |> Gen.map (fun x -> sprintf "%s@test.com" (x.ToString("N")))
        |> Gen.map ContactPreferences.Id
        |> Arb.fromGen

type AutoDataAttribute() =
    inherit FsCheck.Xunit.PropertyAttribute(Arbitrary = [|typeof<FsCheckGenerators>|], MaxTest = 1, QuietOnSuccess = true)

    member val SkipIfRequestedViaEnvironmentVariable : string = null with get, set

    override __.Skip =
        match Option.ofObj __.SkipIfRequestedViaEnvironmentVariable |> Option.map Environment.GetEnvironmentVariable |> Option.bind Option.ofObj with
        | Some value when value.Equals(bool.TrueString, StringComparison.OrdinalIgnoreCase) ->
            sprintf "Skipped as requested via %s" __.SkipIfRequestedViaEnvironmentVariable
        | _ -> null

// Derived from https://github.com/damianh/CapturingLogOutputWithXunit2AndParallelTests
// NB VS does not surface these atm, but other test runners / test reports do
type TestOutputAdapter(testOutput : Xunit.Abstractions.ITestOutputHelper) =
    let formatter = Serilog.Formatting.Display.MessageTemplateTextFormatter("{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level}] {Message}{NewLine}{Properties}{Exception}", null);
    let writeSerilogEvent logEvent =
        use writer = new System.IO.StringWriter()
        formatter.Format(logEvent, writer)
        writer |> string |> testOutput.WriteLine
    interface Serilog.Core.ILogEventSink with member __.Emit logEvent = writeSerilogEvent logEvent

[<AutoOpen>]
module SerilogHelpers =
    open Serilog.Events

    let createLogger sink =
        LoggerConfiguration()
            .WriteTo.Sink(sink)
            .WriteTo.Seq("http://localhost:5341")
            .CreateLogger()

    let (|SerilogScalar|_|) : Serilog.Events.LogEventPropertyValue -> obj option = function
        | (:? ScalarValue as x) -> Some x.Value
        | _ -> None
    open Equinox.Cosmos.Store
    open Equinox.Cosmos.Store.Log
    [<RequireQualifiedAccess>]
    type EqxAct =
        | Tip | TipNotFound | TipNotModified
        | ResponseForward | ResponseBackward
        | QueryForward | QueryBackward
        | Append | Resync | Conflict
        | PruneResponse | Delete | Prune
    let (|EqxAction|) = function
        | Metric.Tip _ -> EqxAct.Tip
        | Metric.TipNotFound _ -> EqxAct.TipNotFound
        | Metric.TipNotModified _ -> EqxAct.TipNotModified

        | Metric.Query (Direction.Forward, _, _) -> EqxAct.QueryForward
        | Metric.Query (Direction.Backward, _, _) -> EqxAct.QueryBackward
        | Metric.QueryResponse (Direction.Forward, _) -> EqxAct.ResponseForward
        | Metric.QueryResponse (Direction.Backward, _) -> EqxAct.ResponseBackward

        | Metric.SyncSuccess _ -> EqxAct.Append
        | Metric.SyncResync _ -> EqxAct.Resync
        | Metric.SyncConflict _ -> EqxAct.Conflict

        | Metric.Prune _ -> EqxAct.Prune
        | Metric.PruneResponse _ -> EqxAct.PruneResponse
        | Metric.Delete _ -> EqxAct.Delete
    let (|Load|Write|Resync|Prune|Delete|Response|) = function
        | Metric.Tip s
        | Metric.TipNotFound s
        | Metric.TipNotModified s

        | Metric.Query (_, _, s) -> Load s
        | Metric.QueryResponse (_, s) -> Response s

        | Metric.SyncSuccess s
        | Metric.SyncConflict s -> Write s
        | Metric.SyncResync s -> Resync s

        | Metric.Prune (_, s) -> Prune s
        | Metric.PruneResponse s -> Response s
        | Metric.Delete s -> Delete s
    let inline (|Rc|) ({ ru = ru }: Equinox.Cosmos.Store.Log.Measurement) = ru
    /// Facilitates splitting between events with direct charges vs synthetic events Equinox generates to avoid double counting
    let (|TotalRequestCharge|ResponseBreakdown|) = function
        | Load (Rc rc) | Write (Rc rc) | Resync (Rc rc) | Delete (Rc rc) | Prune (Rc rc) as e -> TotalRequestCharge (e, rc)
        | Response _ -> ResponseBreakdown
    let (|EqxEvent|_|) (logEvent : LogEvent) : Equinox.Cosmos.Store.Log.Metric option =
        logEvent.Properties.Values |> Seq.tryPick (function
            | SerilogScalar (:? Equinox.Cosmos.Store.Log.Metric as e) -> Some e
            | _ -> None)

    let (|HasProp|_|) (name : string) (e : LogEvent) : LogEventPropertyValue option =
        match e.Properties.TryGetValue name with
        | true, (SerilogScalar _ as s) -> Some s | _ -> None
        | _ -> None
    let (|SerilogString|_|) : LogEventPropertyValue -> string option = function SerilogScalar (:? string as y) -> Some y | _ -> None
    let (|SerilogBool|_|) : LogEventPropertyValue -> bool option = function SerilogScalar (:? bool as y) -> Some y | _ -> None

    type LogCaptureBuffer() =
        let captured = ResizeArray()
        let writeSerilogEvent (logEvent: LogEvent) =
            logEvent.RenderMessage () |> System.Diagnostics.Trace.WriteLine
            captured.Add logEvent
        interface Serilog.Core.ILogEventSink with member __.Emit logEvent = writeSerilogEvent logEvent
        member __.Clear () = captured.Clear()
        member __.ChooseCalls chooser = captured |> Seq.choose chooser |> List.ofSeq
        member __.ExternalCalls = __.ChooseCalls (function EqxEvent (EqxAction act) -> Some act | _ -> None)
        member __.RequestCharges = __.ChooseCalls (function EqxEvent (TotalRequestCharge e) -> Some e | _ -> None)

type TestsWithLogCapture(testOutputHelper) =
    let log, capture = TestsWithLogCapture.CreateLoggerWithCapture testOutputHelper

    /// NB the returned Logger must be Dispose()'d to guarantee all log output has been flushed upon completion of a test
    static member CreateLoggerWithCapture testOutputHelper : Logger* LogCaptureBuffer =
        let testOutput = TestOutputAdapter testOutputHelper
        let capture = LogCaptureBuffer()
        let logger =
            Serilog.LoggerConfiguration()
                .MinimumLevel.Debug()
                .WriteTo.Seq("http://localhost:5341")
                .WriteTo.Sink(testOutput)
                .WriteTo.Sink(capture)
                .CreateLogger()
        logger, capture

    member __.Capture = capture
    member __.Log = log

    interface IDisposable with member __.Dispose() = log.Dispose()
