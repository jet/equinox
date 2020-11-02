[<AutoOpen>]
module Equinox.CosmosStore.Integration.Infrastructure

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
    open Equinox.CosmosStore.Core
    open Equinox.CosmosStore.Core.Log
    [<RequireQualifiedAccess>]
    type EqxAct =
        | Tip | TipNotFound | TipNotModified
        | ResponseForward | ResponseBackward
        | QueryForward | QueryBackward
        | Append | Resync | Conflict
        | PruneResponse | Delete | Trim | Prune
    let (|EqxAction|) = function
        | Event.Tip _ -> EqxAct.Tip
        | Event.TipNotFound _ -> EqxAct.TipNotFound
        | Event.TipNotModified _ -> EqxAct.TipNotModified
        | Event.Response (Direction.Forward,_) -> EqxAct.ResponseForward
        | Event.Response (Direction.Backward,_) -> EqxAct.ResponseBackward
        | Event.Query (Direction.Forward,_,_) -> EqxAct.QueryForward
        | Event.Query (Direction.Backward,_,_) -> EqxAct.QueryBackward
        | Event.SyncSuccess _ -> EqxAct.Append
        | Event.SyncResync _ -> EqxAct.Resync
        | Event.SyncConflict _ -> EqxAct.Conflict
        | Event.PruneResponse _ -> EqxAct.PruneResponse
        | Event.Delete _ -> EqxAct.Delete
        | Event.Trim _ -> EqxAct.Trim
        | Event.Prune _ -> EqxAct.Prune
    let inline (|Stats|) ({ ru = ru }: Equinox.CosmosStore.Core.Log.Measurement) = ru
    let (|CosmosReadRc|CosmosWriteRc|CosmosResyncRc|CosmosResponseRc|CosmosDeleteRc|CosmosTrimRc|CosmosPruneRc|) = function
        | Event.Tip (Stats s)
        | Event.TipNotFound (Stats s)
        | Event.TipNotModified (Stats s)
        // slices are rolled up into batches so be sure not to double-count
        | Event.PruneResponse (Stats s)
        | Event.Response (_,Stats s) -> CosmosResponseRc s
        | Event.Query (_,_, (Stats s)) -> CosmosReadRc s
        | Event.SyncSuccess (Stats s)
        | Event.SyncConflict (Stats s) -> CosmosWriteRc s
        | Event.SyncResync (Stats s) -> CosmosResyncRc s
        | Event.Delete (Stats s) -> CosmosDeleteRc s
        | Event.Trim (Stats s) -> CosmosTrimRc s
        | Event.Prune (_, (Stats s)) -> CosmosPruneRc s
    /// Facilitates splitting between events with direct charges vs synthetic events Equinox generates to avoid double counting
    let (|CosmosRequestCharge|EquinoxChargeRollup|) = function
        | CosmosResponseRc _ ->
            EquinoxChargeRollup
        | CosmosReadRc rc | CosmosWriteRc rc | CosmosResyncRc rc | CosmosDeleteRc rc | CosmosTrimRc rc | CosmosPruneRc rc as e ->
            CosmosRequestCharge (e,rc)
    let (|EqxEvent|_|) (logEvent : LogEvent) : Equinox.CosmosStore.Core.Log.Event option =
        logEvent.Properties.Values |> Seq.tryPick (function
            | SerilogScalar (:? Equinox.CosmosStore.Core.Log.Event as e) -> Some e
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
        member __.RequestCharges = __.ChooseCalls (function EqxEvent (CosmosRequestCharge e) -> Some e | _ -> None)

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
