[<AutoOpen>]
module Equinox.Cosmos.Integration.Infrastructure

open Domain
open FsCheck
open Serilog
open System
open Serilog.Core

type FsCheckGenerators =
    static member SkuId = Arb.generate |> Gen.map SkuId |> Arb.fromGen
    static member RequestId = Arb.generate |> Gen.map RequestId |> Arb.fromGen
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
    let formatter = Serilog.Formatting.Display.MessageTemplateTextFormatter("{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level}] {Message}{NewLine}{Exception}", null);
    let writeSerilogEvent logEvent =
        use writer = new System.IO.StringWriter()
        formatter.Format(logEvent, writer);
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
    open Equinox.Cosmos
    [<RequireQualifiedAccess>]
    type EqxAct =
        | Tip | TipNotFound | TipNotModified
        | ResponseForward | ResponseBackward | ResponseWaste
        | QueryForward | QueryBackward
        | Append | Resync | Conflict
    let (|EqxAction|) (evt : Equinox.Cosmos.Log.Event) =
        match evt with
        | Log.Tip _ -> EqxAct.Tip
        | Log.TipNotFound _ -> EqxAct.TipNotFound
        | Log.TipNotModified _ -> EqxAct.TipNotModified
        | Log.Response (Direction.Forward, {count = 0}) -> EqxAct.ResponseWaste // TODO remove, see comment where these are emitted
        | Log.Response (Direction.Forward,_) -> EqxAct.ResponseForward
        | Log.Response (Direction.Backward, {count = 0}) -> EqxAct.ResponseWaste // TODO remove, see comment where these are emitted
        | Log.Response (Direction.Backward,_) -> EqxAct.ResponseBackward
        | Log.Query (Direction.Forward,_,_) -> EqxAct.QueryForward
        | Log.Query (Direction.Backward,_,_) -> EqxAct.QueryBackward
        | Log.SyncSuccess _ -> EqxAct.Append
        | Log.SyncResync _ -> EqxAct.Resync
        | Log.SyncConflict _ -> EqxAct.Conflict
    let inline (|Stats|) ({ ru = ru }: Equinox.Cosmos.Log.Measurement) = ru
    let (|CosmosReadRc|CosmosWriteRc|CosmosResyncRc|CosmosResponseRc|) = function
        | Log.Tip (Stats s)
        | Log.TipNotFound (Stats s)
        | Log.TipNotModified (Stats s)
        // slices are rolled up into batches so be sure not to double-count
        | Log.Response (_,Stats s) -> CosmosResponseRc s
        | Log.Query (_,_, (Stats s)) -> CosmosReadRc s
        | Log.SyncSuccess (Stats s)
        | Log.SyncConflict (Stats s) -> CosmosWriteRc s
        | Log.SyncResync (Stats s) -> CosmosResyncRc s
    /// Facilitates splitting between events with direct charges vs synthetic events Equinox generates to avoid double counting
    let (|CosmosRequestCharge|EquinoxChargeRollup|) = function
        | CosmosResponseRc _ ->
            EquinoxChargeRollup
        | CosmosReadRc rc | CosmosWriteRc rc | CosmosResyncRc rc as e ->
            CosmosRequestCharge (e,rc)
    let (|EqxEvent|_|) (logEvent : LogEvent) : Equinox.Cosmos.Log.Event option =
        logEvent.Properties.Values |> Seq.tryPick (function
            | SerilogScalar (:? Equinox.Cosmos.Log.Event as e) -> Some e
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
        member __.ExternalCalls = __.ChooseCalls (function EqxEvent (EqxAction act) (*when act <> EqxAct.Waste*) -> Some act | _ -> None)
        member __.RequestCharges = __.ChooseCalls (function EqxEvent (CosmosRequestCharge e) -> Some e | _ -> None)

type TestsWithLogCapture(testOutputHelper) =
    let log, capture = TestsWithLogCapture.CreateLoggerWithCapture testOutputHelper

    /// NB the returned Logger must be Dispose()'d to guarantee all log output has been flushed upon completion of a test
    static member CreateLoggerWithCapture testOutputHelper : Logger* LogCaptureBuffer =
        let testOutput = TestOutputAdapter testOutputHelper
        let capture = LogCaptureBuffer()
        let logger =
            Serilog.LoggerConfiguration()
                .WriteTo.Seq("http://localhost:5341")
                .WriteTo.Sink(testOutput)
                .WriteTo.Sink(capture)
                .CreateLogger()
        logger, capture

    member __.Capture = capture
    member __.Log = log

    interface IDisposable with member __.Dispose() = log.Dispose()