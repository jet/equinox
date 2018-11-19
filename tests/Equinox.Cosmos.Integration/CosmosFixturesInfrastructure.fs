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
    open Serilog
    open Serilog.Events

    let createLogger sink =
        LoggerConfiguration()
            .WriteTo.Sink(sink)
            .CreateLogger()

    let (|SerilogScalar|_|) : Serilog.Events.LogEventPropertyValue -> obj option = function
        | (:? ScalarValue as x) -> Some x.Value
        | _ -> None
    [<RequireQualifiedAccess>]
    type EqxAct = Append | Resync | Conflict | SliceForward | SliceBackward | BatchForward | BatchBackward | Indexed | IndexedNotFound | IndexedCached
    let (|EqxAction|) (evt : Equinox.Cosmos.Log.Event) =
        match evt with
        | Equinox.Cosmos.Log.WriteSuccess _ -> EqxAct.Append
        | Equinox.Cosmos.Log.WriteResync _ -> EqxAct.Resync
        | Equinox.Cosmos.Log.WriteConflict _ -> EqxAct.Conflict
        | Equinox.Cosmos.Log.Slice (Equinox.Cosmos.Store.Direction.Forward,_) -> EqxAct.SliceForward
        | Equinox.Cosmos.Log.Slice (Equinox.Cosmos.Store.Direction.Backward,_) -> EqxAct.SliceBackward
        | Equinox.Cosmos.Log.Batch (Equinox.Cosmos.Store.Direction.Forward,_,_) -> EqxAct.BatchForward
        | Equinox.Cosmos.Log.Batch (Equinox.Cosmos.Store.Direction.Backward,_,_) -> EqxAct.BatchBackward
        | Equinox.Cosmos.Log.Index _ -> EqxAct.Indexed
        | Equinox.Cosmos.Log.IndexNotFound _ -> EqxAct.IndexedNotFound
        | Equinox.Cosmos.Log.IndexNotModified _ -> EqxAct.IndexedCached
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
        member __.Entries = captured.ToArray()
        member __.ChooseCalls chooser = captured |> Seq.choose chooser |> List.ofSeq
        member __.ExternalCalls = __.ChooseCalls (function EqxEvent (EqxAction act) -> Some act | _ -> None)

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