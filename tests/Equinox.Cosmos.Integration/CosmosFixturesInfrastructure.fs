﻿[<AutoOpen>]
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
    type EqxAct = Append | Resync | Conflict | SliceForward | SliceBackward | BatchForward | BatchBackward | Index | IndexNotFound | IndexNotModified | SliceWaste
    let (|EqxAction|) (evt : Equinox.Cosmos.Log.Event) =
        match evt with
        | Log.WriteSuccess _ -> EqxAct.Append
        | Log.WriteResync _ -> EqxAct.Resync
        | Log.WriteConflict _ -> EqxAct.Conflict
        | Log.Slice (Direction.Forward,{count = 0}) -> EqxAct.SliceWaste // TODO remove, see comment where these are emitted
        | Log.Slice (Direction.Forward,_) -> EqxAct.SliceForward
        | Log.Slice (Direction.Backward,{count = 0}) -> EqxAct.SliceWaste // TODO remove, see comment where these are emitted
        | Log.Slice (Direction.Backward,_) -> EqxAct.SliceBackward
        | Log.Batch (Direction.Forward,_,_) -> EqxAct.BatchForward
        | Log.Batch (Direction.Backward,_,_) -> EqxAct.BatchBackward
        | Log.Index _ -> EqxAct.Index
        | Log.IndexNotFound _ -> EqxAct.IndexNotFound
        | Log.IndexNotModified _ -> EqxAct.IndexNotModified
    let inline (|Stats|) ({ ru = ru }: Equinox.Cosmos.Log.Measurement) = ru
    let (|CosmosReadRu|CosmosWriteRu|CosmosResyncRu|CosmosSliceRu|) (evt : Equinox.Cosmos.Log.Event) =
        match evt with
        | Log.Index (Stats s)
        | Log.IndexNotFound (Stats s)
        | Log.IndexNotModified (Stats s)
        | Log.Batch (_,_, (Stats s)) -> CosmosReadRu s
        | Log.WriteSuccess (Stats s)
        | Log.WriteConflict (Stats s) -> CosmosWriteRu s
        | Log.WriteResync (Stats s) -> CosmosResyncRu s
        // slices are rolled up into batches so be sure not to double-count
        | Log.Slice (_,Stats s) -> CosmosSliceRu s
    /// Facilitates splitting between events with direct charges vs synthetic events Equinox generates to avoid double counting
    let (|CosmosRequestCharge|EquinoxChargeRollup|) c =
        match c with
        | CosmosSliceRu _ ->
            EquinoxChargeRollup
        | CosmosReadRu rc | CosmosWriteRu rc | CosmosResyncRu rc as e ->
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