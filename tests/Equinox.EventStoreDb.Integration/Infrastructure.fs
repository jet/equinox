namespace global

open Domain
open FsCheck.FSharp
open System
open System.Diagnostics
open FSharp.UMX

module ArbMap =
    let defGen<'t> = ArbMap.defaults |> ArbMap.generate<'t>

type FsCheckGenerators =
    static member SkuId = ArbMap.defGen |> Gen.map SkuId |> Arb.fromGen
    static member ContactPreferencesId =
        ArbMap.defGen<Guid>
        |> Gen.map (fun x -> sprintf "%s@test.com" (x.ToString("N")))
        |> Gen.map ContactPreferences.ClientId
        |> Arb.fromGen
    static member RequestId = ArbMap.defGen<Guid> |> Gen.map (fun x -> RequestId.parse %x) |> Arb.fromGen

#if STORE_POSTGRES || STORE_MSSQL || STORE_MYSQL
open Equinox.SqlStreamStore
#endif
#if STORE_MESSAGEDB
open Equinox.MessageDb
#endif
#if STORE_EVENTSTOREDB
open Equinox.EventStoreDb
#endif
#if STORE_EVENTSTORE_LEGACY
open Equinox.EventStore
#endif

[<RequireQualifiedAccess>]
type EsAct = Append | AppendConflict | SliceForward | SliceBackward | BatchForward | BatchBackward | ReadLast

[<AutoOpen>]
module OtelHelpers =
    let (|EsOp|_|) (act: Activity): EsAct option =
        match act.OperationName with
        | "Append" -> Some EsAct.Append
        | "AppendConflict" -> Some EsAct.AppendConflict
        | "SliceForward" -> Some EsAct.SliceForward
        | "SliceBackward" -> Some EsAct.SliceBackward
        | "BatchForward" -> Some EsAct.BatchForward
        | "BatchBackward" -> Some EsAct.BatchBackward
        | "ReadLast" -> Some EsAct.ReadLast
        | _ -> None

[<AutoOpen>]
module SerilogHelpers =
    open Serilog.Events

    let (|SerilogScalar|_|): LogEventPropertyValue -> obj option = function
        | :? ScalarValue as x -> Some x.Value
        | _ -> None
    let (|EsAction|) (evt: Log.Metric) =
        match evt with
        | Log.WriteSuccess _ -> EsAct.Append
        | Log.WriteConflict _ -> EsAct.AppendConflict
#if !STORE_EVENTSTOREDB && !STORE_MESSAGEDB // For gRPC, no slice information is available
        | Log.Slice (Direction.Forward,_) -> EsAct.SliceForward
        | Log.Slice (Direction.Backward,_) -> EsAct.SliceBackward
#endif
#if STORE_MESSAGEDB
        | Log.Slice _ -> EsAct.SliceForward
        | Log.Batch _ -> EsAct.BatchForward
        | Log.ReadLast _ -> EsAct.ReadLast
#else
        | Log.Batch (Direction.Forward,_,_) -> EsAct.BatchForward
        | Log.Batch (Direction.Backward,_,_) -> EsAct.BatchBackward
#endif
    let (|EsEvent|_|) (logEvent: LogEvent): Log.Metric option =
        logEvent.Properties.Values |> Seq.tryPick (function
            | SerilogScalar (:? Log.Metric as e) -> Some e
            | _ -> None)

    let (|HasProp|_|) (name: string) (e: LogEvent): LogEventPropertyValue option =
        match e.Properties.TryGetValue name with
        | true, (SerilogScalar _ as s) -> Some s
        | _ -> None
    let (|SerilogString|_|): LogEventPropertyValue -> string option = function SerilogScalar (:? string as y) -> Some y | _ -> None
    let (|SerilogBool|_|): LogEventPropertyValue -> bool option = function SerilogScalar (:? bool as y) -> Some y | _ -> None

/// Captures store-specific Activity emissions for test assertions, using AsyncLocal for per-test isolation
type StoreActivityCapture() =
    static let storeSourceName =
#if STORE_EVENTSTOREDB
        "Equinox.EventStoreDb"
#endif
#if STORE_EVENTSTORE_LEGACY
        "Equinox.EventStore"
#endif
#if STORE_MESSAGEDB
        "Equinox.MessageDb"
#endif
#if STORE_POSTGRES || STORE_MSSQL || STORE_MYSQL
        "Equinox.SqlStreamStore"
#endif
    static let currentOps = new System.Threading.AsyncLocal<ResizeArray<string>>()
    static let gate = obj()
    static let _listener =
        let l = new ActivityListener(
            ShouldListenTo = (fun source -> source.Name = storeSourceName),
            Sample = (fun _ -> ActivitySamplingResult.AllDataAndRecorded),
            ActivityStopped = (fun act ->
                let ops = currentOps.Value
                if ops <> null then lock gate (fun () -> ops.Add(act.OperationName))))
        ActivitySource.AddActivityListener(l)
        l
    let ops = ResizeArray<string>()
    do
        ignore _listener
        currentOps.Value <- ops
    member _.Clear() = lock gate (fun () -> ops.Clear())
    member _.ExternalCalls =
        lock gate (fun () -> ops |> Seq.toList)
        |> List.choose (fun name ->
            match name with
            | "Append" -> Some EsAct.Append
            | "AppendConflict" -> Some EsAct.AppendConflict
            | "SliceForward" -> Some EsAct.SliceForward
            | "SliceBackward" -> Some EsAct.SliceBackward
            | "BatchForward" -> Some EsAct.BatchForward
            | "BatchBackward" -> Some EsAct.BatchBackward
            | "ReadLast" -> Some EsAct.ReadLast
            | _ -> None)
    interface IDisposable with member _.Dispose() = currentOps.Value <- null

type LogCapture() =
    inherit LogCaptureBuffer()
    member _.ExternalCalls = base.ChooseCalls(function EsEvent (EsAction act) -> Some act | _ -> None)

type TestContext(testOutputHelper) =
    let output = TestOutput testOutputHelper

    member _.CreateLogger() = output.CreateLogger()

    member _.CreateLoggerWithCapture() =
        let capture = new StoreActivityCapture()
        output.CreateLogger(), capture

    member _.CreateLoggerWithDualCapture() =
        let serilogCapture = LogCapture()
        let otelCapture = new StoreActivityCapture()
        output.CreateLogger(serilogCapture), otelCapture, serilogCapture
