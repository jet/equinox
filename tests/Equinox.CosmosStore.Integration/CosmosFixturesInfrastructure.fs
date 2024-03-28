namespace global

open Domain
open FsCheck.FSharp
open System

module ArbMap =
    let defGen<'t> = ArbMap.defaults |> ArbMap.generate<'t>

type FsCheckGenerators =
    static member SkuId = ArbMap.defGen |> Gen.map SkuId |> Arb.fromGen
    static member ContactPreferencesId =
        ArbMap.defGen<Guid>
        |> Gen.map (fun x -> sprintf "%s@test.com" (x.ToString("N")))
        |> Gen.map ContactPreferences.ClientId
        |> Arb.fromGen

[<AutoOpen>]
module SerilogHelpers =
    open Serilog.Events

    let (|SerilogScalar|_|): LogEventPropertyValue -> obj option = function
        | :? ScalarValue as x -> Some x.Value
        | _ -> None
#if STORE_DYNAMO
    open Equinox.DynamoStore.Core
    open Equinox.DynamoStore.Core.Log
#else
    open Equinox.CosmosStore.Core
    open Equinox.CosmosStore.Core.Log
#endif
    [<RequireQualifiedAccess>]
    type EqxAct =
        | Tip | TipNotFound | TipNotModified
        | ResponseForward | ResponseBackward
        | Index
        | QueryForward | QueryBackward
        | Append | Resync | Conflict
        | PruneResponse | Delete | Trim | Prune
    let (|EqxAction|) = function
        | Metric.Tip _ -> EqxAct.Tip
        | Metric.TipNotFound _ -> EqxAct.TipNotFound
        | Metric.TipNotModified _ -> EqxAct.TipNotModified

        | Metric.Query (Direction.Forward, _, _) -> EqxAct.QueryForward
        | Metric.Query (Direction.Backward, _, _) -> EqxAct.QueryBackward
        | Metric.QueryResponse (Direction.Forward, _) -> EqxAct.ResponseForward
        | Metric.QueryResponse (Direction.Backward, _) -> EqxAct.ResponseBackward

#if STORE_DYNAMO
        | Metric.SyncAppend _ | Metric.SyncCalve _ -> EqxAct.Append
        | Metric.SyncAppendConflict _ | Metric.SyncCalveConflict _ -> EqxAct.Conflict
#else
        | Metric.Index _ -> EqxAct.Index
        | Metric.SyncSuccess _ -> EqxAct.Append
        | Metric.SyncResync _ -> EqxAct.Resync
        | Metric.SyncConflict _ -> EqxAct.Conflict
#endif

        | Metric.Prune _ -> EqxAct.Prune
        | Metric.PruneResponse _ -> EqxAct.PruneResponse
        | Metric.Delete _ -> EqxAct.Delete
        | Metric.Trim _ -> EqxAct.Trim
#if !STORE_DYNAMO
    let (|Load|Write|Resync|Prune|Delete|Trim|Response|) = function
#else
    let (|Load|Write|Prune|Delete|Trim|Response|) = function
#endif
        | Metric.Tip s
        | Metric.TipNotFound s
        | Metric.TipNotModified s

        | Metric.Query (_, _, s) -> Load s
        | Metric.QueryResponse (_, s) -> Response s

#if STORE_DYNAMO
        | Metric.SyncAppend s | Metric.SyncCalve s
        | Metric.SyncCalveConflict s | Metric.SyncAppendConflict s -> Write s
#else
        | Metric.Index s -> Response s // Stubbed out for now
        | Metric.SyncSuccess s
        | Metric.SyncConflict s -> Write s
        | Metric.SyncResync s -> Resync s
#endif

        | Metric.Prune (_, s) -> Prune s
        | Metric.PruneResponse s -> Response s
        | Metric.Delete s -> Delete s
        | Metric.Trim s -> Trim s

    let inline (|Rc|) ({ ru = ru }: Measurement) = ru
    /// Facilitates splitting between events with direct charges vs synthetic events Equinox generates to avoid double counting
    let (|TotalRequestCharge|ResponseBreakdown|) = function
        | Load (Rc rc) | Write (Rc rc)
#if !STORE_DYNAMO
        | Resync (Rc rc)
#endif
        | Delete (Rc rc) | Trim (Rc rc) | Prune (Rc rc) as e -> TotalRequestCharge (e, rc)
        | Response _ -> ResponseBreakdown
    let (|EqxEvent|_|) (logEvent: LogEvent): Metric option =
        logEvent.Properties.Values |> Seq.tryPick (function
            | SerilogScalar (:? Metric as e) -> Some e
            | _ -> None)

    let (|HasProp|_|) (name: string) (e: LogEvent): LogEventPropertyValue option =
        match e.Properties.TryGetValue name with
        | true, (SerilogScalar _ as s) -> Some s
        | _ -> None
    let (|SerilogString|_|): LogEventPropertyValue -> string option = function SerilogScalar (:? string as y) -> Some y | _ -> None
    let (|SerilogBool|_|): LogEventPropertyValue -> bool option = function SerilogScalar (:? bool as y) -> Some y | _ -> None

type LogCapture() =
    inherit LogCaptureBuffer()
    member _.ExternalCalls = base.ChooseCalls (function EqxEvent (EqxAction act) -> Some act | _ -> None)
    member _.RequestCharges = base.ChooseCalls (function EqxEvent (TotalRequestCharge e) -> Some e | _ -> None)

type TestContext(testOutputHelper) =
    let output = TestOutput testOutputHelper

    member _.CreateLoggerWithCapture(): Serilog.Core.Logger * LogCapture =
        let capture = LogCapture()
        output.CreateLogger(capture), capture
