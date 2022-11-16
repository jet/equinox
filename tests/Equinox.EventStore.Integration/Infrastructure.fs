﻿namespace global

open Domain
open FsCheck
open System

type FsCheckGenerators =
    static member SkuId = Arb.generate |> Gen.map SkuId |> Arb.fromGen
    static member ContactPreferencesId =
        Arb.generate<Guid>
        |> Gen.map (fun x -> sprintf "%s@test.com" (x.ToString("N")))
        |> Gen.map ContactPreferences.ClientId
        |> Arb.fromGen

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

[<AutoOpen>]
module SerilogHelpers =
    open Serilog.Events

    let (|SerilogScalar|_|) : LogEventPropertyValue -> obj option = function
        | :? ScalarValue as x -> Some x.Value
        | _ -> None
    [<RequireQualifiedAccess>]
    type EsAct = Append | AppendConflict | SliceForward | SliceBackward | BatchForward | BatchBackward | ReadLast
    let (|EsAction|) (evt : Log.Metric) =
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
    let (|EsEvent|_|) (logEvent : LogEvent) : Log.Metric option =
        logEvent.Properties.Values |> Seq.tryPick (function
            | SerilogScalar (:? Log.Metric as e) -> Some e
            | _ -> None)

    let (|HasProp|_|) (name : string) (e : LogEvent) : LogEventPropertyValue option =
        match e.Properties.TryGetValue name with
        | true, (SerilogScalar _ as s) -> Some s
        | _ -> None
    let (|SerilogString|_|) : LogEventPropertyValue -> string option = function SerilogScalar (:? string as y) -> Some y | _ -> None
    let (|SerilogBool|_|) : LogEventPropertyValue -> bool option = function SerilogScalar (:? bool as y) -> Some y | _ -> None

type LogCapture() =
    inherit LogCaptureBuffer()
    member _.ExternalCalls = base.ChooseCalls(function EsEvent (EsAction act) -> Some act | _ -> None)

type TestContext(testOutputHelper) =
    let output = TestOutput testOutputHelper

    member x.CreateLoggerWithCapture() =
        let capture = LogCapture()
        output.CreateLogger(capture), capture
