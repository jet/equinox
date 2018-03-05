[<AutoOpen>]
module Foldunk.EventStore.Integration.Infrastructure

open Domain
open FsCheck
open System

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

[<AutoOpen>]
module SerilogHelpers =
    open Serilog
    open Serilog.Events

    let createLogger hookObservers =
        LoggerConfiguration()
            .WriteTo.Observers(System.Action<_> hookObservers)
            .CreateLogger()

    let (|SerilogScalar|_|) : Serilog.Events.LogEventPropertyValue -> obj option = function
        | (:? ScalarValue as x) -> Some x.Value
        | _ -> None
    [<RequireQualifiedAccess>]
    type EsAct = Append | AppendConflict | SliceForward | SliceBackward | BatchForward | BatchBackward
    let (|EsAction|) (evt : Foldunk.EventStore.Log.Event) =
        match evt with
        | Foldunk.EventStore.Log.WriteSuccess _ -> EsAct.Append
        | Foldunk.EventStore.Log.WriteConflict _ -> EsAct.AppendConflict
        | Foldunk.EventStore.Log.Slice (Foldunk.EventStore.Direction.Forward,_) -> EsAct.SliceForward
        | Foldunk.EventStore.Log.Slice (Foldunk.EventStore.Direction.Backward,_) -> EsAct.SliceBackward
        | Foldunk.EventStore.Log.Batch (Foldunk.EventStore.Direction.Forward,_,_) -> EsAct.BatchForward
        | Foldunk.EventStore.Log.Batch (Foldunk.EventStore.Direction.Backward,_,_) -> EsAct.BatchBackward
    let (|EsEvent|_|) (logEvent : LogEvent) : Foldunk.EventStore.Log.Event option =
        logEvent.Properties.Values |> Seq.tryPick (function
            | SerilogScalar (:? Foldunk.EventStore.Log.Event as e) -> Some e
            | _ -> None)

    let (|HasProp|_|) (name : string) (e : LogEvent) : LogEventPropertyValue option =
        match e.Properties.TryGetValue name with
        | true, (SerilogScalar _ as s) -> Some s | _ -> None
        | _ -> None
    let (|SerilogString|_|) : LogEventPropertyValue -> string option = function SerilogScalar (:? string as y) -> Some y | _ -> None
    let (|SerilogBool|_|) : LogEventPropertyValue -> bool option = function SerilogScalar (:? bool as y) -> Some y | _ -> None

    type LogCaptureBuffer() =
        let captured = ResizeArray()
        member __.Subscribe(source: IObservable<Serilog.Events.LogEvent>) =
            source.Subscribe (fun x -> x.RenderMessage () |> System.Diagnostics.Trace.WriteLine; captured.Add x)
        member __.Clear () = captured.Clear()
        member __.Entries = captured.ToArray()
        member __.ChooseCalls chooser = captured |> Seq.choose chooser |> List.ofSeq
        member __.ExternalCalls = __.ChooseCalls (function EsEvent (EsAction act) -> Some act | _ -> None)