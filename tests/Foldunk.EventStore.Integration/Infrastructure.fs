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
            .Destructure.AsScalar<Foldunk.EventStore.Metrics.Metric>()
            .CreateLogger()

    let (|SerilogScalar|_|) : Serilog.Events.LogEventPropertyValue -> obj option = function
        | (:? ScalarValue as x) -> Some x.Value
        | _ -> None
    let (|EsMetric|_|) (logEvent : LogEvent) : Foldunk.EventStore.Metrics.Metric option =
        logEvent.Properties.Values |> Seq.tryPick (function
            | SerilogScalar (:? Foldunk.EventStore.Metrics.Metric as x) -> Some x
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
        member __.ExternalCalls = __.ChooseCalls (function EsMetric metric -> Some metric.action | _ -> None)