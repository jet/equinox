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
    let createLogger hookObservers =
        LoggerConfiguration()
            .WriteTo.Observers(System.Action<_> hookObservers)
            .Destructure.AsScalar<Foldunk.EventStore.Metrics.Metric>()
            .CreateLogger()

    let (|SerilogScalar|_|) : Serilog.Events.LogEventPropertyValue -> obj option = function
        | (:? Serilog.Events.ScalarValue as x) -> Some x.Value
        | _ -> None
    let (|EsMetric|_|) (logEvent : Serilog.Events.LogEvent) : Foldunk.EventStore.Metrics.Metric option =
        logEvent.Properties.Values |> Seq.tryPick (function
            | (SerilogScalar (:? Foldunk.EventStore.Metrics.Metric as x)) -> Some x
            | _ -> None)

    type LogCaptureBuffer() =
        let captured = ResizeArray()
        member __.Subscribe(source: IObservable<Serilog.Events.LogEvent>) =
            source.Subscribe (fun x -> x.RenderMessage () |> System.Diagnostics.Trace.Write; captured.Add x)
        member __.Clear () = captured.Clear()
        member __.Entries = captured.ToArray()
        member __.ExternalCalls =
            captured |> Seq.choose (function EsMetric metric -> Some metric.action | _ -> None) |> List.ofSeq