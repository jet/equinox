[<AutoOpen>]
module Foldunk.EventStore.Integration.Infrastructure

open Domain
open FsCheck
open System

type FsCheckGenerators =
    static member SkuId = Arb.generate |> Gen.map SkuId |> Arb.fromGen
    static member RequestId = Arb.generate |> Gen.map RequestId |> Arb.fromGen

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

    let (|SerilogProperty|) name (logEvent : Serilog.Events.LogEvent) : Serilog.Events.LogEventPropertyValue option =
        match logEvent.Properties.TryGetValue name with
        | true, value -> Some value
        | false, _ -> None
    let (|SerilogScalar|_|) : Serilog.Events.LogEventPropertyValue -> obj option = function
        | (:? Serilog.Events.ScalarValue as x) -> Some x.Value
        | _ -> None

    type LogCaptureBuffer() =
        let captured = ResizeArray()
        member __.Subscribe(source: IObservable<Serilog.Events.LogEvent>) =
            source.Subscribe captured.Add
        member __.Clear () = captured.Clear()
        member __.Entries = captured.ToArray()
        member __.ExternalCalls =
            captured
            |> Seq.choose (function
                | SerilogProperty Foldunk.EventStore.Metrics.ExternalTag
                    (Some (SerilogScalar (:? Foldunk.EventStore.Metrics.Metric as metric))) -> Some metric.action
                | _ -> None)
            |> List.ofSeq