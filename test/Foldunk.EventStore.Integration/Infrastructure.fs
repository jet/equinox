[<AutoOpen>]
module Foldunk.EventStore.Integration.Infrastructure

open Serilog

open Domain
open FsCheck
open Serilog
open System

type FsCheckGenerators =
    static member SkuId = Arb.generate |> Gen.map SkuId |> Arb.fromGen
    static member RequestId = Arb.generate |> Gen.map RequestId |> Arb.fromGen

type AutoDataAttribute() =
    inherit FsCheck.Xunit.PropertyAttribute(Arbitrary = [|typeof<FsCheckGenerators>|], MaxTest = 1, QuietOnSuccess = true)

let createLogger hookObservers =
    LoggerConfiguration()
        .WriteTo.Observers(System.Action<_> hookObservers)
        .CreateLogger()

type LogCaptureBuffer() =
    let captured = ResizeArray()
    member __.Subscribe(source: IObservable<Serilog.Events.LogEvent>) =
        source.Subscribe captured.Add
    member __.Clear () = captured.Clear()
    member __.Entries = captured.ToArray()
    member __.ExternalCalls =
        [ for i in captured do
            let hasProp name = i.Properties.ContainsKey name 
            let prop name = (string i.Properties.[name]).Trim '"' 
            if hasProp "ExternalCall" && prop "ExternalCall" = "True" then
                yield prop "Action" ]