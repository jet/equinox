[<AutoOpen>]
module Foldunk.EventStore.Integration.Infrastructure

open Serilog

open Domain
open FsCheck
open Serilog
open System

type FsCheckGenerators =
    //static member CartId = Arb.generate |> Gen.map CartId |> Arb.fromGen
    static member SkuId = Arb.generate |> Gen.map SkuId |> Arb.fromGen
    //static member RequestId = Arb.generate |> Gen.map RequestId |> Arb.fromGen

type AutoDataAttribute() =
    inherit FsCheck.Xunit.PropertyAttribute(Arbitrary = [|typeof<FsCheckGenerators>|], MaxTest = 1, QuietOnSuccess = true)

let createLogger hookObservers =
    LoggerConfiguration()
        .WriteTo.Observers(System.Action<_> hookObservers)
        .CreateLogger()