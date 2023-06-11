namespace global

open Domain
open FsCheck.FSharp
open System

module Arb =
    let generate<'t> = ArbMap.defaults |> ArbMap.generate<'t>

type FsCheckGenerators =
    static member SkuId = Arb.generate |> Gen.map SkuId |> Arb.fromGen
    static member ContactPreferencesId =
        Arb.generate<Guid>
        |> Gen.map (fun x -> sprintf "%s@test.com" (x.ToString("N")))
        |> Gen.map ContactPreferences.ClientId
        |> Arb.fromGen
