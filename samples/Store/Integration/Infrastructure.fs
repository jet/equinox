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
