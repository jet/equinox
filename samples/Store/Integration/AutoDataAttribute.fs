namespace global

open System

open Domain
open FsCheck.FSharp
open FSharp.UMX

module Arb =
    let generate<'t> = ArbMap.defaults |> ArbMap.generate<'t>
type FsCheckGenerators =
    static member SkuId = Arb.generate |> Gen.map SkuId |> Arb.fromGen
    static member ContactPreferencesId =
        Arb.generate<Guid>
        |> Gen.map (fun x -> sprintf "%s@test.com" (x.ToString("N")))
        |> Gen.map ContactPreferences.ClientId
        |> Arb.fromGen
    static member RequestId = Arb.generate<Guid> |> Gen.map (fun x -> RequestId.parse %x) |> Arb.fromGen


type AutoDataAttribute() =
    inherit FsCheck.Xunit.PropertyAttribute(Arbitrary = [|typeof<FsCheckGenerators>|], MaxTest = 1, QuietOnSuccess = true)

    member val SkipIfRequestedViaEnvironmentVariable: string = null with get, set

    member x.SkipRequested =
        match Option.ofObj x.SkipIfRequestedViaEnvironmentVariable |> Option.map Environment.GetEnvironmentVariable |> Option.bind Option.ofObj with
        | Some value when value.Equals(bool.TrueString, StringComparison.OrdinalIgnoreCase) -> true
        | _ -> false
    override x.Skip = if x.SkipRequested then $"Skipped as requested via %s{x.SkipIfRequestedViaEnvironmentVariable}" else null
