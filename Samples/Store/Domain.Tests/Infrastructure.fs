[<AutoOpen>]
module Domain.Tests.Infrastructure

open Domain
open FsCheck
open Swensen.Unquote
open System
open global.Xunit

type FsCheckGenerators =
    static member SkuId = Arb.generate |> Gen.map SkuId |> Arb.fromGen
    static member RequestId = Arb.generate |> Gen.map RequestId |> Arb.fromGen

type PropertyAttribute() =
    inherit FsCheck.Xunit.PropertyAttribute(QuietOnSuccess = true, Arbitrary=[| typeof<FsCheckGenerators> |])

module IdTypes =
    [<Fact>]
    let ``CartId has structural equality semantics`` () =
        let x = Guid.NewGuid()
        test <@ CartId x = CartId x @> 

    [<Fact>]
    let ``RequestId has structural equality semantics`` () =
        let x = Guid.NewGuid()
        test <@ RequestId x = RequestId x @> 

    [<Fact>]
    let ``SkuId has structural equality semantics`` () =
        let x = Guid.NewGuid()
        test <@ SkuId x = SkuId x @> 