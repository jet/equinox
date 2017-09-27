[<AutoOpen>]
module Samples.Store.Domain.Tests.Infrastructure

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

let rnd = new Random()
// https://www.rosettacode.org/wiki/Knuth_shuffle#F.23
let knuthShuffle (array : 'a []) =
    let swap i j =
        let item = array.[i]
        array.[i] <- array.[j]
        array.[j] <- item
    let ln = array.Length
    for i in 0.. (ln - 2) do        // For all indices except the last
        swap i (rnd.Next(i, ln))    // swap th item at the index with a random one following it (or itself)
    array

module IdTypes =
    [<Fact>]
    let ``CartId has structural equality semantics`` () =
        let x = Guid.NewGuid()
        test <@ CartId x = CartId x @>

    [<Fact>]
    let ``ClientId has structural equality semantics`` () =
        let x = Guid.NewGuid()
        test <@ ClientId x = ClientId x @>

    [<Fact>]
    let ``RequestId has structural equality semantics`` () =
        let x = Guid.NewGuid()
        test <@ RequestId x = RequestId x @>

    [<Fact>]
    let ``SkuId has structural equality semantics`` () =
        let x = Guid.NewGuid()
        test <@ SkuId x = SkuId x @> 