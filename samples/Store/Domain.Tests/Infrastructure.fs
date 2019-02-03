[<AutoOpen>]
module Samples.Store.Domain.Tests.Infrastructure

open Domain
open FSharp.UMX
open FsCheck
open Swensen.Unquote
open System
open global.Xunit

type FsCheckGenerators =
    static member SkuId = Arb.generate |> Gen.map SkuId |> Arb.fromGen
    static member RequestId = Arb.generate |> Gen.map RequestId |> Arb.fromGen

type DomainPropertyAttribute() =
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
        let (x1 : CartId, x2 : CartId) = %x, %x
        test <@ x1 = x2 @>

    [<Fact>]
    let ``ClientId has structural equality semantics`` () =
        let x = Guid.NewGuid()
        let (x1 : ClientId, x2 : ClientId) = %x, %x
        test <@ x1 = x2 @>

    [<Fact>]
    let ``RequestId has structural equality semantics`` () =
        let x = Guid.NewGuid()
        let (x1 : RequestId, x2 : RequestId) = RequestId %x, RequestId %x
        test <@ x1 = x2 @>

    [<Fact>]
    let ``SkuId has structural equality semantics`` () =
        let x = Guid.NewGuid()
        test <@ SkuId x = SkuId x @> 