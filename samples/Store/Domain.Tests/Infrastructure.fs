[<AutoOpen>]
module Samples.Store.Domain.Tests.Infrastructure

open Domain
open FsCheck
open FSharp.UMX
open Swensen.Unquote
open System
open global.Xunit

type FsCheckGenerators =
    static member SkuId = Arb.generate |> Gen.map SkuId |> Arb.fromGen

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
    let ``CartId has structural equality and Guid rendering semantics`` () =
        let x = Guid.NewGuid() in let xs, xn = x.ToString(), x.ToString "N"
        let (x1 : CartId, x2 : CartId) = %x, %x
        test <@ x1 = x2
                && xn = CartId.toString x2
                && string x1 = xs @>

    [<Fact>]
    let ``ClientId has structural equality and Guid rendering semantics`` () =
        let x = Guid.NewGuid() in let xs, xn = x.ToString(), x.ToString "N"
        let (x1 : ClientId, x2 : ClientId) = %x, %x
        test <@ x1 = x2
                && xn = ClientId.toString x2
                && string x1 = xs @>

    [<Fact>]
    let ``RequestId has structural equality and canonical rendering semantics`` () =
        let x = Guid.NewGuid() in let xn = Guid.toStringN x
        let (x1 : RequestId, x2 : RequestId) = RequestId.parse %x, RequestId.parse %x
        test <@ x1 = x2
                && string x1 = xn @>

    [<Fact>]
    let ``SkuId has structural equality and canonical rendering semantics`` () =
        let x = Guid.NewGuid()
        let xn = Guid.toStringN x
        let (x1 : SkuId, x2 : SkuId) = SkuId x, SkuId x
        test <@ x1 = x2
                && string x1 = xn @>