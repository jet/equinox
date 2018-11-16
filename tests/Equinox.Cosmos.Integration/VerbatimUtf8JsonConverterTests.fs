﻿module Equinox.Cosmos.Integration.VerbatimUtf8JsonConverterTests

open Equinox.Cosmos
open FsCheck.Xunit
open Newtonsoft.Json
open Swensen.Unquote
open System
open Xunit

type Embedded = { embed : string }
type Union =
    | A of Embedded
    | B of Embedded
    interface TypeShape.UnionContract.IUnionContract

let mkUnionEncoder () = Equinox.UnionCodec.JsonUtf8.Create<Union>(JsonSerializerSettings())

[<Fact>]
let ``VerbatimUtf8JsonConverter encodes correctly`` () =
    let encoded = mkUnionEncoder().Encode(A { embed = "\"" })
    let e : Store.Event =
        {   p = "streamName"; id = string 0; i = 0L
            c = DateTimeOffset.MinValue
            t = encoded.caseName
            d = encoded.payload
            m = null }
    let res = JsonConvert.SerializeObject(e)
    test <@ res.Contains """"d":{"embed":"\""}""" @>

type Base64ZipUtf8JsonConverterTests() =
    let unionEncoder = mkUnionEncoder ()

    [<Fact>]
    let ``serializes, achieving compression`` () =
        let encoded = unionEncoder.Encode(A { embed = String('x',5000) })
        let e : Store.IndexProjection =
            {   t = encoded.caseName
                d = encoded.payload
                m = null }
        let res = JsonConvert.SerializeObject e
        test <@ res.Contains("\"d\":\"") && res.Length < 100 @>

    [<Property>]
    let roundtrips value =
        let hasNulls =
            match value with
            | A x | B x when obj.ReferenceEquals(null, x) -> true
            | A { embed = x } | B { embed = x } -> obj.ReferenceEquals(null, x)
        if hasNulls then () else

        let encoded = unionEncoder.Encode value
        let e : Store.IndexProjection =
            {   t = encoded.caseName
                d = encoded.payload
                m = null }
        let ser = JsonConvert.SerializeObject(e)
        test <@ ser.Contains("\"d\":\"") @>
        let des = JsonConvert.DeserializeObject<Store.IndexProjection>(ser)
        let d : Equinox.UnionCodec.EncodedUnion<_> = { caseName = des.t; payload=des.d }
        let decoded = unionEncoder.Decode d
        test <@ value = decoded @>