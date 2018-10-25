﻿module Equinox.Cosmos.Integration.VerbatimUtf8JsonConverterTests

open Newtonsoft.Json
open Swensen.Unquote
open System
open Xunit

let serializer = new JsonSerializer()
let inline serialize (x:'t) =
    use sw = new System.IO.StringWriter()
    use w = new JsonTextWriter(sw)
    serializer.Serialize(w,x)
    sw.ToString()

type Embedded = { embed : string }
type Union =
    | A of Embedded
    | B of Embedded
    interface TypeShape.UnionContract.IUnionContract

[<Fact>]
let ``VerbatimUtf8JsonConverter serializes properly`` () =
    let unionEncoder = Equinox.UnionCodec.JsonUtf8.Create<_>(JsonSerializerSettings())
    let encoded = unionEncoder.Encode(A { embed = "\"" })
    let e : Equinox.Cosmos.EquinoxEvent =
        {   id = null
            s = null
            k  = null
            df = null
            mdf = null
            ts = DateTimeOffset.MinValue
            sn = 0L

            et = encoded.caseName
            d = encoded.payload

            md = null }
    let res = serialize e
    test <@ res.Contains """"d":{"embed":"\""}""" @>