﻿module Equinox.Cosmos.Integration.JsonConverterTests

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

let defaultSettings = FsCodec.NewtonsoftJson.Settings.CreateDefault()

type Base64ZipUtf8Tests() =
    let unionEncoder = FsCodec.NewtonsoftJson.Codec.Create(defaultSettings)

    [<Fact>]
    let ``serializes, achieving compression`` () =
        let encoded = unionEncoder.Encode(A { embed = String('x',5000) })
        let e : Store.Unfold =
            {   i = 42L
                c = encoded.EventType
                d = encoded.Data
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
        let e : Store.Unfold =
            {   i = 42L
                c = encoded.EventType
                d = encoded.Data
                m = null }
        let ser = JsonConvert.SerializeObject(e)
        test <@ ser.Contains("\"d\":\"") @>
        let des = JsonConvert.DeserializeObject<Store.Unfold>(ser)
        let d = FsCodec.Core.TimelineEvent.Create(-1L, des.c, des.d)
        let decoded = unionEncoder.TryDecode d |> Option.get
        test <@ value = decoded @>