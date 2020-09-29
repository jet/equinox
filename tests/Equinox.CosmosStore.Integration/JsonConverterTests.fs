﻿module Equinox.CosmosStore.Integration.JsonConverterTests

open Equinox.CosmosStore
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
    let eventCodec = FsCodec.NewtonsoftJson.Codec.Create(defaultSettings)

    [<Fact>]
    let ``serializes, achieving compression`` () =
        let encoded = eventCodec.Encode(None,A { embed = String('x',5000) })
        let e : Core.Unfold =
            {   i = 42L
                c = encoded.EventType
                d = encoded.Data
                m = null
                t = DateTimeOffset.MinValue }
        let res = JsonConvert.SerializeObject e
        test <@ res.Contains("\"d\":\"") && res.Length < 128 @>

    [<Property>]
    let roundtrips value =
        let encoded = eventCodec.Encode(None,value)
        let e : Core.Unfold =
            {   i = 42L
                c = encoded.EventType
                d = encoded.Data
                m = null
                t = DateTimeOffset.MinValue }
        let ser = JsonConvert.SerializeObject(e)
        test <@ ser.Contains("\"d\":\"") @>
        System.Diagnostics.Trace.WriteLine ser
        let des = JsonConvert.DeserializeObject<Core.Unfold>(ser)
        let d = FsCodec.Core.TimelineEvent.Create(-1L, des.c, des.d)
        let decoded = eventCodec.TryDecode d |> Option.get
        test <@ value = decoded @>
