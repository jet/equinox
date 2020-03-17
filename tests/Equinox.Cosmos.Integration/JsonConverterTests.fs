module Equinox.Cosmos.Integration.JsonConverterTests

open Equinox.Cosmos
open FsCheck.Xunit
open Swensen.Unquote
open System
open System.Text.Json
open Xunit

type Embedded = { embed : string }
type Union =
    | A of Embedded
    | B of Embedded
    interface TypeShape.UnionContract.IUnionContract

let defaultOptions = FsCodec.SystemTextJson.Options.Create()

type Base64ZipUtf8Tests() =
    let eventCodec = FsCodec.SystemTextJson.Codec.Create<Union>(defaultOptions)

    [<Fact>]
    let ``serializes, achieving compression`` () =
        let encoded = eventCodec.Encode(None,A { embed = String('x',5000) })
        let e : Store.Unfold =
            {   i = 42L
                c = encoded.EventType
                d = encoded.Data
                m = Unchecked.defaultof<JsonElement>
                t = DateTimeOffset.MinValue }
        let res = FsCodec.SystemTextJson.Serdes.Serialize(e, defaultOptions)
        test <@ res.Contains("\"d\":\"") && res.Length < 138 @>

    [<Property>]
    let roundtrips value =
        let hasNulls =
            match value with
            | A x | B x when obj.ReferenceEquals(null, x) -> true
            | A { embed = x } | B { embed = x } -> obj.ReferenceEquals(null, x)
        if hasNulls then () else

        let encoded = eventCodec.Encode(None,value)
        let e : Store.Unfold =
            {   i = 42L
                c = encoded.EventType
                d = encoded.Data
                m = Unchecked.defaultof<JsonElement>
                t = DateTimeOffset.MinValue }
        let ser = FsCodec.SystemTextJson.Serdes.Serialize(e, defaultOptions)
        test <@ ser.Contains("\"d\":\"") @>
        let des = FsCodec.SystemTextJson.Serdes.Deserialize<Store.Unfold>(ser, defaultOptions)
        let d = FsCodec.Core.TimelineEvent.Create(-1L, des.c, des.d)
        let decoded = eventCodec.TryDecode d |> Option.get
        test <@ value = decoded @>
