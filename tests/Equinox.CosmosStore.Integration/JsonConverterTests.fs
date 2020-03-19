module Equinox.CosmosStore.Integration.JsonConverterTests

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

    let ser eventType data =
        let e : Core.Unfold =
            {   i = 42L
                c = eventType
                d = data
                m = null
                t = DateTimeOffset.MinValue }
        JsonConvert.SerializeObject e

    [<Fact>]
    let ``serializes, achieving expected compression`` () =
        let encoded = eventCodec.Encode(None,A { embed = String('x',5000) })
        let res = ser encoded.EventType (Core.Base64MaybeDeflateUtf8JsonConverter.Compress encoded.Data)
        test <@ res.Contains("\"d\":\"") && res.Length < 138 @>

    [<Property>]
    let roundtrips compress value =
        let encoded = eventCodec.Encode(None, value)
        let maybeCompressor = if compress then Core.Base64MaybeDeflateUtf8JsonConverter.Compress else id
        let actualData = maybeCompressor encoded.Data
        let ser = ser encoded.EventType actualData
        test <@ if compress then ser.Contains("\"d\":\"")
                else ser.Contains("\"d\":{") @>
        let des = JsonConvert.DeserializeObject<Core.Unfold>(ser)
        let d = FsCodec.Core.TimelineEvent.Create(-1L, des.c, des.d)
        let decoded = eventCodec.TryDecode d |> Option.get
        test <@ value = decoded @>

    [<Theory; InlineData false; InlineData true>]
    let handlesNulls compress =
        let maybeCompressor = if compress then Core.Base64MaybeDeflateUtf8JsonConverter.Compress else id
        let maybeCompressed = maybeCompressor null
        let ser = ser "AnEventType" maybeCompressed
        let des = JsonConvert.DeserializeObject<Core.Unfold>(ser)
        test <@ null = des.d @>
