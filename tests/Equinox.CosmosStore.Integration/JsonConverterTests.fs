module Equinox.CosmosStore.Integration.JsonConverterTests

open Equinox.CosmosStore
open FsCheck.Xunit
open Swensen.Unquote
open System
open Xunit

type Embedded = { embed : string }
type Union =
    | A of Embedded
    | B of Embedded
    interface TypeShape.UnionContract.IUnionContract

type Base64ZipUtf8Tests() =
    let defaultOptions = FsCodec.SystemTextJson.Options.CreateDefault() // TODO replace with System.Text.Json.JsonSerializerOptions.Default
    let eventCodec = FsCodec.SystemTextJson.CodecJsonElement.Create(defaultOptions)
    let nullElement = System.Text.Json.JsonSerializer.SerializeToElement null

    let ser eventType data =
        let e : Core.Unfold =
            {   i = 42L
                c = eventType
                d = data
                m = nullElement
                t = DateTimeOffset.MinValue }
        System.Text.Json.JsonSerializer.Serialize e

    [<Fact>]
    let ``serializes, achieving expected compression`` () =
        let encoded = eventCodec.Encode((), A { embed = String('x',5000) })
        let res = ser encoded.EventType (Core.JsonElement.deflate encoded.Data)
        test <@ res.Contains("\"d\":\"") && res.Length < 138 @>

    [<Property>]
    let roundtrips compress value =
        let encoded = eventCodec.Encode((), value)
        let maybeDeflate = if compress then Core.JsonElement.deflate else id
        let actualData = maybeDeflate encoded.Data
        let ser = ser encoded.EventType actualData
        test <@ if compress then ser.Contains("\"d\":\"")
                else ser.Contains("\"d\":{") @>
        let des = System.Text.Json.JsonSerializer.Deserialize<Core.Unfold>(ser)
        let d = FsCodec.Core.TimelineEvent.Create(-1L, des.c, des.d)
        let decoded = eventCodec.TryDecode d |> ValueOption.get
        test <@ value = decoded @>

    [<Theory; InlineData false; InlineData true>]
    let handlesNulls compress =
        let maybeDeflate = if compress then Core.JsonElement.deflate else id
        let maybeCompressed = maybeDeflate nullElement
        let ser = ser "AnEventType" maybeCompressed
        let des = System.Text.Json.JsonSerializer.Deserialize<Core.Unfold>(ser)
        test <@ System.Text.Json.JsonValueKind.Null = let d = des.d in d.ValueKind @>
