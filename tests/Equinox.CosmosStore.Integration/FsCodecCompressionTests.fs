// Prior to version v 4.1.0, CosmosStore owned:
// - compression of snapshots (and APIs controlling conditionally of that)
// - inflation of snapshots
// This is now an external concern, fully implemented by APIs presented in FsCodec.SystemTextJson.Compression V 3.1.0 and later
// These tests are a sanity check pinning the basic mechanisms that are now externalized; any more thorough tests should be maintained in FsCodec
// NOTE there is no strong dependency on FsCodec; CosmosStore is happy to roundtrip arbitrary pairs of D/d and M/m values
//      However, it is recommended to use that implementation as it provides for interop with (Deflate-compressed) snapshots as written by CosmosStore pre 4.1.0
// NOTE prior to v 4.1.0, CosmosStore provided a System.Text.Json integration for Microsoft.Azure.Cosmos
//      Version 4.1.0 and later lean on the integrated support provided from Microsoft.Azure.Cosmos v 3.43.0 onward
module Equinox.CosmosStore.Integration.FsCodecCompressionTests

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

type CoreBehaviors() =
    let defaultOptions = System.Text.Json.JsonSerializerOptions.Default // Rule out dependencies on any FsCodec conversions
    let eventCodec = FsCodec.SystemTextJson.CodecJsonElement.Create(defaultOptions)
    let nullElement = System.Text.Json.JsonSerializer.SerializeToElement null

    let ser_ et struct (D, d) =
        let e: Core.Unfold =
            {   i = 42L
                c = et
                d = d
                D = D
                m = nullElement
                M = Unchecked.defaultof<int>
                t = DateTimeOffset.MinValue }
        System.Text.Json.JsonSerializer.Serialize e
    let ser (e: FsCodec.IEventData<Equinox.CosmosStore.Core.EncodedBody>) = ser_ e.EventType e.Data

    [<Fact>]
    let ``serializes, achieving expected compression`` () =
        let encoded = eventCodec |> FsCodec.SystemTextJson.Compression.EncodeTryCompress |> _.Encode((), A { embed = String('x',5000) })
        let res = ser encoded
        test <@ res.Contains "\"d\":\"" && res.Length < 138 && res.Contains "\"D\":2" @>

    let codec compress =
        let forceCompression: FsCodec.SystemTextJson.CompressionOptions = { minSize = 0; minGain = -1000 }
        if compress then FsCodec.SystemTextJson.Compression.EncodeTryCompress(eventCodec, forceCompression)
        else FsCodec.SystemTextJson.Compression.EncodeUncompressed eventCodec

    [<Property>]
    let roundtrips compress value =
        let codec = codec compress
        let encoded = codec.Encode((), value)
        let ser = ser encoded
        test <@ if compress then ser.Contains("\"d\":\"")
                else ser.Contains("\"d\":{") @>
        let des = System.Text.Json.JsonSerializer.Deserialize<Core.Unfold>(ser)
        let d = FsCodec.Core.TimelineEvent.Create(-1L, des.c, struct (des.D, des.d))
        let decoded = codec.Decode d |> ValueOption.get
        test <@ value = decoded @>

    [<Fact>]
    let handlesNulls () =
        let ser = ser_ "et" (0, nullElement)
        let des = System.Text.Json.JsonSerializer.Deserialize<Core.Unfold>(ser)
        test <@ System.Text.Json.JsonValueKind.Null = let d = des.d in d.ValueKind @>
