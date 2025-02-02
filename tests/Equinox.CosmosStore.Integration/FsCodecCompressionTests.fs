// Prior to version v 4.1.0, CosmosStore owned:
// - compression of snapshots (and APIs controlling conditionally of that)
// - inflation of snapshots
// This is now an external concern, fully implemented by APIs presented in FsCodec.SystemTextJson.Encod* v 3.1.0 and later
// These tests are a sanity check pinning the basic mechanisms that are now externalized; any more thorough tests should be maintained in FsCodec
// NOTE there is no strong dependency on FsCodec; CosmosStore is happy to roundtrip arbitrary pairs of D/d and M/m values
// NOTE prior to v 4.1.0, CosmosStore provided a System.Text.Json integration for Microsoft.Azure.Cosmos
//      Version 4.1.0 and later lean on the integrated support provided from Microsoft.Azure.Cosmos v 3.43.0 onward
module Equinox.CosmosStore.Integration.FsCodecCompressionTests

open Equinox.CosmosStore
open FsCheck.Xunit
open Swensen.Unquote
open System
open Xunit

type Embedded = { embed: string }
type Union =
    | A of Embedded
    | B of Embedded
    interface TypeShape.UnionContract.IUnionContract

type CoreBehaviors() =
    let defaultOptions = System.Text.Json.JsonSerializerOptions.Default // Rule out dependencies on any FsCodec conversions
    let eventCodec = FsCodec.SystemTextJson.CodecJsonElement.Create(defaultOptions)

    let ser_ et struct (D, d) =
        let e: Core.Unfold =
            {   i = 42L
                c = et
                d = d
                D = D
                m = Unchecked.defaultof<_>
                M = Unchecked.defaultof<int>
                t = DateTimeOffset.MinValue }
        System.Text.Json.JsonSerializer.Serialize e
    let ser (e: FsCodec.IEventData<Equinox.CosmosStore.Core.EncodedBody>) = ser_ e.EventType e.Data

    [<Fact>]
    let ``serializes, achieving expected compression`` () =
        let encoded = eventCodec |> FsCodec.SystemTextJson.Encoder.Compressed |> _.Encode((), A { embed = String('x',5000) })
        let res = ser encoded
        test <@ res.Contains "\"d\":\"" && res.Length < 138 && res.Contains "\"D\":2" @>

    let codec compress =
        let forceCompression: FsCodec.CompressionOptions = { minSize = 0; minGain = -1000 }
        if compress then FsCodec.SystemTextJson.Encoder.Compressed(eventCodec, options = forceCompression)
        else FsCodec.SystemTextJson.Encoder.Uncompressed eventCodec

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
        let ser = ser_ "et" (0, System.Text.Json.JsonSerializer.SerializeToElement null)
        let des = System.Text.Json.JsonSerializer.Deserialize<Core.Unfold>(ser)
        test <@ System.Text.Json.JsonValueKind.Null = let d = des.d in d.ValueKind @>

module Pre41IntegratedDeflate =
    let private deflate (uncompressedBytes: byte[]) =
        let output = new System.IO.MemoryStream()
        let compressor = new System.IO.Compression.DeflateStream(output, System.IO.Compression.CompressionLevel.Optimal, leaveOpen = true)
        compressor.Write(uncompressedBytes)
        compressor.Flush() // Could `Close`, but not required
        output.ToArray()
    let encode (x: 't) = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes x |> deflate |> Convert.ToBase64String |> System.Text.Json.JsonSerializer.SerializeToElement

type BackCompatTests() =

    /// Validates that an Unfold with a compressed `d` value without an associated `D` value of `1` is correctly inflated via Core.EncodedBody.parseUnfold
    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ```Can inflate pre v 4.1 Deflate encoded snapshots`` () = Async.RunSynchronously <| async {
        let context = createPrimaryContext Serilog.Log.Logger 10
        let cat = Equinox.Store.Integration.DocumentStoreIntegration.Cart.createCategorySnapshot context
        let expected: Domain.Cart.Fold.State = {
            items = [
                { skuId = SkuId Guid.Empty; quantity = 4; returnsWaived = Some true } ] }
        TypeShape.Empty.register (fun () -> Unchecked.defaultof<System.Text.Json.JsonElement>)
        let unfold = {
            TypeShape.Empty.empty<Equinox.CosmosStore.Core.Unfold> with
                c = nameof Domain.Cart.Events.Event.Snapshotted
                d = Pre41IntegratedDeflate.encode expected }
        Some expected =! cat.TryLoad [| unfold |] }
