module Equinox.CosmosStore.Integration.JsonConverterTests

open Equinox.CosmosStore
open Equinox.CosmosStore.Core
open FsCheck.Xunit
open Swensen.Unquote
open System
open System.Text.Json

type Embedded = { embed : string }
type Union =
    | A of Embedded
    | B of Embedded
    interface TypeShape.UnionContract.IUnionContract

let defaultOptions = FsCodec.SystemTextJson.Options.Create()

module JsonElement =
    let d = JsonDocument.Parse "null"
    let Null = d.RootElement

type Base64ZipUtf8Tests() =
    let eventCodec = FsCodec.SystemTextJson.Codec.Create<Union>(defaultOptions)

    [<Property>]
    let ``Can read uncompressed and compressed`` compress value =
        let encoded = eventCodec.Encode(None,value)
        let compressor = if compress then JsonCompressedBase64Converter.Compress else id
        let e : Core.Unfold =
            {   i = 42L
                c = encoded.EventType
                d = encoded.Data |> JsonHelper.fixup |> compressor
                m = Unchecked.defaultof<_> |> JsonHelper.fixup
                t = DateTimeOffset.MinValue }
        let ser = FsCodec.SystemTextJson.Serdes.Serialize(e, defaultOptions)
        System.Diagnostics.Trace.WriteLine ser
        let des = FsCodec.SystemTextJson.Serdes.Deserialize<Core.Unfold>(ser, defaultOptions)
        let d = FsCodec.Core.TimelineEvent.Create(-1L, des.c, des.d)
        let decoded = eventCodec.TryDecode d |> Option.get
        test <@ value = decoded @>
