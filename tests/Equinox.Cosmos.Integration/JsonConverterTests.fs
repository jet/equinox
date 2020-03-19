module Equinox.Cosmos.Integration.JsonConverterTests

open Equinox.Cosmos
open Equinox.Cosmos.Store
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

    [<Property>]
    let ``Can read uncompressed and compressed`` compress value =
        let hasNulls =
            match value with
            | A x | B x when obj.ReferenceEquals(null, x) -> true
            | A { embed = x } | B { embed = x } -> obj.ReferenceEquals(null, x)
        if hasNulls then () else

        let encoded = eventCodec.Encode(None,value)
        let compressor = if compress then JsonCompressedBase64Converter.Compress else id
        let e : Store.Unfold =
            {   i = 42L
                c = encoded.EventType
                d = compressor encoded.Data
                m = Unchecked.defaultof<JsonElement>
                t = DateTimeOffset.MinValue }
        let ser = FsCodec.SystemTextJson.Serdes.Serialize(e, defaultOptions)
        let des = FsCodec.SystemTextJson.Serdes.Deserialize<Store.Unfold>(ser, defaultOptions)
        let d = FsCodec.Core.TimelineEvent.Create(-1L, des.c, des.d)
        let decoded = eventCodec.TryDecode d |> Option.get
        test <@ value = decoded @>
