[<AutoOpen>]
module Equinox.Cosmos.Integration.Json

open System
open System.Text.Json.Serialization
open Domain

type JsonSkuIdConverter() =
    inherit JsonConverter<SkuId>()

    override __.Read (reader, _typ, _options) =
        reader.GetString() |> Guid.Parse |> SkuId

    override __.Write (writer, value, _options) =
        writer.WriteStringValue(string value)

module IntegrationJsonSerializer =
    let options = FsCodec.SystemTextJson.Options.Create()
    options.Converters.Add <| JsonSkuIdConverter()
