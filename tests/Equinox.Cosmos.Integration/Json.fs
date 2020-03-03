[<AutoOpen>]
module Equinox.Cosmos.Integration.Json

open System
open System.Text.Json
open System.Text.Json.Serialization
open Domain
open FsCodec.SystemTextJson
open FsCodec.SystemTextJson.Serialization

type JsonSkuIdConverter () =
    inherit JsonConverter<SkuId>()

    override __.Read (reader, _typ, _options) =
        reader.GetString() |> Guid.Parse |> SkuId

    override __.Write (writer, value, _options) =
        writer.WriteStringValue(string value)

module IntegrationJsonSerializer =
    let options = JsonSerializer.defaultOptions
    options.Converters.Add(JsonSkuIdConverter())

    let serialize (value: 'T) = JsonSerializer.Serialize(value, options)
    let serializeToElement (value: 'T) = JsonSerializer.SerializeToElement(value, options)
    let deserialize<'T> (json: string) = JsonSerializer.Deserialize<'T>(json, options)
    let deserializeElement<'T> (jsonElement: JsonElement) = JsonSerializer.DeserializeElement<'T>(jsonElement, options)
