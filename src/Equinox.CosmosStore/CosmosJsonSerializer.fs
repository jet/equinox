namespace Equinox.CosmosStore.Core

open Azure.Cosmos.Serialization
open Equinox.Core
open System
open System.IO
open System.Text.Json
open System.Text.Json.Serialization

type CosmosJsonSerializer (options: JsonSerializerOptions) =
    inherit CosmosSerializer()

    override __.FromStream<'T> (stream) =
        using (stream) (fun stream ->
            if stream.Length = 0L then
                Unchecked.defaultof<'T>
            elif typeof<Stream>.IsAssignableFrom(typeof<'T>) then
                stream :> obj :?> 'T
            else
                JsonSerializer.DeserializeAsync<'T>(stream, options)
                |> Async.AwaitValueTask
                |> Async.RunSynchronously
        )

    override __.ToStream<'T> (input: 'T) =
        let memoryStream = new MemoryStream()

        JsonSerializer.SerializeAsync(memoryStream, input, input.GetType(), options)
        |> Async.AwaitTaskCorrect
        |> Async.RunSynchronously

        memoryStream.Position <- 0L
        memoryStream :> Stream

/// Manages zipping of the UTF-8 json bytes to make the index record minimal from the perspective of the writer stored proc
/// Only applied to snapshots in the Tip
and JsonCompressedBase64Converter() =
    inherit JsonConverter<JsonElement>()

    static member Compress (value: JsonElement) =
        if value.ValueKind = JsonValueKind.Null || value.ValueKind = JsonValueKind.Undefined then
            value
        else
            let input = System.Text.Encoding.UTF8.GetBytes(value.GetRawText())
            use output = new MemoryStream()
            use compressor = new System.IO.Compression.DeflateStream(output, System.IO.Compression.CompressionLevel.Optimal)
            compressor.Write(input, 0, input.Length)
            compressor.Close()
            JsonDocument.Parse("\"" + System.Convert.ToBase64String(output.ToArray()) + "\"").RootElement

    override __.Read (reader, _typeToConvert, options) =
        if reader.TokenType <> JsonTokenType.String then
            JsonSerializer.Deserialize<JsonElement>(&reader, options)
        else
            let compressedBytes = reader.GetBytesFromBase64()
            use input = new MemoryStream(compressedBytes)
            use decompressor = new System.IO.Compression.DeflateStream(input, System.IO.Compression.CompressionMode.Decompress)
            use output = new MemoryStream()
            decompressor.CopyTo(output)
            JsonSerializer.Deserialize<JsonElement>(ReadOnlySpan.op_Implicit(output.ToArray()), options)

    override __.Write (writer, value, options) =
        JsonSerializer.Serialize<JsonElement>(writer, value, options)

type JsonCompressedBase64ConverterAttribute () =
    inherit JsonConverterAttribute(typeof<JsonCompressedBase64Converter>)

    static let converter = JsonCompressedBase64Converter()

    override __.CreateConverter _typeToConvert =
        converter :> JsonConverter
