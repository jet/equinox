namespace Equinox.CosmosStore.Core

open System.IO
open System.Text.Json

module private Deflate =

    let compress (uncompressedBytes: byte[]) =
        let output = new MemoryStream()
        let compressor = new System.IO.Compression.DeflateStream(output, System.IO.Compression.CompressionLevel.Optimal, leaveOpen = true)
        compressor.Write(uncompressedBytes)
        compressor.Flush() // Could `Close`, but not required
        output.ToArray()

    let inflateTo output (compressedBytes: byte[]) =
        let input = new MemoryStream(compressedBytes)
        let decompressor = new System.IO.Compression.DeflateStream(input, System.IO.Compression.CompressionMode.Decompress, leaveOpen = true)
        decompressor.CopyTo(output)
    let inflate compressedBytes =
        let output = new MemoryStream()
        compressedBytes |> inflateTo output
        output.ToArray()

module JsonElement =

    let private nullElement = JsonSerializer.SerializeToElement null
    let undefinedToNull (e : JsonElement) = if e.ValueKind = JsonValueKind.Undefined then nullElement else e

    // Avoid introduction of HTML escaping for things like quotes etc (Options.Default uses Options.Create(), which defaults to unsafeRelaxedJsonEscaping=true)
    let private optionsNoEscaping = JsonSerializerOptions(Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping)
    let private toUtf8Bytes (value: JsonElement) = JsonSerializer.SerializeToUtf8Bytes(value, options = optionsNoEscaping)
    let deflate (value: JsonElement): JsonElement =
        if value.ValueKind = JsonValueKind.Null then value
        else value |> toUtf8Bytes |> Deflate.compress |> JsonSerializer.SerializeToElement
    let tryInflateTo ms (x: JsonElement) =
        if x.ValueKind <> JsonValueKind.String then false
        else x.GetBytesFromBase64() |> Deflate.inflateTo ms; true

type CosmosJsonSerializer(options : JsonSerializerOptions) =
    inherit Microsoft.Azure.Cosmos.CosmosSerializer()

    override _.FromStream<'T>(stream) =
        use _ = stream

        if stream.Length = 0L then Unchecked.defaultof<'T>
        elif typeof<Stream>.IsAssignableFrom(typeof<'T>) then box stream :?> 'T
        else JsonSerializer.Deserialize<'T>(stream, options)

    override _.ToStream<'T>(input : 'T) =
        let memoryStream = new MemoryStream()
        JsonSerializer.Serialize(memoryStream, input, input.GetType(), options)
        memoryStream.Position <- 0L
        memoryStream :> Stream

/// Manages inflating of the UTF-8 json bytes to make the index record minimal from the perspective of the writer stored proc
/// Only relevant for unfolds in the Tip
type JsonCompressedBase64Converter() =
    inherit System.Text.Json.Serialization.JsonConverter<JsonElement>()

    override _.Read(reader, _typeToConvert, options) =
        if reader.TokenType = JsonTokenType.String then // TryGetBytesFromBase64 throws on Null
            let decompressedBytes = reader.GetBytesFromBase64() |> Deflate.inflate
            JsonSerializer.Deserialize<JsonElement>(decompressedBytes, options)
        else JsonSerializer.Deserialize<JsonElement>(&reader, options)

    override _.Write(writer, value, options) =
        JsonSerializer.Serialize<JsonElement>(writer, value, options)
