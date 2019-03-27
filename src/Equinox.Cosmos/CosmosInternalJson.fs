namespace Equinox.Cosmos.Internal.Json

open Newtonsoft.Json.Linq
open Newtonsoft.Json

/// Manages injecting prepared json into the data being submitted to DocDb as-is, on the basis we can trust it to be valid json as DocDb will need it to be
type VerbatimUtf8JsonConverter() =
    inherit JsonConverter()
    
    static let enc = System.Text.Encoding.UTF8
    
    override __.ReadJson(reader, _, _, _) =
        let token = JToken.Load reader
        if token.Type = JTokenType.Null then null
        else token |> string |> enc.GetBytes |> box

    override __.CanConvert(objectType) =
        typeof<byte[]>.Equals(objectType)

    override __.WriteJson(writer, value, serializer) =
        let array = value :?> byte[]
        if array = null || array.Length = 0 then serializer.Serialize(writer, null)
        else writer.WriteRawValue(enc.GetString(array))

open System.IO
open System.IO.Compression

/// Manages zipping of the UTF-8 json bytes to make the index record minimal from the perspective of the writer stored proc
/// Only applied to snapshots in the Index
type Base64ZipUtf8JsonConverter() =
    inherit JsonConverter()
    let pickle (input : byte[]) : string =
        if input = null then null else

        use output = new MemoryStream()
        use compressor = new DeflateStream(output, CompressionLevel.Optimal)
        compressor.Write(input,0,input.Length)
        compressor.Close()
        System.Convert.ToBase64String(output.ToArray())
    let unpickle str : byte[] =
        if str = null then null else

        let compressedBytes = System.Convert.FromBase64String str
        use input = new MemoryStream(compressedBytes)
        use decompressor = new DeflateStream(input, CompressionMode.Decompress)
        use output = new MemoryStream()
        decompressor.CopyTo(output)
        output.ToArray()

    override __.CanConvert(objectType) =
        typeof<byte[]>.Equals(objectType)
    override __.ReadJson(reader, _, _, serializer) =
        //(   if reader.TokenType = JsonToken.Null then null else
        serializer.Deserialize(reader, typedefof<string>) :?> string |> unpickle |> box
    override __.WriteJson(writer, value, serializer) =
        let pickled = value |> unbox |> pickle
        serializer.Serialize(writer, pickled)