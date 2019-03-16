namespace Equinox.Cosmos.Internal.Json

open Newtonsoft.Json.Linq
open Newtonsoft.Json
open Utf8Json

/// Manages injecting prepared json into the data being submitted to DocDb as-is, on the basis we can trust it to be valid json as DocDb will need it to be
type VerbatimUtf8JsonConverter() =
    inherit JsonConverter()
    
    static let enc = System.Text.Encoding.UTF8
    
    override __.ReadJson(reader, _, _, _) =
        let token = JToken.Load reader
        if token = null then null
        else token |> string |> enc.GetBytes |> box

    override __.CanConvert(objectType) =
        typeof<byte[]>.Equals(objectType)

    override __.WriteJson(writer, value, serializer) =
        let array = value :?> byte[]
        if array = null then serializer.Serialize(writer, null)
        else writer.WriteRawValue(enc.GetString(array))
        
    interface IJsonFormatter<byte[]> with
    
        member __.Serialize(writer, value, formatterResolver) =
            if value = null then writer.WriteNull()
            else
                writer.WriteRaw value
                printfn ""
        
        //This is very convoluted way of escaping non-escaped strings from verbatim UTF8 string as byte array
        member __.Deserialize(reader, formatterResolver) =
            if reader.ReadIsNull() then null else
                
            let startToken = reader.GetCurrentJsonToken()
            let block = reader.ReadNextBlockSegment()
            let mutable negativeOffset = 0
            
            let tokensDontMatch struct (startTok, endToken) =
                match struct (startTok, endToken) with
                | struct (JsonToken.BeginObject, JsonToken.EndObject)
                | struct (JsonToken.BeginArray, JsonToken.EndArray)
                | struct (JsonToken.String, JsonToken.String)
                | struct (JsonToken.True, JsonToken.True)
                | struct (JsonToken.False, JsonToken.False)
                | struct (JsonToken.Null, JsonToken.Null)
                | struct (JsonToken.Number, JsonToken.Number) -> false
                | _ -> true
            
            let mutable lastToken = reader.GetCurrentJsonToken()
            let mutable negativeOffset = 0
            while tokensDontMatch struct (startToken, lastToken) do
                reader.AdvanceOffset -1
                negativeOffset <- negativeOffset - 1
                lastToken <- reader.GetCurrentJsonToken()
            
            if negativeOffset <> 0 then
                negativeOffset <- negativeOffset + 1
                reader.AdvanceOffset 1
            
            let array = Array.zeroCreate (block.Count + negativeOffset)
            
            for i = block.Offset to block.Offset + block.Count - 1 + negativeOffset do
                array.[i-block.Offset] <- block.Array.[i]
            array
            
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
       