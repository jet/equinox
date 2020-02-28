namespace FsCodec.SystemTextJson

open System
open System.Buffers
open System.Runtime.InteropServices
open System.Text.Json

[<AutoOpen>]
module JsonSerializerExtensions =
    type JsonSerializer with
        static member SerializeToElement(value: 'T, [<Optional; DefaultParameterValue(null)>] ?options: JsonSerializerOptions) =
            JsonSerializer.Deserialize<JsonElement>(ReadOnlySpan.op_Implicit(JsonSerializer.SerializeToUtf8Bytes(value, defaultArg options null)))

        static member DeserializeElement<'T>(element: JsonElement, [<Optional; DefaultParameterValue(null)>] ?options: JsonSerializerOptions) =
#if NETSTANDARD2_1
            let bufferWriter = ArrayBufferWriter<byte>()
            (
                use jsonWriter = new Utf8JsonWriter(bufferWriter)
                element.WriteTo(jsonWriter)
            )
            JsonSerializer.Deserialize<'T>(bufferWriter.WrittenSpan, defaultArg options null)
#else
            let json = element.GetRawText()
            JsonSerializer.Deserialize<'T>(json, defaultArg options null)
#endif
