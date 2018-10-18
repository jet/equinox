module Equinox.UnionCodec

open Newtonsoft.Json
open System.IO
open TypeShape

/// Newtonsoft.Json implementation of IEncoder that encodes direct to a UTF-8 Buffer
type JsonUtf8Encoder(settings : JsonSerializerSettings) =
    let serializer = JsonSerializer.Create(settings)
    interface UnionEncoder.IEncoder<byte[]> with
        member __.Encode (value : 'T) =
            use ms = new MemoryStream()
            (   use jsonWriter = new JsonTextWriter(new StreamWriter(ms))
                serializer.Serialize(jsonWriter, value, typeof<'T>))
            ms.ToArray()
        member __.Decode(json : byte[]) =
            use ms = new MemoryStream(json)
            use jsonReader = new JsonTextReader(new StreamReader(ms))
            serializer.Deserialize<'T>(jsonReader)

/// Generates an event sum encoder using Newtonsoft.Json for individual event types that serializes to a UTF-8 array buffer
let generateJsonUtf8UnionCodec<'Union>(settings) =
    UnionEncoder.UnionEncoder<'Union>.Create(new JsonUtf8Encoder(settings))