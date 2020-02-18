namespace Equinox.Cosmos.Store

open System.IO;
open System.Text;
open Azure.Cosmos.Serialization;
open Newtonsoft.Json;
open Newtonsoft.Json.Serialization;

type NewtonsoftJsonSerializer () =
    inherit CosmosSerializer ()

    let encoding = new UTF8Encoding(false, true)
    let serializer = JsonSerializer.Create()

    override __.FromStream<'T> (stream: Stream): 'T =
        use stream = stream

        if typeof<Stream>.IsAssignableFrom(typeof<'T>) then
            stream :> obj :?> 'T
        else
            use streamReader = new StreamReader(stream)
            use jsonReader = new JsonTextReader(streamReader)
            serializer.Deserialize<'T>(jsonReader)

    override __.ToStream<'T> (input: 'T): Stream =
        let payload = new MemoryStream()

        (
            use streamWriter = new StreamWriter(payload, encoding = encoding, bufferSize = 1024, leaveOpen = true)
            use jsonWriter = new JsonTextWriter(streamWriter)

            jsonWriter.Formatting <- Formatting.None
            serializer.Serialize(jsonWriter, input)
            jsonWriter.Flush()
            streamWriter.Flush()
        )

        payload.Position <- 0L
        payload :> Stream
