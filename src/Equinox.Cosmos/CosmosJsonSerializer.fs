namespace Equinox.Cosmos.Store

open System.IO
open System.Text.Json
open Azure.Cosmos.Serialization
open Equinox.Core

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
        async {
            let memoryStream = new MemoryStream()

            do!
                JsonSerializer.SerializeAsync(memoryStream, input, input.GetType(), options)
                |> Async.AwaitTaskCorrect

            memoryStream.Position <- 0L
            return memoryStream :> Stream
        }
        |> Async.RunSynchronously
