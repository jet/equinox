namespace FsCodec.SystemTextJson.Serialization

open System.Text.Json

[<AutoOpen>]
module JsonSerializerOptionExtensions =
    type JsonSerializerOptions with
        static member Create() =
            let options = JsonSerializerOptions()
            options.Converters.Add(new JsonRecordConverter())
            options

module JsonSerializer =
    let defaultOptions = JsonSerializerOptions.Create()
