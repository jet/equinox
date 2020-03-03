namespace FsCodec.SystemTextJson.Serialization

open System.Text.Json
open System.Runtime.CompilerServices

[<Extension>]
type Utf8JsonReaderExtension =
    [<Extension>]
    static member ValidateTokenType(reader: Utf8JsonReader, expectedTokenType) =
        if reader.TokenType <> expectedTokenType then
            sprintf "Expected a %A token, but encountered a %A token when parsing JSON." expectedTokenType (reader.TokenType)
            |> JsonException
            |> raise

    [<Extension>]
    static member ValidatePropertyName(reader: Utf8JsonReader, expectedPropertyName: string) =
        reader.ValidateTokenType(JsonTokenType.PropertyName)
        
        if not <| reader.ValueTextEquals expectedPropertyName then
            sprintf "Expected a property named '%s', but encountered property with name '%s'." expectedPropertyName (reader.GetString())
            |> JsonException
            |> raise
