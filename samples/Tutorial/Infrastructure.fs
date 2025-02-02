namespace global

open FSharp.UMX // see https://github.com/fsprojects/FSharp.UMX - % operator and ability to apply units of measure to Guids+strings

type SequenceId = string<sequenceId>
and [<Measure>] sequenceId
module SequenceId =
    let toString (value : SequenceId) : string = %value

type SetId = string<setId>
and [<Measure>] setId
module SetId =
    let parse (value : string) : SetId = %value
    let toString (value : SetId) : string = %value

type IndexId = string<indexId>
and [<Measure>] indexId
module IndexId =
    let parse (value : string) : IndexId = %value
    let toString (value : IndexId) : string = %value

module EventCodec =

    /// For CosmosStore - we encode to JsonElement as that's what the store talks
    let genJsonElement<'t when 't :> TypeShape.UnionContract.IUnionContract> =
        FsCodec.SystemTextJson.CodecJsonElement.Create<'t>() |> FsCodec.SystemTextJson.Encoder.Uncompressed

    /// For stores other than CosmosStore, we encode to UTF-8 and have the store do the right thing
    let gen<'t when 't :> TypeShape.UnionContract.IUnionContract> =
        FsCodec.SystemTextJson.Codec.Create<'t>()
