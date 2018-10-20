namespace Equinox.UnionCodec

open Newtonsoft.Json
open System.IO
open TypeShape

module private Impl =
    /// Newtonsoft.Json implementation of IEncoder that encodes direct to a UTF-8 Buffer
    type JsonUtf8Encoder(settings : JsonSerializerSettings) =
        let serializer = JsonSerializer.Create(settings)
        interface UnionContract.IEncoder<byte[]> with
            member __.Empty = Unchecked.defaultof<_>
            member __.Encode (value : 'T) =
                use ms = new MemoryStream()
                (   use jsonWriter = new JsonTextWriter(new StreamWriter(ms))
                    serializer.Serialize(jsonWriter, value, typeof<'T>))
                ms.ToArray()
            member __.Decode(json : byte[]) =
                use ms = new MemoryStream(json)
                use jsonReader = new JsonTextReader(new StreamReader(ms))
                serializer.Deserialize<'T>(jsonReader)

/// Provides a Codec that infers Event Type Names based on either explicit `DataMember(Name=` Attributes, or (if unspecified) the Case Name
/// Event bodies are rendered to a UTF-8 array buffer using `Newtonsoft.Json`, with the supplied <c>settings</c>.
/// The Union must be tagged with `interface TypeShape.UnionContract.IUnionContract` to signify this scheme applies.
/// See https://github.com/eiriktsarpalis/TypeShape/blob/master/tests/TypeShape.Tests/UnionContractTests.fs for example usage.
type JsonUtf8UnionCodec =

    /// <summary>
    ///    Generate a codec suitable for use with <c>Equinox.EventStore</c> or <c>Equinox.Cosmos</c>,
    ///    using the supplied <c>settings</c> when encoding. </summary>
    /// <param name="settings">Configuration to be used by the underlying <c>Newtonsoft.Json</c> Serializer when encoding/decoding.</param>
    /// <param name="requireRecordFields">Fail encoder generation if union cases contain fields that are not F# records. Defaults to <c>false</c>.</param>
    /// <param name="allowNullaryCases">Fail encoder generation if union contains nullary cases. Defaults to <c>true</c>.</param>
    static member Create<'Union when 'Union :> UnionContract.IUnionContract>(settings, ?requireRecordFields, ?allowNullaryCases)
        : UnionContract.UnionContractEncoder<'Union,byte[]> =
        UnionContract.UnionContractEncoder.Create<'Union,byte[]>(
            new Impl.JsonUtf8Encoder(settings),
            ?requireRecordFields=requireRecordFields,
            ?allowNullaryCases=allowNullaryCases)