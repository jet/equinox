namespace Equinox.UnionCodec

open Newtonsoft.Json
open System.IO
open TypeShape

/// Represents an encoded union case
type EncodedUnion<'Encoding> =
    {
        caseName : string
        payload  : 'Encoding
    }

/// Defines a contract interpreter for a Discriminated Union representing a set of events borne by a stream
type IUnionEncoder<'Union, 'Format> =
    /// Gets the union case string identifier for given union instance
    abstract GetCaseName : value:'Union -> string
    /// Encodes a union instance into a decoded representation
    abstract Encode      : value:'Union -> EncodedUnion<'Format>
    /// Decodes a formatted representation into a union instance. Can potentially throw FormatException
    abstract Decode      : encodedUnion:EncodedUnion<'Format> -> 'Union
    /// Decodes a formatted representation into a union instance. Does not throw exception on format mismatches
    abstract TryDecode   : encodedUnion:EncodedUnion<'Format> -> 'Union option

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

    /// Provide an IUnionContractEncoder based on a pair of encode and a tryDecode methods
    type EncodeTryDecodeCodec<'Union,'Format>(encode : 'Union -> string * 'Format, tryDecode : string * 'Format -> 'Union option) =
        interface IUnionEncoder<'Union, 'Format> with
            member __.GetCaseName(_) =
                invalidOp "Not implemented"

            member __.Encode e =
                let eventType, payload = encode e
                { caseName = eventType; payload = payload }

            member __.Decode encodedEvent =
                match tryDecode (encodedEvent.caseName, encodedEvent.payload) with
                | None -> raise (System.FormatException <| sprintf "Unrecognized event type '%s'" encodedEvent.caseName)
                | Some e -> e

            member __.TryDecode ee =
                tryDecode (ee.caseName, ee.payload)


/// Provides Codecs that render to a UTF-8 array suitable for storage in EventStore or CosmosDb.
type JsonUtf8 =

    /// <summary>
    ///     Generate a codec suitable for use with <c>Equinox.EventStore</c> or <c>Equinox.Cosmos</c>,
    ///       using the supplied `Newtonsoft.Json` <c>settings</c>.
    ///     The Event Type Names are inferred based on either explicit `DataMember(Name=` Attributes,
    ///       or (if unspecified) the Discriminated Union Case Name
    ///     The Union must be tagged with `interface TypeShape.UnionContract.IUnionContract` to signify this scheme applies.
    ///     See https://github.com/eiriktsarpalis/TypeShape/blob/master/tests/TypeShape.Tests/UnionContractTests.fs for example usage.</summary>
    /// <param name="settings">Configuration to be used by the underlying <c>Newtonsoft.Json</c> Serializer when encoding/decoding.</param>
    /// <param name="requireRecordFields">Fail encoder generation if union cases contain fields that are not F# records. Defaults to <c>false</c>.</param>
    /// <param name="allowNullaryCases">Fail encoder generation if union contains nullary cases. Defaults to <c>true</c>.</param>
    static member Create<'Union when 'Union :> UnionContract.IUnionContract>(settings, ?requireRecordFields, ?allowNullaryCases)
        : IUnionEncoder<'Union,byte[]> =
        let inner =
            UnionContract.UnionContractEncoder.Create<'Union,byte[]>(
                new Impl.JsonUtf8Encoder(settings),
                ?requireRecordFields=requireRecordFields,
                ?allowNullaryCases=allowNullaryCases)
        { new IUnionEncoder<'Union,byte[]> with
            member __.GetCaseName value = inner.GetCaseName value
            member __.Encode value =
                let r = inner.Encode value
                { caseName = r.CaseName; payload = r.Payload}
            member __.Decode encoded = inner.Decode { CaseName = encoded.caseName; Payload = encoded.payload }
            member __.TryDecode encoded = inner.TryDecode { CaseName = encoded.caseName; Payload = encoded.payload } }

    /// <summary>
    ///    Generate a codec suitable for use with <c>Equinox.EventStore</c> or <c>Equinox.Cosmos</c>,
    ///    using the supplied pair of <c>encode</c> and <c>tryDecode</code> functions. </summary>
    /// <param name="encode">Maps a 'Union to an Event Type Name and a UTF-8 array.</param>
    /// <param name="tryDecode">Attempts to map an Event Type Name and a UTF-8 array to a 'Union case, or None if not mappable.</param>
    static member Create<'Union>(encode : 'Union -> string * byte[], tryDecode : string * byte[] -> 'Union option)
        : IUnionEncoder<'Union,byte[]> =
        new Impl.EncodeTryDecodeCodec<'Union,byte[]>(encode, tryDecode) :> _