namespace Equinox.Codec

type OAttribute = System.Runtime.InteropServices.OptionalAttribute
type DAttribute = System.Runtime.InteropServices.DefaultParameterValueAttribute

open Newtonsoft.Json
open System.IO
open TypeShape

/// Common form for either a Domain Event or an Unfolded Event
type IEvent<'Format> =
    /// The Event Type, used to drive deserialization
    abstract member EventType : string
    /// Event body, as UTF-8 encoded json ready to be injected into the Store
    abstract member Data : 'Format
    /// Optional metadata (null, or same as Data, not written if missing)
    abstract member Meta : 'Format

/// Represents a Domain Event or Unfold, together with it's Index in the event sequence
type IIndexedEvent<'Format> =
    inherit IEvent<'Format>
    /// The index into the event sequence of this event
    abstract member Index : int64
    /// Indicates this is not a Domain Event, but actually an Unfolded Event based on the state inferred from the events up to `Index`
    abstract member IsUnfold: bool

/// An Event about to be written, see IEvent for further information
type EventData<'Format> =
    { eventType : string; data : 'Format; meta : 'Format }
    interface IEvent<'Format> with member __.EventType = __.eventType member __.Data = __.data member __.Meta = __.meta
type EventData =
    static member Create(eventType, data, ?meta) = { eventType = eventType; data = data; meta = defaultArg meta null}

/// Defines a contract interpreter for a Discriminated Union representing a set of events borne by a stream
type IUnionEncoder<'Union, 'Format> =
    /// Encodes a union instance into a decoded representation
    abstract Encode      : value:'Union -> IEvent<'Format>
    /// Decodes a formatted representation into a union instance. Does not throw exception on format mismatches
    abstract TryDecode   : encoded:IEvent<'Format> -> 'Union option

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
    type EncodeTryDecodeCodec<'Union,'Format when 'Format: null>(encode : 'Union -> string * 'Format * 'Format, tryDecode : string * 'Format -> 'Union option) =
        interface IUnionEncoder<'Union, 'Format> with
            member __.Encode e =
                let eventType, payload, metadata = encode e
                EventData.Create(eventType, payload, metadata) :> _
            member __.TryDecode ee =
                tryDecode (ee.EventType, ee.Data)

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
    static member Create<'Union when 'Union :> UnionContract.IUnionContract>(settings, [<O;D(null)>]?requireRecordFields, [<O;D(null)>]?allowNullaryCases)
        : IUnionEncoder<'Union,byte[]> =
        let inner =
            UnionContract.UnionContractEncoder.Create<'Union,byte[]>(
                new Impl.JsonUtf8Encoder(settings),
                ?requireRecordFields=requireRecordFields,
                ?allowNullaryCases=allowNullaryCases)
        { new IUnionEncoder<'Union,byte[]> with
            member __.Encode value =
                let enc = inner.Encode value
                EventData.Create(enc.CaseName, enc.Payload) :> _
            member __.TryDecode encoded =
                inner.TryDecode { CaseName = encoded.EventType; Payload = encoded.Data } }

    /// <summary>
    ///    Generate a codec suitable for use with <c>Equinox.EventStore</c> or <c>Equinox.Cosmos</c>,
    ///    using the supplied pair of <c>encode</c> and <c>tryDecode</code> functions. </summary>
    /// <param name="encode">Maps a 'Union to an Event Type Name and a UTF-8 array representing the `Data`.</param>
    /// <param name="tryDecode">Attempts to map an Event Type Name and a UTF-8 `Data` array to a 'Union case, or None if not mappable.</param>
    static member Create<'Union>(encode : 'Union -> string * byte[], tryDecode : string * byte[] -> 'Union option)
        : IUnionEncoder<'Union,byte[]> =
        let encode' value = let c, d = encode value in c, d, null
        JsonUtf8.Create(encode', tryDecode)

    /// <summary>
    ///    Generate a codec suitable for use with <c>Equinox.EventStore</c> or <c>Equinox.Cosmos</c>,
    ///    using the supplied pair of <c>encode</c> and <c>tryDecode</code> functions. </summary>
    /// <param name="encode">Maps a 'Union to an Event Type Name with UTF-8 arrays representing the `Data` and `Metadata`.</param>
    /// <param name="tryDecode">Attempts to map from an Event Type Name and UTF-8 arrays representing the `Data` and `Metadata` to a 'Union case, or None if not mappable.</param>
    static member Create<'Union>(encode : 'Union -> string * byte[] * byte[], tryDecode : string * byte[] -> 'Union option)
        : IUnionEncoder<'Union,byte[]> =
        new Impl.EncodeTryDecodeCodec<'Union,byte[]>(encode, tryDecode) :> _