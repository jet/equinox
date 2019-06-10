namespace Equinox.Codec

/// Common form for either a Domain Event or an Unfolded Event
type IEvent<'Format> =
    /// The Event Type, used to drive deserialization
    abstract member EventType : string
    /// Event body, as UTF-8 encoded json ready to be injected into the Store
    abstract member Data : 'Format
    /// Optional metadata (null, or same as Data, not written if missing)
    abstract member Meta : 'Format
    /// The Event's Creation Time (as defined by the writer, i.e. in a mirror, this is intended to reflect the original time)
    /// <remarks>
    /// - For EventStore, this value is not honored when writing; the server applies an authoritative timestamp when accepting the write.
    /// - For Cosmos, the value is not exposed where the event `IsUnfold`.
    /// </remarks>
    abstract member Timestamp : System.DateTimeOffset

/// Defines a contract interpreter for a Discriminated Union representing a set of events borne by a stream
type IUnionEncoder<'Union, 'Format> =
    /// Encodes a union instance into a decoded representation
    abstract Encode      : value:'Union -> IEvent<'Format>
    /// Decodes a formatted representation into a union instance. Does not throw exception on format mismatches
    abstract TryDecode   : encoded:IEvent<'Format> -> 'Union option

/// Provides Codecs that render to a UTF-8 array suitable for storage in EventStore or CosmosDb based on explicit functions you supply
/// i.e., with using conventions / Type Shapes / Reflection or specific Json processing libraries - see Equinox.Codec.NewtonsoftJson.Json for batteries-included Coding/Decoding
type Custom =

    /// <summary>
    ///    Generate a codec suitable for use with <c>Equinox.EventStore</c> or <c>Equinox.Cosmos</c>,
    ///    using the supplied pair of <c>encode</c> and <c>tryDecode</code> functions. </summary>
    /// <param name="encode">Maps a 'Union to an Event Type Name with UTF-8 arrays representing the `Data` and `Metadata`.</param>
    /// <param name="tryDecode">Attempts to map from an Event Type Name and UTF-8 arrays representing the `Data` and `Metadata` to a 'Union case, or None if not mappable.</param>
    // Leaving this private until someone actually asks for this (IME, while many systems have some code touching the metadata, it tends to fall into disuse)
    static member private Create<'Union>(encode : 'Union -> string * byte[] * byte[], tryDecode : string * byte[] * byte[] -> 'Union option)
        : IUnionEncoder<'Union,byte[]> =
        { new IUnionEncoder<'Union, byte[]> with
            member __.Encode e =
                let eventType, payload, metadata = encode e
                let timestamp = System.DateTimeOffset.UtcNow
                { new IEvent<_> with
                    member __.EventType = eventType
                    member __.Data = payload
                    member __.Meta = metadata
                    member __.Timestamp = timestamp }
            member __.TryDecode ee =
                tryDecode (ee.EventType, ee.Data, ee.Meta) }

    /// <summary>
    ///    Generate a codec suitable for use with <c>Equinox.EventStore</c> or <c>Equinox.Cosmos</c>,
    ///    using the supplied pair of <c>encode</c> and <c>tryDecode</code> functions. </summary>
    /// <param name="encode">Maps a 'Union to an Event Type Name and a UTF-8 array representing the `Data`.</param>
    /// <param name="tryDecode">Attempts to map an Event Type Name and a UTF-8 `Data` array to a 'Union case, or None if not mappable.</param>
    static member Create<'Union>(encode : 'Union -> string * byte[], tryDecode : string * byte[] -> 'Union option)
        : IUnionEncoder<'Union,byte[]> =
        let encode' value = let c, d = encode value in c, d, null
        let tryDecode' (et,d,_md) = tryDecode (et, d)
        Custom.Create(encode', tryDecode')

namespace Equinox.Codec.Core

open System

/// Represents a Domain Event or Unfold, together with it's Index in the event sequence
// Included here to enable extraction of this ancillary information (by downcasting IEvent in one's IUnionEncoder.TryDecode implementation)
// in the corner cases where this coupling is absolutely definitely better than all other approaches
type IIndexedEvent<'Format> =
    inherit Equinox.Codec.IEvent<'Format>
    /// The index into the event sequence of this event
    abstract member Index : int64
    /// Indicates this is not a Domain Event, but actually an Unfolded Event based on the state inferred from the events up to `Index`
    abstract member IsUnfold: bool

/// An Event about to be written, see IEvent for further information
type EventData<'Format> =
    { eventType : string; data : 'Format; meta : 'Format; timestamp: DateTimeOffset }
    interface Equinox.Codec.IEvent<'Format> with
        member __.EventType = __.eventType
        member __.Data = __.data
        member __.Meta = __.meta
        member __.Timestamp = __.timestamp

type EventData =
    static member Create(eventType, data, ?meta, ?timestamp) =
        {   eventType = eventType
            data = data
            meta = defaultArg meta null
            timestamp = match timestamp with Some ts -> ts | None -> DateTimeOffset.UtcNow }

namespace Equinox.Codec.NewtonsoftJson

open Newtonsoft.Json
open System.IO
open System.Runtime.InteropServices

/// Reuse interim buffers when coding/encoding
// https://stackoverflow.com/questions/55812343/newtonsoft-json-net-jsontextreader-garbage-collector-intensive
module private CharBuffersPool =
    let private inner = System.Buffers.ArrayPool<char>.Shared
    let instance =
        { new IArrayPool<char> with
            member __.Rent minLen = inner.Rent minLen
            member __.Return x = inner.Return x }

/// Newtonsoft.Json implementation of TypeShape.UnionContractEncoder's IEncoder that encodes direct to a UTF-8 Buffer
type BytesEncoder(settings : JsonSerializerSettings) =
    let serializer = JsonSerializer.Create(settings)
    interface TypeShape.UnionContract.IEncoder<byte[]> with
        member __.Empty = Unchecked.defaultof<_>
        member __.Encode (value : 'T) =
            use ms = new MemoryStream()
            (   use jsonWriter = new JsonTextWriter(new StreamWriter(ms), ArrayPool = CharBuffersPool.instance)
                serializer.Serialize(jsonWriter, value, typeof<'T>))
            ms.ToArray()
        member __.Decode(json : byte[]) =
            use ms = new MemoryStream(json)
            use jsonReader = new JsonTextReader(new StreamReader(ms), ArrayPool = CharBuffersPool.instance)
            serializer.Deserialize<'T>(jsonReader)

/// Provides Codecs that render to a UTF-8 array suitable for storage in EventStore or CosmosDb based on explicit functions you supply using `Newtonsoft.Json` and 
/// `TypeShape.UnionContract.UnionContractEncoder` - if you need full control and/or have have your own codecs, see `Equinox.Codec.Custom.Create` instead
type Json =

    /// <summary>
    ///     Generate a codec suitable for use with <c>Equinox.EventStore</c> or <c>Equinox.Cosmos</c>,
    ///       using the supplied `Newtonsoft.Json` <c>settings</c>.
    ///     The Event Type Names are inferred based on either explicit `DataMember(Name=` Attributes,
    ///       or (if unspecified) the Discriminated Union Case Name
    ///     The Union must be tagged with `interface TypeShape.UnionContract.IUnionContract` to signify this scheme applies.
    ///     See https://github.com/eiriktsarpalis/TypeShape/blob/master/tests/TypeShape.Tests/UnionContractTests.fs for example usage.</summary>
    /// <param name="settings">Configuration to be used by the underlying <c>Newtonsoft.Json</c> Serializer when encoding/decoding.</param>
    /// <param name="allowNullaryCases">Fail encoder generation if union contains nullary cases. Defaults to <c>true</c>.</param>
    static member Create<'Union when 'Union :> TypeShape.UnionContract.IUnionContract>
        (   settings,
            [<Optional;DefaultParameterValue(null)>]?allowNullaryCases)
        : Equinox.Codec.IUnionEncoder<'Union,byte[]> =
        let dataCodec =
            TypeShape.UnionContract.UnionContractEncoder.Create<'Union,byte[]>(
                new BytesEncoder(settings),
                requireRecordFields=true, // See JsonConverterTests - roundtripping UTF-8 correctly with Json.net is painful so for now we lock up the dragons
                ?allowNullaryCases=allowNullaryCases)
        { new Equinox.Codec.IUnionEncoder<'Union,byte[]> with
            member __.Encode value =
                let enc = dataCodec.Encode value
                Equinox.Codec.Core.EventData.Create(enc.CaseName, enc.Payload) :> _
            member __.TryDecode encoded =
                dataCodec.TryDecode { CaseName = encoded.EventType; Payload = encoded.Data } } 