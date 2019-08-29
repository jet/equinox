namespace Gardelloyd.NewtonsoftJson

open Newtonsoft.Json
open Newtonsoft.Json.Serialization
open System
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

// http://www.philosophicalgeek.com/2015/02/06/announcing-microsoft-io-recycablememorystream/
module private Utf8BytesEncoder =
    let private streamManager = Microsoft.IO.RecyclableMemoryStreamManager()
    let rentStream () = streamManager.GetStream("bytesEncoder")
    let wrapAsStream json =
        // This is the most efficient way of approaching this without using Spans etc.
        // RecyclableMemoryStreamManager does not have any wins to provide us
        new MemoryStream(json,writable=false)
    let makeJsonReader(ms : MemoryStream) =
        new JsonTextReader(new StreamReader(ms), ArrayPool = CharBuffersPool.instance)
    let private utf8NoBom = new System.Text.UTF8Encoding(false, true)
    let makeJsonWriter ms =
        // We need to `leaveOpen` in order to allow .Dispose of the `.rentStream`'d to return it
        let sw = new StreamWriter(ms, utf8NoBom, 1024, leaveOpen=true) // same middle args as StreamWriter default ctor 
        new JsonTextWriter(sw, ArrayPool = CharBuffersPool.instance)

module Core =
    /// Newtonsoft.Json implementation of TypeShape.UnionContractEncoder's IEncoder that encodes direct to a UTF-8 Buffer
    type BytesEncoder(settings : JsonSerializerSettings) =
        let serializer = JsonSerializer.Create(settings)
        interface TypeShape.UnionContract.IEncoder<byte[]> with
            member __.Empty = Unchecked.defaultof<_>
            member __.Encode (value : 'T) =
                use ms = Utf8BytesEncoder.rentStream ()
                (   use jsonWriter = Utf8BytesEncoder.makeJsonWriter ms
                    serializer.Serialize(jsonWriter, value, typeof<'T>))
                // TOCONSIDER as noted in the comments on RecyclableMemoryStream.ToArray, ideally we'd be continuing the rental and passing out a Span
                ms.ToArray()
            member __.Decode(json : byte[]) =
                use ms = Utf8BytesEncoder.wrapAsStream json
                use jsonReader = Utf8BytesEncoder.makeJsonReader ms
                serializer.Deserialize<'T>(jsonReader)

/// Provides Codecs that render to a UTF-8 array suitable for storage in EventStore or CosmosDb based on explicit functions you supply using `Newtonsoft.Json` and 
/// `TypeShape.UnionContract.UnionContractEncoder` - if you need full control and/or have have your own codecs, see `Gardelloyd.Custom.Create` instead
type Codec private () =

    static let defaultSettings = lazy Settings.Create()

    /// <summary>
    ///     Generate a codec suitable for use with <c>Equinox.EventStore</c>, <c>Equinox.Cosmos</c> or <c>Propulsion</c> libraries,
    ///       using the supplied `Newtonsoft.Json` <c>settings</c>.
    ///     The Event Type Names are inferred based on either explicit `DataMember(Name=` Attributes,
    ///       or (if unspecified) the Discriminated Union Case Name
    ///     The Union must be tagged with `interface TypeShape.UnionContract.IUnionContract` to signify this scheme applies.
    ///     See https://github.com/eiriktsarpalis/TypeShape/blob/master/tests/TypeShape.Tests/UnionContractTests.fs for example usage.</summary>
    /// <param name="settings">Configuration to be used by the underlying <c>Newtonsoft.Json</c> Serializer when encoding/decoding. Defaults to same as `Settings.Create()`</param>
    /// <param name="allowNullaryCases">Fail encoder generation if union contains nullary cases. Defaults to <c>true</c>.</param>
    static member Create<'Union when 'Union :> TypeShape.UnionContract.IUnionContract>
        (   ?settings,
            [<Optional;DefaultParameterValue(null)>]?allowNullaryCases)
        : Gardelloyd.IUnionEncoder<'Union,byte[]> =
        let settings = match settings with Some x -> x | None -> defaultSettings.Value
        let dataCodec =
            TypeShape.UnionContract.UnionContractEncoder.Create<'Union,byte[]>(
                new Core.BytesEncoder(settings),
                requireRecordFields=true, // See JsonConverterTests - roundtripping UTF-8 correctly with Json.net is painful so for now we lock up the dragons
                ?allowNullaryCases=allowNullaryCases)
        { new Gardelloyd.IUnionEncoder<'Union,byte[]> with
            member __.Encode value =
                let enc = dataCodec.Encode value
                Gardelloyd.Core.EventData.Create(enc.CaseName, enc.Payload) :> _
            member __.TryDecode encoded =
                dataCodec.TryDecode { CaseName = encoded.EventType; Payload = encoded.Data } }

and Settings private () =
    /// <summary>
    ///     Creates a default set of serializer settings used by Json serialization. When used with no args, same as JsonSerializerSettings.CreateDefault()
    /// </summary>
    /// <param name="camelCase">Render idiomatic camelCase for PascalCase items by using `CamelCasePropertyNamesContractResolver`. Defaults to false.</param>
    /// <param name="indent">Use multi-line, indented formatting when serializing json; defaults to false.</param>
    /// <param name="ignoreNulls">Ignore null values in input data; defaults to false.</param>
    /// <param name="errorOnMissing">Error on missing values (as opposed to letting them just be default-initialized); defaults to false.</param>
    static member CreateDefault
        (   [<Optional;ParamArray>]converters : JsonConverter[],
            [<Optional;DefaultParameterValue(null)>]?indent : bool,
            [<Optional;DefaultParameterValue(null)>]?camelCase : bool,
            [<Optional;DefaultParameterValue(null)>]?ignoreNulls : bool,
            [<Optional;DefaultParameterValue(null)>]?errorOnMissing : bool) =
        let indent = defaultArg indent false
        let camelCase = defaultArg camelCase false
        let ignoreNulls = defaultArg ignoreNulls false
        let errorOnMissing = defaultArg errorOnMissing false
        let resolver : IContractResolver =
             if camelCase then CamelCasePropertyNamesContractResolver() :> _
             else DefaultContractResolver() :> _
        JsonSerializerSettings(
            ContractResolver = resolver,
            Converters = converters,
            DateTimeZoneHandling = DateTimeZoneHandling.Utc, // Override default of RoundtripKind
            DateFormatHandling = DateFormatHandling.IsoDateFormat, // Pin Json.Net claimed default
            Formatting = (if indent then Formatting.Indented else Formatting.None),
            MissingMemberHandling = (if errorOnMissing then MissingMemberHandling.Error else MissingMemberHandling.Ignore),
            NullValueHandling = (if ignoreNulls then NullValueHandling.Ignore else NullValueHandling.Include))

    /// <summary>Optionated helper that creates a set of serializer settings that fail fast, providing less surprises when working in F#.</summary>
    /// <param name="camelCase">
    ///     Render idiomatic camelCase for PascalCase items by using `CamelCasePropertyNamesContractResolver`.
    ///     Defaults to false on basis that you'll use record and tuple field names that are camelCase (and hence not `CLSCompliant`).</param>
    /// <param name="indent">Use multi-line, indented formatting when serializing json; defaults to false.</param>
    /// <param name="ignoreNulls">Ignore null values in input data; defaults to `true`. NB OOTB, Json.Net defaults to false.</param>
    /// <param name="errorOnMissing">Error on missing values (as opposed to letting them just be default-initialized); defaults to false.</param>
    static member Create
        (   [<Optional;ParamArray>]converters : JsonConverter[],
            [<Optional;DefaultParameterValue(null)>]?indent : bool,
            [<Optional;DefaultParameterValue(null)>]?camelCase : bool,
            [<Optional;DefaultParameterValue(null)>]?ignoreNulls : bool,
            [<Optional;DefaultParameterValue(null)>]?errorOnMissing : bool) =
        Settings.CreateDefault(
            converters=converters,
            // the key impact of this is that Nullables/options start to render as absent (same for strings etc)
            ignoreNulls=defaultArg ignoreNulls true,
            ?errorOnMissing=errorOnMissing,
            ?indent=indent,
            ?camelCase=camelCase)