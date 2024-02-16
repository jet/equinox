namespace global

open FSharp.UMX
open System

/// Endows any type that inherits this class with standard .NET comparison semantics using a supplied token identifier
[<AbstractClass>]
type Comparable<'TComp, 'Token when 'TComp :> Comparable<'TComp, 'Token> and 'Token : comparison>(token: 'Token) =
    member val private Token = token
    override x.Equals y = match y with :? Comparable<'TComp, 'Token> as y -> x.Token = y.Token | _ -> false
    override _.GetHashCode() = hash token
    interface IComparable with
        member x.CompareTo y =
            match y with
            | :? Comparable<'TComp, 'Token> as y -> compare x.Token y.Token
            | _ -> invalidArg "y" "invalid comparand"

/// Endows any type that inherits this class with standard .NET comparison semantics using a supplied token identifier
/// + treats the token as the canonical rendition for `ToString()` purposes
[<AbstractClass>]
type StringId<'TComp when 'TComp :> Comparable<'TComp, string>>(token: string) =
    inherit Comparable<'TComp,string>(token)
    override _.ToString() = token

// If using >1 serializer, it can be useful to set up a type alias
// even if you're not, putting a type Alias in `namespace global` can make type definitions cleaner to read/remove opens
type NsjConverterAttribute = Newtonsoft.Json.JsonConverterAttribute
type NsjIgnoreAttribute = Newtonsoft.Json.JsonIgnoreAttribute
type NsjNameAttribute = Newtonsoft.Json.JsonConverterAttribute

type StjConverterAttribute = System.Text.Json.Serialization.JsonConverterAttribute
type StjNameAttribute = System.Text.Json.Serialization.JsonPropertyNameAttribute
type StjIgnoreAttribute = System.Text.Json.Serialization.JsonIgnoreAttribute

(* Benefit of using JsonIsomorphism rather than binding direct to the API is that the code should be identical across serializers *)

[<AbstractClass>]
type StringIdConverter<'T when 'T :> StringId<'T> >(parse: string -> 'T) =
    inherit FsCodec.NewtonsoftJson.JsonIsomorphism<'T, string>()
    override _.Pickle value = value.ToString()
    override _.UnPickle input = parse input
[<AbstractClass>]
type StjStringIdConverter<'T when 'T :> StringId<'T> >(parse: string -> 'T) =
    inherit FsCodec.SystemTextJson.JsonIsomorphism<'T, string>()
    override _.Pickle value = value.ToString()
    override _.UnPickle input = parse input

module Guid =
    let inline gen () = Guid.NewGuid()
    let inline toStringN (x: Guid) = x.ToString "N"

/// SkuId strongly typed id
/// - Ensures canonical rendering without dashes via ToString + Newtonsoft.Json OR System.Text.Json
/// - Guards against XSS by only permitting initialization based on Guid.Parse
/// - Implements comparison/equality solely to enable tests to leverage structural equality
[<Sealed; AutoSerializable false; NsjConverter(typeof<SkuIdJsonConverter>); StjConverter(typeof<SkuIdStjConverter>)>]
type SkuId =
    inherit StringId<SkuId>
    new (value: Guid) = { inherit StringId<SkuId>(Guid.toStringN value) }
    /// Required to support TypeShape.Empty
    /// See FsCheckGenerators.SkuId for how to define it otherwise
    [<Obsolete>] new () = SkuId(Guid.gen ())
and private SkuIdJsonConverter() = inherit StringIdConverter<SkuId>(Guid.Parse >> SkuId)
and private SkuIdStjConverter() = inherit StjStringIdConverter<SkuId>(Guid.Parse >> SkuId)

(* Per type, an associated module for parsing, generating or converting etc works well *)
module SkuId =
    let gen () = Guid.gen () |> SkuId

/// RequestId strongly typed id, represented internally as a string
/// - Ensures canonical rendering without dashes via ToString, Newtonsoft.Json, sprintf "%s" etc
/// - using string enables one to lean on structural equality for types embedding one
type RequestId = string<requestId>
and [<Measure>] requestId
module RequestId =
    /// - For web inputs, should guard against XSS by only permitting initialization based on RequestId.parse
    /// - For json deserialization where the saved representation is not trusted to be in canonical Guid form,
    ///     it is recommended to bind to a Guid and then upconvert to string<requestId>
    let parse (value: Guid<requestId>): string<requestId> = % Guid.toStringN %value

/// CartId strongly typed id; represented internally as a Guid; not used for storage so rendering is not significant
type CartId = Guid<cartId>
and [<Measure>] cartId
module CartId = let toString (value: CartId): string = Guid.toStringN %value

/// ClientId strongly typed id; represented internally as a Guid; not used for storage so rendering is not significant
type ClientId = Guid<clientId>
and [<Measure>] clientId
module ClientId = let toString (value: ClientId): string = Guid.toStringN %value

/// InventoryItemId strongly typed id
type InventoryItemId = Guid<inventoryItemId>
and [<Measure>] inventoryItemId
module InventoryItemId = let toString (value: InventoryItemId): string = Guid.toStringN %value

(* Single Case Discriminated unions don't give much over the type or FSharp.UMX based approach
   Before you even think about using one, required reading:
   - https://paul.blasuc.ci/posts/really-scu.html
   - https://paul.blasuc.ci/posts/even-more-scu.html
   - https://paul.blasuc.ci/posts/really-scu.html*)

[<Struct; NsjConverter(typeof<IntIdConverter>); StjConverter(typeof<StjIntIdConverter>)>]
type IntId =
    private IntId of int
and private IntIdConverter() =
    inherit FsCodec.NewtonsoftJson.JsonIsomorphism<IntId, string>()
    override _.Pickle(IntId value) = string value
    override _.UnPickle input = input |> Int32.Parse |> IntId
(* Again, using a JsonIsomorphism means identical code per serializer *)
and private StjIntIdConverter() =
    inherit FsCodec.SystemTextJson.JsonIsomorphism<IntId, string>()
    override _.Pickle(IntId value) = string value
    override _.UnPickle input = input |> Int32.Parse |> IntId
module IntId =
    let create (value: int) = IntId value

module EventCodec =

    /// For CosmosStore - we encode to JsonElement as that's what the store talks
    let genJsonElement<'t when 't :> TypeShape.UnionContract.IUnionContract> =
        FsCodec.SystemTextJson.CodecJsonElement.Create<'t>()

    /// For stores other than CosmosStore, we encode to UTF-8 and have the store do the right thing
    let gen<'t when 't :> TypeShape.UnionContract.IUnionContract> =
        FsCodec.NewtonsoftJson.Codec.Create<'t>()
