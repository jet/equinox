[<AutoOpen>]
module Domain.Infrastructure

open Foldunk.Serialization
open Newtonsoft.Json
open System
open System.Runtime.Serialization

/// Endows any type that inherits this class with standard .NET comparison semantics using a supplied token identifier
[<AbstractClass>]
type Comparable<'TComp, 'Token when 'TComp :> Comparable<'TComp, 'Token> and 'Token : comparison>(token : 'Token) =
    member private __.Token = token // I can haz protected?
    override x.Equals y = match y with :? Comparable<'TComp, 'Token> as y -> x.Token = y.Token | _ -> false
    override __.GetHashCode() = hash token
    interface IComparable with
        member x.CompareTo y =
            match y with
            | :? Comparable<'TComp, 'Token> as y -> compare x.Token y.Token
            | _ -> invalidArg "y" "invalid comparand"

/// SkuId strongly typed id
[<Sealed; JsonConverter(typeof<SkuIdJsonConverter>); AutoSerializable(false); StructuredFormatDisplay("{Value}")>]
// (Internally a string for most efficient copying semantics)
type SkuId private (id : string) =
    inherit Comparable<SkuId, string>(id)
    [<IgnoreDataMember>] // Prevent swashbuckle inferring there's a "value" field
    member __.Value = id
    override __.ToString () = id
    // NB tests lean on having a ctor of this shape
    new (guid: Guid) = SkuId (guid.ToString("N"))
    //new() = SkuId(Guid.NewGuid())
    // NB for validation [and XSS] purposes we prove it translatable to a Guid
    static member Parse(input: string) = SkuId (Guid.Parse input)
/// Represent as a Guid.ToString("N") output externally
and private SkuIdJsonConverter() =
    inherit JsonIsomorphism<SkuId, string>()
    /// Renders as per Guid.ToString("N")
    override __.Pickle value = value.Value
    /// Input must be a Guid.Parseable value
    override __.UnPickle input = SkuId.Parse input

/// RequestId strongly typed id
[<Sealed; JsonConverter(typeof<RequestIdJsonConverter>); AutoSerializable(false); StructuredFormatDisplay("{Value}")>]
// (Internally a string for most efficient copying semantics)
type RequestId private (id : string) =
    inherit Comparable<RequestId, string>(id)
    [<IgnoreDataMember>] // Prevent swashbuckle inferring there's a "value" field
    member __.Value = id
    override __.ToString () = id
    // NB tests lean on having a ctor of this shape
    new (guid: Guid) = RequestId (guid.ToString("N"))
    //new() = RequestId(Guid.NewGuid())
    // NB for validation [and XSS] purposes we prove it translatable to a Guid
    static member Parse(input: string) = RequestId (Guid.Parse input)
/// Represent as a Guid.ToString("N") output externally
and private RequestIdJsonConverter() =
    inherit JsonIsomorphism<RequestId, string>()
    /// Renders as per Guid.ToString("N")
    override __.Pickle value = value.Value
    /// Input must be a Guid.Parseable value
    override __.UnPickle input = RequestId.Parse input

/// CartId strongly typed id
[<Sealed; JsonConverter(typeof<CartIdJsonConverter>); AutoSerializable(false); StructuredFormatDisplay("{Value}")>]
// (Internally a string for most efficient copying semantics)
type CartId private (id : string) =
    inherit Comparable<CartId, string>(id)
    [<IgnoreDataMember>] // Prevent swashbuckle inferring there's a "value" field
    member __.Value = id
    override __.ToString () = id
    // NB tests lean on having a ctor of this shape
    new (guid: Guid) = CartId (guid.ToString("N"))
    // NB for validation [and XSS] purposes we must prove it translatable to a Guid
    static member Parse(input: string) = CartId (Guid.Parse input)
/// Represent as a Guid.ToString("N") output externally
and private CartIdJsonConverter() =
    inherit JsonIsomorphism<CartId, string>()
    /// Renders as per Guid.ToString("N")
    override __.Pickle value = value.Value
    /// Input must be a Guid.Parseable value
    override __.UnPickle input = CartId.Parse input