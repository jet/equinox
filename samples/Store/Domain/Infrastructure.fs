[<AutoOpen>]
module Domain.Infrastructure

open FSharp.UMX
open Newtonsoft.Json
open Newtonsoft.Json.Converters.FSharp
open System
open System.Runtime.Serialization

#if NET461
module Seq =
    let tryLast (source : seq<_>) =
        use e = source.GetEnumerator()
        if e.MoveNext() then
            let mutable res = e.Current
            while (e.MoveNext()) do res <- e.Current
            Some res
        else
            None
#endif

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
    new (guid: Guid) = SkuId (guid.ToString("N"))
    // NB tests (specifically, empty) lean on having a ctor of this shape
    new() = SkuId(Guid.NewGuid())
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
    new (guid: Guid) = RequestId (guid.ToString("N"))
    // NB tests (specifically, empty) lean on having a ctor of this shape
    new() = RequestId(Guid.NewGuid())
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
type CartId = Guid<cartId>
and [<Measure>] cartId
module CartId =
    let toStringN (value : CartId) : string = let g : Guid = %value in g.ToString "N"

/// ClientId strongly typed id
type ClientId = Guid<clientId>
and [<Measure>] clientId
module ClientId =
    let toStringN (value : ClientId) : string = let g : Guid = %value in g.ToString "N"