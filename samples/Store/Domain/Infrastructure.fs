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
    member private __.Token = token
    override x.Equals y = match y with :? Comparable<'TComp, 'Token> as y -> x.Token = y.Token | _ -> false
    override __.GetHashCode() = hash token
    interface IComparable with
        member x.CompareTo y =
            match y with
            | :? Comparable<'TComp, 'Token> as y -> compare x.Token y.Token
            | _ -> invalidArg "y" "invalid comparand"

/// Endows any type that inherits this class with standard .NET comparison semantics using a supplied token identifier
/// + treats the token as the canonical rendition for `ToString()` purposes
[<AbstractClass>]
type StringId<'TComp when 'TComp :> Comparable<'TComp, string>>(token : string) =
    inherit Comparable<'TComp,string>(token)
    override __.ToString() = token

module Guid =
    let inline toStringN (x : Guid) = x.ToString "N"

/// SkuId strongly typed id
/// - Ensures canonical rendering without dashes via ToString + Newtonsoft.Json
/// - Guards against XSS by only permitting initialization based on Guid.Parse
/// - Implements comparison/equality solely to enable tests to leverage structural equality 
[<Sealed; AutoSerializable(false); JsonConverter(typeof<SkuIdJsonConverter>)>]
type SkuId private (id : string) =
    inherit StringId<SkuId>(id)
    new(value : Guid) = SkuId(value.ToString "N")
    /// Required to support empty
    [<Obsolete>] new() = SkuId(Guid.NewGuid())
/// Represent as a Guid.ToString("N") output externally
and private SkuIdJsonConverter() =
    inherit JsonIsomorphism<SkuId, string>()
    /// Renders as per `Guid.ToString("N")`, i.e. no dashes
    override __.Pickle value = string value
    /// Input must be a `Guid.Parse`able value
    override __.UnPickle input = Guid.Parse input |> SkuId

/// RequestId strongly typed id
/// - Ensures canonical rendering without dashes via ToString + Newtonsoft.Json
/// - Guards against XSS by only permitting initialization based on Guid.Parse
/// - Implements comparison/equality solely to enable tests to leverage structural equality 
[<Sealed; AutoSerializable(false); JsonConverter(typeof<RequestIdJsonConverter>)>]
type RequestId private (id : string) =
    inherit StringId<RequestId>(id)
    new(value : Guid) = RequestId(value.ToString "N")
    /// Required to support empty
    [<Obsolete>] new() = RequestId(Guid.NewGuid())
/// Represent as a Guid.ToString("N") output externally
and private RequestIdJsonConverter() =
    inherit JsonIsomorphism<RequestId, string>()
    /// Renders as per `Guid.ToString("N")`, i.e. no dashes
    override __.Pickle value = string value
    /// Input must be a `Guid.Parse`able value
    override __.UnPickle input = Guid.Parse input |> RequestId

/// CartId strongly typed id; not used for storage so rendering is not significant
type CartId = Guid<cartId>
and [<Measure>] cartId
module CartId = let toStringN (value : CartId) : string = Guid.toStringN %value

/// CartId strongly typed id; not used for storage so rendering is not significant
type ClientId = Guid<clientId>
and [<Measure>] clientId
module ClientId = let toStringN (value : ClientId) : string = Guid.toStringN %value

/// InventoryItemId strongly typed id
type InventoryItemId = Guid<inventoryItemId>
and [<Measure>] inventoryItemId
module InventoryItemId = let toStringN (value : InventoryItemId) : string = Guid.toStringN %value