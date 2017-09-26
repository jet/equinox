module Foldunk.EventSum.Tests

open Foldunk.EventSum
open FsCheck.Xunit
open FSharp.Reflection
open Swensen.Unquote.Assertions
open System
open System.Runtime.Serialization
open TypeShape_Empty
open Xunit

// Placeholder event records salvaged from Hulk domain
type CartCreated = { id: string ; client_id: string }
type ZipCodeUpdated = { id: string ; zip_code: string }
type ItemReturnWaived = { id : string ; retail_sku_id : string ; value: bool }
type JetCashUsed = { id: string ; value: bool }
type JetCreditsUsed = { id: string ; value: bool }
type CartItemRemoved = { id : string ; retail_sku_id : string ; date : DateTime }

type BasicEventSum =
    | NullaryEvent
    | CartCreated of CartCreated
    | ZipCodeUpdated of ZipCodeUpdated
    | ItemReturnWaived of ZipCodeUpdated
    | JetCashUsed of JetCashUsed
    | JetCreditsUsed of JetCreditsUsed
    | CartItemRemoved of CartItemRemoved
    | [<DataMember(Name = "Legacy")>] Old_Stuff of CartItemRemoved

let encoder = lazy(generateJsonSumEncoder<BasicEventSum>)

[<Property>]
let ``Basic EventSum enc/dec roundtrip`` (sum : BasicEventSum) =
    let e = encoder.Value
    e.Decode(e.Encode sum) = sum

[<Property>]
let ``Should use correct event types in encodings`` (sum : BasicEventSum) =
    let e = encoder.Value.Encode sum
    let expectedLabel =
        match sum with
        | Old_Stuff _ -> "Legacy"
        | _ ->
            let uci, _ = FSharpValue.GetUnionFields(sum, typeof<BasicEventSum>, true)
            uci.Name

    test <@ expectedLabel = e.EventType @>

[<Fact>]
let ``Should throw FormatException on Decode of unrecognized event types`` () =
    let enc = encoder.Value
    let e = enc.Encode (CartCreated empty)
    let e' = { e with EventType = "__UNKNOWN_TYPE__" }
    raises<FormatException> <@ enc.Decode e' @>

[<Fact>]
let ``Should return None on TryDecode of unrecognized event types`` () =
    let enc = encoder.Value
    let e = enc.Encode (CartCreated empty)
    let e' = { e with EventType = "__UNKNOWN_TYPE__" }
    test <@ None = enc.TryDecode e' @>


type private PrivateEventSum = A of CartCreated

[<Fact>]
let ``Should work with private types`` () =
    let enc = generateJsonSumEncoder<PrivateEventSum>
    let value = A empty
    test <@ value = enc.Decode(enc.Encode value) @>


[<Fact>]
let ``Should fail on non-union inputs`` () =
    raises<ArgumentException> <@ generateJsonSumEncoder<CartCreated> @>


type EventSumWithMultipleFields =
    | A of CartCreated
    | B of JetCreditsUsed
    | C of CartCreated * JetCreditsUsed

[<Fact>]
let ``Should fail on event sums with multiple fields`` () =
    raises<ArgumentException> <@ generateJsonSumEncoder<EventSumWithMultipleFields> @>


type EventSumWithNonRecordCase =
    | A of CartCreated
    | B of JetCashUsed
    | C of string

[<Fact>]
let ``Should fail on event sums with non-record cases`` () =
    raises<ArgumentException> <@ generateJsonSumEncoder<EventSumWithNonRecordCase> @>


type EventSumWithDuplicateLabels =
    | A of CartCreated
    | B of JetCashUsed
    | [<DataMember(Name = "A")>] C of string

[<Fact>]
let ``Should fail on event sums with duplicate labels`` () =
    raises<ArgumentException> <@ generateJsonSumEncoder<EventSumWithDuplicateLabels> @>