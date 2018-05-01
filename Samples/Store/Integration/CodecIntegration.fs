[<AutoOpen>]
module Samples.Store.Integration.CodecIntegration

open Domain
open Swensen.Unquote

let settings =
    let instance = Foldunk.Serialization.Settings.CreateDefault()
    // For whatever reason, EventStore does not hyphenate guids; respect this approach
    instance.Converters.Add(Foldunk.Serialization.Converters.GuidConverter())
    // Don't let json.net treat 't option as the DU it is internally
    // ... instead, use the optionality to inhibit the emission of a value
    instance.Converters.Add(Foldunk.Serialization.Converters.OptionConverter())
    // Alter the rendering to collapse the `fields` of the union into the top level,
    // alongside the `case`
    instance.Converters.Add(Foldunk.Serialization.Converters.UnionConverter())
    instance

let genCodec<'T> = Foldunk.UnionCodec.generateJsonUtf8UnionCodec<'T> settings

type EventWithId = { id : CartId }
type EventWithOption = { age : int option }
type Union = I of Int | S of MaybeInt
 and Int = { i : int }
 and MaybeInt = { maybeI : int option }
type EventWithUnion = { value : Union }
type SimpleDu =
    | EventA of EventWithId
    | EventB of EventWithOption
    | EventC of EventWithUnion

let render = function
    | EventA { id = id } -> sprintf """{"id":"%s"}""" id.Value
    | EventB { age = None } -> sprintf "{}"
    | EventB { age = Some age } -> sprintf """{"age":%d}""" age
    | EventC { value = I { i = i } } -> sprintf """{"value":{"case":"I","i":%d}}""" i
    | EventC { value = S { maybeI = None } } -> sprintf """{"value":{"case":"S"}}"""
    | EventC { value = S { maybeI = Some i } } -> sprintf """{"value":{"case":"S","maybeI":%d}}""" i

let codec = genCodec<SimpleDu>

[<AutoData(MaxTest=100)>]
let ``Can roundtrip, rendering correctly`` (x: SimpleDu) =
    let serialized = codec.Encode x
    render x =! System.Text.Encoding.UTF8.GetString(serialized.Payload)
    let deserialized = codec.Decode serialized
    deserialized =! x