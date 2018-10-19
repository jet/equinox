﻿[<AutoOpen>]
module Samples.Store.Integration.CodecIntegration

open Domain
open Swensen.Unquote

let settings =
    Newtonsoft.Json.Converters.FSharp.Settings.CreateCorrect(converters=
        [|  // Don't let json.net treat 't option as the DU it is internally
            Newtonsoft.Json.Converters.FSharp.OptionConverter()
            // Collapse the `fields` of the union into the top level, alongside the `case`
            Newtonsoft.Json.Converters.FSharp.UnionConverter() |])

let genCodec<'T> = Equinox.UnionCodec.generateJsonUtf8UnionCodec<'T> settings

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