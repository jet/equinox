[<AutoOpen>]
module Samples.Store.Integration.CodecIntegration

open Domain
open Swensen.Unquote
open TypeShape.UnionContract

type EventWithId = { id : CartId } // where CartId uses FSharp.UMX

type EventWithOption = { age : int option }

type EventWithUnion = { value : Union }
 // Using converter to collapse the `fields` of the union into the top level, alongside the `case`
 and [<Newtonsoft.Json.JsonConverter(typeof<FsCodec.NewtonsoftJson.UnionConverter>)>] Union =
    | I of Int
    | S of MaybeInt
 and Int = { i : int }
 and MaybeInt = { maybeI : int option }

type SimpleDu =
    | EventA of EventWithId
    | EventB of EventWithOption
    | EventC of EventWithUnion
    | EventD
    // See JsonConverterTests for why these are ruled out atm
    //| EventE of int // works but disabled due to Strings and DateTimes not working
    //| EventF of string // has wierd semantics, particularly when used with a VerbatimJsonConverter in Equinox.Cosmos
    interface IUnionContract

let render = function
    | EventA { id = id } -> sprintf """{"id":"%O"}""" id
    | EventB { age = None } -> sprintf "{\"age\":null}"
    | EventB { age = Some age } -> sprintf """{"age":%d}""" age
    | EventC { value = I { i = i } } -> sprintf """{"value":{"case":"I","i":%d}}""" i
    | EventC { value = S { maybeI = None } } -> sprintf """{"value":{"case":"S","maybeI":null}}"""
    | EventC { value = S { maybeI = Some i } } -> sprintf """{"value":{"case":"S","maybeI":%d}}""" i
    | EventD -> null
    //| EventE i -> string i
    //| EventF s ->  Newtonsoft.Json.JsonConvert.SerializeObject s

let codec = FsCodec.NewtonsoftJson.Codec.Create()

[<AutoData(MaxTest=100)>]
let ``Can roundtrip, rendering correctly`` (x: SimpleDu) =
    let serialized = codec.Encode(None,x)
    render x =! if serialized.Data = null then null else System.Text.Encoding.UTF8.GetString(serialized.Data)
    let adapted = FsCodec.Core.TimelineEvent.Create(-1L, serialized.EventType, serialized.Data)
    let deserialized = codec.TryDecode adapted |> Option.get
    deserialized =! x