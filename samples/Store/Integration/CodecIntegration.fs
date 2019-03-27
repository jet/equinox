[<AutoOpen>]
module Samples.Store.Integration.CodecIntegration

open Domain
open Swensen.Unquote
open TypeShape.UnionContract

let serializationSettings =
    Newtonsoft.Json.Converters.FSharp.Settings.CreateCorrect(converters=
        [|  // Don't let json.net treat 't option as the DU it is internally
            Newtonsoft.Json.Converters.FSharp.OptionConverter() |])

let genCodec<'Union when 'Union :> TypeShape.UnionContract.IUnionContract>() =
    Equinox.Codec.NewtonsoftJson.Json.Create<'Union>(serializationSettings)

type EventWithId = { id : CartId } // where CartId uses FSharp.UMX

type EventWithOption = { age : int option }

type EventWithUnion = { value : Union }
 // Using converter to collapse the `fields` of the union into the top level, alongside the `case`
 and [<Newtonsoft.Json.JsonConverter(typeof<Newtonsoft.Json.Converters.FSharp.UnionConverter>)>] Union =
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
    | EventB { age = None } -> sprintf "{}"
    | EventB { age = Some age } -> sprintf """{"age":%d}""" age
    | EventC { value = I { i = i } } -> sprintf """{"value":{"case":"I","i":%d}}""" i
    | EventC { value = S { maybeI = None } } -> sprintf """{"value":{"case":"S"}}"""
    | EventC { value = S { maybeI = Some i } } -> sprintf """{"value":{"case":"S","maybeI":%d}}""" i
    | EventD -> null
    //| EventE i -> string i
    //| EventF s ->  Newtonsoft.Json.JsonConvert.SerializeObject s

let codec = genCodec<SimpleDu>()

[<AutoData(MaxTest=100)>]
let ``Can roundtrip, rendering correctly`` (x: SimpleDu) =
    let serialized = codec.Encode x
    render x =! if serialized.Data = null then null else System.Text.Encoding.UTF8.GetString(serialized.Data)
    let deserialized = codec.TryDecode serialized |> Option.get
    deserialized =! x