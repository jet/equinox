[<AutoOpen>]
module Samples.Store.Integration.CodecIntegration

open Domain
open Swensen.Unquote
open TypeShape.UnionContract
open Xunit

let serializationSettings =
    Newtonsoft.Json.Converters.FSharp.Settings.CreateCorrect(converters=
        [|  // Don't let json.net treat 't option as the DU it is internally
            Newtonsoft.Json.Converters.FSharp.OptionConverter() |])

let genCodec<'Union when 'Union :> TypeShape.UnionContract.IUnionContract>() =
    Equinox.UnionCodec.JsonUtf8.Create<'Union>(serializationSettings)

type EventWithId = { id : SkuId }

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
    interface IUnionContract

let render = function
    | EventA { id = id } -> sprintf """{"id":"%O"}""" id
    | EventB { age = None } -> sprintf "{}"
    | EventB { age = Some age } -> sprintf """{"age":%d}""" age
    | EventC { value = I { i = i } } -> sprintf """{"value":{"case":"I","i":%d}}""" i
    | EventC { value = S { maybeI = None } } -> sprintf """{"value":{"case":"S"}}"""
    | EventC { value = S { maybeI = Some i } } -> sprintf """{"value":{"case":"S","maybeI":%d}}""" i

let codec = genCodec<SimpleDu>()

[<AutoData(MaxTest=100)>]
#if NET461
    [<Trait("KnownFailOn","Mono")>] // Likely due to net461 not having consistent json.net refs and no binding redirects
#endif
let ``Can roundtrip, rendering correctly`` (x: SimpleDu) =
    let serialized = codec.Encode x
    render x =! System.Text.Encoding.UTF8.GetString(serialized.payload)
    let deserialized = codec.TryDecode serialized |> Option.get
    deserialized =! x