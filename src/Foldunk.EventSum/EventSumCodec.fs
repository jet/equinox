module Foldunk.EventSumCodec

open Foldunk.Serialization
open Newtonsoft.Json
open TypeShape

/// Newtonsoft.Json implementation of IEncoder that encodes direct to a UTF-8 Buffer
type Utf8JsonEncoder() =
    // For whatever reason, EventStore does not hyphenate guids respect this approach
    let settings = Newtonsoft.GetDefaultSettings(useHyphenatedGuids = false, indent = false)
    interface EventSum.IEncoder<byte[]> with
        member __.Empty = null
        member __.Encode (value : 'T) = JsonConvert.SerializeObject(value, settings) |> System.Text.Encoding.UTF8.GetBytes
        member __.Decode (json : byte[]) = let x = System.Text.Encoding.UTF8.GetString(json) in JsonConvert.DeserializeObject<'T>(x, settings)

/// Generates an event sum encoder using Newtonsoft.Json for individual event types that serializes to a byte buffer
let generateJsonUtf8SumEncoder<'Union> =
    EventSum.generateSumEventEncoder<'Union, _> (new Utf8JsonEncoder())