module Equinox.Projection.Codec

open Newtonsoft.Json
open System

type [<NoEquality; NoComparison>] EqxHeader =
    {   /// Timestamp of original write
        t: DateTimeOffset // ISO 8601

        /// Event Type associated with event data in `d`
        [<JsonProperty(PropertyName="et")>]
        c: string

        /// Stream Name
        s: string

        /// Index within stream
        [<JsonProperty(PropertyName="id")>]
        i: int64 }

/// Default rendition of an event when being projected to Kafka
type [<NoEquality; NoComparison>] KafkaEvent =
    {   /// TODO inline into d, which needs to be top level for backcompat
        [<JsonProperty(PropertyName="~eqxheader")>]
        h: EqxHeader
        
        /// Event body, as UTF-8 encoded json ready to be injected into the Json being rendered for DocDb
        // TOCONSIDER if we don't inline `h`, we need to inline this
        [<JsonConverter(typeof<Equinox.Cosmos.Internal.Json.VerbatimUtf8JsonConverter>)>]
        d: byte[] // required

        /// Optional metadata, as UTF-8 encoded json, ready to emit directly (null, not written if missing)
        [<JsonConverter(typeof<Equinox.Cosmos.Internal.Json.VerbatimUtf8JsonConverter>)>]
        [<JsonProperty(Required=Required.Default, NullValueHandling=NullValueHandling.Ignore)>]
        m: byte[] }