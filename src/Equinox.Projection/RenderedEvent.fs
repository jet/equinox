namespace Equinox.Projection

open Newtonsoft.Json
open System

module Codec =
    /// Default rendition of an event when being projected to Kafka
    type [<NoEquality; NoComparison>] RenderedEvent =
        {   /// Stream Name
            s: string

            /// Index within stream
            i: int64

            /// Event Type associated with event data in `d`
            c: string

            /// Timestamp of original write
            t: DateTimeOffset // ISO 8601

            /// Event body, as UTF-8 encoded json ready to be injected directly into the Json being rendered
            [<JsonConverter(typeof<Equinox.Cosmos.Internal.Json.VerbatimUtf8JsonConverter>)>]
            d: byte[] // required

            /// Optional metadata, as UTF-8 encoded json, ready to emit directly (entire field is not written if value is null)
            [<JsonConverter(typeof<Equinox.Cosmos.Internal.Json.VerbatimUtf8JsonConverter>)>]
            [<JsonProperty(Required=Required.Default, NullValueHandling=NullValueHandling.Ignore)>]
            m: byte[] }