namespace Equinox.Projection.Codec

open Newtonsoft.Json
open System

/// Rendition of an event within a Span
type [<NoEquality; NoComparison>] RenderedEvent =
    {   /// Event Type associated with event data in `d`
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

/// Rendition of a continguous span of events for a given stream
type [<NoEquality; NoComparison>] RenderedSpan =
    {   /// Stream Name
        s: string

        /// base 'i' value for the Events held herein, reflecting the index associated with the first event in the span
        i: int64

        /// The Events comprising this span
        e: RenderedEvent[] }

/// Helpers for mapping to/from `Equinox.Codec` types
module RenderedSpan =
    let ofStreamSpan (stream : string) (index : int64) (events : seq<Equinox.Codec.IEvent<byte[]>>) : RenderedSpan =
        {   s = stream
            i = index
            e = [| for x in events -> { c = x.EventType; t = x.Timestamp; d = x.Data; m = x.Meta } |] }
    let enumEvents (span : RenderedSpan) : seq<Equinox.Codec.IEvent<byte[]>> = seq {
        for x in span.e ->
            Equinox.Codec.Core.EventData.Create(x.c, x.d, x.m, timestamp=x.t) }