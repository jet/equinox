namespace Equinox.Cosmos.Projection

open Microsoft.Azure.Documents

[<RequireQualifiedAccess>]
module DocumentParser =
    type Document with
        member document.Cast<'T>() =
            let tmp = new Document()
            tmp.SetPropertyValue("content", document)
            tmp.GetPropertyValue<'T>("content")
    /// Sanity check to determine whether the Document represents an `Equinox.Cosmos` >= 1.0 based batch
    let isEquinoxBatch (d : Document) = 
        d.GetPropertyValue "p" <> null && d.GetPropertyValue "i" <> null
        && d.GetPropertyValue "n" <> null && d.GetPropertyValue "e" <> null
    /// Maps fields in an Event within an Equinox.Cosmos V1+ Event (in a Batch or Tip) to the interface defined by the default Codec
    let (|CodecEvent|) (x: Equinox.Cosmos.Store.Event) =
        { new Propulsion.Streams.IEvent<byte[]> with
            member __.EventType = x.c
            member __.Data = x.d
            member __.Meta = x.m
            member __.Timestamp = x.t }

    /// Enumerates the events represented within a batch
    // NOTE until `tip-isa-batch` gets merged, this causes a null-traversal of `-1`-index pages which presently do not contain data
    // This is intentional in the name of forward compatibility for projectors - enabling us to upgrade the data format without necessitating
    //   updates of all projectors (even if there can potentially be significant at-least-once-ness to the delivery)
    let enumEquinoxCosmosEvents (batch : Equinox.Cosmos.Store.Batch) =
        batch.e |> Seq.mapi (fun offset (CodecEvent x) -> { stream = batch.p; index = batch.i + int64 offset; event = x } : Propulsion.Streams.StreamEvent<byte[]>)
    /// Collects all events with a Document [typically obtained via the CosmosDb ChangeFeed] that potentially represents an Equinox.Cosmos event-batch
    let enumEvents (d : Document) : Propulsion.Streams.StreamEvent<byte[]> seq =
        if isEquinoxBatch d then d.Cast<Equinox.Cosmos.Store.Batch>() |> enumEquinoxCosmosEvents
        else Seq.empty