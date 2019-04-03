namespace Equinox.Cosmos.Projection

open Microsoft.Azure.Documents

[<RequireQualifiedAccess>]
module DocumentParser =
    type Document with
        member document.Cast<'T>() =
            let tmp = new Document()
            tmp.SetPropertyValue("content", document)
            tmp.GetPropertyValue<'T>("content")
    type IEvent =
        inherit Equinox.Codec.Core.IIndexedEvent<byte[]>
            abstract member Stream : string
    /// Sanity check to determine whether the Document represents an `Equinox.Cosmos` >= 1.0 based batch
    let isEquinoxBatch (d : Document) = 
        d.GetPropertyValue "p" <> null && d.GetPropertyValue "i" <> null
        && d.GetPropertyValue "n" <> null && d.GetPropertyValue "e" <> null
    /// Enumerates the events represented within a batch
    // NOTE until `tip-isa-batch` gets merged, this causes a null-traversal of `-1`-index pages which presently do not contain data
    // This is intentional in the name of forward compatibility for projectors - enabling us to upgrade the data format without necessitating
    //   updates of all projectors (even if there can potentially be significant at-least-once-ness to the delivery)
    let enumEquinoxCosmosEvents (batch : Equinox.Cosmos.Store.Batch) =
        batch.e |> Seq.mapi (fun offset x ->
            { new IEvent with
                  member __.Index = batch.i + int64 offset
                  member __.IsUnfold = false
                  member __.EventType = x.c
                  member __.Data = x.d
                  member __.Meta = x.m
                  member __.Timestamp = x.t
                  member __.Stream = batch.p } )
    /// Collects all events with a Document [typically obtained via the CosmosDb ChangeFeed] that potentially represents an Equinox.Cosmos event-batch
    let enumEvents (d : Document) : IEvent seq =
        if isEquinoxBatch d then d.Cast<Equinox.Cosmos.Store.Batch>() |> enumEquinoxCosmosEvents
        else Seq.empty