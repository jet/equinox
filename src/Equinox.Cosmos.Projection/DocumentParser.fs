namespace Equinox.Cosmos.Projection

open Microsoft.Azure.Documents
open System

[<RequireQualifiedAccess>]
module DocumentParser =
    type Document with
        member document.Cast<'T>() =
            let tmp = new Document()
            tmp.SetPropertyValue("content", document)
            tmp.GetPropertyValue<'T>("content")
    /// Determines whether this document represents an index page [and hence should not be expected to contain any events]
    let isIndex (d : Document) = d.Id = "-1"
    type IEvent =
        inherit Equinox.Codec.Core.IIndexedEvent<byte[]>
            abstract member Stream : string
            abstract member TimeStamp : DateTimeOffset
    /// Infers whether the document represents a valid Event-Batch
    let enumEvents (d : Document) = seq {
        if not (isIndex d)
           && d.GetPropertyValue("p") <> null && d.GetPropertyValue("i") <> null
           && d.GetPropertyValue("n") <> null && d.GetPropertyValue("e") <> null then
            let batch = d.Cast<Equinox.Cosmos.Store.Batch>()
            yield! batch.e |> Seq.mapi (fun offset x ->
                { new IEvent with
                      member __.Index = batch.i + int64 offset
                      member __.IsUnfold = false
                      member __.EventType = x.c
                      member __.Data = x.d
                      member __.Meta = x.m
                      member __.TimeStamp = x.t
                      member __.Stream = batch.p } ) }