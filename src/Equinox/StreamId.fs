namespace Equinox.Core

open System

module StreamName =

    /// Throws if a candidate categoryName includes a '-', is null, or is empty
    let inline validateCategoryName (rawCategory : string) =
        if rawCategory |> String.IsNullOrEmpty then invalidArg (nameof rawCategory) "may not be null or empty"
        if rawCategory.IndexOf '-' <> -1 then invalidArg (nameof rawCategory) "may not contain embedded '-' symbols"
    /// Throws if a candidate streamId fragment includes a '_', is null, or is empty
    let inline validateStreamIdFragment (rawFragment : string) =
        if rawFragment |> String.IsNullOrEmpty then invalidArg (nameof rawFragment) "may not contain null or empty fragments"
        if rawFragment.IndexOf '_' <> -1 then invalidArg (nameof rawFragment) "may not contain embedded '_' symbols"
    /// Combines streamId fragments. Throws if any of the fragments embed a `_`, are `null`, or are empty
    let combineStreamIdFragments (fragments : string seq) : string =
        fragments |> Seq.iter validateStreamIdFragment
        String.Join("_", fragments)
    /// Render in canonical {categoryName}-{streamId} format. Throws if categoryName contains embedded `-` symbols
    let render categoryName streamId =
        validateCategoryName categoryName
        String.Concat(categoryName, '-', streamId)

namespace Equinox

open FSharp.UMX

/// Represents the second half of a canonical StreamName, i.e., the streamId in "{categoryName}-{streamId}"
type StreamId = string<streamId>
and [<Measure>] streamId
/// Helpers for composing and rendering StreamId values
module StreamId =
    /// Create a StreamId, trusting the input to be well-formed (see the gen* functions for composing with validation)
    let inline ofRaw (raw : string) : StreamId = UMX.tag raw
    /// Render as a string for external use
    let toString : StreamId -> string = UMX.untag
    /// Render as a canonical "{categoryName}-{streamId}" StreamName. Throws if the categoryName embeds `-` chars.
    let renderStreamName categoryName (x : StreamId) : string = toString x |> Core.StreamName.render categoryName

    /// Generate a StreamId from a single application-level id, given a rendering function that maps to a non empty fragment without embedded `_` chars
    let gen (f : 'a -> string) (id : 'a) : StreamId =
        let element = f id
        Core.StreamName.validateStreamIdFragment element
        ofRaw element
    /// Generate a StreamId from a tuple of application-level ids, given two rendering functions that map to a non empty fragment without embedded `_` chars
    let gen2 f f2 struct (id1, id2) : StreamId = seq { yield f id1; yield f2 id2 } |> Core.StreamName.combineStreamIdFragments |> ofRaw
    /// Generate a StreamId from a triple of application-level ids, given three rendering functions that map to a non empty fragment without embedded `_` chars
    let gen3 f f2 f3 struct (id1, id2, id3) = seq { yield f id1; yield f2 id2; yield f3 id3 } |> Core.StreamName.combineStreamIdFragments |> ofRaw
