namespace Equinox.Core

open FSharp.UMX
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

/// Represents the second half of a canonical StreamName, i.e., the streamId in "{categoryName}-{streamId}"
type StreamId = string<streamId>
and [<Measure>] streamId
/// Helpers for composing and rendering StreamId values
module StreamId =

    /// Create a StreamId, trusting the input to be well-formed (see the gen* functions for composing with validation)
    let ofRaw (raw : string) : StreamId = UMX.tag raw
    /// Render as a string for external use
    let toString : StreamId -> string = UMX.untag
    /// Render as a canonical "{categoryName}-{streamId}" StreamName. Throws if the categoryName embeds `-` chars.
    let renderStreamName categoryName (x : StreamId) : string = toString x |> StreamName.render categoryName

namespace Equinox

/// Helpers for composing and rendering StreamId values
type StreamId =

    /// Generate a StreamId from a single application-level id, given a rendering function that maps to a non empty fragment without embedded `_` chars
    static member Map(f : 'a -> string) = System.Func<'a, Core.StreamId>(fun id ->
        let element = f id
        Core.StreamName.validateStreamIdFragment element
        Core.StreamId.ofRaw element)
    /// Generate a StreamId from a tuple of application-level ids, given two rendering functions that map to a non empty fragment without embedded `_` chars
    static member Map(f, f2) = System.Func<_, _, _>(fun id1 id2 -> seq { yield f id1; yield f2 id2 } |> Core.StreamName.combineStreamIdFragments |> Core.StreamId.ofRaw)
    /// Generate a StreamId from a triple of application-level ids, given three rendering functions that map to a non empty fragment without embedded `_` chars
    static member Map(f1, f2, f3) = System.Func<_, _, _, _>(fun id1 id2 id3 -> seq { yield f1 id1; yield f2 id2; yield f3 id3 } |> Core.StreamName.combineStreamIdFragments |> Core.StreamId.ofRaw)

/// Helpers for composing and rendering StreamId values
module StreamId =

    /// Generate a StreamId from a single application-level id, given a rendering function that maps to a non empty fragment without embedded `_` chars
    let gen (f : 'a -> string) : 'a -> Core.StreamId = StreamId.Map(f).Invoke
    /// Generate a StreamId from a tuple of application-level ids, given two rendering functions that map to a non empty fragment without embedded `_` chars
    let gen2 f1 f2 : 'a * 'b -> Core.StreamId = StreamId.Map(f1, f2).Invoke
    /// Generate a StreamId from a triple of application-level ids, given three rendering functions that map to a non empty fragment without embedded `_` chars
    let gen3 f1 f2 f3 : 'a * 'b * 'c -> Core.StreamId = StreamId.Map(f1, f2, f3).Invoke
