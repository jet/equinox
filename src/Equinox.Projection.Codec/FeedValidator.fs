module Equinox.Projection.Validation

open System.Collections.Generic

module private Option =
    let defaultValue def option = defaultArg option def

/// Represents the categorisation of an item being ingested
type IngestResult =
    /// The item is a correct item at the tail of the known sequence
    | New
    /// Consistent as far as we know (but this Validator has not seen the head)
    | Ok 
    /// The item represents a duplicate of an earlier item
    | Duplicate 
    /// The item is beyond the tail of the stream and likely represets a gap
    | Gap

/// Represents the present state of a given stream 
type StreamState =
    /// We've observed the stream from the start
    | All of pos: int
    /// We've not observed the stream from the start
    | Partial of min: int * pos: int

module StreamState =
    let combine (state : StreamState option) index : IngestResult*StreamState =
        match state, index with
        | None, 0 -> New, All 0
        | None, x -> Ok, Partial (x,x)
        | Some (All x), i when i <= x -> Duplicate, All x
        | Some (All x), i when i = x + 1 -> New, All i
        | Some (All x), _ -> Gap, All x
        | Some (Partial (min=1; pos=pos)), 0 -> Duplicate, All pos
        | Some (Partial (min=min; pos=x)), i when i <= min -> Duplicate, Partial (i, x)
        | Some (Partial (min=min; pos=x)), i when i = x + 1 -> Ok, Partial (min, i)
        | Some (Partial (min=min)), i -> New, Partial (min, i) 


type FeedStats = { complete: int; partial: int }

/// Maintains the state of a set of streams being ingested into a processor for consistency checking purposes
/// - to determine whether an incoming event on a stream should be considered a duplicate and hence not processed
/// - to allow inconsistencies to be logged
type FeedValidator() =
    let streamStates = System.Collections.Generic.Dictionary<string,StreamState>()
    
    /// Thread safe operation to a) classify b) track change implied by a new message as encountered
    member __.Ingest(stream, index) : IngestResult * StreamState =
        lock streamStates <| fun () ->
            let state = 
                match streamStates.TryGetValue stream with
                | true, state -> Some state
                | false, _ -> None
            let (res, state') = StreamState.combine state index
            streamStates.[stream] <- state'
            res, state'

    /// Determine count of streams being tracked
    member __.Stats =
        lock streamStates <| fun () ->
            let raw = streamStates |> Seq.countBy (fun x -> match x.Value with All _ -> true | Partial _ -> false) |> Seq.toList
            {   complete = raw |> List.tryPick (function (true,c) -> Some c | (false,_) -> None) |> Option.defaultValue 0 
                partial = raw |> List.tryPick (function (false,c) -> Some c | (true,_) -> None) |> Option.defaultValue 0 } 

type [<NoComparison;NoEquality>] StreamSummary = { mutable fresh : int; mutable ok : int; mutable dup : int; mutable gap : int; mutable complete: bool }

type BatchStats = { fresh : int; ok : int; dup : int; gap : int; categories : int; streams : BatchStreamStats } with
    member s.TotalStreams = let s = s.streams in s.complete + s.incomplete
    
and BatchStreamStats = { complete: int; incomplete: int }

/// Used to establish aggregate stats for a batch of inputs for logging purposes
/// The Ingested inputs are passed to the supplied validator in order to classify them
type BatchValidator(validator : FeedValidator) =
    let streams = System.Collections.Generic.Dictionary<string,StreamSummary>()
    let streamSummary (streamName : string) =
        match streams.TryGetValue streamName with
        | true, acc -> acc
        | false, _ -> let t = { fresh = 0; ok = 0; dup = 0; gap = 0; complete = false } in streams.[streamName] <- t; t 

    /// Collate into Feed Validator and Batch stats
    member __.TryIngest(stream, index) : IngestResult =
        let res, state = validator.Ingest(stream, index)
        let streamStats = streamSummary stream
        match state with
        | All _ -> streamStats.complete <- true
        | Partial _ -> streamStats.complete <- false
        match res with
        | New -> streamStats.fresh <- streamStats.fresh + 1
        | Ok -> streamStats.ok <- streamStats.ok + 1
        | Duplicate -> streamStats.dup <- streamStats.dup + 1
        | Gap -> streamStats.gap <- streamStats.gap + 1
        res

    member __.Enum() : IEnumerable<KeyValuePair<string,StreamSummary>> = upcast streams

    member __.Stats : BatchStats =
        let mutable fresh, ok, dup, gap, complete, incomplete = 0, 0, 0, 0, 0, 0
        let cats = HashSet()
        for KeyValue(k,v) in streams do
            fresh <- fresh + v.fresh
            ok <- ok + v.ok
            dup <- dup + v.dup
            gap <- gap + v.gap
            match k.IndexOf('-') with
            | -1 -> ()
            | i -> cats.Add(k.Substring(0, i)) |> ignore
            if v.complete then complete <- complete + 1 else incomplete <- incomplete + 1
        { fresh = fresh; ok = ok; dup = dup; gap = gap; categories = cats.Count; streams = { complete = complete; incomplete = incomplete } }