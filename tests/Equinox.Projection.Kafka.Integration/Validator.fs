module Validator

open System
open System.Collections.Concurrent
open System.Threading

/// Represents the categorisation of an item being ingested
type IngestResult =
    /// The item is a correct item at the tail of the known sequence
    | Consistent
    /// Consistent as far as we know (but this Validator has not seen the head)
    | New
    /// The item represents a duplicate of an earlier item
    | Duplicate 
    /// The item is beyond the tail of the stream and likely represets a gap
    | Gap

/// Represents the present state of a given stream 
type StreamState =
    /// Nothing observed so far
    | Initial
    /// The stream last received a message that's logically at the known tip
    | Ok of pos: int
    /// The stream is consistent, however we didn't observe from the start
    | ConsistentSuffix of min: int * pos: int
    /// The last observed item was a repeat
    | Repeating of last: int * pos: int

module private StreamState =
    let ingest (state : StreamState) index : IngestResult*StreamState =
        failwith "TODO"

/// Maintains the state of a set of streams being ingested into a processor for consistency checking purposes
/// - to determine whether an incoming event on a stream should be considered a duplicate and hence not processed
/// - to allow inconsistencies to be logged
type FeedValidator() =
    let streamStates = ConcurrentDictionary<string,StreamState>()
    member __.Ingest(stream, index) : IngestResult =
        let mutable res = None
        let inline tee (res',state) =
            res <- Some res'
            state
        streamStates.AddOrUpdate(
            stream,
            (fun _ -> StreamState.ingest Initial index |> tee),
            fun _ state -> StreamState.ingest state index |> tee) |> ignore
        Option.get res

module Tests =
    open Xunit
    open Swensen.Unquote

    let [<Fact>] ``Zero on unknown stream is Consistent`` () =
        let sut = FeedValidator()
        let result = sut.Ingest("A", 0)
        test <@ Consistent = result @>

    let [<Fact>] ``Non-zero on unknown stream is New`` () =
        let sut = FeedValidator()

        let result = sut.Ingest("A", 1)
        test <@ New = result @>

    let [<Fact>] ``valid successor is Consistent`` () =
        let sut = FeedValidator()
        let _ = sut.Ingest("A", 0)

        let result = sut.Ingest("A", 1)
        test <@ Consistent = result @>

    let [<Fact>] ``single immediate repeat is flagged`` () =
        let sut = FeedValidator()
        let _ = sut.Ingest("A", 0)

        let result = sut.Ingest("A", 0)
        test <@ Duplicate = result @>

    let [<Fact>] ``non-immediate repeat is flagged`` () =
        let sut = FeedValidator()
        let _ = sut.Ingest("A", 0)

        let result = sut.Ingest("A", 0)
        test <@ Duplicate = result @>

    let [<Fact>] ``Gap is flagged`` () =
        let sut = FeedValidator()
        let _ = sut.Ingest("A", 0)

        let result = sut.Ingest("A", 0)
        test <@ Duplicate = result @>
