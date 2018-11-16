module Equinox.Cosmos.Tests.Tokens

open Equinox
open Equinox.Cosmos
open FsCheck.Xunit
open Swensen.Unquote.Assertions
open Xunit

let unpack (token : Storage.StreamToken) =
    let { pos = pos; rollingSnapshotEventIndex = compactionEventNumber } : Cosmos.Token = unbox token.value
    defaultArg pos.index 0L, compactionEventNumber, token.batchCapacityLimit

let (|Pos|) index : Store.Position = { collectionUri = null; streamName = null; index = Some index; etag = None }
[<Theory
    ; InlineData( 0, 3, (*2*)3)
    ; InlineData( 0, 2, (*1*)2)
    ; InlineData( 1, 2, (*0*)1)
    ; InlineData( 2, 2, 0)
    ; InlineData( 3, 2, 0)
    ; InlineData( 4, 2, 0)>]
let ``ofUncompactedVersion - batchCapacityLimit`` (Pos pos) batchSize expectedCapacity =
    let _, _, batchCapacityLimit = Token.ofUncompactedVersion batchSize pos |> unpack
    test <@ Some expectedCapacity = batchCapacityLimit @>

[<Theory
    // Non-compacted cases, should match ofUncompactedVersion
    ; InlineData(null,  0, 2, 4, 2)
    ; InlineData(null,  0, 2, 3, 1)
    ; InlineData(null,  0, 2, 2, 0)
    ; InlineData(null,  1, 1, 2, 0)
    ; InlineData(null,  2, 1, 2, 0)
    ; InlineData(null,  3, 1, 2, 0)
    // Normal cases
    ; InlineData(   0,  3, 1, 3, 0)
    ; InlineData(   1,  3, 1, 3, 0)
    ; InlineData(   2,  3, 1, 3, 0)
    ; InlineData(   3,  3, 1, 3, 1)>]
let ``ofPreviousTokenAndEventsLength - batchCapacityLimit`` (previousCompactionEventNumber : System.Nullable<int64>) (Pos pos) eventsLength batchSize expectedCapacity =
    let previousToken =
        if not previousCompactionEventNumber.HasValue then Token.ofRollingSnapshotEventIndex None 0 -84 ((|Pos|) -42L)
        else Token.ofRollingSnapshotEventIndex (Some previousCompactionEventNumber.Value) 0 -84 ((|Pos|) -42L)
    let _, _, batchCapacityLimit = unpack <| Token.ofPreviousTokenAndEventsLength previousToken eventsLength batchSize pos
    test <@ Some expectedCapacity = batchCapacityLimit @>

[<Property>]
let ``Properties of tokens based on various generation mechanisms `` (Pos streamVersion) (previousCompactionEventNumber : int64 option) eventsLength batchSize =
    let ovStreamVersion, ovCompactionEventNumber, ovBatchCapacityLimit =
        unpack <| Token.ofNonCompacting streamVersion
    let uvStreamVersion, uvCompactionEventNumber, uvBatchCapacityLimit =
        unpack <| Token.ofUncompactedVersion batchSize streamVersion
    let previousToken = Token.ofRollingSnapshotEventIndex previousCompactionEventNumber 0 -84 ((|Pos|) -42L)
    let peStreamVersion, peCompactionEventNumber, peBatchCapacityLimit =
        unpack <| Token.ofPreviousTokenAndEventsLength previousToken eventsLength batchSize streamVersion

    // StreamVersion
    test <@ defaultArg streamVersion.index 0L = ovStreamVersion @>
    test <@ defaultArg streamVersion.index 0L = uvStreamVersion @>
    test <@ defaultArg streamVersion.index 0L = peStreamVersion @>

    // CompactionEventNumber
    test <@ None = ovCompactionEventNumber @>
    test <@ None = uvCompactionEventNumber @>
    test <@ previousCompactionEventNumber = peCompactionEventNumber @>

    // BatchCapacityLimit
    test <@ None = ovBatchCapacityLimit @>
    // TODO REWRITE THESE !
    let rawUncompactedBatchCapacityLimit batchSize (pos : Store.Position) =
        batchSize - int (defaultArg pos.index 0L) - 1 - 2
    let rawCompactedBatchCapacityLimit compactionEventNumber batchSize (streamVersion : Store.Position) =
        batchSize - int ((defaultArg streamVersion.index 0L) - 1L - compactionEventNumber) - 1
    test <@ Some (rawUncompactedBatchCapacityLimit batchSize streamVersion |> max 0) = uvBatchCapacityLimit @>
    let rawExpectedFromPreviousCompactionEventNumber =
        match previousCompactionEventNumber with
        | None -> rawUncompactedBatchCapacityLimit batchSize streamVersion
        | Some pci -> rawCompactedBatchCapacityLimit pci batchSize streamVersion
    test <@ Some (max 0 (rawExpectedFromPreviousCompactionEventNumber - eventsLength)) = peBatchCapacityLimit @>