module Equinox.Cosmos.Tests.Tokens

open Equinox
open Equinox.Cosmos
open FsCheck.Xunit
open Swensen.Unquote.Assertions
open Xunit

let unpack (Token.Unpack token : Storage.StreamToken) =
    token.pos.index, token.rollingSnapshotEventIndex, token.batchCapacityLimit

let (|TokenFromIndex|) index : Token =
    {   stream = { collectionUri = null; name = null }
        pos = { index = index; self = None; etag = None }
        rollingSnapshotEventIndex = Some 0L
        batchCapacityLimit = None }
let (|StreamPos|) (TokenFromIndex token) = token.stream, token.pos
[<Theory
    ; InlineData( 0, 3, (*2*)3)
    ; InlineData( 0, 2, (*1*)2)
    ; InlineData( 1, 2, (*0*)1)
    ; InlineData( 2, 2, 0)
    ; InlineData( 3, 2, 0)
    ; InlineData( 4, 2, 0)>]
let ``ofUncompactedVersion - batchCapacityLimit`` (StreamPos streamPos) batchSize expectedCapacity =
    let _, _, batchCapacityLimit = Token.ofUncompactedVersion batchSize streamPos |> unpack
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
let ``ofPreviousTokenAndEventsLength - batchCapacityLimit`` (previousCompactionEventNumber : System.Nullable<int64>) (StreamPos pos) eventsLength batchSize expectedCapacity =
    let previousToken =
        if not previousCompactionEventNumber.HasValue then Token.ofRollingSnapshotEventIndex None 0 -84 ((|StreamPos|) -42L)
        else Token.ofRollingSnapshotEventIndex (Some previousCompactionEventNumber.Value) 0 -84 ((|StreamPos|) -42L)
    let _, _, batchCapacityLimit = unpack <| Token.ofPreviousTokenAndEventsLength previousToken eventsLength batchSize pos
    test <@ Some expectedCapacity = batchCapacityLimit @>

[<Property(MaxTest=1000)>]
let ``Properties of tokens based on various generation mechanisms `` (StreamPos (stream,pos)) (previousCompactionEventNumber : int64 option) eventsLength batchSize =
    let ovStreamVersion, ovCompactionEventNumber, ovBatchCapacityLimit =
        unpack <| Token.ofNonCompacting (stream,pos)
    let uvStreamVersion, uvCompactionEventNumber, uvBatchCapacityLimit =
        unpack <| Token.ofUncompactedVersion batchSize (stream,pos)
    let (StreamPos sp0, StreamPos sp) = 0L, pos.index
    let previousToken = Token.ofRollingSnapshotEventIndex previousCompactionEventNumber 0 -84 sp0
    let peStreamVersion, peCompactionEventNumber, peBatchCapacityLimit =
        unpack <| Token.ofPreviousTokenAndEventsLength previousToken eventsLength batchSize sp

    // StreamVersion
    test <@ pos.index = ovStreamVersion @>
    test <@ pos.index = uvStreamVersion @>
    test <@ pos.index = peStreamVersion @>

    // CompactionEventNumber
    test <@ None = ovCompactionEventNumber @>
    test <@ None = uvCompactionEventNumber @>
    test <@ previousCompactionEventNumber = peCompactionEventNumber @>

    // BatchCapacityLimit
    test <@ None = ovBatchCapacityLimit @>
    let rawUncompactedBatchCapacityLimit batchSize (pos : Store.Position) =
        batchSize - int pos.index - 1 - 2 |> max 0
    let rawCompactedBatchCapacityLimit compactionEventNumber batchSize (pos : Store.Position) =
        batchSize - int (pos.index - compactionEventNumber - 1L) |> max 0
    test <@ Some (rawUncompactedBatchCapacityLimit batchSize pos |> max 0) = uvBatchCapacityLimit @>
    let rawExpectedFromPreviousCompactionEventNumber =
        match previousCompactionEventNumber with
        | None -> rawUncompactedBatchCapacityLimit batchSize pos
        | Some pci -> rawCompactedBatchCapacityLimit pci batchSize pos
    test <@ Some (max 0 (rawExpectedFromPreviousCompactionEventNumber - eventsLength)) = peBatchCapacityLimit @>