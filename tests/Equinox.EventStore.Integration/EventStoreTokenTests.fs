module Equinox.EventStore.Tests.EventStoreTokenTests

open Equinox.Core
open Equinox.EventStore
open FsCheck.Xunit
open Swensen.Unquote.Assertions
open Xunit

let unpack (Token.Unpack token : StreamToken) =
    token.pos.streamVersion, token.pos.compactionEventNumber, token.pos.batchCapacityLimit

[<Theory
    ; InlineData(-1, 3, 2)
    ; InlineData(-1, 2, 1)
    ; InlineData( 0, 2, 0)
    ; InlineData( 1, 2, 0)
    ; InlineData( 2, 2, 0)
    ; InlineData( 3, 2, 0)>]
let ``ofUncompactedVersion - batchCapacityLimit`` streamVersion batchSize expectedCapacity =
    let _, _, batchCapacityLimit = Token.ofUncompactedVersion batchSize null streamVersion |> unpack
    test <@ Some expectedCapacity = batchCapacityLimit @>

[<Theory
    // Non-compacted cases, should match ofUncompactedVersion
    ; InlineData(null, -1, 1, 4, 2)
    ; InlineData(null, -1, 1, 3, 1)
    ; InlineData(null, -1, 1, 2, 0)
    ; InlineData(null,  0, 1, 2, 0)
    ; InlineData(null,  1, 1, 2, 0)
    ; InlineData(null,  2, 1, 2, 0)
    // Normal cases
    ; InlineData(  -1,  2, 1, 3, 0)
    ; InlineData(   0,  2, 1, 3, 0)
    ; InlineData(   1,  2, 1, 3, 0)
    ; InlineData(   2,  2, 1, 3, 1)>]
#if NET461
[<Trait("KnownFailOn","Mono")>]
#endif
let ``ofPreviousTokenAndEventsLength - batchCapacityLimit`` (previousCompactionEventNumber : System.Nullable<int64>) streamVersion eventsLength batchSize expectedCapacity =
    let previousToken =
        if not previousCompactionEventNumber.HasValue then Token.ofCompactionEventNumber None 0 -84 null -42L
        else Token.ofCompactionEventNumber (Some previousCompactionEventNumber.Value) 0 -84 null -42L
    let _, _, batchCapacityLimit = unpack <| Token.ofPreviousTokenAndEventsLength previousToken eventsLength batchSize streamVersion
    test <@ Some expectedCapacity = batchCapacityLimit @>

[<Property>]
let ``Properties of tokens based on various generation mechanisms `` streamVersion (previousCompactionEventNumber : int64 option) eventsLength batchSize =
    let ovStreamVersion, ovCompactionEventNumber, ovBatchCapacityLimit =
        unpack <| Token.ofNonCompacting null streamVersion
    let uvStreamVersion, uvCompactionEventNumber, uvBatchCapacityLimit =
        unpack <| Token.ofUncompactedVersion batchSize null streamVersion
    let previousToken = Token.ofCompactionEventNumber previousCompactionEventNumber 0 -84 null -42L
    let peStreamVersion, peCompactionEventNumber, peBatchCapacityLimit =
        unpack <| Token.ofPreviousTokenAndEventsLength previousToken eventsLength batchSize streamVersion

    // StreamVersion
    test <@ streamVersion = ovStreamVersion @>
    test <@ streamVersion = uvStreamVersion @>
    test <@ streamVersion = peStreamVersion @>

    // CompactionEventNumber
    test <@ None = ovCompactionEventNumber @>
    test <@ None = uvCompactionEventNumber @>
    test <@ previousCompactionEventNumber = peCompactionEventNumber @>

    // BatchCapacityLimit
    test <@ None = ovBatchCapacityLimit @>
    let rawUncompactedBatchCapacityLimit batchSize streamVersion = batchSize - int streamVersion - 2
    let rawCompactedBatchCapacityLimit compactionEventNumber batchSize streamVersion = batchSize - int (streamVersion - compactionEventNumber) - 1
    test <@ Some (rawUncompactedBatchCapacityLimit batchSize streamVersion |> max 0) = uvBatchCapacityLimit @>
    let rawExpectedFromPreviousCompactionEventNumber =
        match previousCompactionEventNumber with
        | None -> rawUncompactedBatchCapacityLimit batchSize streamVersion
        | Some pci -> rawCompactedBatchCapacityLimit pci batchSize streamVersion
    test <@ Some (max 0 (rawExpectedFromPreviousCompactionEventNumber - eventsLength)) = peBatchCapacityLimit @>