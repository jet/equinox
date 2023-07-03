module Equinox.Store.Integration.AccessStrategies

open Equinox.Core
#if STORE_DYNAMO
open Equinox.DynamoStore
open Equinox.DynamoStore.Core
open Equinox.DynamoStore.Integration.CosmosFixtures
#else
open Equinox.CosmosStore
open Equinox.CosmosStore.Core
open Equinox.CosmosStore.Integration.CosmosFixtures
#endif
open Swensen.Unquote
open System
open Xunit

[<AutoOpen>]
module WiringHelpers =

    let private createCategoryUncached codec initial fold accessStrategy context =
        let noCachingCacheStrategy = CachingStrategy.NoCaching
        StoreCategory(context, codec, fold, initial, noCachingCacheStrategy, accessStrategy)
    let private createCategory codec initial fold accessStrategy (context, cache) =
        let sliding20mCacheStrategy = CachingStrategy.SlidingWindow (cache, TimeSpan.FromMinutes 20.)
        StoreCategory(context, codec, fold, initial, sliding20mCacheStrategy, accessStrategy)

    let createCategoryUnoptimizedUncached codec initial fold context =
        let accessStrategy = AccessStrategy.Unoptimized
        createCategoryUncached codec initial fold accessStrategy context

    let createCategoryUnoptimized codec initial fold (context, cache) =
        let accessStrategy = AccessStrategy.Unoptimized
        createCategory codec initial fold accessStrategy (context, cache)

/// Test Aggregation used to validate that the reading logic in Equinox.CosmosStore correctly reads, deserializes and folds the events
/// This is especially relevant when events are spread between a Tip page and preceding pages as the Tip reading logic is special cased compared to querying
module SequenceCheck =

    let [<Literal>] Category = "_SequenceCheck"
    let streamId = Equinox.StreamId.gen (fun (g : Guid) -> g.ToString "N")

    module Events =

        type Event =
            | Add of {| value : int |}
            interface TypeShape.UnionContract.IUnionContract
#if STORE_DYNAMO
        let codec = FsCodec.SystemTextJson.Codec.Create<Event>() |> FsCodec.Deflate.EncodeTryDeflate
#else
        let codec = FsCodec.SystemTextJson.CodecJsonElement.Create<Event>()
#endif

    module Fold =

        type State = int[]
        let initial : State = [||]
        let evolve state = function
            | Events.Add e -> Array.append state [| e.value |]
        let fold state = Seq.fold evolve state

    let decide (value, count) (state : Fold.State) =
        if (value = 0 && Array.isEmpty state) || Array.last state = (value - 1)
        then List.init count (fun i -> Events.Add {| value = value + i |})
        else failwith $"Invalid Add of %d{value} to list %A{state}"

    type Service(resolve : Guid -> Equinox.Decider<Events.Event, Fold.State>) =

        member _.Read(instance : Guid) : Async<int[]> =
            let decider = resolve instance
            decider.Query(id)

        member _.Add(instance : Guid, value : int, count) : Async<int[]> =
            let decider = resolve instance
            decider.Transact(decide (value, count), id)

    let private create resolve =
        Service(streamId >> resolve Category)

    module Config =

        let createUncached log context =
            createCategoryUnoptimizedUncached Events.codec Fold.initial Fold.fold context |> Equinox.Decider.resolve log |> create
        let create log (context, cache) =
            createCategoryUnoptimized Events.codec Fold.initial Fold.fold (context, cache) |> Equinox.Decider.resolve log |> create

module Props =
    open FsCheck
    open FsCheck.FSharp
    type EventsInTip = EventsInTip of int
    type EventCount = EventCount of int
    type GapGen =
        static member InTip : Arbitrary<EventsInTip> = Gen.constant 5 |> Gen.map EventsInTip |> Arb.fromGen
        static member EventCount : Arbitrary<EventCount> = Gen.choose (0, 25) |> Gen.map EventCount |> Arb.fromGen
    #if DEBUG
    let [<Literal>] maxTest = 100
    #else
    // In release mode, don't run quite as many cases of the test
    let [<Literal>] maxTest = 5
    #endif
    type FsCheckAttribute() =
        inherit AutoDataAttribute(MaxTest = maxTest, Arbitrary=[|typeof<GapGen>|])

[<Xunit.Collection "DocStore">]
type UnoptimizedTipReadingCorrectness(testOutputHelper) =
    let output = TestOutput(testOutputHelper)
    let log = output.CreateLogger()

    let cache = Equinox.Cache("Test", sizeMb = 10)
    let createContext (Props.EventsInTip eventsInTip) =
        let queryMaxItems = 10
        createPrimaryContextEx log queryMaxItems eventsInTip

    /// This test compares the experiences of cached and uncached paths to reading the same data within a given stream
    /// This is in order to shake out bugs and/or variation induced by the presence of stale state in the cache entry
    [<Props.FsCheck(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``Can sync with competing writer with and without cache`` (instanceId, contextArgs, firstIsCached, Props.EventCount count1, Props.EventCount count2) = async {
        let context = createContext contextArgs
        let service1, service2 =
            let uncached = SequenceCheck.Config.createUncached log context
            let cached = SequenceCheck.Config.create log (context, cache)
            if firstIsCached then cached, uncached
            else uncached, cached

        let! s1 = service1.Read instanceId
        test <@ Array.isEmpty s1 @>

        let! s2' = service2.Add(instanceId, 0, count1)
        let expected = Array.init count1 id
        test <@ expected = s2' @>

        let! s1' = service1.Read instanceId
        test <@ s2' = s1' @>

        let! s1'' = service1.Add(instanceId, count1, count2)
        let expected = Array.init (count1 + count2) id
        test <@ expected = s1'' @>

        let! s2'' = service2.Read instanceId
        test <@ s1'' = s2'' @> }


module Token =
#if STORE_DYNAMO
    let getPos index = { index = index; etag = ""; calvedBytes = 0; baseBytes = 0; unfoldsBytes = 0; events = Array.empty }
#else
    let getPos index = { index = index; etag = None }
#endif
    let [<Fact>] ``Candidate is not stale if we have no current`` () =
        let emptyToken = Unchecked.defaultof<StreamToken>
        let pos = getPos 0
        let candidate = Token.create pos

        test <@ false = Token.isStale emptyToken candidate @>

    let [<Fact>] ``Candidate is not stale if higher index than current`` () =
        let pos1 = getPos 1
        let pos2 = getPos 2
        let current = Token.create pos1
        let candidate = Token.create pos2

        test <@ false = Token.isStale current candidate @>

    let [<Fact>] ``Candidate is stale if lower index than current`` () =
        let pos1 = getPos 1
        let pos2 = getPos 2
        let current = Token.create pos2
        let candidate = Token.create pos1

        test <@ true = Token.isStale current candidate @>
