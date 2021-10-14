module Equinox.CosmosStore.Integration.AccessStrategies

open Swensen.Unquote
open System

[<AutoOpen>]
module WiringHelpers =

    let createDecider log stream = Equinox.Decider(log, stream, maxAttempts = 3)
    let private createCategoryUncached codec initial fold accessStrategy context =
        let noCachingCacheStrategy = Equinox.CosmosStore.CachingStrategy.NoCaching
        Equinox.CosmosStore.CosmosStoreCategory(context, codec, fold, initial, noCachingCacheStrategy, accessStrategy)
    let private createCategory codec initial fold accessStrategy (context, cache) =
        let sliding20mCacheStrategy = Equinox.CosmosStore.CachingStrategy.SlidingWindow (cache, TimeSpan.FromMinutes 20.)
        Equinox.CosmosStore.CosmosStoreCategory(context, codec, fold, initial, sliding20mCacheStrategy, accessStrategy)

    let createCategoryUnoptimizedUncached codec initial fold context =
        let accessStrategy = Equinox.CosmosStore.AccessStrategy.Unoptimized
        createCategoryUncached codec initial fold accessStrategy context

    let createCategoryUnoptimized codec initial fold (context, cache) =
        let accessStrategy = Equinox.CosmosStore.AccessStrategy.Unoptimized
        createCategory codec initial fold accessStrategy (context, cache)

module SequenceCheck =

    let streamName (id : Guid) = FsCodec.StreamName.create "_SequenceCheck" (id.ToString "N")

    module Events =

        type Event =
            | Add of {| value : int |}
            interface TypeShape.UnionContract.IUnionContract
        let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()

    module Fold =

        type State = int array
        let initial : State = [||]
        let evolve state = function
            | Events.Add e -> Array.append state [| e.value |]
        let fold state = Seq.fold evolve state

    let decide (value, count) (state : Fold.State) =
        if value = 0 || Array.last state = (value - 1)
        then List.init count (fun i -> Events.Add {| value = value + i |})
        else failwith $"Invalid Add of %d{value} to list %A{state}"

    type Service(resolve : Guid -> Equinox.Decider<Events.Event, Fold.State>) =

        member _.Read(instance : Guid) : Async<int array> =
            let decider = resolve instance
            decider.Query(id)

        member _.Add(instance : Guid, value : int, count) : Async<int array> =
            let decider = resolve instance
            decider.TransactEx((fun c -> async { return (), decide (value, count) c.State }), (fun () c -> c.State))

    let private create log resolveStream =
        let resolve = streamName >> resolveStream >> (createDecider log)
        Service(resolve)

    module Config =

        let createUncached log context =
            let cat = createCategoryUnoptimizedUncached Events.codec Fold.initial Fold.fold context
            create log cat.Resolve
        let create log (context, cache) =
            let cat = createCategoryUnoptimized Events.codec Fold.initial Fold.fold (context, cache)
            create log cat.Resolve

module Props =

    open FsCheck
    type EventsInTip = EventsInTip of int
    type EventCount = EventCount of int
    type GapGen =
        static member InTip = Gen.constant 5 |> Arb.fromGen
        static member EventCount = Gen.choose (0, 25) |> Arb.fromGen
    type FsCheckAttribute() =
    #if DEBUG
        inherit FsCheck.Xunit.PropertiesAttribute(MaxTest = 100, Arbitrary=[|typeof<GapGen>|])
    #else
        inherit FsCheck.Xunit.PropertiesAttribute(MaxTest = 1; Arbitrary=[|typeof<GapGen>|])
    #endif
        member val SkipIfRequestedViaEnvironmentVariable : string = null with get, set
        override __.Skip =
            match Option.ofObj __.SkipIfRequestedViaEnvironmentVariable |> Option.map Environment.GetEnvironmentVariable |> Option.bind Option.ofObj with
            | Some value when value.Equals(bool.TrueString, StringComparison.OrdinalIgnoreCase) ->
                sprintf "Skipped as requested via %s" __.SkipIfRequestedViaEnvironmentVariable
            | _ -> null

type UnoptimizedTipReadingCorrectness(testOutputHelper) =
    inherit TestsWithLogCapture(testOutputHelper)
    let log = base.Log

    let cache = Equinox.Cache("Test", sizeMb = 10)
    let createContext (Props.EventsInTip eventsInTip) =
        let queryMaxItems = 10
        createPrimaryContextEx log queryMaxItems eventsInTip

    [<Props.FsCheck(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let ``Can sync with competing writer with and without cache`` (instanceId, contextArgs, firstIsCached, Props.EventCount count1, Props.EventCount count2) = Async.RunSynchronously <| async {
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
        let expected = Array.init count1 (fun i -> count1 + i)
        test <@ expected = s1'' @>

        let! s2'' = service2.Read instanceId
        test <@ s1'' = s2'' @> }
