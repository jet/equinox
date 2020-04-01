module Samples.Store.Domain.Tests.SavedForLaterTests

open Domain
open Domain.SavedForLater
open Domain.SavedForLater.Fold
open Swensen.Unquote.Assertions
open System
open System.Collections.Generic
open Xunit

(* Test execution helpers *)

let decide cmd state: bool * Events.Event list =
    decide Int32.MaxValue cmd state
let interpret cmd state: Events.Event list =
    decide cmd state |> snd
let run (commands : Command list) : State * Events.Event list =
    let folder (s,eAcc) c =
        let events = interpret c s
        fold s events, eAcc @ events
    List.fold folder (initial,[]) commands

(* State extraction helpers *)

let contains sku (state : State) = state |> Array.exists (fun s -> s.skuId = sku)
let find sku (state : State) = state |> Array.find (fun s -> s.skuId = sku)

let genSku () = Guid.NewGuid() |> SkuId

[<Fact>]
let ``Adding one item to mysaves should appear in aggregate`` () =
    let sku = genSku()
    let state',_ = run [ Add(DateTimeOffset.Now, [|sku|]) ]
    test <@ state'.Length = 1
            && (state' |> contains sku) @>

[<Fact>]
let ``Adding two items to mysaves should appear in aggregate`` () =
    let sku1, sku2 = genSku(), genSku()
    let state',_ = run [ Add(DateTimeOffset.Now, [|sku1; sku2|])]
    test <@ state'.Length = 2
            && (state' |> contains sku1)
            && (state' |> contains sku2) @>

[<Fact>]
let ``Added items should record date of addition`` () =
    let sku1, sku2 = genSku(), genSku()
    let date = DateTimeOffset.Now
    let state',_ = run [ Add(date, [|sku1; sku2|])]
    test <@ state'.Length = 2
            && date = (state' |> find sku1).dateSaved
            && date = (state' |> find sku2).dateSaved @>

[<DomainProperty>]
let ``Adding the same sku many times should surface the most recent date`` (dates : DateTimeOffset []) =
    match dates with
    | null | [||] -> ()
    | _ ->

    let mostRecentDate = Array.max dates

    let sku = genSku()
    let folder s d =
        let c = Add (d,[|sku|])
        interpret c s |> fold s
    let state' = dates |> Array.fold folder initial
    test <@ ({ skuId = sku; dateSaved = mostRecentDate } : Events.Item) = Array.exactlyOne state' @>

[<DomainProperty>]
let ``Commands that push saves above the limit should fail to process`` (state : State) (command : Command) =
    let maxItems = state.Length
    let result = SavedForLater.decide maxItems command state
    test <@ match result with
            | true, events -> (fold state events).Length <= maxItems
            | false, _ -> true @>

[<DomainProperty>]
let ``Event aggregation should carry set semantics`` (commands : Command list) =
    // fold over events using regular set semantics and compare outcomes
    let evolveSet (state : HashSet<SkuId>) (event : Events.Event) =
        match event with
        | Events.Added appended -> state.UnionWith appended.skus
        | Events.Removed removed -> state.ExceptWith removed.skus
        | Events.Merged merged -> state.UnionWith (merged.items |> Seq.map (fun s -> s.skuId))
        | Events.Compacted compacted ->
            state.Clear()
            state.UnionWith (compacted.items |> Seq.map (fun s -> s.skuId))
        state

    let state',events = run commands
    let expectedSkus = events |> List.fold evolveSet (HashSet())
    let actualSkus = seq { for item in state' -> item.skuId }
    test <@ expectedSkus.SetEquals actualSkus @>

[<DomainProperty>]
let ``State should produce a stable output for skus with the same saved date`` (skus : SkuId []) =
    let now = DateTimeOffset.Now

    let shuffledSkus =
        let rnd = new Random()
        skus |> Array.sortBy (fun _ -> rnd.Next())

    let getSimplifiedState skus =
        let state',_ = run [ Add(now, skus)]
        [| for item in state' -> item.skuId |]

    let skusState = getSimplifiedState skus
    let shuffledSkusState = getSimplifiedState shuffledSkus

    test <@ skusState = shuffledSkusState @>

module Specification =
    let mkAppendDated d skus = if Array.isEmpty skus then [] else [ Events.Added { dateSaved = d; skus = skus }]
    let mkMerged items = [ Events.Merged { items = items } ]

    /// Processing should allow for any given Command to be retried at will, without avoidable side-effects
    let verifyIdempotency (command: Command) state =
        // Establish a state where the command should not trigger an event
        let eventsToEstablish: Events.Event list = command |> function
            | Add (d,skus) ->      mkAppendDated d skus
            | Merge items ->       mkMerged items
            | Remove _ ->          []
        let state = fold state eventsToEstablish
        let events = interpret command state
        // Assert we decided nothing needs to happen
        test <@ List.isEmpty events @>

    let (|TakeHalf|) items = items |> Seq.mapi (fun i x -> if i % 2 = 0 then Some x else None) |> Seq.choose id |> Seq.toArray
    let mkAppend skus = mkAppendDated DateTimeOffset.Now skus
    let asSkus (s : State) = seq { for x in s -> x.skuId }
    let asSkuToState (s : State) = seq { for x in s -> x.skuId, x } |> dict

    /// Put the aggregate into a state where the command should trigger an event; verify correct state achieved and correct events yielded
    let verify variant command originState =
        let initialEvents: Events.Event list =
            match command, variant with
            | Remove skus, Choice1Of2 randomSkus ->            mkAppend <| Array.append skus randomSkus
            | Remove (TakeHalf skus), Choice2Of2 randomSkus -> mkAppend <| Array.append skus randomSkus
            | Add (d,_skus), Choice1Of2 randomSkus ->          mkAppendDated d randomSkus
            | Add (d,TakeHalf skus), Choice2Of2 moreSkus ->    mkAppendDated d skus @ mkAppendDated (let n = DateTimeOffset.Now in n.AddDays -1.) moreSkus
            | Merge items, Choice1Of2 randomSkus ->            mkAppend randomSkus @ mkMerged items
            | Merge (TakeHalf items), Choice2Of2 randomSkus -> mkAppend randomSkus @ mkMerged items
        let state = fold originState initialEvents
        let events = interpret command state
        let state' = fold state events
        match command, events with
        // Remove command that resulted in no action
        | Remove requested,    [] ->
            let original, remaining = state |> asSkus |> set, state' |> asSkus |> set
            // Verify there definitely wasn't anything to do
            test <@ requested |> Seq.forall (not << original.Contains) @>
            // Verify there still isn't anything to do
            test <@ requested |> Seq.forall (not << remaining.Contains) @>
        // A removal event should be optimal, with no redundant skus
        | Remove requested,    [ Events.Removed { skus = removed } ] ->
            let original, remaining = state |> asSkus |> set, state' |> asSkus
            let requested, removed = set requested, set removed
            // Verify the request maps to the event correctly
            test <@ Set.isSuperset requested removed
                    && Set.isSuperset original removed @>
            // Verify there's nothing let to do
            test <@ remaining |> Seq.forall (not << requested.Contains) @>
        // Any append event should reflect only variances from the current content
        | Add (date,skus),  events ->
            let original, updated = state |> asSkus, state' |> asSkuToState
            // Verify the request maps to the event (or absence thereof) correctly
            match events with
            | [] -> ()
            | [Events.Added e] ->
                test <@ e.dateSaved = date
                        && e.skus |> Seq.forall updated.ContainsKey @>
            | x -> x |> failwithf "Unexpected %A"
            // Verify the post state is correct and there is no remaining work
            let updatedIsSameOrNewerThan date sku = not (updated.[sku] |> Fold.isSupersededAt date)
            test <@ original |> Seq.forall updated.ContainsKey
                    && skus |> Seq.forall (updatedIsSameOrNewerThan date) @>
        // Any merge event should onl reflect variances from current contet
        | Merge donorState,    events ->
            let original, updated = state |> asSkuToState, state' |> asSkuToState
            // Verify the request maps to the event (or absence thereof) correctly
            match events with
            | [] -> ()
            | [Events.Merged e] ->
                let originalIsSupersededByMerged (item : Events.Item) =
                    match original.TryGetValue item.skuId with
                    | true, originalItem -> originalItem |> Fold.isSupersededAt item.dateSaved
                    | false, _ -> true
                test <@ e.items |> Seq.forall originalIsSupersededByMerged @>
            | x -> x |> failwithf "Unexpected %A"
            // Verify the post state is correct and there is no remaining work
            let updatedIsSameOrNewerThan (item : Events.Item) = not (updated.[item.skuId] |> Fold.isSupersededAt item.dateSaved)
            let combined = Seq.append state donorState |> Array.ofSeq
            let combinedSkus = combined |> asSkus |> set
            test <@ combined |> Seq.forall updatedIsSameOrNewerThan
                    && updated.Keys |> Seq.forall combinedSkus.Contains @>
        | c,e -> failwithf "Invalid result - Command %A yielded Events %A in State %A" c e state

    [<DomainProperty>]
    let ``Command -> Event -> State flows`` variant (cmd: Command) (state : State) =
        //verifyIdempotency cmd state
        verify variant cmd state
