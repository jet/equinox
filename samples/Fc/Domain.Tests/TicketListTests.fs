module TicketListTests

open FsCheck.Xunit
open Swensen.Unquote
open TicketList

let [<Property>] properties c1 c2 =
    let events = interpret c1 Folds.initial
    let state1 = Folds.fold Folds.initial events
    match c1, events, state1 with
    // Empty request -> no Event
    | (_,[]), [], state ->
        test <@ Set.isEmpty state @>
    | (a,t), [Events.Allocated { allocatorId = ea; ticketIds = et }], state ->
        test <@ a = ea @>
        test <@ state = set t @>
        test <@ state = set et @>
    | _, l, _ ->
        test <@ List.isEmpty l @>

    let events = interpret c2 state1
    let state2 = Folds.fold state1 events
    test <@ Folds.fold state2 [Folds.snapshot state2] = state2 @>
    match state1, c2, events, state2 with
    // Empty request -> no Event, same state
    | s1, (_,[]), [], state ->
        test <@ state = s1 @>
    // Redundant request -> No Event, same state
    | s1, (_,t), [], _ ->
        test <@ Set.isSuperset s1 (set t) @>
    // Two consecutive commands should both manifest in the state
    | s1, (a,t), [Events.Allocated { allocatorId = ea; ticketIds = et }], state ->
        test <@ a = ea @>
        let et = Set et
        test <@ Set.isSuperset (set t) et @>
        test <@ Set.intersect s1 et |> Set.isEmpty @>
        test <@ Set.isSuperset state s1 @>
        test <@ Set.isSuperset state et @>
    | _, _, l, _ ->
        test <@ List.isEmpty l @>

let [<Property>] ``codec can roundtrip`` event =
    let ee = Events.codec.Encode(None,event)
    let ie = FsCodec.Core.TimelineEvent.Create(0L, ee.EventType, ee.Data)
    test <@ Some event = Events.codec.TryDecode ie @>