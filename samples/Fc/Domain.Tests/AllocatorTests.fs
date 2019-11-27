module AllocatorTests

open Allocator
open FsCheck.Xunit
open Swensen.Unquote
open System

type Command =
    | Commence of AllocationId * DateTimeOffset
    | Complete of AllocationId * Events.Reason

type Result =
    | Accepted
    | Conflict of AllocationId

let execute cmd state =
    match cmd with
    | Commence (a,c) ->
        match decideCommence a c state with
        | CommenceResult.Accepted, es ->  Accepted,es
        | CommenceResult.Conflict a, es -> Conflict a,es
    | Complete (a,r) -> let es = decideComplete a r state in Accepted, es

let [<Property>] properties c1 c2 =
    let res,events = execute c1 Folds.initial
    let state1 = Folds.fold Folds.initial events
    match c1, res, events, state1 with
    | Commence (a,c), Accepted, [Events.Commenced ({ allocationId = ea; cutoff = ec } as e)], state ->
        test <@ a = ea && c = ec && state = Some e @>
    | Complete _, Accepted, [], None ->
        () // Non-applicable Complete requests are simply ignored
    | _, res, l, _ ->
        test <@ List.isEmpty l && res = Accepted @>

    let res,events = execute c2 state1
    let state2 = Folds.fold state1 events
    match state1, c2, res, events, state2 with
    // As per above, normal commence
    | None, Commence (a,c), Accepted, [Events.Commenced ({ allocationId = ea; cutoff = ec } as e)], state ->
        test <@ a = ea && c = ec && state = Some e @>
    // Idempotent accept if same allocationId
    | Some active as s1, Commence (a,_), Accepted, [], s2 ->
        test <@ s1 = s2 && active.allocationId = a @>
    // Conflict reports owner allocator
    | Some active as s1, Commence (a2,_), Conflict a1, [], s2 ->
        test <@ s1 = s2 && a2 <> a1 && a1 = active.allocationId @>
    // Correct complete for same allocator is accepted
    | Some active, Complete (a,r), Accepted, [Events.Completed { allocationId = ea; reason = er }], None ->
        test <@ er = r && ea = a && active.allocationId = a @>
    // Completes not for the same allocator are ignored
    | Some active as s1, Complete (a,_), Accepted, [], s2 ->
        test <@ active.allocationId <> a && s2 = s1 @>
    | _, _, res, l, _ ->
        test <@ List.isEmpty l && res = Accepted @>

let [<Property>] ``codec can roundtrip`` event =
    let ee = Events.codec.Encode(None,event)
    let ie = FsCodec.Core.TimelineEvent.Create(0L, ee.EventType, ee.Data)
    test <@ Some event = Events.codec.TryDecode ie @>