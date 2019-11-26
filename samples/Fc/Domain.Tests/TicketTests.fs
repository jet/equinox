module TicketTests

open FsCheck.Xunit
open Swensen.Unquote
open Ticket
open Ticket.Folds

/// We want to generate Allocate requests with and without the same listId in some cases
let (|MaybeSameCommands|) = function
    | Allocate _ as x, Allocate _, cmd3, Choice1Of2 () -> x, x, cmd3
    | cmd1, (Allocate _ as x), Allocate _, Choice1Of2 () -> cmd1, x, x
    | cmd1, cmd2, cmd3, (Choice1Of2 ()|Choice2Of2 ()) -> cmd1, cmd2, cmd3

/// Explicitly generate sequences with the same allocator running twice or three times
let (|MaybeSameIds|) = function
    | Choice1Of4 a -> a, a, a
    | Choice2Of4 (a,b) -> a, a, b
    | Choice3Of4 (a,b) -> a, b, b
    | Choice4Of4 (a,b,c) -> a, b, c

let (|Invariants|) = function
    // Revokes always succeed iff Unallocated
    | Unallocated, Revoke, true, [], Unallocated ->
        ()
    // Everything else fails
    | _, _, res, e, _ ->
        test <@ not res && List.isEmpty e @>

let (|ReservedCases|_|) allocator = function
    // Reserve given unallocated
    | Unallocated, Reserve, true, [Events.Reserved { allocatorId = a }], state ->
        test <@ a = allocator && state = Reserved a  @>
        Some ()
    // Idempotent reserve request
    | Reserved a, Reserve, true, [], _ ->
        test <@ a = allocator @>
        Some ()
    // Revokes not by the owner are reported as successful, but we force the real owner to do the real relinquish
    | (Reserved by | Allocated(by,_)), Revoke, true, [], _ ->
        test <@ by <> allocator @>
        Some ()
    // Revokes succeed iff by the owner
    | (Reserved by | Allocated(by,_)), Revoke, true, [Events.Revoked], Unallocated ->
        test <@ by = allocator @>
        Some ()
    // Reservations can transition to Allocations as long as it's the same Allocator requesting
    | Reserved a, Allocate l, true, [Events.Allocated { allocatorId = ea; listId = el }], Allocated (sa,sl) ->
        test <@ a = allocator && a = ea && a = sa && l = el && l = sl @>
        Some()
    | _ -> None

let [<Property>] properties (MaybeSameIds (a1,a2,a3)) (MaybeSameCommands (c1,c2,c3)) =
    let res, events = decide a1 c1 Folds.initial
    let state1 = Folds.fold Folds.initial events

    match Folds.initial, c1, res, events, state1 with
    | _, Reserve, true, [Events.Reserved { allocatorId = a }], Reserved sa ->
        test <@ a = a1 && sa = a1  @>
    | Invariants -> ()

    let res, events = decide a2 c2 state1
    let state2 = Folds.fold state1 events
    match state1, c2, res, events, state2 with
    | ReservedCases a2 -> ()
    | Invariants -> ()

    let res, events = decide a3 c3 state2
    let state3 = Folds.fold state2 events
    match state2, c3, res, events, state3 with
    // Idempotent allocate ignore
    | Allocated (a,l), Allocate l3, true, [], _ ->
        test <@ a = a3 && l = l3 @>
    // Allocated -> Revoked
    | Allocated (a,_), Revoke, true, [Events.Revoked], Unallocated ->
        test <@ a = a3 @>
    | ReservedCases a3 -> ()
    | Invariants -> ()

let [<Property>] ``codec can roundtrip`` event =
    let ee = Events.codec.Encode(None,event)
    let ie = FsCodec.Core.TimelineEvent.Create(0L, ee.EventType, ee.Data)
    test <@ Some event = Events.codec.TryDecode ie @>