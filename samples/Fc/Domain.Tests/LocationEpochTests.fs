module LocationEpochTests

open FsCheck.Xunit
open Location.Epoch
open Swensen.Unquote

let interpret delta _balance =
    match delta with
    | 0 -> (),[]
    | delta -> (),[Events.Delta { value = delta }]

let validateAndInterpret expectedBalance delta balance =
    test <@ expectedBalance = balance @>
    interpret delta balance

let verifyDeltaEvent delta events =
    let dEvents = events |> List.filter (function Events.Delta _ -> true | _ -> false)
    test <@ interpret delta (Unchecked.defaultof<_>) = ((),dEvents) @>

let [<Property>] properties carriedForward delta1 closeImmediately delta2 close =

    (* Starting with an empty stream, we'll need to supply the balance carried forward, optionally we apply a delta and potentially close *)

    let initialShouldClose _state = closeImmediately
    let res,events = sync (Some carriedForward) (validateAndInterpret carriedForward delta1) initialShouldClose Folds.initial
    let cfEvents events = events |> List.filter (function Events.CarriedForward _ -> true | _ -> false)
    let closeEvents events = events |> List.filter (function Events.Closed -> true | _ -> false)
    let state1 = Folds.fold Folds.initial events
    let expectedBalance = carriedForward + delta1
    // Only expect closing if it was requested
    let expectImmediateClose = closeImmediately
    test <@ Option.isSome res.result
            && expectedBalance = res.balance @>
    test <@ [Events.CarriedForward { initial = carriedForward }] = cfEvents events
            && (not expectImmediateClose || 1 = Seq.length (closeEvents events)) @>
    verifyDeltaEvent delta1 events

    (* After initializing, validate we don't need to supply a carriedForward, and don't produce a CarriedForward event *)

    let shouldClose _state = close
    let { isOpen = isOpen; result = worked; balance = bal },events = sync None (validateAndInterpret expectedBalance delta2) shouldClose state1
    let expectedBalance = if expectImmediateClose then expectedBalance else expectedBalance + delta2
    test <@ [] = cfEvents events
            && (expectImmediateClose || not close || 1 = Seq.length (closeEvents events)) @>
    test <@ (expectImmediateClose || close || isOpen)
            && expectedBalance = bal @>
    if not expectImmediateClose then
        test <@ Option.isSome worked @>
        verifyDeltaEvent delta2 events

let [<Property>] ``codec can roundtrip`` event =
    let ee = Events.codec.Encode(None,event)
    let ie = FsCodec.Core.TimelineEvent.Create(0L, ee.EventType, ee.Data)
    test <@ Some event = Events.codec.TryDecode ie @>