namespace Tutorial.ClosingBooks

/// Buffers events accumulated from a series of decisions while evolving the presented `state` to reflect said proposed `Events`
type private Accumulator<'event, 'state>(originState, fold : 'state -> 'event seq -> 'state) =
    let pendingEvents = ResizeArray()
    let mutable state = originState

    let apply (events : 'event seq) =
        pendingEvents.AddRange events
        state <- fold state events

    /// Run a decision function, buffering and applying any Events yielded
    member _.Transact decide =
        let r, events = decide state
        apply events
        r

    /// Run an Async decision function, buffering and applying any Events yielded
    member _.TransactAsync decide = async {
        let! r, events = decide state
        apply events
        return r }

    /// Accumulated events based on the Decisions applied to date
    member _.Events = List.ofSeq pendingEvents

open FSharp.UMX
type EpochId = int<epochId>
and [<Measure>] epochId
module EpochId =
    let parse (value : int) : EpochId = %value
    let tryPrev (value : EpochId) : EpochId option = match %value with 0 -> None | x -> Some %(x - 1)
    let toString (value : EpochId) : string = string %value

module Epoch =

    let [<Literal>] Category = "Epoch"
    let streamName epochId = FsCodec.StreamName.create Category (EpochId.toString epochId)

    // NOTE - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
    module Events =

        type ItemIds = { items : string[] }
        type Balance = ItemIds
        type Event =
            | BroughtForward of Balance
            | Added of ItemIds
            | Removed of ItemIds
            | CarryForward of Balance
            interface TypeShape.UnionContract.IUnionContract
        let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()

    module Fold =

        type State = Initial | Open of items : OpenState | Closed of items : string[] * carryingForward : string[]
        and OpenState = string[]
        let initial : State = Initial
        let (|Items|) = function Initial -> [||] | Open i | Closed (i, _) -> i
        open Events
        let evolve (Items items) = function
            | BroughtForward e
            | Added e ->        Open (Array.append items e.items)
            | Removed e ->      Open (items |> Array.except e.items)
            | CarryForward e -> Closed (items, e.items)
        let fold = Seq.fold evolve

        /// Handles one-time opening of the Epoch, if applicable
        let maybeOpen (getIncomingBalance : unit -> Async<Balance>) state = async {
            match state with
            | Initial ->        let! balance = getIncomingBalance ()
                                return (), [BroughtForward balance]
            | Open _
            | Closed _ ->       return (), [] }

        /// Handles attempting to apply the request to the stream (assuming it's not already closed)
        /// The `decide` function can signal a need to close and/or split the request by emitting it as the residual
        let tryIngest (decide : 'req -> State -> 'req * 'result * Event list) req = function
            | Initial ->        failwith "Invalid tryIngest; stream not Open"
            | Open _ as s ->    let residual, result, events = decide req s
                                (residual, Some result), events
            | Closed _ ->       (req, None), []

        /// Yields or computes the Balance to be Carried forward and/or application of the event representing that decision
        let maybeClose (decideCarryForward : 'residual -> OpenState -> Async<Balance option>) residual state = async {
            match state with
            | Initial ->        return failwith "Invalid maybeClose; stream not Open"
            | Open s ->         let! cf = decideCarryForward residual s
                                let events = cf |> Option.map CarryForward |> Option.toList
                                return (residual, cf), events
            | Closed (_, b) ->  return (residual, Some { items = b }), [] }

    [<NoComparison; NoEquality>]
    type Rules<'request, 'result> =
        {   getIncomingBalance  : unit -> Async<Events.Balance>
            decideIngestion     : 'request -> Fold.State -> 'request * 'result * Events.Event list
            decideCarryForward  : 'request -> Fold.OpenState -> Async<Events.Balance option> }

    /// The result of the overall ingestion, consisting of
    type Result<'request, 'result> =
        {   /// residual of the request, in the event where it was not possible to ingest it completely
            residual            : 'request
            /// The result of the decision (assuming processing took place)
            result              : 'result option
            /// balance being carried forward in the event that the successor epoch has yet to have the BroughtForward event generated
            carryForward        : Events.Balance option }

    /// Decision function ensuring the high level rules of an Epoch are adhered to viz.
    /// 1. Streams must open with a BroughtForward event (obtained via Rules.getIncomingBalance if this is an uninitialized Epoch)
    /// 2. (If the Epoch has not closed) Rules.decide gets to map the request to events and a residual
    /// 3. Rules.decideCarryForward may trigger the closing of the Epoch based on the residual and the stream State by emitting Some balance
    let decideIngestWithCarryForward rules req s : Async<Result<'result, 'req> * Events.Event list> = async {
        let acc = Accumulator(s, Fold.fold)
        do! acc.TransactAsync(Fold.maybeOpen rules.getIncomingBalance)
        let residual, result = acc.Transact(Fold.tryIngest rules.decideIngestion req)
        let! residual, carryForward = acc.TransactAsync(Fold.maybeClose rules.decideCarryForward residual)
        return { result = result; residual = residual; carryForward = carryForward }, acc.Events
    }

    /// Manages Application of Request's to the Epoch's stream
    type Service(resolve : EpochId -> Equinox.Decider<Events.Event, Fold.State>) =

        let calcBalance state =
            let createEventsBalance items : Events.Balance = { items = items }
            async { return createEventsBalance state }
        let close (state : Fold.OpenState) = async {
            let! bal = calcBalance state
            return Some bal }

        /// Walks back as far as necessary to ensure preceding Epochs are Closed
        member private x.CloseUntil epochId : Async<Events.Balance> =
            let closePrecedingEpochs () =
                match EpochId.tryPrev epochId with
                | None -> calcBalance [||]
                | Some prevEpoch -> x.CloseUntil prevEpoch
            let rules : Rules<unit, unit> =
                {   getIncomingBalance = closePrecedingEpochs
                    decideIngestion = fun () _state -> (), (), []
                    decideCarryForward = fun () -> close }
            let decider = resolve epochId
            decider.TransactEx((fun c -> decideIngestWithCarryForward rules () c.State), fun r _c -> Option.get r.carryForward)

        /// Runs the decision function on the specified Epoch, closing and bringing forward balances from preceding Epochs if necessary
        member x.Transact(epochId, decide : Fold.State -> 'result * Events.Event list) : Async<Result<unit, 'result>> =
            let rules : Rules<unit, 'result> =
                {   getIncomingBalance = fun () -> x.CloseUntil epochId
                    decideIngestion = fun () state -> let result, events = decide state in (), result, events
                    decideCarryForward = fun _result _state -> async { return None } }
            let decider = resolve epochId
            decider.TransactEx((fun c -> decideIngestWithCarryForward rules () c.State), fun r _c -> r)
