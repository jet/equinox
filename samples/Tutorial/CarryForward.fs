namespace Tutorial.ClosingBooks

open FSharp.UMX

/// Buffers events accumulated from a series of decisions while evolving the presented `state` to reflect said proposed `Events`
type private Accumulator(originState, fold) =
    let mutable state = originState
    let pendingEvents = ResizeArray()

    /// Run a decision function, buffering and applying any Events yielded
    member _.Decide decide =
        let r, es = decide state
        state <- fold state (Seq.ofList es)
        r

    /// Run an Async decision function, buffering and applying any Events yielded
    member _.DecideAsync decide = async {
        let! r, es = decide state
        state <- fold state (Seq.ofList es)
        return r }

    /// Accumulated events based on the Decisions applied to date
    member _.Events = List.ofSeq pendingEvents

type EpochId = string<epochId>
and [<Measure>] epochId
module EpochId =
    let parse (value : string) : EpochId = %value
    let toString (value : EpochId) : string = %value

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

        type State = Initial | Open of items : string[] | Closed of items : string[] * carryingForward : string[]
        let initial : State = Initial
        open Events
        let (|Items|) = function Initial -> [||] | Open i | Closed (i, _) -> i
        let evolve (Items items) = function
            | BroughtForward e
            | Added e ->        Open (Array.append items e.items)
            | Removed e ->      Open (items |> Array.except e.items)
            | CarryForward e -> Closed (items, e.items)
        let fold = Seq.fold evolve

        /// Handles one-time opening of the Epoch, if applicable
        let maybeOpen (getIncomingBalance : unit -> Balance option) = function
            | Initial ->        (), getIncomingBalance () |> Option.map BroughtForward |> Option.toList
            | Open _
            | Closed _ ->       (), []

        /// Handles attempting to apply the request to the stream (assuming it's not already closed)
        /// The `decide` function can signal a need to close and/or split the request by emitting it as the residual
        let tryIngest (decide : State -> 'residual * Events.Event list) req = function
            | Initial
            | Open _ as s ->    decide s
            | Closed _ ->       req, []

        /// Yields or computes the Balance to be Carried forward and/or application of the event representing that decision
        let maybeClose (decideCarryForward : 'residual -> State -> Async<Balance option>) residual state = async {
            match state with
            | Initial
            | Open _ as s ->    let! cf = decideCarryForward residual s
                                let events = cf |> Option.map CarryForward |> Option.toList
                                return (residual, cf), events
            | Closed (_, b) ->  return (residual, Some { items = b }), [] }

    [<NoComparison; NoEquality>]
    type Rules<'residual> =
        {   getIncomingBalance  : unit -> Events.Balance option
            decideIngestion     : Fold.State -> 'residual * Events.Event list
            decideCarryForward  : 'residual -> Fold.State -> Async<Events.Balance option> }

    /// The result of the overall ingestion, consisting of
    type Result<'residual> =
        {   /// residual of the request, in the event where it was not possible to ingest it completely
            residual            : 'residual
            /// balance being carried forward in the event that the successor epoch has yet to have the BroughtForward event generated
            carryForward        : Events.Balance option }

    /// Decision function ensuring the high level rules of an Epoch are adhered to viz.
    /// 1. Streams may open with a BroughtForward event (decided by Rules.getIncomingBalance)
    /// 2. Rules.decide gets to map the request to events and a residual iff the stream, assuming the Epoch has not yet closed
    /// 3. Rules.decideCarryForward controls the closing of an Epoch based on either the residual or the stream State
    let decideIngestWithCarryForward rules req s : Async<Result<'residual> * Events.Event list> = async {
        let acc = Accumulator(s, Fold.fold)
        acc.Decide(Fold.maybeOpen rules.getIncomingBalance)
        let req' = acc.Decide(Fold.tryIngest rules.decideIngestion req)
        let! residual, carryForward = acc.DecideAsync(Fold.maybeClose rules.decideCarryForward req')
        return { residual = residual; carryForward = carryForward }, acc.Events
    }

    /// Manages Application of Request's to the Epoch's stream
    type Service(resolve : EpochId -> Equinox.Decider<Events.Event, Fold.State>) =

        member _.TryIngest(epochId, rules, req) : Async<Result<_>> =
            let decider = resolve epochId
            decider.TransactEx((fun c -> decideIngestWithCarryForward rules req c.State), fun r _c -> r)
