module Location.Epoch

// NOTE - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
[<RequireQualifiedAccess>]
module Events =

    type CarriedForward = { initial : int }
    type Delta = { value : int }
    type Event =
        | CarriedForward of CarriedForward
        | Closed
        | Delta of Delta
        interface TypeShape.UnionContract.IUnionContract
    let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()
    let [<Literal>] categoryId = "LocationEpoch"

module Folds =

    type Balance = int
    type OpenState = { count : int; value : Balance }
    type State = Initial | Open of OpenState | Closed of Balance
    let initial = Initial
    let evolve state event =
        match event, state with
        | Events.CarriedForward e, Initial -> Open { count = 0; value = e.initial }
        | Events.Delta e, Open bal -> Open { count = bal.count + 1; value = bal.value + e.value }
        | Events.Closed, Open { value = bal } -> Closed bal
        | Events.CarriedForward _, (Open _|Closed _ as x) -> failwithf "CarriedForward : Unexpected %A" x
        | Events.Delta _, (Initial|Closed _ as x) -> failwithf "Delta : Unexpected %A" x
        | Events.Closed, (Initial|Closed _ as x) -> failwithf "Closed : Unexpected %A" x
    let fold = Seq.fold evolve

/// Holds events accumulated from a series of decisions while also evolving the presented `state` to reflect the pended events
type private Accumulator() =
    let acc = ResizeArray()
    member __.Ingest state : 'res * Events.Event list -> 'res * Folds.State = function
        | res, [] ->                   res, state
        | res, [e] -> acc.Add e;       res, Folds.evolve state e
        | res, xs ->  acc.AddRange xs; res, Folds.fold state (Seq.ofList xs)
    member __.Accumulated = List.ofSeq acc

type Result<'t> = { balance : Folds.Balance; result : 't option; isOpen : bool }

let sync (balanceCarriedForward : Folds.Balance option) (decide : (Folds.Balance -> 't*Events.Event list)) shouldClose state : Result<'t>*Events.Event list =
    let acc = Accumulator()
    // We always want to have a CarriedForward event at the start of any Epoch's event stream
    let (), state =
        acc.Ingest state <|
            match state with
            | Folds.Initial -> (), [Events.CarriedForward { initial = Option.get balanceCarriedForward }]
            | Folds.Open _ | Folds.Closed _ -> (), []
    // Run, unless we determine we're in Closed state
    let result, state =
        acc.Ingest state <|
            match state with
            | Folds.Initial -> failwith "We've just guaranteed not Initial"
            | Folds.Open { value = bal } -> let r,es = decide bal in Some r,es
            | Folds.Closed _ -> None, []
    // Finally (iff we're `Open`, have run a `decide` and `shouldClose`), we generate a Closed event
    let (balance, isOpen), _ =
        acc.Ingest state <|
            match state with
            | Folds.Initial -> failwith "Can't be Initial"
            | Folds.Open ({ value = bal } as openState) when shouldClose openState -> (bal, false), [Events.Closed]
            | Folds.Open { value = bal } -> (bal, true), []
            | Folds.Closed bal -> (bal, false), []
    { balance = balance; result = result; isOpen = isOpen }, acc.Accumulated

type Service internal (resolve, ?maxAttempts) =

    let log = Serilog.Log.ForContext<Service>()
    let (|AggregateId|) (locationId, epochId) =
        let id = sprintf "%s_%s" (LocationId.toString locationId) (LocationEpochId.toString epochId)
        Equinox.AggregateId(Events.categoryId, id)
    let (|Stream|) (AggregateId id) = Equinox.Stream<Events.Event,Folds.State>(log, resolve id, maxAttempts = defaultArg maxAttempts 2)

    member __.Sync<'R>(locationId, epochId, prevEpochBalanceCarriedForward, decide, shouldClose) : Async<Result<'R>> =
        let (Stream stream) = (locationId, epochId)
        stream.Transact(sync prevEpochBalanceCarriedForward decide shouldClose)

let create resolve maxAttempts = Service(resolve, maxAttempts)

module Cosmos =

    open Equinox.Cosmos
    let resolve (context,cache) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        Resolver(context, Events.codec, Folds.fold, Folds.initial, cacheStrategy, AccessStrategy.LatestKnownEvent).Resolve
    let createService (context,cache,maxAttempts) =
        create (resolve (context,cache)) maxAttempts