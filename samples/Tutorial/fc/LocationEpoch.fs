module Fc.Location.Epoch

open Fc

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
    type State = Initial | Open of Balance | Closed of Balance
    let initial = Initial
    let evolve state event =
        match event,state with
        | Events.CarriedForward e,Initial -> Open e.initial
        | Events.Delta e,Open bal -> Open (bal + e.value)
        | Events.Closed,Open bal -> Closed bal
        | Events.CarriedForward _,(Open _|Closed _ as x) -> failwithf "CarriedForward : Unexpected %A" x
        | Events.Delta _,(Initial|Closed _ as x) -> failwithf "Delta : Unexpected %A" x
        | Events.Closed,(Initial|Closed _ as x) -> failwithf "Closed : Unexpected %A" x
    let fold = Seq.fold evolve

type Result = { balance : Folds.Balance; worked : bool; isClosed : bool }

let sync (balanceCarriedForward : Folds.Balance option) (interpret : (Folds.Balance -> Events.Event list) option) shouldClose state : Result*Events.Event list =
    let acc = ResizeArray()
    let stashAndFold state = function
        | res,[] -> res,state
        | res,[e] -> acc.Add e; res,Folds.evolve state e
        | res,xs -> acc.AddRange xs; res,Folds.fold state (Seq.ofList xs)
    // We always want to have a CarriedForward event at the start of any Epoch's event stream
    let (),state =
        match state with
        | Folds.Initial -> (),[Events.CarriedForward { initial = balanceCarriedForward |> Option.defaultValue 0 }]
        | Folds.Open _ | Folds.Closed _ -> (),[]
        |> stashAndFold state
    // If an `interpret` is supplied, we run that (unless we determine we're in Closed state)
    let worked,state =
        match state, interpret with
        | Folds.Initial,_ -> failwith "We've just guaranteed not Initial"
        | Folds.Open _,None -> false,[]
        | Folds.Open bal,Some interpret -> true,interpret bal
        | Folds.Closed _,_ -> false,[]
        |> stashAndFold state
    /// Finally (iff we're `Open`, have `worked`, and `shouldClose`, we generate a Closed event)
    let (balance,isClosed),_ =
        match state with
        | Folds.Initial -> failwith "Can't be Initial"
        | Folds.Open bal as state when worked && shouldClose state -> (bal,true),[Events.Closed]
        | Folds.Open bal -> (bal,false),[]
        | Folds.Closed bal -> (bal,true),[]
        |> stashAndFold state
    { balance = balance; worked = worked; isClosed = isClosed },Seq.toList acc

type Service internal (resolve, ?maxAttempts) =

    let log = Serilog.Log.ForContext<Service>()
    let (|AggregateId|) (locationId,epochId) =
        let id = sprintf "%s_%s" (LocationId.toString locationId) (LocationEpochId.toString epochId)
        Equinox.AggregateId(Events.categoryId, id)
    let (|Stream|) (AggregateId id) = Equinox.Stream<Events.Event,Folds.State>(log, resolve id, maxAttempts = defaultArg maxAttempts 2)
    let decide (Stream stream) : (Folds.State -> 'r * Events.Event list) -> Async<'r> = stream.Transact

    member __.Sync(locationId,epochId,prevEpochBalanceCarriedForward,interpret,shouldClose) : Async<Result> =
        decide (locationId,epochId) (sync prevEpochBalanceCarriedForward interpret shouldClose)

let createService resolve = Service(resolve)

module Cosmos =

    open Equinox.Cosmos
    let resolve (context,cache) =
        let cacheStrategy = CachingStrategy.SlidingWindow (cache, System.TimeSpan.FromMinutes 20.)
        Resolver(context, Events.codec, Folds.fold, Folds.initial, cacheStrategy, AccessStrategy.LatestKnownEvent).Resolve
    let createService (context,cache) =
        createService (resolve (context,cache))