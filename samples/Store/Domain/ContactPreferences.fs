module Domain.ContactPreferences

type ClientId = ClientId of email: string
module ClientId = let toString (ClientId email) = email

module Stream =
    let [<Literal>] Category = "ContactPreferences"
    let id = FsCodec.StreamId.gen ClientId.toString // TODO hash >> base64
    let name = id >> FsCodec.StreamName.create Category

// NOTE - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
module Events =

    type Preferences = { manyPromotions: bool; littlePromotions: bool; productReview: bool; quickSurveys: bool }
    type Value = { email: string; preferences: Preferences }

    type Event =
        | [<System.Runtime.Serialization.DataMember(Name = "contactPreferencesChanged")>]Updated of Value
        interface TypeShape.UnionContract.IUnionContract
    let codec = EventCodec.gen<Event>
    let codecJe = EventCodec.genJsonElement<Event>

module Fold =

    type State = Events.Preferences

    let initial: State = { manyPromotions = false; littlePromotions = false; productReview = false; quickSurveys = false }
    let private evolve _ignoreState = function
        | Events.Updated { preferences = value } -> value
    let fold (state: State) (events: seq<Events.Event>): State =
        Seq.tryLast events |> Option.fold evolve state
    let isOrigin _ = true
    /// When using AccessStrategy.Custom, we use the (single) event become an unfold, but the logic remains identical
    let transmute events _state =
        [||],events

type Command =
    | Update of Events.Value

let interpret command (state: Fold.State) = [|
    match command with
    | Update ({ preferences = preferences } as value) ->
        if state <> preferences then
            Events.Updated value |]

type Service internal (resolve: ClientId -> Equinox.Decider<Events.Event, Fold.State>) =

    let update email value: Async<unit> =
        let decider = resolve email
        let command = let (ClientId email) = email in Update { email = email; preferences = value }
        decider.Transact(interpret command)

    member _.Update(email, value) =
        update email value

    member _.Read(email) =
        let decider = resolve email
        decider.Query id

    member _.ReadStale(email) =
        let decider = resolve email
        decider.Query(id, Equinox.LoadOption.AnyCachedValue)

let create resolve =
    Service(Stream.id >> resolve)
