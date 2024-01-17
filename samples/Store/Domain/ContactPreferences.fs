﻿module Domain.ContactPreferences

type ClientId = ClientId of email: string
module ClientId =
    let toString (ClientId email) = email
    let parse = ClientId

let [<Literal>] CategoryName = "ContactPreferences"
let private streamId = FsCodec.StreamId.gen ClientId.toString // TODO hash >> base64
let private decodeId = FsCodec.StreamId.dec ClientId.parse
let streamName = streamId >> FsCodec.StreamName.create CategoryName

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

let interpretUpdate ({ preferences = preferences }: Events.Value as value) (state: Fold.State) = [|
    if state <> preferences then
        Events.Updated value |]

type Service internal (resolve: ClientId -> Equinox.Decider<Events.Event, Fold.State>) =

    member _.Update(ClientId email as clientId, value) =
        let decider = resolve clientId
        decider.Transact(interpretUpdate { email = email; preferences = value })

    member _.Read(clientId) =
        let decider = resolve clientId
        decider.Query id

    member _.ReadVersion(clientId) =
        let decider = resolve clientId
        decider.QueryEx(fun x -> x.Version)

    member _.ReadStale(clientId) =
        let decider = resolve clientId
        decider.Query(id, Equinox.LoadOption.AnyCachedValue)

let create resolve =
    Service(streamId >> resolve)
