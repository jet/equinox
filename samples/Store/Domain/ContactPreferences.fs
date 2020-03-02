module Domain.ContactPreferences

open System.Text.Json

type Id = Id of email: string

// NOTE - these types and the union case names reflect the actual storage formats and hence need to be versioned with care
module Events =

    let (|ForClientId|) (email: string) = FsCodec.StreamName.create "ContactPreferences" email // TODO hash >> base64

    type Preferences = { manyPromotions : bool; littlePromotions : bool; productReview : bool; quickSurveys : bool }
    type Value = { email : string; preferences : Preferences }

    type Event =
        | [<System.Runtime.Serialization.DataMember(Name = "contactPreferencesChanged")>]Updated of Value
        interface TypeShape.UnionContract.IUnionContract

    module Utf8ArrayCodec =
        let codec = FsCodec.NewtonsoftJson.Codec.Create<Event>()

    module JsonElementCodec =
        open FsCodec.SystemTextJson

        let private encode (options: JsonSerializerOptions) =
            fun (evt: Event) ->
                match evt with
                | Updated value -> "contactPreferencesChanged", JsonSerializer.SerializeToElement(value, options)

        let private tryDecode (options: JsonSerializerOptions) =
            fun (eventType, data: JsonElement) ->
                match eventType with
                | "contactPreferencesChanged" -> Some (Updated <| JsonSerializer.DeserializeElement<Value>(data, options))
                | _ -> None
        
        let codec options = FsCodec.Codec.Create<Event, JsonElement>(encode options, tryDecode options)

module Fold =

    type State = Events.Preferences

    let initial : State = { manyPromotions = false; littlePromotions = false; productReview = false; quickSurveys = false }
    let private evolve _ignoreState = function
        | Events.Updated { preferences = value } -> value
    let fold (state: State) (events: seq<Events.Event>) : State =
        Seq.tryLast events |> Option.fold evolve state
    let isOrigin _ = true
    /// When using AccessStrategy.Custom, we use the (single) event become an unfold, but the logic remains identical
    let transmute events _state =
        [],events

type Command =
    | Update of Events.Value

module Commands =
    let interpret command (state : Fold.State) =
        match command with
        | Update ({ preferences = preferences } as value) ->
            if state = preferences then [] else
            [ Events.Updated value ] 
