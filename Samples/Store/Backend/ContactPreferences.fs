module Backend.ContactPreferences

open Domain.ContactPreferences
open Foldunk

type Handler(streamer : IEventStream<Folds.State, Events.Event>, id : Id, maxAttempts) =
    let load = Handler.load Folds.fold Folds.initial streamName streamer
    member __.Run log command : Async<unit> = async {
        let decide (ctx : DecisionContext<_,_>) = async {
            ctx.Execute <| Commands.interpret command
            return ctx.Complete () }
        let! syncState = load log id
        return! Handler.run log maxAttempts syncState decide }
    member __.Load log : Async<Folds.State> = async {
        let! syncState = load log id
        return syncState.State }

type Service(createStream) =
    static let codec = EventSum.generateJsonUtf8SumEncoder<Events.Event>
    let createHandler email = Handler(createStream codec, Id email, maxAttempts = 5)
    member __.Update log email value : Async<unit> = async {
        let handler = createHandler email
        let command = Commands.Update { email = email; preferences = value }
        do! handler.Run log command }
    member __.Load log email : Async<Folds.State> = async {
        let handler = createHandler email
        return! handler.Load log }