namespace Equinox.Core

open System

type CacheItemOptions =
    | AbsoluteExpiration of DateTimeOffset
    | RelativeExpiration of TimeSpan

[<AllowNullLiteral>]
type CacheEntry<'state>(initialToken: StreamToken, initialState: 'state, supersedes: StreamToken -> StreamToken -> bool) =
    let mutable currentToken, currentState = initialToken, initialState
    member __.UpdateIfNewer(other: CacheEntry<'state>) =
        lock __ <| fun () ->
            let otherToken, otherState = other.Value
            if otherToken |> supersedes currentToken then
                currentToken <- otherToken
                currentState <- otherState
    member __.Value: StreamToken * 'state =
        lock __ <| fun () ->
            currentToken, currentState


type ICache =
    abstract member UpdateIfNewer: cacheItemOptions:CacheItemOptions -> key: string -> CacheEntry<'state> -> Async<unit>
    abstract member TryGet: key: string -> Async<(StreamToken * 'state) option>
