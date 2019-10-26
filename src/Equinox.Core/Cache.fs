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
    member __.Value : StreamToken * 'state =
        lock __ <| fun () ->
            currentToken, currentState

type ICache =
    abstract member UpdateIfNewer : key: string * options: CacheItemOptions * entry: CacheEntry<'state> -> Async<unit>
    abstract member TryGet: key: string -> Async<(StreamToken * 'state) option>

namespace Equinox

open System.Runtime.Caching
open Equinox.Core

type Cache(name, sizeMb : int) =
    let cache =
        let config = System.Collections.Specialized.NameValueCollection(1)
        config.Add("cacheMemoryLimitMegabytes", string sizeMb);
        new MemoryCache(name, config)

    let toPolicy (cacheItemOption: CacheItemOptions) =
        match cacheItemOption with
        | AbsoluteExpiration absolute -> new CacheItemPolicy(AbsoluteExpiration = absolute)
        | RelativeExpiration relative -> new CacheItemPolicy(SlidingExpiration = relative)

    interface ICache with
        member this.UpdateIfNewer(key, options, entry) = async {
            let policy = toPolicy options
            match cache.AddOrGetExisting(key, box entry, policy) with
            | null -> ()
            | :? CacheEntry<'state> as existingEntry -> existingEntry.UpdateIfNewer entry
            | x -> failwithf "UpdateIfNewer Incompatible cache entry %A" x }

        member this.TryGet key = async {
            return
                match cache.Get key with
                | null -> None
                | :? CacheEntry<'state> as existingEntry -> Some existingEntry.Value
                | x -> failwithf "TryGet Incompatible cache entry %A" x }