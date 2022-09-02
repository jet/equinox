namespace Equinox.Core

open System
open System.Threading.Tasks

type [<NoEquality; NoComparison>] CacheItemOptions =
    | AbsoluteExpiration of DateTimeOffset
    | RelativeExpiration of TimeSpan

[<AllowNullLiteral>]
type CacheEntry<'state>(initialToken: StreamToken, initialState: 'state) =
    let mutable currentToken = initialToken
    let mutable currentState = initialState
    member x.UpdateIfNewer(supersedes : struct (StreamToken * StreamToken) -> bool, other : CacheEntry<'state>) =
        lock x <| fun () ->
            let struct (otherToken, otherState) = other.Value
            if supersedes (currentToken, otherToken) then
                currentToken <- otherToken
                currentState <- otherState

    member x.Value : struct (StreamToken * 'state) =
        lock x <| fun () ->
            currentToken, currentState

type ICache =
    abstract member UpdateIfNewer : key: string * options: CacheItemOptions * (struct (StreamToken * StreamToken) -> bool) * entry: CacheEntry<'state> -> Task<unit>
    abstract member TryGet : key: string -> Task<struct (StreamToken * 'state) voption>

namespace Equinox

open Equinox.Core
open System.Runtime.Caching
open System.Threading.Tasks

type Cache(name, sizeMb : int) =

    let cache =
        let config = System.Collections.Specialized.NameValueCollection(1)
        config.Add("cacheMemoryLimitMegabytes", string sizeMb);
        new MemoryCache(name, config)

    let toPolicy (cacheItemOption: CacheItemOptions) =
        match cacheItemOption with
        | AbsoluteExpiration absolute -> CacheItemPolicy(AbsoluteExpiration = absolute)
        | RelativeExpiration relative -> CacheItemPolicy(SlidingExpiration = relative)

    interface ICache with
        member _.UpdateIfNewer(key, options, supersedes, entry) =
            let policy = toPolicy options
            match cache.AddOrGetExisting(key, box entry, policy) with
            | null -> Task.FromResult()
            | :? CacheEntry<'state> as existingEntry -> existingEntry.UpdateIfNewer(supersedes, entry); Task.FromResult()
            | x -> failwithf "UpdateIfNewer Incompatible cache entry %A" x

        member _.TryGet key =
            match cache.Get key with
            | null -> ValueNone |> Task.FromResult
            | :? CacheEntry<'state> as existingEntry -> ValueSome existingEntry.Value |> Task.FromResult
            | x -> failwithf "TryGet Incompatible cache entry %A" x
