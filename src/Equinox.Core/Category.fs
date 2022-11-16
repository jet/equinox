﻿namespace Equinox.Core

open Serilog
open System.Threading
open System.Threading.Tasks

/// Store-agnostic interface representing interactions an Application can have with a set of streams with a given pair of Event and State types
type ICategory<'event, 'state, 'context> =
    /// Obtain the state from the target stream
    abstract Load : log: ILogger * categoryName: string * streamId: string * streamName: string * allowStale: bool * requireLeader: bool
                    * ct: CancellationToken -> Task<struct (StreamToken * 'state)>

    /// Given the supplied `token`, attempt to sync to the proposed updated `state'` by appending the supplied `events` to the underlying stream, yielding:
    /// - Written: signifies synchronization has succeeded, implying the included StreamState should now be assumed to be the state of the stream
    /// - Conflict: signifies the sync failed, and the proposed decision hence needs to be reconsidered in light of the supplied conflicting Stream State
    /// NB the central precondition upon which the sync is predicated is that the stream has not diverged from the `originState` represented by `token`
    ///    where the precondition is not met, the SyncResult.Conflict bears a [lazy] async result (in a specific manner optimal for the store)
    abstract TrySync : log: ILogger * categoryName: string * streamId: string * streamName: string * 'context * maybeInit: (CancellationToken -> Task<unit>) voption
                       * StreamToken * 'state * events: 'event array * CancellationToken -> Task<SyncResult<'state>>

// Low level stream impl, used by Store-specific Category types that layer policies such as Caching in
namespace Equinox

open System.Threading
open System.Threading.Tasks

/// Store-agnostic baseline functionality for a Category of 'event representations that fold to a given 'state
[<NoComparison; NoEquality>]
type Category<'event, 'state, 'context>
    (   resolveInner : struct (string * string) -> struct (Core.ICategory<'event, 'state, 'context> * string * (CancellationToken -> Task<unit>) voption),
        empty : struct (Core.StreamToken * 'state)) =
    /// Provides access to the low level store operations used for Loading and/or Syncing updates via the Decider
    /// (Normal usage is via the adjacent `module Decider` / `DeciderExtensions.Resolve` helpers)
    member _.Stream(log : Serilog.ILogger, context : 'context, categoryName, streamId) =
        let struct (inner, streamName, init) = resolveInner (categoryName, streamId)
        { new Core.IStream<'event, 'state> with
            member _.LoadEmpty() =
                empty
            member x.Load(allowStale, requireLeader, ct) =
                inner.Load(log, categoryName, streamId, streamName, allowStale, requireLeader, ct)
            member _.TrySync(attempt, (token, originState), events, ct) =
                let log = if attempt = 1 then log else log.ForContext("attempts", attempt)
                inner.TrySync(log, categoryName, streamId, streamName, context, init, token, originState, events, ct) }

type private Stream =
    static member Resolve(cat : Category<'event, 'state, 'context>, log, context) : System.Func<string, StreamId, Core.IStream<'event, 'state>> =
        System.Func<string, StreamId, _>(fun categoryName streamId -> cat.Stream(log, context, categoryName, StreamId.toString streamId))

[<System.Runtime.CompilerServices.Extension>]
type DeciderCore =
    [<System.Runtime.CompilerServices.Extension>]
    static member Resolve(cat : Category<'event, 'state, 'context>, log, context) : System.Func<string, StreamId, DeciderCore<'event, 'state>> =
         System.Func<_, _, _>(fun c s -> Stream.Resolve(cat, log, context).Invoke(c,s) |> DeciderCore)
    [<System.Runtime.CompilerServices.Extension>]
    static member Resolve(cat : Category<'event, 'state, unit>, log) : System.Func<string, StreamId, DeciderCore<'event, 'state>> =
        DeciderCore.Resolve(cat, log, ())

module Decider =
    let resolveWithContext log (cat : Category<'event, 'state, 'context>) context categoryName streamId =
        DeciderCore.Resolve(cat, log, context).Invoke(categoryName, streamId) |> Decider
    let resolve log (cat : Category<'event, 'state, unit>) categoryName streamId =
        resolveWithContext log cat () categoryName streamId
