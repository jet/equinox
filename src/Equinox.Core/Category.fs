namespace Equinox.Core

open Serilog
open System.Threading
open System.Threading.Tasks

/// Store-agnostic interface representing interactions an Application can have with a set of streams with a given pair of Event and State types
type ICategory<'event, 'state, 'context> =
    /// Obtain the state from the target stream
    abstract Load: log: ILogger * categoryName: string * streamId: string * streamName: string
                   * maxAge: System.TimeSpan * requireLeader: bool
                   * ct: CancellationToken -> Task<struct (StreamToken * 'state)>

    /// Given the supplied `token`, attempt to sync to the proposed updated `state'` by appending the supplied `events` to the underlying stream, yielding:
    /// - Written: signifies synchronization has succeeded, implying the included StreamState should now be assumed to be the state of the stream
    /// - Conflict: signifies the sync failed, and the proposed decision hence needs to be reconsidered in light of the supplied conflicting Stream State
    /// NB the central precondition upon which the sync is predicated is that the stream has not diverged from the `originState` represented by `token`
    ///    where the precondition is not met, the SyncResult.Conflict bears a [lazy] async result (in a specific manner optimal for the store)
    abstract TrySync: log: ILogger * categoryName: string * streamId: string * streamName: string * 'context
                      * maybeInit: (CancellationToken -> Task<unit>) voption
                      * originToken: StreamToken * originState: 'state * events: 'event[]
                      * CancellationToken -> Task<SyncResult<'state>>

// Low level stream impl, used by Store-specific Category types that layer policies such as Caching in
namespace Equinox

open Equinox.Core.Tracing
open System.Diagnostics
open System.Threading
open System.Threading.Tasks

/// Store-agnostic baseline functionality for a Category of 'event representations that fold to a given 'state
[<NoComparison; NoEquality>]
type Category<'event, 'state, 'context>
    (   resolveInner: string -> string-> struct (Core.ICategory<'event, 'state, 'context> * string * (CancellationToken -> Task<unit>) voption),
        empty: struct (Core.StreamToken * 'state)) =
    /// Provides access to the low level store operations used for Loading and/or Syncing updates via the Decider
    /// (Normal usage is via the adjacent `module Decider` / `DeciderExtensions.Resolve` helpers)
    member _.Stream(log: Serilog.ILogger, context: 'context, categoryName, streamId) =
        let struct (inner, streamName, init) = resolveInner categoryName streamId
        { new Core.IStream<'event, 'state> with
            member _.LoadEmpty() =
                empty
            member _.Load(maxAge, requireLeader, ct) = task {
                use act = source.StartActivity("Load", ActivityKind.Client)
                if act <> null then act.AddStream(categoryName, streamId, streamName).AddLeader(requireLeader).AddStale(maxAge) |> ignore
                return! inner.Load(log, categoryName, streamId, streamName, maxAge, requireLeader, ct) }
            member _.TrySync(attempt, (token, originState), events, ct) = task {
                use act = source.StartActivity("TrySync", ActivityKind.Client)
                if act <> null then act.AddStream(categoryName, streamId, streamName).AddSyncAttempt(attempt) |> ignore
                let log = if attempt = 1 then log else log.ForContext("attempts", attempt)
                return! inner.TrySync(log, categoryName, streamId, streamName, context, init, token, originState, events, ct) } }

type private Stream =
    static member Resolve(cat: Category<'event, 'state, 'context>, log, context): System.Func<string, Core.StreamId, Core.IStream<'event, 'state>> =
        System.Func<string, Core.StreamId, _>(fun categoryName streamId -> cat.Stream(log, context, categoryName, Core.StreamId.toString streamId))

[<System.Runtime.CompilerServices.Extension>]
type DeciderCore =
    [<System.Runtime.CompilerServices.Extension>]
    static member Resolve(cat: Category<'event, 'state, 'context>, log, context): System.Func<string, Core.StreamId, DeciderCore<'event, 'state>> =
         System.Func<_, _, _>(fun c s -> Stream.Resolve(cat, log, context).Invoke(c, s) |> DeciderCore)
    [<System.Runtime.CompilerServices.Extension>]
    static member Resolve(cat: Category<'event, 'state, unit>, log): System.Func<string, Core.StreamId, DeciderCore<'event, 'state>> =
        DeciderCore.Resolve(cat, log, ())

module Decider =
    let resolveWithContext log (cat: Category<'event, 'state, 'context>) categoryName context streamId =
        DeciderCore.Resolve(cat, log, context).Invoke(categoryName, streamId) |> Decider
    let resolve log (cat: Category<'event, 'state, unit>) categoryName streamId =
        resolveWithContext log cat categoryName () streamId
