namespace Equinox.Core

open Serilog

/// Store-agnostic interface representing interactions a Decider can have with a set of streams with a given pair of Event and State types
type ICategory<'event, 'state, 'context> =
    /// Obtain a Null state for optimistic processing
    abstract Empty: struct (StreamToken * 'state)
    /// Obtain the state from the target stream
    abstract Load: log: ILogger * categoryName: string * streamId: string * streamName: string
                   * maxAge: System.TimeSpan * requireLeader: bool
                   * ct: CancellationToken -> Task<struct (StreamToken * 'state)>

    /// Given the supplied `token`, attempt to sync to the proposed updated `state'` by appending the supplied `events` to the underlying stream, yielding:
    /// - Written: signifies synchronization has succeeded, implying the included StreamState should now be assumed to be the state of the stream
    /// - Conflict: signifies the sync failed, and the proposed decision hence needs to be reconsidered in light of the supplied conflicting Stream State
    /// NB the central precondition upon which the sync is predicated is that the stream has not diverged from the `originState` represented by `token`
    ///    where the precondition is not met, the SyncResult.Conflict bears a [lazy] async result (in a specific manner optimal for the store)
    abstract Sync: log: ILogger * categoryName: string * streamId: string * streamName: string * 'context
                   * maybeInit: (CancellationToken -> Task<unit>) voption
                   * originToken: StreamToken * originState: 'state * events: 'event[]
                   * CancellationToken -> Task<SyncResult<'state>>

// Low level stream operations skeleton; base type for Store-specific Category types
namespace Equinox

open Equinox.Core.Tracing

/// Store-agnostic baseline functionality for Load and Syncing a Category of 'event representations that fold to a given 'state
[<NoComparison; NoEquality>]
type Category<'event, 'state, 'context>(categoryName, resolveStream) =

    /// Stores without custom routing for categoryName/streamId to Table/Container etc use this default impl
    new(categoryName, inner) = Category(categoryName, fun streamId -> struct (inner, Core.StreamName.render categoryName streamId, ValueNone))

    /// Provides access to the low level store operations used for Loading and/or Syncing updates via the Decider
    /// (Normal usage is via the adjacent `module Decider` / `Stream.Resolve` helpers)
    member _.Stream(log: Serilog.ILogger, context: 'context, streamId: string) =
        let struct (inner: Core.ICategory<'event, 'state, 'context>, streamName, init) = resolveStream streamId
        { new Core.IStream<'event, 'state> with
            member _.Name = streamName
            member _.LoadEmpty() = inner.Empty
            member _.Load(maxAge, requireLeader, ct) = task {
                use act = source.StartActivity("Load", System.Diagnostics.ActivityKind.Client)
                if act <> null then act.AddStream(categoryName, streamId, streamName).AddLeader(requireLeader).AddStale(maxAge) |> ignore
                return! inner.Load(log, categoryName, streamId, streamName, maxAge, requireLeader, ct) }
            member _.Sync(attempt, token, originState, events, ct) = task {
                use act = source.StartActivity("Sync", System.Diagnostics.ActivityKind.Client)
                if act <> null then act.AddStream(categoryName, streamId, streamName).AddSyncAttempt(attempt) |> ignore
                let log = if attempt = 1 then log else log.ForContext("attempts", attempt)
                return! inner.Sync(log, categoryName, streamId, streamName, context, init, token, originState, events, ct) } }

[<AbstractClass; Sealed; System.Runtime.CompilerServices.Extension>]
type Stream private () =
    [<System.Runtime.CompilerServices.Extension>]
    static member Resolve(cat: Category<'event, 'state, 'context>, log, context): System.Func<Core.StreamId, DeciderCore<'event, 'state>> =
         System.Func<_, _>(fun sid -> cat.Stream(log, context, Core.StreamId.toString sid) |> DeciderCore<'event, 'state>)
    [<System.Runtime.CompilerServices.Extension>]
    static member Resolve(cat: Category<'event, 'state, unit>, log): System.Func<Core.StreamId, DeciderCore<'event, 'state>> =
        Stream.Resolve(cat, log, ())

module Decider =
    let forContext log (cat: Category<'event, 'state, 'context>) context streamId =
        Stream.Resolve(cat, log, context).Invoke(streamId) |> Decider<'event, 'state>
    let forStream log (cat: Category<'event, 'state, unit>) streamId =
        forContext log cat () streamId
