// Defines base Contract with Stores; Decider talks to an IStream, which is implemented in full within this file
namespace Equinox.Core

/// Store-agnostic interface representing interactions a Decider can have with a set of streams with a given pair of Event and State types
type ICategory<'event, 'state, 'req> =
    /// Obtain a Null state for optimistic processing
    abstract Empty: struct (StreamToken * 'state)
    /// Obtain the state from the target stream
    abstract Load: log: Serilog.ILogger * categoryName: string * streamId: string * streamName: string
                   * maxAge: System.TimeSpan * requireLeader: bool
                   * ct: CancellationToken -> Task<struct (StreamToken * 'state)>
    /// Given the supplied `token`, attempt to sync to the proposed updated `state'` by appending the supplied `events` to the underlying stream, yielding:
    /// - Written: signifies synchronization has succeeded, implying the included StreamState should now be assumed to be the state of the stream
    /// - Conflict: signifies the sync failed, and the proposed decision hence needs to be reconsidered in light of the supplied conflicting Stream State
    /// NB the central precondition upon which the sync is predicated is that the stream has not diverged from the `originState` represented by `token`
    ///    where the precondition is not met, the SyncResult.Conflict bears a [lazy] async result (in a specific manner optimal for the store)
    abstract Sync: log: Serilog.ILogger * categoryName: string * streamId: string * streamName: string * 'req
                   * originToken: StreamToken * originState: 'state * events: 'event[]
                   * CancellationToken -> Task<SyncResult<'state>>

namespace Equinox

open Equinox.Core.Tracing

/// Store-agnostic baseline functionality for Load and Syncing a Category of 'event representations that fold to a given 'state
/// Provides access to the low level store operations used for Loading and/or Syncing updates via the Decider
/// (Normal usage is via the adjacent `module Decider` / `Stream.Resolve` helpers)
[<NoComparison; NoEquality>]
type Category<'event, 'state, 'req>(categoryName, inner: Core.ICategory<'event, 'state, 'req>) =
    member _.Stream(log: Serilog.ILogger, req: 'req, streamId: FsCodec.StreamId) =
        let streamName = FsCodec.StreamName.create categoryName streamId |> FsCodec.StreamName.toString
        let streamId = FsCodec.StreamId.toString streamId
        { new Core.IStream<'event, 'state> with
            member _.LoadEmpty() = inner.Empty
            member _.Load(maxAge, requireLeader, ct) = task {
                use act = source.StartActivity("Load", System.Diagnostics.ActivityKind.Client)
                if act <> null then act.AddStream(categoryName, streamId, streamName).AddLeader(requireLeader).AddStale(maxAge) |> ignore
                return! inner.Load(log, categoryName, streamId, streamName, maxAge, requireLeader, ct) }
            member _.Sync(attempt, token, originState, events, ct) = task {
                use act = source.StartActivity("Sync", System.Diagnostics.ActivityKind.Client)
                if act <> null then act.AddStream(categoryName, streamId, streamName).AddSyncAttempt(attempt) |> ignore
                let log = if attempt = 1 then log else log.ForContext("attempts", attempt)
                return! inner.Sync(log, categoryName, streamId, streamName, req, token, originState, events, ct) } }

[<AbstractClass; Sealed>]
type Stream private () =
    [<System.Runtime.CompilerServices.Extension>]
    static member Resolve(cat: Category<'event, 'state, 'req>, log, req): System.Func<FsCodec.StreamId, DeciderCore<'event, 'state>> =
         System.Func<_, _>(fun sid -> cat.Stream(log, req, sid) |> DeciderCore<'event, 'state>)
    [<System.Runtime.CompilerServices.Extension>]
    static member Resolve(cat: Category<'event, 'state, unit>, log): System.Func<FsCodec.StreamId, DeciderCore<'event, 'state>> =
        Stream.Resolve(cat, log, ())

module Decider =
    let forRequest log (cat: Category<'event, 'state, 'req>) context streamId =
        Stream.Resolve(cat, log, context).Invoke(streamId) |> Decider<'event, 'state>
    let forStream log (cat: Category<'event, 'state, unit>) streamId =
        forRequest log cat () streamId
