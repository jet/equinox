// Low level stream impl, used by Store-specific Category types that layer policies such as Caching in
namespace Equinox

open System.Threading
open System.Threading.Tasks

/// Store-agnostic interface representing interactions a Decider can have with the state of a given event stream.
[<NoComparison; NoEquality>]
type Category<'event, 'state, 'context>(
        resolveInner : struct (string * string) -> struct (Core.ICategory<'event, 'state, 'context> * string * (CancellationToken -> Task<unit>) voption),
        empty : struct (Core.StreamToken * 'state)) =

    /// Provides access to the low level store operations used for Loading and/or Syncing updates via the Decider
    /// (Normal usage is via the adjacent `module Decider` / `DeciderExtensions.Resolve` helpers)
    member _.Stream(log : Serilog.ILogger, context : 'context, categoryName, streamId) =
        let struct (inner, streamName, init) = resolveInner (categoryName, streamId)
        { new Core.IStream<'event, 'state> with
            member _.LoadEmpty() =
                empty
            member x.Load(allowStale, ct) =
                inner.Load(log, categoryName, streamId, streamName, allowStale, ct)
            member _.TrySync(attempt, (token, originState), events, ct) =
                let log = if attempt = 1 then log else log.ForContext("attempts", attempt)
                inner.TrySync(log, categoryName, streamId, streamName, context, init, token, originState, events, ct) }

module Stream =

    let resolveWithContext (ctx : 'context) log (cat : Category<'event, 'state, 'context>) : struct (string * string) -> Core.IStream<'event, 'state> =
         fun struct (categoryName, streamId) ->
             cat.Stream(log, ctx, categoryName, streamId)

    let resolve log (cat : Category<'event, 'state, unit>)  =
        resolveWithContext () log cat

module Decider =

    let resolveWithContext context log (cat : Category<'event, 'state, 'context>) : struct (string * string) -> Decider<'event, 'state> =
         Stream.resolveWithContext context log cat >> Decider

    let resolve log (cat : Category<'event, 'state, unit>) =
         resolveWithContext () log cat

[<System.Runtime.CompilerServices.Extension>]
type DeciderExtensions =

    [<System.Runtime.CompilerServices.Extension>]
    static member Resolve(cat : Category<'event, 'state, 'context>, context : 'context, log) : struct (string * string) -> Decider<'event, 'state> =
         Decider.resolveWithContext context log cat

    [<System.Runtime.CompilerServices.Extension>]
    static member Resolve(cat : Category<'event, 'state, unit>, log) : struct (string * string) -> Decider<'event, 'state> =
         Decider.resolveWithContext () log cat
