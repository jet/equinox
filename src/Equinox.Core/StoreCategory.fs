/// Low level stream builders, generally consumed via Store-specific Stream Builders that layer policies such as Caching in at the Category level
namespace Equinox.Core

/// Represents a specific stream in a ICategory
[<NoComparison; NoEquality>]
type private Stream<'event, 'state, 'context>(category : ICategory<'event, 'state, string, 'context>, streamId: string, empty : StreamToken * 'state, ?context : 'context, ?init : unit -> Async<unit>) =

    interface IStream<'event, 'state> with
        member _.Load(log, opt) =
            match opt with
            | Equinox.LoadOption.AssumeEmpty -> async { return empty }
            | Equinox.LoadOption.FromMemento (streamToken, state) -> async { return (streamToken, state) }
            | Equinox.LoadOption.AllowStale -> category.Load(log, streamId, true)
            | Equinox.LoadOption.Load -> category.Load(log, streamId, false)
        member _.TrySync(log, token: StreamToken, originState: 'state, events: 'event list) =
            let sync = category.TrySync(log, streamId, token, originState, events, context)
            match init with
            | None -> sync
            | Some f -> async { do! f ()
                                return! sync }

/// Store-agnostic interface representing interactions a Flow can have with the state of a given event stream. Not intended for direct use by consumer code.
type StoreCategory<'event, 'state, 'streamId, 'context>(resolve, empty) =

    member _.Resolve(streamName : 'streamId, [<O; D null>]?context) =
        let category, streamName, maybeContainerInitializationGate = resolve streamName
        Stream<'event, 'state, 'context>(category, streamName, empty, ?context = context, ?init = maybeContainerInitializationGate) :> IStream<'event, 'state>
