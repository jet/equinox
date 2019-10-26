/// Low level stream builders, generally consumed via Store-specific Stream Builders that layer policies such as Caching in at the Category level
module Equinox.Core.Stream

/// Represents a specific stream in a ICategory
type private Stream<'event, 'state, 'streamId, 'context>(category : ICategory<'event, 'state, 'streamId, 'context>, streamId: 'streamId, opt, context) =
    interface IStream<'event, 'state> with
        member __.Load log =
            category.Load(log, streamId, opt)
        member __.TrySync(log: Serilog.ILogger, token: StreamToken, originState: 'state, events: 'event list) =
            category.TrySync(log, token, originState, events, context)

let create (category : ICategory<'event, 'state, 'streamId, 'context>) streamId opt context : IStream<'event, 'state> = Stream(category, streamId, opt, context) :> _

/// Handles case where some earlier processing has loaded or determined a the state of a stream, allowing us to avoid a read roundtrip
type private InitializedStream<'event, 'state>(inner : IStream<'event, 'state>, memento : StreamToken * 'state) =
    let mutable preloadedTokenAndState = Some memento
    interface IStream<'event, 'state> with
        member __.Load log =
            match preloadedTokenAndState with
            | Some value -> async { preloadedTokenAndState <- None; return value }
            | None -> inner.Load log
        member __.TrySync(log: Serilog.ILogger, token: StreamToken, originState: 'state, events: 'event list) =
            inner.TrySync(log, token, originState, events)

let ofMemento (memento : StreamToken * 'state) (inner : IStream<'event,'state>) : IStream<'event, 'state> = InitializedStream(inner, memento) :> _