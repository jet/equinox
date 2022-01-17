/// Low level stream builders, generally consumed via Store-specific Stream Builders that layer policies such as Caching in at the Category level
module Equinox.Core.Stream

/// Represents a specific stream in a ICategory
type private Stream<'event, 'state, 'streamId, 'context>(category : ICategory<'event, 'state, 'streamId, 'context>, streamId: 'streamId, context) =
    interface IStream<'event, 'state> with
        member _.Load(log, opt) =
            category.Load(log, streamId, opt)

        member _.TrySync(log: Serilog.ILogger, token: StreamToken, originState: 'state, events: 'event list) =
            category.TrySync(log, streamId, token, originState, events, context)

let create (category : ICategory<'event, 'state, 'streamId, 'context>) streamId context : IStream<'event, 'state> =
    Stream(category, streamId, context) :> _
