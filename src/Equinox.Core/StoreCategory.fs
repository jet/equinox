/// Low level stream builders, generally consumed via Store-specific Stream Builders that layer policies such as Caching in at the Category level
namespace Equinox.Core

/// Represents a specific stream in a ICategory
type private Stream<'event, 'state, 'streamId, 'context>(category : ICategory<'event, 'state, 'streamId, 'context>, streamId: 'streamId, opt, context) =
    interface IStream<'event, 'state> with
        member _.Load log =
            category.Load(log, streamId, opt)

        member _.TrySync(log: Serilog.ILogger, token: StreamToken, originState: 'state, events: 'event list) =
            category.TrySync(log, streamId, token, originState, events, context)

module Stream =

    let create (category : ICategory<'event, 'state, 'streamId, 'context>) streamId opt context : IStream<'event, 'state> = Stream(category, streamId, opt, context) :> _
