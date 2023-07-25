module Equinox.Core.Tracing

open System.Diagnostics

let source = new ActivitySource("Equinox")

module Tags =
    // General tags
    [<Literal>]
    let stream_name = "eqx.stream_name" // The full stream name
    [<Literal>]
    let stream_id = "eqx.stream_id" // The ID of the stream
    [<Literal>]
    let category = "eqx.category" // The category of the stream
    [<Literal>]
    let store = "eqx.store" // The store being used

    // Information about loading events
    [<Literal>]
    let read_version = "eqx.read_version" // The version of the stream at read time
    [<Literal>]
    let batch_size = "eqx.batch_size" // The configured batch size
    [<Literal>]
    let batches = "eqx.batches" // The total number of batches loaded from the store
    [<Literal>]
    let loaded_count = "eqx.count" // The total number of events loaded from the store
    [<Literal>]
    let loaded_bytes = "eqx.bytes" // The total bytes loaded during Transact/Query
    [<Literal>]
    let loaded_from_version = "eqx.load.from_version" // The version we load forwards from
    [<Literal>]
    let load_method = "eqx.load.method" // The load method (BatchForward, LatestEvent, BatchBackward, etc)
    [<Literal>]
    let requires_leader = "eqx.load.requires_leader" // Whether we needed to use the leader node
    [<Literal>]
    let cache_hit = "eqx.cache.hit" // Whether we used a cached value (regardless of whether we also reloaded)
    [<Literal>]
    let cache_age = "eqx.cache.item_age_ms" // If we found a cached value, how long ago was it cached?
    [<Literal>]
    let any_cached_value = "eqx.cache.any_value" // Whether we accept AnyCachedValue
    [<Literal>]
    let max_stale = "eqx.cache.max_stale_ms" // the age of a cached value that indicates it has expired
    [<Literal>]
    let snapshot_version = "eqx.snapshot.version" // If a snapshot was loaded, which version was it generated from

    // Information about appending of events
    [<Literal>]
    let conflict = "eqx.conflict" // Whether there was at least one conflict during transact
    [<Literal>]
    let sync_retries = "eqx.sync_retries" // Sync attempts - 1
    [<Literal>]
    let retries = "eqx.retries" // Store level retry
    [<Literal>]
    let append_count = "eqx.append_count" // The number of events we appended
    [<Literal>]
    let append_bytes = "eqx.append_bytes" // The total bytes we appended
    [<Literal>]
    let snapshot_written = "eqx.snapshot.written" // Whether a snapshot was written during this transaction
    [<Literal>]
    let new_version = "eqx.new_version" // The new version of the stream after appending events
    [<Literal>]
    let append_types = "eqx.append_types" // In case of conflict, which event types did we try to append

module Load =
    let setTags (category, streamId, streamName, requiresLeader, maxAge) =
        let act = Activity.Current
        if act <> null then
            act.SetTag(Tags.category, category)
               .SetTag(Tags.stream_id, streamId)
               .SetTag(Tags.stream_name, streamId)
               .SetTag(Tags.requires_leader, requiresLeader)
               .SetTag(Tags.any_cached_value, System.TimeSpan.Zero = maxAge)
               .SetTag(Tags.max_stale, maxAge.TotalMilliseconds) |> ignore

module TrySync =
    let setTags (attempt, events: 'a[]) =
        let act = Activity.Current
        if act <> null then
            act.SetTag(Tags.sync_retries, attempt - 1)
               .SetTag(Tags.append_count, events.Length) |> ignore
