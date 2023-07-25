module Equinox.Core.Tracing

open System.Diagnostics

let source = new ActivitySource("Equinox")

module Tags =
    // General tags

    /// The full stream name
    [<Literal>]
    let stream_name = "eqx.stream_name"
    /// The ID of the stream
    [<Literal>]
    let stream_id = "eqx.stream_id"
    /// The category of the stream
    [<Literal>]
    let category = "eqx.category"
    /// The store being used
    [<Literal>]
    let store = "eqx.store"

    // Information about loading events

    /// The version of the stream at read time
    [<Literal>]
    let read_version = "eqx.read_version"
    /// The configured batch size
    [<Literal>]
    let batch_size = "eqx.batch_size"
    /// The total number of batches loaded from the store
    [<Literal>]
    let batches = "eqx.batches"
    /// The total number of events loaded from the store
    [<Literal>]
    let loaded_count = "eqx.count"
    /// The total bytes loaded during Transact/Query
    [<Literal>]
    let loaded_bytes = "eqx.bytes"
    /// The version we load forwards from
    [<Literal>]
    let loaded_from_version = "eqx.load.from_version"
    /// The load method (BatchForward, LatestEvent, BatchBackward, etc)
    [<Literal>]
    let load_method = "eqx.load.method"
    /// Whether we needed to use the leader node
    [<Literal>]
    let requires_leader = "eqx.load.requires_leader"
    /// Whether we used a cached value (regardless of whether we also reloaded)
    [<Literal>]
    let cache_hit = "eqx.cache.hit"
    /// If we found a cached value, how long ago was it cached?
    [<Literal>]
    let cache_age = "eqx.cache.item_age_ms"
    /// Whether we accept AnyCachedValue
    [<Literal>]
    let any_cached_value = "eqx.cache.any_value"
    /// the age of a cached value that indicates it has expired
    [<Literal>]
    let max_stale = "eqx.cache.max_stale_ms"
    /// If a snapshot was loaded, which version was it generated from
    [<Literal>]
    let snapshot_version = "eqx.snapshot.version"
    /// Information about appending of events

    /// Whether there was at least one conflict during transact
    [<Literal>]
    let conflict = "eqx.conflict"
    /// Sync attempts - 1
    [<Literal>]
    let sync_retries = "eqx.sync_retries"
    /// Store level retry
    [<Literal>]
    let retries = "eqx.retries"
    /// The number of events we appended
    [<Literal>]
    let append_count = "eqx.append_count"
    /// The total bytes we appended
    [<Literal>]
    let append_bytes = "eqx.append_bytes"
    /// Whether a snapshot was written during this transaction
    [<Literal>]
    let snapshot_written = "eqx.snapshot.written"
    /// The new version of the stream after appending events
    [<Literal>]
    let new_version = "eqx.new_version"
    /// In case of conflict, which event types did we try to append
    [<Literal>]
    let append_types = "eqx.append_types"

module Load =
    let setTags (category, streamId, streamName, requiresLeader, maxAge) =
        let act = Activity.Current
        if act <> null then
            act.SetTag(Tags.category, category)
               .SetTag(Tags.stream_id, streamId)
               .SetTag(Tags.stream_name, streamName)
               .SetTag(Tags.requires_leader, requiresLeader)
               .SetTag(Tags.any_cached_value, System.TimeSpan.Zero = maxAge)
               .SetTag(Tags.max_stale, maxAge.TotalMilliseconds) |> ignore

module TrySync =
    let setTags (attempt, events: 'a[]) =
        let act = Activity.Current
        if act <> null then
            act.SetTag(Tags.sync_retries, attempt - 1)
               .SetTag(Tags.append_count, events.Length) |> ignore
