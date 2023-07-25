module Equinox.Core.Tracing

let [<Literal>] SourceName = "Equinox"
let internal source = new System.Diagnostics.ActivitySource(SourceName)

module Tags =
    (* General tags *)

    /// The full stream name
    let [<Literal>] stream_name = "eqx.stream_name"
    /// The <c>id</c> of the stream
    let [<Literal>] stream_id = "eqx.stream_id"
    /// The category of the stream
    let [<Literal>] category = "eqx.category"
    /// The store being used
    let [<Literal>] store = "eqx.store"

    (* Information about loading events *)

    /// The version of the stream at read time (empty stream = 0)
    let [<Literal>] read_version = "eqx.load.version"
    /// The configured batch size
    let [<Literal>] batch_size = "eqx.load.batch_size"
    /// The total number of batches loaded from the store
    let [<Literal>] batches = "eqx.load.batches"
    /// The total number of events loaded from the store
    let [<Literal>] loaded_count = "eqx.load.count"
    /// The total size of events loaded
    let [<Literal>] loaded_bytes = "eqx.load.bytes"
    /// The version we load forwards from
    let [<Literal>] loaded_from_version = "eqx.load.from_version"
    /// The load method (BatchForward, LatestEvent, BatchBackward, etc)
    let [<Literal>] load_method = "eqx.load.method"
    /// Whether the load specified leader node / consistent read etc
    let [<Literal>] requires_leader = "eqx.load.requires_leader"
    /// Whether we used a cached state (independent of whether we reloaded)
    let [<Literal>] cache_hit = "eqx.cache.hit"
    /// Elapsed ms since cached state was stored or revalidated at time of inspection
    let [<Literal>] cache_age = "eqx.cache.age_ms"
    /// Staleness tolerance specified for this request, in ms
    let [<Literal>] max_staleness = "eqx.cache.max_stale_ms"
    /// If a snapshot was read, what version of the stream was it based on
    let [<Literal>] snapshot_version = "eqx.snapshot.version"

    (* Information about appending of events *)

    /// Whether there was at least one conflict during transact
    let [<Literal>] conflict = "eqx.conflict"
    /// (if conflict) Sync attempts - 1
    let [<Literal>] sync_retries = "eqx.conflict.retries"

    /// Store level retry
    let [<Literal>] append_retries = "eqx.append.retries"
    /// The number of events we appended
    let [<Literal>] append_count = "eqx.append.count"
    /// The total bytes we appended
    let [<Literal>] append_bytes = "eqx.append.bytes"
    /// Whether a snapshot was written during this transaction
    let [<Literal>] snapshot_written = "eqx.snapshot.written"
    /// The new version of the stream after appending events
    let [<Literal>] append_version = "eqx.append.version"
    /// In case of conflict, which event types did we try to append
    let [<Literal>] append_types = "eqx.append.types"

module Load =
    let setTags (category, streamId, streamName, requiresLeader, maxAge: System.TimeSpan) =
        let act = System.Diagnostics.Activity.Current
        if act <> null then
            act.SetTag(Tags.category, category)
               .SetTag(Tags.stream_id, streamId)
               .SetTag(Tags.stream_name, streamName)
               .SetTag(Tags.requires_leader, requiresLeader)
               .SetTag(Tags.max_staleness, maxAge.TotalMilliseconds) |> ignore

module TrySync =
    let setTags (attempt, events: 'a[]) =
        let act = System.Diagnostics.Activity.Current
        if act <> null then
            act.SetTag(Tags.sync_retries, attempt - 1)
               .SetTag(Tags.append_count, events.Length) |> ignore
