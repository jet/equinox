namespace Equinox.CosmosStore.Prometheus

module private Impl =

    let baseName stat = "equinox_" + stat
    let baseDesc desc = "Equinox CosmosDB " + desc

module private Histograms =

    let labelNames tagNames = Array.append tagNames [| "rut"; "facet"; "op"; "db"; "con"; "cat" |]
    let labelValues tagValues (rut, facet, op, db, con, cat) = Array.append tagValues [| rut; facet; op; db; con; cat |]
    let private mkHistogram (cfg : Prometheus.HistogramConfiguration) name desc =
        let h = Prometheus.Metrics.CreateHistogram(name, desc, cfg)
        fun tagValues (rut, facet : string, op : string) (db, con, cat : string) s ->
            h.WithLabels(labelValues tagValues (rut, facet, op, db, con, cat)).Observe(s)
    // Given we also have summary metrics with equivalent labels, we focus the bucketing on LAN latencies
    let private sHistogram tagNames =
        let sBuckets = [| 0.0005; 0.001; 0.002; 0.004; 0.008; 0.016; 0.5; 1.; 2.; 4.; 8. |]
        let sCfg = Prometheus.HistogramConfiguration(Buckets = sBuckets, LabelNames = labelNames tagNames)
        mkHistogram sCfg
    let private ruHistogram tagNames =
        let ruBuckets = Prometheus.Histogram.ExponentialBuckets(1., 2., 11) // 1 .. 1024
        let ruCfg = Prometheus.HistogramConfiguration(Buckets = ruBuckets, LabelNames = labelNames tagNames)
        mkHistogram ruCfg
    let sAndRuPair (tagNames, tagValues) stat desc =
        let baseName, baseDesc = Impl.baseName stat, Impl.baseDesc desc
        let observeS = sHistogram tagNames (baseName + "_seconds") (baseDesc + " latency")
        let observeRu = ruHistogram tagNames (baseName + "_ru") (baseDesc + " charge")
        fun (rut, facet, op) (db, con, cat, s : System.TimeSpan, ru) ->
            observeS tagValues (rut, facet, op) (db, con, cat) s.TotalSeconds
            observeRu tagValues (rut, facet, op) (db, con, cat) ru

module private Summaries =

    let labelNames tagNames = Array.append tagNames [| "facet"; "db"; "con" |]
    let labelValues tagValues (facet, db, con) = Array.append tagValues [| facet; db; con |]
    let private mkSummary (cfg : Prometheus.SummaryConfiguration) name desc  =
        let s = Prometheus.Metrics.CreateSummary(name, desc, cfg)
        fun tagValues (facet : string) (db, con) o -> s.WithLabels(labelValues tagValues (facet, db, con)).Observe(o)
    let config tagNames =
        let inline qep q e = Prometheus.QuantileEpsilonPair(q, e)
        let objectives = [| qep 0.50 0.05; qep 0.95 0.01; qep 0.99 0.01 |]
        Prometheus.SummaryConfiguration(Objectives = objectives, LabelNames = labelNames tagNames, MaxAge = System.TimeSpan.FromMinutes 1.)
    let sAndRuPair (tagNames, tagValues) stat desc =
        let baseName, baseDesc = Impl.baseName stat, Impl.baseDesc desc
        let observeS = mkSummary (config tagNames) (baseName + "_seconds") (baseDesc + " latency") tagValues
        let observeRu = mkSummary (config tagNames) (baseName + "_ru") (baseDesc + " charge") tagValues
        fun facet (db, con, s : System.TimeSpan, ru) ->
            observeS facet (db, con) s.TotalSeconds
            observeRu facet (db, con) ru

module private Counters =

    let labelNames tagNames = Array.append tagNames [| "facet"; "op"; "outcome"; "db"; "con"; "cat" |]
    let labelValues tagValues (facet, op, outcome, db, con, cat) = Array.append tagValues [| facet; op; outcome; db; con; cat |]
    let private mkCounter (cfg : Prometheus.CounterConfiguration) name desc =
        let h = Prometheus.Metrics.CreateCounter(name, desc, cfg)
        fun tagValues (facet : string, op : string, outcome : string) (db, con, cat) c ->
            h.WithLabels(labelValues tagValues (facet, op, outcome, db, con, cat)).Inc(c)
    let config tagNames = Prometheus.CounterConfiguration(LabelNames = labelNames tagNames)
    let total (tagNames, tagValues) stat desc =
        let name = Impl.baseName (stat + "_total")
        let desc = Impl.baseDesc desc
        mkCounter (config tagNames) name desc tagValues
    let eventsAndBytesPair tags stat desc =
        let observeE = total tags (stat + "_events") (desc + "Events")
        let observeB = total tags (stat + "_bytes") (desc + "Bytes")
        fun ctx (db, con, cat, e, b) ->
            observeE ctx (db, con, cat) e
            match b with None -> () | Some b -> observeB ctx (db, con, cat) b

open Equinox.CosmosStore.Core.Log

/// <summary>An ILogEventSink that publishes to Prometheus</summary>
/// <param name="customTags">Custom tags to annotate the metric we're publishing where such tag manipulation cannot better be achieved via the Prometheus scraper config.</param>
type LogSink(customTags: seq<string * string>) =

    let tags = Array.ofSeq customTags |> Array.unzip

    let opHistogram =         Histograms.sAndRuPair       tags "op"                "Operation"
    let roundtripHistogram =  Histograms.sAndRuPair       tags "roundtrip"         "Fragment"
    let opSummary =           Summaries.sAndRuPair        tags "op_summary"        "Operation Summary"
    let roundtripSummary =    Summaries.sAndRuPair        tags "roundtrip_summary" "Fragment Summary"
    let payloadCounters =     Counters.eventsAndBytesPair tags "payload"           "Payload, "
    let cacheCounter =        Counters.total              tags "cache"             "Cache"

    let observeLatencyAndCharge (rut, facet, op) (db, con, cat, s, ru) =
        opHistogram (rut, facet, op) (db, con, cat, s, ru)
        opSummary facet (db, con, s, ru)
    let observeLatencyAndChargeWithEventCounts (rut, facet, op, outcome) (db, con, cat, s, ru, count, bytes) =
        observeLatencyAndCharge (rut, facet, op) (db, con, cat, s, ru)
        payloadCounters (facet, op, outcome) (db, con, cat, float count, if bytes = -1 then None else Some (float bytes))

    let (|CatSRu|) ({ interval = i; ru = ru } : Measurement as m) =
        let struct (cat, _id) = FsCodec.StreamName.splitCategoryAndStreamId (FSharp.UMX.UMX.tag m.stream)
        struct (m.database, m.container, cat, i.Elapsed, ru)
    let observeRes (_rut, facet, _op as stat) (CatSRu (db, con, cat, s, ru)) =
        roundtripHistogram stat (db, con, cat, s, ru)
        roundtripSummary facet (db, con, s, ru)
    let observe_ stat (CatSRu (db, con, cat, s, ru)) =
        observeLatencyAndCharge stat (db, con, cat, s, ru)
    let observe (rut, facet, op, outcome) (CatSRu (db, con, cat, s, ru) as m) =
        observeLatencyAndChargeWithEventCounts (rut, facet, op, outcome) (db, con, cat, s, ru, m.count, m.bytes)
    let observeTip (rut, facet, op, outcome, cacheOutcome) (CatSRu (db, con, cat, s, ru) as m) =
        observeLatencyAndChargeWithEventCounts (rut, facet, op, outcome) (db, con, cat, s, ru, m.count, m.bytes)
        cacheCounter (facet, op, cacheOutcome) (db, con, cat) 1.

    interface Serilog.Core.ILogEventSink with
        member _.Emit logEvent = logEvent |> function
            | MetricEvent cm -> cm |> function
                | Op       (Operation.Tip,      m) -> observeTip  ("R", "query",    "tip",           "ok", "200") m
                | Op       (Operation.Tip404,   m) -> observeTip  ("R", "query",    "tip",           "ok", "404") m
                | Op       (Operation.Tip304,   m) -> observeTip  ("R", "query",    "tip",           "ok", "304") m
                | Op       (Operation.Query,    m) -> observe     ("R", "query",    "query",         "ok")        m
                | QueryRes (_direction,         m) -> observeRes  ("R", "query",    "queryPage")                  m
                | Op       (Operation.Write,    m) -> observe     ("W", "transact", "sync",          "ok")        m
                | Op       (Operation.Conflict, m) -> observe     ("W", "transact", "conflict",      "conflict")  m
                | Op       (Operation.Resync,   m) -> observe     ("W", "transact", "resync",        "conflict")  m
                | Op       (Operation.Prune,    m) -> observe_    ("R", "prune",    "pruneQuery")                 m
                | PruneRes (                    m) -> observeRes  ("R", "prune",    "pruneQueryPage")             m
                | Op       (Operation.Delete,   m) -> observe     ("W", "prune",    "delete",        "ok")        m
                | Op       (Operation.Trim,     m) -> observe     ("W", "prune",    "trim",          "ok")        m
            | _ -> ()
