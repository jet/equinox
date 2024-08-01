namespace Equinox.DynamoStore.Prometheus

module private Impl =

    let baseName stat = "equinox_ddb_" + stat
    let baseDesc desc = "Equinox DynamoDB " + desc

module private Histograms =

    let labelNames tagNames = Array.append tagNames [| "rut"; "facet"; "op"; "table"; "cat" |]
    let labelValues tagValues (rut, facet, op, table, cat) = Array.append tagValues [| rut; facet; op; table; cat |]
    let private mkHistogram (cfg: Prometheus.HistogramConfiguration) name desc =
        let h = Prometheus.Metrics.CreateHistogram(name, desc, cfg)
        fun tagValues (rut, facet: string, op: string) (table, cat: string) s ->
            h.WithLabels(labelValues tagValues (rut, facet, op, table, cat)).Observe(s)
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
        fun (rut, facet, op) (table, cat, s: System.TimeSpan, ru) ->
            observeS tagValues (rut, facet, op) (table, cat) s.TotalSeconds
            observeRu tagValues (rut, facet, op) (table, cat) ru

module private Summaries =

    let labelNames tagNames = Array.append tagNames [| "facet"; "table" |]
    let labelValues tagValues (facet, table) = Array.append tagValues [| facet; table |]
    let private mkSummary (cfg: Prometheus.SummaryConfiguration) name desc  =
        let s = Prometheus.Metrics.CreateSummary(name, desc, cfg)
        fun tagValues (facet: string) table o -> s.WithLabels(labelValues tagValues (facet, table)).Observe(o)
    let config tagNames =
        let inline qep q e = Prometheus.QuantileEpsilonPair(q, e)
        let objectives = [| qep 0.50 0.05; qep 0.95 0.01; qep 0.99 0.01 |]
        Prometheus.SummaryConfiguration(Objectives = objectives, LabelNames = labelNames tagNames, MaxAge = System.TimeSpan.FromMinutes 1.)
    let sAndRuPair (tagNames, tagValues) stat desc =
        let baseName, baseDesc = Impl.baseName stat, Impl.baseDesc desc
        let observeS = mkSummary (config tagNames) (baseName + "_seconds") (baseDesc + " latency") tagValues
        let observeRu = mkSummary (config tagNames) (baseName + "_ru") (baseDesc + " charge") tagValues
        fun facet (table, s: System.TimeSpan, ru) ->
            observeS facet table s.TotalSeconds
            observeRu facet table ru

module private Counters =

    let labelNames tagNames = Array.append tagNames [| "facet"; "op"; "outcome"; "table"; "cat" |]
    let labelValues tagValues (facet, op, outcome, table, cat) = Array.append tagValues [| facet; op; outcome; table; cat |]
    let private mkCounter (cfg: Prometheus.CounterConfiguration) name desc =
        let h = Prometheus.Metrics.CreateCounter(name, desc, cfg)
        fun tagValues (facet: string, op: string, outcome: string) (table, cat) c ->
            h.WithLabels(labelValues tagValues (facet, op, outcome, table, cat)).Inc(c)
    let config tagNames = Prometheus.CounterConfiguration(LabelNames = labelNames tagNames)
    let total (tagNames, tagValues) stat desc =
        let name = Impl.baseName (stat + "_total")
        let desc = Impl.baseDesc desc
        mkCounter (config tagNames) name desc tagValues
    let eventsAndBytesPair tags stat desc =
        let observeE = total tags (stat + "_events") (desc + "Events")
        let observeB = total tags (stat + "_bytes") (desc + "Bytes")
        fun ctx (table, cat, e, b) ->
            observeE ctx (table, cat) e
            match b with None -> () | Some b -> observeB ctx (table, cat) b

open Equinox.DynamoStore.Core.Log

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

    let observeLatencyAndCharge (rut, facet, op) (table, cat, s, ru) =
        opHistogram (rut, facet, op) (table, cat, s, ru)
        opSummary facet (table, s, ru)
    let observeLatencyAndChargeWithEventCounts (rut, facet, op, outcome) (table, cat, s, ru, count, bytes) =
        observeLatencyAndCharge (rut, facet, op) (table, cat, s, ru)
        payloadCounters (facet, op, outcome) (table, cat, float count, if bytes = -1 then None else Some (float bytes))

    let (|CatSRu|) ({ interval = i; ru = ru }: Measurement as m) =
        struct (m.table, m.Category, i.Elapsed, ru)
    let observeRes (_rut, facet, _op as stat) (CatSRu (table, cat, s, ru)) =
        roundtripHistogram stat (table, cat, s, ru)
        roundtripSummary facet (table, s, ru)
    let observe_ stat (CatSRu (table, cat, s, ru)) =
        observeLatencyAndCharge stat (table, cat, s, ru)
    let observe (rut, facet, op, outcome) (CatSRu (table, cat, s, ru) as m) =
        observeLatencyAndChargeWithEventCounts (rut, facet, op, outcome) (table, cat, s, ru, m.count, m.bytes)
    let observeTip (rut, facet, op, outcome, cacheOutcome) (CatSRu (table, cat, s, ru) as m) =
        observeLatencyAndChargeWithEventCounts (rut, facet, op, outcome) (table, cat, s, ru, m.count, m.bytes)
        cacheCounter (facet, op, cacheOutcome) (table, cat) 1.

    interface Serilog.Core.ILogEventSink with
        member _.Emit logEvent = logEvent |> function
            | MetricEvent cm -> cm |> function
                | Op       (Operation.Tip,      m) -> observeTip  ("R", "query",    "tip",           "ok", "200") m
                | Op       (Operation.Tip404,   m) -> observeTip  ("R", "query",    "tip",           "ok", "404") m
                | Op       (Operation.Tip304,   m) -> observeTip  ("R", "query",    "tip",           "ok", "304") m
                | Op       (Operation.Query,    m) -> observe     ("R", "query",    "query",         "ok")        m
                | QueryRes (_direction,         m) -> observeRes  ("R", "query",    "queryPage")                  m
                | Op       (Operation.Append,   m) -> observe     ("W", "transact", "append",        "ok")        m
                | Op       (Operation.Calve,    m) -> observe     ("W", "transact", "calve",         "ok")        m
                | Op       (Operation.AppendConflict, m) ->observe("W", "transact", "append",        "conflict")  m
                | Op       (Operation.CalveConflict, m) -> observe("W", "transact", "calve",         "conflict")  m
                | Op       (Operation.Prune,    m) -> observe_    ("R", "prune",    "pruneQuery")                 m
                | PruneRes                      m  -> observeRes  ("R", "prune",    "pruneQueryPage")             m
                | Op       (Operation.Delete,   m) -> observe     ("W", "prune",    "delete",        "ok")        m
                | Op       (Operation.Trim,     m) -> observe     ("W", "prune",    "trim",          "ok")        m
            | _ -> ()
