namespace Equinox.Cosmos.Prometheus

module private Impl =

    let baseName stat = "equinox_" + stat
    let baseDesc desc = "Equinox CosmosDB " + desc

module private Histograms =

    let labelNames = [| "facet"; "op"; "app"; "db"; "con"; "cat" |]
    let private mkHistogram (cfg : Prometheus.HistogramConfiguration) name desc =
        let h = Prometheus.Metrics.CreateHistogram(name, desc, cfg)
        fun (facet : string, op : string) app (db, con, cat : string) s ->
            h.WithLabels(facet, op, app, db, con, cat).Observe(s)
    // Given we also have summary metrics with equivalent labels, we focus the bucketing on LAN latencies
    let private sHistogram =
        let sBuckets = [| 0.0005; 0.001; 0.002; 0.004; 0.008; 0.016; 0.5; 1.; 2.; 4.; 8. |]
        let sCfg = Prometheus.HistogramConfiguration(Buckets = sBuckets, LabelNames = labelNames)
        mkHistogram sCfg
    let private ruHistogram =
        let ruBuckets = Prometheus.Histogram.ExponentialBuckets(1., 2., 11) // 1 .. 1024
        let ruCfg = Prometheus.HistogramConfiguration(Buckets = ruBuckets, LabelNames = labelNames)
        mkHistogram ruCfg
    let sAndRuPair stat desc =
        let baseName, baseDesc = Impl.baseName stat, Impl.baseDesc desc
        let observeS = sHistogram (baseName + "_seconds") (baseDesc + " latency")
        let observeRu = ruHistogram (baseName + "_ru") (baseDesc + " charge")
        fun (facet, op) app (db, con, cat, s : System.TimeSpan, ru) ->
            observeS (facet, op) app (db, con, cat) s.TotalSeconds
            observeRu (facet, op) app (db, con, cat) ru

module private Summaries =

    let labelNames = [| "facet"; "app"; "db"; "con" |]
    let private mkSummary (cfg : Prometheus.SummaryConfiguration) name desc  =
        let s = Prometheus.Metrics.CreateSummary(name, desc, cfg)
        fun (facet : string) app (db, con) o -> s.WithLabels(facet, app, db, con).Observe(o)
    let config =
        let inline qep q e = Prometheus.QuantileEpsilonPair(q, e)
        let objectives = [| qep 0.50 0.05; qep 0.95 0.01; qep 0.99 0.01 |]
        Prometheus.SummaryConfiguration(Objectives = objectives, LabelNames = labelNames, MaxAge = System.TimeSpan.FromMinutes 1.)
    let sAndRuPair stat desc =
        let baseName, baseDesc = Impl.baseName stat, Impl.baseDesc desc
        let observeS = mkSummary config (baseName + "_seconds") (baseDesc + " latency")
        let observeRu = mkSummary config (baseName + "_ru") (baseDesc + " charge")
        fun facet app (db, con, s : System.TimeSpan, ru) ->
            observeS facet app (db, con) s.TotalSeconds
            observeRu facet app (db, con) ru

module private Counters =

    let labelNames = [| "facet"; "op"; "outcome"; "app"; "db"; "con"; "cat" |]
    let private mkCounter (cfg : Prometheus.CounterConfiguration) name desc =
        let h = Prometheus.Metrics.CreateCounter(name, desc, cfg)
        fun (facet : string, op : string, outcome : string) app (db, con, cat) c ->
            h.WithLabels(facet, op, outcome, app, db, con, cat).Inc(c)
    let config = Prometheus.CounterConfiguration(LabelNames = labelNames)
    let total stat desc =
        let name = Impl.baseName (stat + "_total")
        let desc = Impl.baseDesc desc
        mkCounter config name desc
    let eventsAndBytesPair stat desc =
        let observeE = total (stat + "_events") (desc + "Events")
        let observeB = total (stat + "_bytes") (desc + "Bytes")
        fun ctx app (db, con, cat, e, b) ->
            observeE ctx app (db, con, cat) e
            match b with None -> () | Some b -> observeB ctx app (db, con, cat) b

module private Stats =

    let opHistogram =         Histograms.sAndRuPair       "op"                "Operation"
    let roundtripHistogram =  Histograms.sAndRuPair       "roundtrip"         "Fragment"
    let opSummary =           Summaries.sAndRuPair        "op_summary"        "Operation Summary"
    let roundtripSummary =    Summaries.sAndRuPair        "roundtrip_summary" "Fragment Summary"
    let payloadCounters =     Counters.eventsAndBytesPair "payload"           "Payload, "
    let cacheCounter =        Counters.total              "cache"             "Cache"

    let observeLatencyAndCharge (facet, op) app (db, con, cat, s, ru) =
        opHistogram (facet, op) app (db, con, cat, s, ru)
        opSummary facet app (db, con, s, ru)
    let observeLatencyAndChargeWithEventCounts (facet, op, outcome) app (db, con, cat, s, ru, count, bytes) =
        observeLatencyAndCharge (facet, op) app (db, con, cat, s, ru)
        payloadCounters (facet, op, outcome) app (db, con, cat, float count, if bytes = -1 then None else Some (float bytes))

    let inline (|CatSRu|) ({ interval = i; ru = ru } : Equinox.Cosmos.Store.Log.Measurement as m) =
        let cat, _id = FsCodec.StreamName.splitCategoryAndId (FSharp.UMX.UMX.tag m.stream)
        m.database, m.container, cat, i.Elapsed, ru
    let observeRes (facet, _op as stat) app (CatSRu (db, con, cat, s, ru)) =
        roundtripHistogram stat app (db, con, cat, s, ru)
        roundtripSummary facet app (db, con, s, ru)
    let observe_ stat app (CatSRu (db, con, cat, s, ru)) =
        observeLatencyAndCharge stat app (db, con, cat, s, ru)
    let observe (facet, op, outcome) app (CatSRu (db, con, cat, s, ru) as m) =
        observeLatencyAndChargeWithEventCounts (facet, op, outcome) app (db, con, cat, s, ru, m.count, m.bytes)
    let observeTip (facet, op, outcome, cacheOutcome) app (CatSRu (db, con, cat, s, ru) as m) =
        observeLatencyAndChargeWithEventCounts (facet, op, outcome) app (db, con, cat, s, ru, m.count, m.bytes)
        cacheCounter (facet, op, cacheOutcome) app (db, con, cat) 1.

open Equinox.Cosmos.Store.Log

type LogSink(app) =
    interface Serilog.Core.ILogEventSink with
        member __.Emit logEvent = logEvent |> function
            | MetricEvent cm -> cm |> function
                | Op       (Operation.Tip,      m) -> Stats.observeTip  ("query",    "tip",           "ok", "200") app m
                | Op       (Operation.Tip404,   m) -> Stats.observeTip  ("query",    "tip",           "ok", "404") app m
                | Op       (Operation.Tip302,   m) -> Stats.observeTip  ("query",    "tip",           "ok", "302") app m
                | Op       (Operation.Query,    m) -> Stats.observe     ("query",    "query",         "ok")        app m
                | QueryRes (_direction,         m) -> Stats.observeRes  ("query",    "queryPage")                  app m
                | Op       (Operation.Write,    m) -> Stats.observe     ("transact", "sync",          "ok")        app m
                | Op       (Operation.Conflict, m) -> Stats.observe     ("transact", "conflict",      "conflict")  app m
                | Op       (Operation.Resync,   m) -> Stats.observe     ("transact", "resync",        "conflict")  app m
                | Op       (Operation.Prune,    m) -> Stats.observe_    ("prune",    "pruneQuery")                 app m
                | PruneRes (                    m) -> Stats.observeRes  ("prune",    "pruneQueryPage")             app m
                | Op       (Operation.Delete,   m) -> Stats.observe     ("prune",    "delete",        "ok")        app m
            | _ -> ()
