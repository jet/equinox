namespace Equinox.CosmosStore.Prometheus

module private Histograms =

    let private mkHistogram (cfg : Prometheus.HistogramConfiguration) name desc =
        let h = Prometheus.Metrics.CreateHistogram(name, desc, cfg)
        fun (facet : string, op : string) (app : string, cat : string) s -> h.WithLabels(facet, op, app, cat).Observe(s)
    let labelNames = [| "facet"; "op"; "app"; "cat" |]
    let private sHistogram =
        let sBuckets = [| 0.0005; 0.001; 0.002; 0.004; 0.008; 0.016; 0.5; 1.; 2.; 4.; 8. |]
        let sCfg = Prometheus.HistogramConfiguration(Buckets = sBuckets, LabelNames = labelNames)
        mkHistogram sCfg
    let private ruHistogram =
        let ruBuckets = Prometheus.Histogram.ExponentialBuckets(1., 2., 11) // 1 .. 1024
        let ruCfg = Prometheus.HistogramConfiguration(Buckets = ruBuckets, LabelNames = labelNames)
        mkHistogram ruCfg
    let sAndRuPair stat desc =
        let baseName = "equinox_" + stat
        let baseDesc = "Equinox CosmosDB " + desc
        let observeS = sHistogram (baseName + "_seconds") (baseDesc + " latency")
        let observeRu = ruHistogram (baseName + "_ru") (baseDesc + " charge")
        fun (facet, op) app (cat, s, ru) ->
            observeS (facet, op) (app, cat) s
            observeRu (facet, op) (app, cat) ru

module private Counters =

    let private mkCounter (cfg : Prometheus.CounterConfiguration) name desc =
        let h = Prometheus.Metrics.CreateCounter(name, desc, cfg)
        fun (facet : string, op : string, outcome : string) (app : string) (cat : string, c) -> h.WithLabels(facet, op, outcome, app, cat).Inc(c)
    let labelNames = [| "facet"; "op"; "outcome"; "app"; "cat" |]
    let config = Prometheus.CounterConfiguration(LabelNames = labelNames)
    let total stat desc =
        let name = sprintf "equinox_%s_total" stat
        let desc = sprintf "Equinox CosmosDB %s" desc
        mkCounter config name desc
    let eventsAndBytesPair stat desc =
        let observeE = total (stat + "_events") (desc + "Events")
        let observeB = total (stat + "_bytes") (desc + "Bytes")
        fun ctx app (cat, e, b) ->
            observeE ctx app (cat, e)
            match b with None -> () | Some b -> observeB ctx app (cat, b)

module private Summaries =

    let labelNames = [| "facet"; "app" |]

    let private mkSummary (cfg : Prometheus.SummaryConfiguration) name desc  =
        let s = Prometheus.Metrics.CreateSummary(name, desc, cfg)
        fun (facet : string) (app : string) o -> s.WithLabels(facet, app).Observe(o)
    let config =
        let inline qep q e = Prometheus.QuantileEpsilonPair(q, e)
        let objectives = [| qep 0.50 0.05; qep 0.95 0.01; qep 0.99 0.01 |]
        Prometheus.SummaryConfiguration(Objectives = objectives, LabelNames = labelNames, MaxAge = System.TimeSpan.FromMinutes 1.)
    let sAndRuPair stat desc =
        let baseName = "equinox_" + stat
        let baseDesc = "Equinox CosmosDB " + desc
        let observeS = mkSummary config (baseName + "_seconds") (baseDesc + " latency")
        let observeRu = mkSummary config (baseName + "_ru") (baseDesc + " charge")
        fun facet app (s, ru) ->
            observeS facet app s
            observeRu facet app ru

module private Stats =

    let opHistogram = Histograms.sAndRuPair "op" "Operation"
    let roundtripHistogram = Histograms.sAndRuPair "roundtrip" "Fragment"
    let payloadCounters = Counters.eventsAndBytesPair "payload" "Payload, "
    let cacheCounter = Counters.total "cache" "Cache"
    let opSummary = Summaries.sAndRuPair "op_summary" "Operation Summary"
    let roundtripSummaries = Summaries.sAndRuPair "roundtrip_summary" "Fragment Summary"

    let observeLatencyAndCharge (facet, op) app (cat, s, ru) =
        opHistogram (facet, op) app (cat, s, ru)
        opSummary facet app (s, ru)
    let observeWithEventCounts (facet, op, outcome) app (cat, s, ru, count, bytes) =
        observeLatencyAndCharge (facet, op) app (cat, s, ru)
        payloadCounters (facet, op, outcome) app (cat, float count, if bytes = -1 then None else Some (float bytes))

    let cat (streamName : string) =
        let cat, _id = FsCodec.StreamName.splitCategoryAndId (FSharp.UMX.UMX.tag streamName)
        cat

    let inline (|CatSRu|) ({ interval = i; ru = ru } : Equinox.CosmosStore.Core.Log.Measurement as m) =
        let s = let e = i.Elapsed in e.TotalSeconds
        cat m.stream, s, ru
    let observe_ stat app (CatSRu (cat, s, ru)) =
        observeLatencyAndCharge stat app (cat, s, ru)
    let observe (facet, op, outcome) app (CatSRu (cat, s, ru) as m) =
        observeWithEventCounts (facet, op, outcome) app (cat, s, ru, m.count, m.bytes)
    let observeTip (facet, op, outcome, cacheOutcome) app (CatSRu (cat, s, ru) as m) =
        observeWithEventCounts (facet, op, outcome) app (cat, s, ru, m.count, m.bytes)
        cacheCounter (facet, op, cacheOutcome) app (cat, 1.)
    let observeRes (facet, _op as stat) app (CatSRu (cat, s, ru)) =
        roundtripHistogram stat app (cat, s, ru)
        roundtripSummaries facet app (s, ru)

open Equinox.CosmosStore.Core.Log

type LogSink(app) =
    interface Serilog.Core.ILogEventSink with
        member __.Emit logEvent =
            match logEvent with
            | MetricEvent cm ->
                match cm with
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
                | Op       (Operation.Trim,     m) -> Stats.observe     ("prune",    "trim",          "ok")        app m
            | _ -> ()
