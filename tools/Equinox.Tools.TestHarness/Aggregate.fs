[<AutoOpen>]
module Equinox.Tools.TestHarness.Aggregate

open MathNet.Numerics.Statistics
open Serilog
open System
open System.Reactive.Linq
open System.Text

type TestResultAggregate with
    static member FromEventBucket (span : TimeSpan) (bucket : seq<LoadTestEvent>) =
        let sessionErrorCount =
            bucket
            |> Seq.choose (function SessionCreationFailed e -> Some e | _ -> None)
            |> Seq.map LoadTestOutcomeType.FromException
            |> Seq.countBy id
            |> Seq.sortByDescending snd
            |> Seq.toArray

        let testResults =
            bucket
            |> Seq.choose (function SingleTestRun info -> Some info | _ -> None)
            |> Seq.toArray

        let testCountByOutcome =
            testResults
            |> Seq.countBy (fun t -> t.OutcomeType)
            |> Seq.sortByDescending snd
            |> Seq.toArray

        let latency =
            if Array.isEmpty testResults then None else
            let sortedLatencies = testResults |> Seq.map (fun r -> let l = r.Latency in l.TotalMilliseconds) |> Seq.sort |> Seq.toArray
            let pc p = SortedArrayStatistics.Percentile(sortedLatencies, p) |> TimeSpan.FromMilliseconds
            Some {
                avg = ArrayStatistics.Mean sortedLatencies |> TimeSpan.FromMilliseconds
                stddev =
                    let stdDev = ArrayStatistics.StandardDeviation sortedLatencies
                    // stdev of singletons is NaN
                    if Double.IsNaN stdDev then None else TimeSpan.FromMilliseconds stdDev |> Some

                min = SortedArrayStatistics.Minimum sortedLatencies |> TimeSpan.FromMilliseconds
                max = SortedArrayStatistics.Maximum sortedLatencies |> TimeSpan.FromMilliseconds
                p50 = pc 50
                p95 = pc 95
                p99 = pc 99
            }

        {
            count = Array.length testResults
            span = span
            testOutcomes = testCountByOutcome
            sessionErrors = sessionErrorCount
            latency = latency
        }

    static member Render(e : Envelope<TestResultAggregate>) =
        let sb = new StringBuilder()

        let renderOutcome (sb:StringBuilder) (kvs : OutcomeDistribution) =
            for k,v in kvs |> Seq.sortBy fst do sb.Appendf " %s=%d" k.Id v
            sb.AppendLine() |> ignore

        sb.AppendLine() |> ignore
        sb.Appendfn "Bucket   : span=%gs timestamp=%s" e.payload.span.TotalSeconds (e.timestamp.ToString "s")

        match e.payload.testOutcomes with
        | [||] -> ()
        | outcomes -> sb.Appendf "Tests    : total=%d" e.payload.count ; renderOutcome sb outcomes

        match e.payload.sessionErrors with
        | [||] -> ()
        | errors -> sb.Appendf "Sessions :" ; renderOutcome sb errors

        match e.payload.latency with
        | None -> ()
        | Some l ->
            let inline sec(t:TimeSpan) = t.TotalSeconds
            let stdDev = match l.stddev with None -> Double.NaN | Some d -> sec d
            sb.AppendFormat( "Latency  : max={0:n3}s p99={1:n3}s p95={2:n3}s p50={3:n3}s min={4:n3}s avg={5:n3}s stddev={6:n3}s",
                             sec l.max, sec l.p99, sec l.p95, sec l.p50, sec l.min, sec l.avg, stdDev) |> ignore

        sb.ToString()

[<RequireQualifiedAccess>]
module Observable =

    /// Aggregates a stream of load test events into a stream of bucketed aggregates with given timespan
    let aggregateLoadTests (bucketSize : TimeSpan) (source : IObservable<Envelope<LoadTestEvent>>) =
        Observable.Buffer(source, bucketSize)
        |> Observable.filter (fun bucket -> bucket.Count > 0)
        |> Observable.map (fun bucket ->
            let agg = bucket |> Seq.map (fun e -> e.payload) |> TestResultAggregate.FromEventBucket bucketSize
            let e = bucket |> Seq.maxBy (fun e -> e.timestamp)
            { payload = agg ; timestamp = e.timestamp ; hostname = e.hostname })
        |> Observable.filter (fun agg -> agg.payload.count > 0 || agg.payload.sessionErrors.Length > 0)

    /// Logs a stream of aggregates using supplied logger function
    let logAggregate (log: ILogger) (source : IObservable<Envelope<TestResultAggregate>>) : IDisposable =
        source
        |> Observable.map TestResultAggregate.Render
        |> Observable.subscribe (fun r -> log.Information("Aggregate: {result:l}",r))

    /// Logs load test events with provided log level
    let logEvents (log : ILogger) (events : IObservable<Envelope<LoadTestEvent>>) =
        let eventLogger (e : Envelope<LoadTestEvent>) =
            match e.payload with
            | SessionCreated _ -> ()
            | SingleTestRun _ -> ()
            | LoadTestStarted -> log.Information "Load test started"
            | SessionCreationFailed _ as  e -> log.Debug("Error creating session {info}", e)
            | SessionPoolInfo info ->
                log.Debug("Session pool Allocations={allocations} Active={active} Min={min} Max={max}",
                    info.allocatedSessions, info.activeSessions, info.minSessions, info.maxSessions)
            | SessionPoolCreating count -> log.Information("Initializing session pool count={count}", count)
            | SessionPoolReady -> log.Information "Session pool ready"
            | SessionPoolCapacityReached -> log.Warning "Session pool capacity reached"
            | LoadTestCompleting None -> log.Information "Signaled load test completion"
            | LoadTestCompleting (Some reason) -> log.Information("Signaled load test completion reason={reason}", reason)
            | LoadTestCompleted -> log.Information "Load test completed"

        events.Subscribe eventLogger