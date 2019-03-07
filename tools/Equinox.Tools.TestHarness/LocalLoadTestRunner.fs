namespace Equinox.Tools.TestHarness

open Serilog
open System
open System.Collections.Concurrent

[<RequireQualifiedAccess>]
module Local =

    /// <summary>
    ///     Core in-memory load testing workflow
    /// </summary>
    /// <param name="logger">Logger function.</param>
    /// <param name="logLevel">Log level for load test events.</param>
    /// <param name="aggregationIntervals">Test result bucket aggregation intervals.</param>
    /// <param name="targetTestsPerSecond">Target throughput per second.</param>
    /// <param name="errorCutoff">Number of errors after which the load test should be automatically aborted.</param>
    /// <param name="duration">Duration of the load test.</param>
    /// <param name="sessionFactoryFactory">Asynchronous session factory. Responsible for obtaining a context for running a single test. Sessions may be reused for multiple test runs, but never concurrently.</param>
    /// <param name="singleTestRunner">Load test runner lambda. Performs the actual load test given a session context. Values are treated are succesful test runs, exceptions are treated as failed runs.</param>
    let runLoadTest (log : ILogger) (aggregationIntervals : TimeSpan[])
                    (targetTestsPerSecond : int) (errorCutoff : int64) (duration : TimeSpan)
                    (sessionFactoryFactory : Async<Async<'Session>>)
                    (executeSingleIteration : 'Session -> Async<'T>) = async {

        let! sessionFactory = sessionFactoryFactory
        let testRunner =
            LoadTestRunner.Create(sessionFactory, executeSingleIteration, targetTestsPerSecond)
                            .WithDuration(duration)
                            .WithErrorCutoff(errorCutoff)

        // subscribe a logger
        let _ = testRunner.Events |> Observable.logEvents log
        // create bucket aggregators per requested interval
        let aggregates =
            [| for aggregateBucketSize in aggregationIntervals ->
                testRunner.Events |> Observable.aggregateLoadTests aggregateBucketSize |]
        // subscribe aggregate logging
        for agg in aggregates do agg |> Observable.logAggregate log |> ignore
        // subscribe aggregate collector
        let aggregateQ = new ConcurrentQueue<Envelope<TestResultAggregate>>()
        for agg in aggregates do agg |> Observable.subscribe aggregateQ.Enqueue |> ignore

        // start test, await completion, return aggregates
        do testRunner.Start()
        do! testRunner.AwaitCompletion()
        return aggregateQ |> Seq.sortBy (fun x -> -x.payload.span, x.timestamp) |> Seq.toArray
    }