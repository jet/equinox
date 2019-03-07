namespace Equinox.Tools.TestHarness

open System
open System.Net
open System.Threading.Tasks

open HttpHelpers

/// Generic message envelope
[<NoEquality; NoComparison>]
type Envelope<'T> = { payload : 'T ; timestamp : DateTimeOffset ; hostname : string }

/// Load Test outcome classification
type LoadTestOutcomeType =
    | Completed
    | Timeout
    | InvalidHttpResponse of status:HttpStatusCode
    | OtherError of errorType:string
with
    member o.Id =
        match o with
        | Completed -> "success"
        | Timeout -> "timeout"
        | InvalidHttpResponse s -> sprintf "HttpStatusCode.%O" s
        | OtherError e -> e

    static member FromException(e : exn) =
        match e with
        | InnermostExn (:? TimeoutException | :? TaskCanceledException) -> Timeout
        | InnermostExn (:? InvalidHttpResponseException as e) -> InvalidHttpResponse e.StatusCode
        | e ->
            let et = e.GetType()
            if et = typeof<exn> then OtherError e.Message
            else OtherError et.Name


/// Single load test outcome information
[<NoComparison>]
type TestRunInfo =
    {
        sessionId   : string
        testId      : string
        startTime   : DateTimeOffset
        endTime     : DateTimeOffset
        error       : exn option
    }
with
    member r.Latency = r.endTime - r.startTime
    member r.IsSuccess = Option.isNone r.error
    member r.IsFailure = Option.isSome r.error
    member r.OutcomeType =
        match r.error with
        | None -> Completed
        | Some e -> LoadTestOutcomeType.FromException e

/// Session pool metadata for logging purposes
type SessionPoolInfo =
    {
        minSessions : int
        maxSessions : int
        allocatedSessions : int
        activeSessions : int
    }

/// The main event type emitted by the load test runner
[<NoComparison>]
type LoadTestEvent =
    | SessionCreated of sessionId:string
    | SessionCreationFailed of error:exn
    | SessionPoolCreating of sessionCount:int
    | SessionPoolInfo of SessionPoolInfo
    | SessionPoolReady
    | SessionPoolCapacityReached
    | LoadTestStarted
    | SingleTestRun of info:TestRunInfo
    | LoadTestCompleting of reason:string option
    | LoadTestCompleted
with
    static member IsErrorEvent (e : LoadTestEvent) =
        match e with
        | SessionCreationFailed _ -> true
        | SingleTestRun { error = Some _ } -> true
        | _ -> false

//----------------------------------------------

/// Load test result aggregate computed from a
/// periodic bucket of load test events
type TestResultAggregate =
    {
        /// Declared span of result aggregate
        span          : TimeSpan
        /// Load test count
        count         : int
        /// Latency stats, None if the number of tests is zero
        latency       : LatencyStatistics option
        /// Load test outcome distribution
        testOutcomes  : OutcomeDistribution
        /// Session initialization error distribution
        sessionErrors : OutcomeDistribution
    }

and LatencyStatistics =
    {
        min    : TimeSpan
        p50    : TimeSpan
        p95    : TimeSpan
        p99    : TimeSpan
        max    : TimeSpan

        avg    : TimeSpan
        stddev : TimeSpan option
    }

/// Load test outcome distribution
and OutcomeDistribution = (LoadTestOutcomeType * int) []