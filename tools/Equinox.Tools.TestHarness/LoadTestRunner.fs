namespace Equinox.Tools.TestHarness

open System
open System.Threading
open System.Threading.Tasks
open System.Collections.Concurrent

// Provides a load test runner implementation for executing parallelized tests in-memory.
// Load tests are abstracted under a simple pattern that uses session factories and load tests
// that are parameterized by sessions. The simplified signature looks as follows:
//
//    val runLoadTest : sessionFactory:Async<'Session> -> singleTest:('Session -> Async<unit>) -> throughput:int -> Async<unit>
//
// Each session instance can be reused for multiple test runs, but is guaranteed to be consumed by at most
// one test run at any given point in time. Sessions are managed by a session pool which pre-emptively
// allocates instances in order to best preserve throughput and to minimize coordinated omission issues.

module private LocalLoadTestImpl =

    do if not <| System.Threading.ThreadPool.SetMinThreads(768, 768) then exit 1

    /// asynchronously executes action after supplied delay
    let delay : TimeSpan -> (unit -> unit) -> unit =
        fun waitTime action ->
            Task.Delay(waitTime).ContinueWith(fun (_:Task) -> action ())
            |> ignore

    /// Thread-safe latch
    type Latch() =
        let mutable isTriggered = 0
        member __.Enter() = Interlocked.CompareExchange(&isTriggered, 1, 0) = 0
        member __.Reset() = isTriggered <- 0

    /// Thread-safe counter
    type ConcurrentCounter() =
        let mutable count = 0L
        member __.Increment() = Interlocked.Increment &count
        member __.Decrement() = Interlocked.Decrement &count
        member __.Value = count

    /// Atomic enum cell
    type EnumAtom<'Enum when 'Enum : enum<int>>(init : 'Enum) =
        static let toInt x = LanguagePrimitives.EnumToValue<'Enum, int> x
        static let fromInt x = LanguagePrimitives.EnumOfValue<int, 'Enum> x
        let mutable state = toInt init
        /// Atomically updates to newValue assuming that cell is currentValue,
        /// returning a boolean indicating success
        member __.CompareExchange (currentValue : 'Enum) (newValue : 'Enum) : bool =
            Interlocked.CompareExchange(&state, toInt newValue, toInt currentValue) = toInt currentValue

        member __.Value
            with get() = fromInt state
            and set v = state <- toInt v

    type SessionContext<'Session> =
        {
            SessionId : string
            Session   : 'Session
            TestCount : int64
        }

    and private SessionContextInner<'Session> =
        {
            SessionId : string
            Session : 'Session
            mutable TestCount : int64
            mutable IsLeased : bool
        }

    /// Manages a pool of reusable load test sessions that get allocated on demand
    type SessionPool<'Session> private (factory : Async<'Session>, eventSink : LoadTestEvent -> unit, throughput : int, minSessions : int, maxSessions : int) =
        static let mkSessionId() = let f = Guid.NewGuid() in string f
        let sessions = new ConcurrentDictionary<string, SessionContextInner<'Session>>()
        let pooledSessions = ConcurrentQueue<string>()

        let cts = new CancellationTokenSource()
        let allocationCount = new ConcurrentCounter()
        let leaseCount = new ConcurrentCounter()
        let capacityReached = new Latch()

        let shouldAllocateNewSession() =
            if allocationCount.Value > int64 maxSessions then
                // do not allocate if capacity reached
                false

            elif allocationCount.Increment() > int64 maxSessions then
                // roll back counter if failed
                let _ = allocationCount.Decrement()
                false
            else
                true

        let tryAllocateSession() = async {
            if shouldAllocateNewSession() then
                let! result = Async.Catch factory
                match result with
                | Choice2Of2 e ->
                    // roll back allocation count if failed
                    let _ = allocationCount.Increment()
                    eventSink (LoadTestEvent.SessionCreationFailed e)
                    return None
                | Choice1Of2 session ->
                    let ctx = { SessionId = mkSessionId() ; Session = session ; TestCount = 0L ; IsLeased = false }
                    do sessions.[ctx.SessionId] <- ctx
                    eventSink (LoadTestEvent.SessionCreated ctx.SessionId)
                    return Some ctx
            else
                return None
        }

        let exportAsLease (ctx : SessionContextInner<'Session>) : SessionContext<'Session> =
            ctx.IsLeased <- true
            ctx.TestCount <- ctx.TestCount + 1L
            let _ = leaseCount.Increment()
            { SessionId = ctx.SessionId ; Session = ctx.Session ; TestCount = ctx.TestCount }

        let rec allocatorLoop n = async {
            let allocationCount = int allocationCount.Value
            let leaseCount = int leaseCount.Value
            let availableSessions = allocationCount - leaseCount

            // only occassionally log this information
            if n % 20L = 0L then
                { minSessions = minSessions ; maxSessions = maxSessions ; allocatedSessions = allocationCount ; activeSessions = leaseCount }
                |> SessionPoolInfo
                |> eventSink

            if availableSessions < minSessions && allocationCount < maxSessions then
                let alloc = async {
                    let! result = tryAllocateSession()
                    result |> Option.iter (fun ctx -> pooledSessions.Enqueue ctx.SessionId)
                }

                do! [|for _i in 1 .. (minSessions - availableSessions) -> alloc |]
                    |> Async.ParallelThrottled (max (throughput / 10) 1)
                    |> Async.Ignore

                do! allocatorLoop (n + 1L)

            else
                do! Async.Sleep 100
                do! allocatorLoop (n + 1L)
        }

        let _ = Async.StartAsTask(allocatorLoop 1L, cancellationToken = cts.Token)

        member __.LeaseCount = int leaseCount.Value
        member __.AllocationCount = sessions.Count
        member __.AvailableSessions = sessions.Count - int leaseCount.Value

        /// Request a session from the pool
        member __.TryRequestSession() : Async<SessionContext<'Session> option> = async {
            let foundPooledSession, sessionId = pooledSessions.TryDequeue()
            if foundPooledSession then
                let ctx = sessions.[sessionId]
                return exportAsLease ctx |> Some
            else
                let! ctx = tryAllocateSession()
                match ctx with
                | Some c -> return exportAsLease c |> Some
                | None ->
                    // occasionally log capacity reached events
                    if capacityReached.Enter() then
                        eventSink SessionPoolCapacityReached
                        delay (TimeSpan.FromSeconds 10.) capacityReached.Reset

                    return None
        }

        /// Release a session lease back to the pool
        member __.YieldSession(sessionId : string) : unit =
            let ok,ctx = sessions.TryGetValue sessionId
            if not ok || not ctx.IsLeased then invalidArg "sessionId" "Invalid session id"
            ctx.IsLeased <- false
            let _ = leaseCount.Decrement()
            pooledSessions.Enqueue ctx.SessionId

        interface IDisposable with
            member  __.Dispose() =
                cts.Cancel()
                for ctx in sessions.Values do
                    match box ctx.Session with
                    | :? IDisposable as d -> d.Dispose()
                    | _ -> ()

        static member Create(sessionFactory : Async<'Session>, eventSink : LoadTestEvent -> unit, throughput : int, minSessions : int, maxSessions : int) = async {
            eventSink (SessionPoolCreating minSessions)
            let pool = new SessionPool<'Session>(sessionFactory, eventSink, throughput, minSessions, maxSessions)
            // poll until min number of sessions has been allocated
            while pool.AllocationCount < minSessions do do! Async.Sleep 100
            eventSink SessionPoolReady
            return pool
        }

    type SessionPool<'T> with
        member pool.AwaitAllLeases(?timeout : TimeSpan) = async {
            let awaiter = async { while pool.LeaseCount > 0 do do! Async.Sleep 200 }
            match timeout with
            | None -> return! awaiter
            | Some t -> return! Async.choose awaiter (Async.Sleep (int t.TotalMilliseconds))
        }

    let runSingleLoadTest (pool : SessionPool<'Session>)
                          (eventSink : LoadTestEvent -> unit)
                          (singleTestRunner : 'Session -> Async<'T>) : Async<unit> = async {

        let! sessionResult = pool.TryRequestSession()

        match sessionResult with
        | None -> () // pool reached capacity, skip this run
        | Some ctx ->

        let startTime = DateTimeOffset.Now

        let! outcome = async {
            try
                let! _r = singleTestRunner ctx.Session
                return None
            with e -> return Some e
        }

        let endTime = DateTimeOffset.Now

        let testInfo =
            {
                sessionId = ctx.SessionId
                testId = sprintf "%s-%d" ctx.SessionId ctx.TestCount
                startTime = startTime
                endTime = endTime
                error = outcome
            }

        eventSink (SingleTestRun testInfo)

        pool.YieldSession ctx.SessionId
    }

    // executes lambda on given MS interval
    let runOnInterval : int -> (unit -> unit) -> IDisposable =
        fun intervalMS f -> new Timer(new TimerCallback(fun _ -> f ()), null, 0, intervalMS) :> _

    // Load tests are being sent in batches every 50ms. Compute the batch size based
    // on given requests/sec target and current slot
    let [<Literal>] batchIntervalMilliseconds = 50
    let [<Literal>] batchesPerSecond = 20
    let calculateNextBatchSize (batchIndex : int64) (targetRequestsPerSecond : int) =
        let q,r = targetRequestsPerSecond / batchesPerSecond, targetRequestsPerSecond % batchesPerSecond
        if r = 0 then q else

        let slot = batchIndex % int64 batchesPerSecond |> int
        if slot <= r then q + 1 else q

    let mkCompletionToken<'T>() =
        let tcs = new TaskCompletionSource<'T>()
        let setter t = tcs.TrySetResult t |> ignore
        let awaiter = Async.AwaitTaskCorrect tcs.Task
        setter, awaiter

    // the main load test runner implementation
    let runLoadTest (sessionFactory : Async<'Session>) (singleTestRunner : 'Session -> Async<'T>)
                    (targetTestsPerSecond : int) (minSessions : int) (maxSessions : int)
                    (completionToken : Async<string option>) (eventSink : LoadTestEvent -> unit) : Async<unit> = async {

        use! sessionPool = SessionPool<'Session>.Create(sessionFactory, eventSink, targetTestsPerSecond, minSessions, maxSessions)
        let singleRunner = runSingleLoadTest sessionPool eventSink singleTestRunner

        let innerCts = new CancellationTokenSource()
        let batchCount = new ConcurrentCounter()

        let runSingleBatch () =
            let slot = batchCount.Increment()
            let batchCount = calculateNextBatchSize (abs slot) targetTestsPerSecond
            for i = 1 to batchCount do
                Async.StartAsTask(singleRunner, cancellationToken = innerCts.Token)
                |> ignore

        do eventSink LoadTestStarted
        let d = runOnInterval batchIntervalMilliseconds runSingleBatch
        let! completionReason = completionToken // await outer completion signal
        do d.Dispose() // stop the periodic runner
        do eventSink (LoadTestCompleting completionReason)

        // wait until all current tests complete or 5 seconds
        do! sessionPool.AwaitAllLeases(timeout = TimeSpan.FromSeconds 5.)
        do innerCts.Cancel()
        do eventSink LoadTestCompleted
    }

open LocalLoadTestImpl

type LoadTestRunnerState =
    | NotStarted    = 0
    | Running       = 1
    | Completing    = 2
    | Completed     = 3

[<Sealed; AutoSerializable(false)>]
type LoadTestRunner private (runner : Async<string option> -> (LoadTestEvent -> unit) -> Async<unit>) =
    let hostname = Environment.MachineName
    let complete, token = mkCompletionToken<string option>()
    let event = new Event<Envelope<LoadTestEvent>>()
    let state = new EnumAtom<LoadTestRunnerState>(LoadTestRunnerState.NotStarted)
    let runner = runner token (fun e -> event.Trigger { payload = e ; timestamp = DateTimeOffset.Now ; hostname = hostname })
    let mutable task = None

    /// Load test event observable
    [<CLIEvent>]
    member __.Events = event.Publish
    member __.State = state.Value

    /// Starts the the load test runner
    member __.Start() =
        if state.CompareExchange LoadTestRunnerState.NotStarted LoadTestRunnerState.Running then
            let t = Async.StartAsTask(async { let! _ = runner in state.Value <- LoadTestRunnerState.Completed })
            task <- Some t

    /// Stops the load test runner
    member __.Stop(?reason : string) =
        if state.CompareExchange LoadTestRunnerState.Running LoadTestRunnerState.Completing then
            complete reason

    /// Asynchronously awaits for the load test to complete
    member __.AwaitCompletion() =
        match task with
        | None -> invalidOp "Load test has not been started yet"
        | Some t -> Async.AwaitTaskCorrect t

    interface IDisposable with member __.Dispose() = __.Stop(reason = "disposed")

    /// <summary>
    ///     Creates an in-memory load test runner instance with supplied parameters.
    /// </summary>
    /// <param name="sessionFactory">Asynchronous session factory. Generates a context for running a single test. Sessions may be reused for multiple test runs, but never concurrently.</param>
    /// <param name="testRunner">Load test runner lambda. Performs the actual load test given a session context. Values are treated are succesful test runs, exceptions are treated as failed runs.</param>
    /// <param name="targetTestsPerSecond">Target load throughput in requests per second.</param>
    /// <param name="minSessions">Minimum sessions to be allocated by the session pool. Defaults to 2 * targetTestsPerSecond.</param>
    /// <param name="maxSessions">Maximum number of sessions to be allocated by the session pool. Defaults to 20 * targetTestsPerSecond.</param>
    static member Create(sessionFactory : Async<'Session>, testRunner : 'Session -> Async<'T>, targetTestsPerSecond : int, ?minSessions : int, ?maxSessions : int) =
        let minSessions = defaultArg minSessions (2 * targetTestsPerSecond)
        let maxSessions = defaultArg maxSessions (20 * targetTestsPerSecond)

        if targetTestsPerSecond < 1 then invalidArg "testsPerSecondTarget" "must be positive value"
        if minSessions < 1 then invalidArg "minSessions" "must be positive value"
        if maxSessions < 1 then invalidArg "maxSessions" "must be positive value"
        if minSessions > maxSessions then invalidArg "maxSessions" "should be greater or equal to 'minSessions'"

        let runner = runLoadTest sessionFactory testRunner targetTestsPerSecond minSessions maxSessions
        new LoadTestRunner(runner)


//------------------------------------------------------------

type LoadTestRunner with

    /// Specifies a duration for the load test
    member runner.WithDuration(duration : TimeSpan) : LoadTestRunner =
        if duration < TimeSpan.FromSeconds 1. then invalidArg "duration" "must be at least one second"
        let sub = ref Unchecked.defaultof<IDisposable>
        let latch = new Latch()
        let callback _ =
            if latch.Enter() then
                use d = !sub
                let _t = delay duration (fun () -> runner.Stop())
                ()

        sub := runner.Events |> Observable.subscribe callback
        runner

    /// Specifies a threshold after which the load test should be aborted
    member runner.WithErrorCutoff(errorThreshold : int64) : LoadTestRunner =
        let errors = new ConcurrentCounter()
        let callback e =
            if LoadTestEvent.IsErrorEvent e.payload &&
               errors.Increment() = errorThreshold
            then runner.Stop(reason = "error threshold reached") |> ignore

        let _d = runner.Events.Subscribe callback
        runner