namespace Equinox.Projection

open Serilog
open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Diagnostics
open System.Threading

/// Item from a reader as supplied to the `IIngester`
type [<NoComparison>] StreamItem = { stream: string; index: int64; event: Equinox.Codec.IEvent<byte[]> }

/// Core interface for projection system, representing the complete contract a feed consumer uses to deliver batches of work for projection
type IIngester =
    /// Passes a (lazy) batch of items into the Ingestion Engine; the batch will be materialized out of band and submitted to the Projection engine for scheduling
    /// Admission is Async in order that the Projector and Ingester can together contrive to force backpressure on the producer of the batches
    /// Returns dynamic position in the queue at time of posting, together with current max number of items permissible
    abstract member Submit: progressEpoch: int64 * markCompleted: Async<unit> * items: StreamItem seq -> Async<int*int>
    /// Requests immediate cancellation of 
    abstract member Stop: unit -> unit

/// Gathers stats relating to how many items of a given category have been observed
type CatStats() =
    let cats = Dictionary<string,int64>()
    member __.Ingest(cat,?weight) = 
        let weight = defaultArg weight 1L
        match cats.TryGetValue cat with
        | true, catCount -> cats.[cat] <- catCount + weight
        | false, _ -> cats.[cat] <- weight
    member __.Any = cats.Count <> 0
    member __.Clear() = cats.Clear()
#if NET461
    member __.StatsDescending = cats |> Seq.map (|KeyValue|) |> Seq.sortBy (fun (_,s) -> -s)
#else
    member __.StatsDescending = cats |> Seq.map (|KeyValue|) |> Seq.sortByDescending snd
#endif

[<AutoOpen>]
module private Impl =
    let (|NNA|) xs = if xs = null then Array.empty else xs
    let arrayBytes (x:byte[]) = if x = null then 0 else x.Length
    let inline eventSize (x : Equinox.Codec.IEvent<_>) = arrayBytes x.Data + arrayBytes x.Meta + x.EventType.Length + 16
    let inline mb x = float x / 1024. / 1024.
    let expiredMs ms =
        let timer = Stopwatch.StartNew()
        fun () ->
            let due = timer.ElapsedMilliseconds > ms
            if due then timer.Restart()
            due
    type Sem(max) =
        let inner = new SemaphoreSlim(max)
        member __.Release(?count) = match defaultArg count 1 with 0 -> () | x -> inner.Release x |> ignore
        member __.State = max-inner.CurrentCount,max 
        /// Wait infinitely to get the semaphore
        member __.Await() = inner.Await() |> Async.Ignore
        /// Wait for the specified timeout to acquire (or return false instantly)
        member __.TryAwait(?timeout) = inner.Await(defaultArg timeout TimeSpan.Zero)
        member __.HasCapacity = inner.CurrentCount > 0

module Progress =

    type [<NoComparison; NoEquality>] internal BatchState = { markCompleted: unit -> unit; streamToRequiredIndex : Dictionary<string,int64> }

    type State<'Pos>() =
        let pending = Queue<_>()
        let trim () =
            while pending.Count <> 0 && pending.Peek().streamToRequiredIndex.Count = 0 do
                let batch = pending.Dequeue()
                batch.markCompleted()
        member __.AppendBatch(markCompleted, reqs : Dictionary<string,int64>) =
            pending.Enqueue { markCompleted = markCompleted; streamToRequiredIndex = reqs }
            trim ()
        member __.MarkStreamProgress(stream, index) =
            for x in pending do
                match x.streamToRequiredIndex.TryGetValue stream with
                | true, requiredIndex when requiredIndex <= index -> x.streamToRequiredIndex.Remove stream |> ignore
                | _, _ -> ()
            trim ()
        member __.InScheduledOrder getStreamWeight =
            let raw = seq {
                let streams = HashSet()
                let mutable batch = 0
                for x in pending do
                    batch <- batch + 1
                    for s in x.streamToRequiredIndex.Keys do
                        if streams.Add s then
                            yield s,(batch,getStreamWeight s) }
            raw |> Seq.sortBy (fun (_s,(b,l)) -> b,-l) |> Seq.map fst

    /// Manages writing of progress
    /// - Each write attempt is always of the newest token (each update is assumed to also count for all preceding ones)
    /// - retries until success or a new item is posted
    type Writer<'Res when 'Res: equality>() =
        let pumpSleepMs = 100
        let due = expiredMs 5000L
        let mutable committedEpoch = None
        let mutable validatedPos = None
        let result = Event<Choice<'Res,exn>>()
        [<CLIEvent>] member __.Result = result.Publish
        member __.Post(version,f) =
            Volatile.Write(&validatedPos,Some (version,f))
        member __.CommittedEpoch = Volatile.Read(&committedEpoch)
        member __.Pump() = async {
            let! ct = Async.CancellationToken
            while not ct.IsCancellationRequested do
                match Volatile.Read &validatedPos with
                | Some (v,f) when Volatile.Read(&committedEpoch) <> Some v && due () ->
                    try do! f
                        Volatile.Write(&committedEpoch, Some v)
                        result.Trigger (Choice1Of2 v)
                    with e -> result.Trigger (Choice2Of2 e)
                | _ -> do! Async.Sleep pumpSleepMs }

module Buffer =

    type [<NoComparison>] Span = { index: int64; events: Equinox.Codec.IEvent<byte[]>[] }
    module Span =
        let (|End|) (x : Span) = x.index + if x.events = null then 0L else x.events.LongLength
        let trim min : Span -> Span = function
            | x when x.index >= min -> x // don't adjust if min not within
            | End n when n < min -> { index = min; events = [||] } // throw away if before min
#if NET461
            | x -> { index = min; events = x.events |> Seq.skip (min - x.index |> int) |> Seq.toArray }
#else
            | x -> { index = min; events = x.events |> Array.skip (min - x.index |> int) }  // slice
#endif
        let merge min (xs : Span seq) =
            let xs =
                seq { for x in xs -> { x with events = (|NNA|) x.events } }
                |> Seq.map (trim min)
                |> Seq.filter (fun x -> x.events.Length <> 0)
                |> Seq.sortBy (fun x -> x.index)
            let buffer = ResizeArray()
            let mutable curr = None
            for x in xs do
                match curr, x with
                // Not overlapping, no data buffered -> buffer
                | None, _ ->
                    curr <- Some x
                // Gap
                | Some (End nextIndex as c), x when x.index > nextIndex ->
                    buffer.Add c
                    curr <- Some x
                // Overlapping, join
                | Some (End nextIndex as c), x  ->
                    curr <- Some { c with events = Array.append c.events (trim nextIndex x).events }
            curr |> Option.iter buffer.Add
            if buffer.Count = 0 then null else buffer.ToArray()
    type [<NoComparison>] StreamSpan = { stream: string; span: Span }
    type [<NoComparison>] StreamState = { isMalformed: bool; write: int64 option; queue: Span[] } with
        member __.Size =
            if __.queue = null then 0
            else __.queue |> Seq.collect (fun x -> x.events) |> Seq.sumBy eventSize
        member __.IsReady =
            if __.queue = null || __.isMalformed then false
            else
                match __.write, Array.tryHead __.queue with
                | Some w, Some { index = i } -> i = w
                | None, _ -> true
                | _ -> false
    module StreamState =
        let inline optionCombine f (r1: int64 option) (r2: int64 option) =
            match r1, r2 with
            | Some x, Some y -> f x y |> Some
            | None, None -> None
            | None, x | x, None -> x
        let combine (s1: StreamState) (s2: StreamState) : StreamState =
            let writePos = optionCombine max s1.write s2.write
            let items = let (NNA q1, NNA q2) = s1.queue, s2.queue in Seq.append q1 q2
            { write = writePos; queue = Span.merge (defaultArg writePos 0L) items; isMalformed = s1.isMalformed || s2.isMalformed }

module Scheduling =

    open Buffer

    type StreamStates() =
        let mutable streams = Set.empty 
        let states = Dictionary<string, StreamState>()
        let update stream (state : StreamState) =
            match states.TryGetValue stream with
            | false, _ ->
                states.Add(stream, state)
                streams <- streams.Add stream
                stream, state
            | true, current ->
                let updated = StreamState.combine current state
                states.[stream] <- updated
                stream, updated
        let updateWritePos stream isMalformed pos span = update stream { isMalformed = isMalformed; write = pos; queue = span }
        let markCompleted stream index = updateWritePos stream false (Some index) null |> ignore

        let busy = HashSet<string>()
        let pending trySlipstreamed (requestedOrder : string seq) = seq {
            let proposed = HashSet()
            for s in requestedOrder do
                let state = states.[s]
                if state.IsReady && not (busy.Contains s) then
                    proposed.Add s |> ignore
                    yield state.write, { stream = s; span = state.queue.[0] }
            if trySlipstreamed then
                // [lazily] Slipstream in futher events that have been posted to streams which we've already visited
                for KeyValue(s,v) in states do
                    if v.IsReady && not (busy.Contains s) && proposed.Add s then
                        yield v.write, { stream = s; span = v.queue.[0] } }
        let markBusy stream = busy.Add stream |> ignore
        let markNotBusy stream = busy.Remove stream |> ignore

        // Result is intentionally a thread-safe persisent data structure
        // This enables the (potentially multiple) Ingesters to determine streams (for which they potentially have successor events) that are in play
        // Ingesters then supply these 'preview events' in advance of the processing being scheduled
        // This enables the projection logic to roll future work into the current work in the interests of medium term throughput
        member __.All = streams
        member __.InternalMerge(stream, state) = update stream state |> ignore
        member __.InternalUpdate stream pos queue = update stream { isMalformed = false; write = Some pos; queue = queue }
        member __.Add(stream, index, event, ?isMalformed) =
            updateWritePos stream (defaultArg isMalformed false) None [| { index = index; events = [| event |] } |]
        member __.Add(batch: StreamSpan, isMalformed) =
            updateWritePos batch.stream isMalformed None [| { index = batch.span.index; events = batch.span.events } |]
        member __.SetMalformed(stream,isMalformed) =
            updateWritePos stream isMalformed None [| { index = 0L; events = null } |]
        member __.QueueWeight(stream) =
            states.[stream].queue.[0].events |> Seq.sumBy eventSize
        member __.MarkBusy stream =
            markBusy stream
        member __.MarkCompleted(stream, index) =
            markNotBusy stream
            markCompleted stream index
        member __.MarkFailed stream =
            markNotBusy stream
        member __.Pending(trySlipstreamed, byQueuedPriority : string seq) : (int64 option * StreamSpan) seq =
            pending trySlipstreamed byQueuedPriority
        member __.Dump(log : ILogger, categorize) =
            let mutable busyCount, busyB, ready, readyB, unprefixed, unprefixedB, malformed, malformedB, synced = 0, 0L, 0, 0L, 0, 0L, 0, 0L, 0
            let busyCats, readyCats, readyStreams, unprefixedStreams, malformedStreams = CatStats(), CatStats(), CatStats(), CatStats(), CatStats()
            let kb sz = (sz + 512L) / 1024L
            for KeyValue (stream,state) in states do
                match int64 state.Size with
                | 0L ->
                    synced <- synced + 1
                | sz when busy.Contains stream ->
                    busyCats.Ingest(categorize stream)
                    busyCount <- busyCount + 1
                    busyB <- busyB + sz
                | sz when state.isMalformed ->
                    malformedStreams.Ingest(stream, mb sz |> int64)
                    malformed <- malformed + 1
                    malformedB <- malformedB + sz
                | sz when not state.IsReady ->
                    unprefixedStreams.Ingest(stream, mb sz |> int64)
                    unprefixed <- unprefixed + 1
                    unprefixedB <- unprefixedB + sz
                | sz ->
                    readyCats.Ingest(categorize stream)
                    readyStreams.Ingest(sprintf "%s@%dx%d" stream (defaultArg state.write 0L) state.queue.[0].events.Length, kb sz)
                    ready <- ready + 1
                    readyB <- readyB + sz
            log.Information("Streams Synced {synced:n0} Active {busy:n0}/{busyMb:n1}MB Ready {ready:n0}/{readyMb:n1}MB Waiting {waiting}/{waitingMb:n1}MB Malformed {malformed}/{malformedMb:n1}MB",
                synced, busyCount, mb busyB, ready, mb readyB, unprefixed, mb unprefixedB, malformed, mb malformedB)
            if busyCats.Any then log.Information("Active Categories, events {@busyCats}", Seq.truncate 5 busyCats.StatsDescending)
            if readyCats.Any then log.Information("Ready Categories, events {@readyCats}", Seq.truncate 5 readyCats.StatsDescending)
            if readyCats.Any then log.Information("Ready Streams, KB {@readyStreams}", Seq.truncate 5 readyStreams.StatsDescending)
            if unprefixedStreams.Any then log.Information("Waiting Streams, KB {@missingStreams}", Seq.truncate 3 unprefixedStreams.StatsDescending)
            if malformedStreams.Any then log.Information("Malformed Streams, MB {@malformedStreams}", malformedStreams.StatsDescending)

    /// Messages used internally by projector, including synthetic ones for the purposes of the `Stats` listeners
    [<NoComparison; NoEquality>]
    type InternalMessage<'R> =
        /// Submit new data pertaining to a stream that has commenced processing
        | Merge of KeyValuePair<string,StreamState>[]
        /// Stats per submitted batch for stats listeners to aggregate
        | Added of streams: int * skip: int * events: int
        /// Result of processing on stream - result (with basic stats) or the `exn` encountered
        | Result of stream: string * outcome: Choice<'R,exn>

    type BufferState = Idle | Full | Slipstreaming
    /// Gathers stats pertaining to the core projection/ingestion activity
    type Stats<'R>(log : ILogger, statsInterval : TimeSpan, stateInterval : TimeSpan) =
        let states, fullCycles, cycles, resultCompleted, resultExn = CatStats(), ref 0, ref 0, ref 0, ref 0
        let merges, mergedStreams, batchesPended, streamsPended, eventsSkipped, eventsPended = ref 0, ref 0, ref 0, ref 0, ref 0, ref 0
        let statsDue, stateDue = expiredMs (int64 statsInterval.TotalMilliseconds), expiredMs (int64 stateInterval.TotalMilliseconds)
        let dumpStats (used,maxDop) pendingCount =
            log.Information("Cycles {cycles}/{fullCycles} {@states} Projecting {busy}/{processors} Completed {completed} Exceptions {exns}",
                !cycles, !fullCycles, states.StatsDescending, used, maxDop, !resultCompleted, !resultExn)
            cycles := 0; fullCycles := 0; states.Clear(); resultCompleted := 0; resultExn:= 0
            log.Information("Batches Pending {pending} Started {batches} ({streams:n0}s {events:n0}-{skipped:n0}e) Merged {merges}b {mergedStreams}s",
                pendingCount, !batchesPended, !streamsPended, !eventsSkipped + !eventsPended, !eventsSkipped, !merges, !mergedStreams)
            batchesPended := 0; streamsPended := 0; eventsSkipped := 0; eventsPended := 0; merges := 0; mergedStreams := 0
        abstract member Handle : InternalMessage<'R> -> unit
        default __.Handle msg = msg |> function
            | Merge items ->
                mergedStreams := !mergedStreams + items.Length
                incr merges
            | Added (streams, skipped, events) ->
                incr batchesPended
                streamsPended := !streamsPended + streams
                eventsPended := !eventsPended + events
                eventsSkipped := !eventsSkipped + skipped
            | Result (_stream, Choice1Of2 _) ->
                incr resultCompleted
            | Result (_stream, Choice2Of2 _) ->
                incr resultExn
        member __.DumpStats((used,max), pendingCount) =
            incr cycles
            if statsDue () then
                dumpStats (used,max) pendingCount
                __.DumpExtraStats()
        member __.TryDumpState(state,dump) =
            incr fullCycles
            states.Ingest(string state)
            let due = stateDue ()
            if due then
                dump log
            due
        /// Allows an ingester or projector to wire in custom stats (typically based on data gathered in a `Handle` override)
        abstract DumpExtraStats : unit -> unit
        default __.DumpExtraStats () = ()

    /// Coordinates the dispatching of work and emission of results, subject to the maxDop concurrent processors constraint
    type Dispatcher<'R>(maxDop) =
        // Using a Queue as a) the ordering is more correct, favoring more important work b) we are adding from many threads so no value in ConcurrentBag'sthread-affinity
        let work = new BlockingCollection<_>(ConcurrentQueue<_>()) 
        let result = Event<'R>()
        let dop = new Sem(maxDop)
        let dispatch work = async {
            let! res = work
            result.Trigger res
            dop.Release() } 
        [<CLIEvent>] member __.Result = result.Publish
        member __.HasCapacity = dop.HasCapacity
        member __.State = dop.State
        member __.TryAdd(item,?timeout) = async {
            let! got = dop.TryAwait(?timeout=timeout)
            if got then
                work.Add(item)
            return got }
        member __.Pump () = async {
            let! ct = Async.CancellationToken
            for item in work.GetConsumingEnumerable ct do
                Async.Start(dispatch item) }
    /// Consolidates ingested events into streams; coordinates dispatching of these to projector/ingester in the order implied by the submission order
    /// a) does not itself perform any reading activities
    /// b) triggers synchronous callbacks as batches complete; writing of progress is managed asynchronously by the TrancheEngine(s)
    /// c) submits work to the supplied Dispatcher (which it triggers pumping of)
    /// d) periodically reports state (with hooks for ingestion engines to report same)
    type Engine<'R>(dispatcher : Dispatcher<_>, project : int64 option * StreamSpan -> Async<Choice<'R,exn>>, interpretProgress, dumpStreams) =
        let sleepIntervalMs = 1
        let cts = new CancellationTokenSource()
        let work = ConcurrentStack<InternalMessage<'R>>() // dont need so complexity of Queue is unwarranted and usage is cross thread so Bag is not better
        let pending = ConcurrentQueue<_*StreamItem[]>() // Queue as need ordering
        let streams = StreamStates()
        let progressState = Progress.State()

        static let workLocalBuffer = Array.zeroCreate 1024
        let tryDrainResults feedStats =
            let mutable worked, more = false, true
            while more do
                let c = work.TryPopRange(workLocalBuffer)
                if c = 0 && work.IsEmpty then more <- false else worked <- true
                for i in 0..c-1 do
                    let x = workLocalBuffer.[i]
                    match x with
                    | Added _  -> () // Only processed in Stats (and actually never enters this queue)
                    | Merge events -> for e in events do streams.InternalMerge(e.Key,e.Value)
                    | Result (stream,res) ->
                        match interpretProgress streams stream res with
                        | None -> streams.MarkFailed stream
                        | Some index ->
                            progressState.MarkStreamProgress(stream,index)
                            streams.MarkCompleted(stream,index)
                    feedStats x
            worked
        let tryFillDispatcher includeSlipstreamed = async {
            let mutable hasCapacity, dispatched = dispatcher.HasCapacity, false
            if hasCapacity then
                let potential = streams.Pending(includeSlipstreamed, progressState.InScheduledOrder streams.QueueWeight)
                let xs = potential.GetEnumerator()
                while xs.MoveNext() && hasCapacity do
                    let (_,{stream = s} : StreamSpan) as item = xs.Current
                    let! succeeded = dispatcher.TryAdd(async { let! r = project item in return s, r })
                    if succeeded then streams.MarkBusy s
                    dispatched <- dispatched || succeeded // if we added any request, we also don't sleep
                    hasCapacity <- succeeded
            return hasCapacity, dispatched }
        let ingestPendingBatch feedStats (markCompleted, items : StreamItem seq) = 
            let inline validVsSkip (streamState : StreamState) (item : StreamItem) =
                match streamState.write, item.index + 1L with
                | Some cw, required when cw >= required -> 0, 1
                | _ -> 1, 0
            let reqs = Dictionary()
            let mutable count, skipCount = 0, 0
            for item in items do
                let stream,streamState = streams.Add(item.stream,item.index,item.event)
                match validVsSkip streamState item with
                | 0, skip ->
                    skipCount <- skipCount + skip
                | required, _ ->
                    count <- count + required
                    reqs.[stream] <- item.index+1L
            progressState.AppendBatch(markCompleted,reqs)
            feedStats <| Added (reqs.Count,skipCount,count)

        member private __.Pump(stats : Stats<'R>) = async {
            use _ = dispatcher.Result.Subscribe(Result >> work.Push)
            Async.Start(dispatcher.Pump(), cts.Token)
            while not cts.IsCancellationRequested do
                let mutable idle, dispatcherState, finished = true, Idle, false
                while not finished do
                    // 1. propagate write write outcomes to buffer (can mark batches completed etc)
                    let processedResults = tryDrainResults stats.Handle
                    // 2. top up provisioning of writers queue
                    let! hasCapacity, dispatched = tryFillDispatcher (dispatcherState = Slipstreaming)
                    idle <- idle && not processedResults && not dispatched
                    match dispatcherState with
                    | Idle when hasCapacity -> // need to bring more work into the pool as we can't fill the work queue
                        match pending.TryDequeue() with
                        | true, batch ->                    ingestPendingBatch stats.Handle batch
                        | false,_ ->                        dispatcherState <- Slipstreaming // TODO preload extra spans from active submitters
                    | Idle ->                               dispatcherState <- Full; finished <- true
                    | Slipstreaming ->                      finished <- true
                    | _ -> ()
                    // This loop can take a long time; attempt logging of stats per iteration
                    stats.DumpStats(dispatcher.State,pending.Count)
                // 3. Record completion state once per full iteration; dumping streams is expensive so needs to be done infrequently
                if not (stats.TryDumpState(dispatcherState,dumpStreams streams)) && not idle then
                    // 4. Do a minimal sleep so we don't run completely hot when empty (unless we did something non-trivial)
                    do! Async.Sleep sleepIntervalMs }

        static member Start<'R>(stats, projectorDop, project, interpretProgress, dumpStreams) =
            let dispatcher = Dispatcher(projectorDop)
            let instance = new Engine<'R>(dispatcher, project, interpretProgress, dumpStreams)
            Async.Start <| instance.Pump(stats)
            instance

        /// Enqueue a batch of items with supplied progress marking function
        /// Submission is accepted on trust; they are internally processed in order of submission
        /// caller should ensure that (when multiple submitters are in play) no single Range submits more than their fair share
        member __.Submit(markCompleted: (unit -> unit), items: StreamItem[]) =
            pending.Enqueue (markCompleted, items)

        member __.AddOpenStreamData(events) =
            work.Push <| Merge events

        member __.AllStreams = streams.All

        member __.Stop() =
            cts.Cancel()

type Projector =

    static member Start(log, projectorDop, project : Buffer.StreamSpan -> Async<int>, categorize, ?statsInterval, ?statesInterval) =
        let project (_maybeWritePos, batch) = async {
            try let! count = project batch
                return Choice1Of2 (batch.span.index + int64 count)
            with e -> return Choice2Of2 e }
        let interpretProgress _streams _stream = function
            | Choice1Of2 index -> Some index
            | Choice2Of2 _ -> None
        let stats = Scheduling.Stats(log, defaultArg statsInterval (TimeSpan.FromMinutes 1.), defaultArg statesInterval (TimeSpan.FromMinutes 5.))
            //let category (streamName : string) = streamName.Split([|'-';'_'|],2).[0]
        let dumpStreams (streams: Scheduling.StreamStates) log = streams.Dump(log, categorize)
        Scheduling.Engine<int64>.Start(stats, projectorDop, project, interpretProgress, dumpStreams)

module Ingestion =

    [<NoComparison; NoEquality>]
    type Message =
        | Batch of seriesIndex: int * epoch: int64 * markCompleted: Async<unit> * items: StreamItem seq
        //| StreamSegment of span: StreamSpan
        | EndOfSeries of seriesIndex: int

    type private Streams() =
        let states = Dictionary<string, Buffer.StreamState>()
        let merge stream (state : Buffer.StreamState) =
            match states.TryGetValue stream with
            | false, _ ->
                states.Add(stream, state)
            | true, current ->
                let updated = Buffer.StreamState.combine current state
                states.[stream] <- updated

        member __.Merge(items : StreamItem seq) =
            for item in items do
                merge item.stream { isMalformed = false; write = None; queue = [| { index = item.index; events = [| item.event |] } |] }

        member __.Take(processingContains) =
            let forward = [| for x in states do if processingContains x.Key then yield x |]
            for x in forward do states.Remove x.Key |> ignore
            forward

        member __.Dump(categorize, log : ILogger) =
            let mutable waiting, waitingB = 0, 0L
            let waitingCats, waitingStreams = CatStats(), CatStats()
            for KeyValue (stream,state) in states do
                let sz = int64 state.Size
                waitingCats.Ingest(categorize stream)
                waitingStreams.Ingest(sprintf "%s@%dx%d" stream (defaultArg state.write 0L) state.queue.[0].events.Length, (sz + 512L) / 1024L)
                waiting <- waiting + 1
                waitingB <- waitingB + sz
            if waiting <> 0 then log.Information("Streams Waiting {busy:n0}/{busyMb:n1}MB ", waiting, mb waitingB)
            if waitingCats.Any then log.Information("Waiting Categories, events {@readyCats}", Seq.truncate 5 waitingCats.StatsDescending)
            if waitingCats.Any then log.Information("Waiting Streams, KB {@readyStreams}", Seq.truncate 5 waitingStreams.StatsDescending)

    type private Stats(log : ILogger, maxPendingBatches, categorize, statsInterval : TimeSpan) =
        let mutable pendingBatchCount, validatedEpoch, comittedEpoch : int * int64 option * int64 option = 0, None, None
        let progCommitFails, progCommits = ref 0, ref 0 
        let cycles, batchesPended, streamsPended, eventsPended = ref 0, ref 0, ref 0, ref 0
        let statsDue = expiredMs (int64 statsInterval.TotalMilliseconds)
        let dumpStats (available,maxDop) (readingAhead,ready) =
            log.Information("Buffering Cycles {cycles} Ingested {batches} ({streams:n0}s {events:n0}e) Submissions {active}/{writers}",
                !cycles, !batchesPended, !streamsPended, !eventsPended, available, maxDop)
            cycles := 0; batchesPended := 0; streamsPended := 0; eventsPended := 0
            let mutable buffered = 0
            let count (xs : IDictionary<int,ResizeArray<_>>) = seq { for x in xs do buffered <- buffered + x.Value.Count; yield x.Key, x.Value.Count } |> Seq.sortBy fst |> Seq.toArray
            let ahead, ready = count readingAhead, count ready
            if buffered <> 0 then log.Information("Holding {buffered} Reading {@reading} Ready {@ready}", buffered, ahead, ready)
            if !progCommitFails <> 0 || !progCommits <> 0 then
                match comittedEpoch with
                | None ->
                    log.Error("Uncommitted {pendingBatches}/{maxPendingBatches} @ {validated}; writing failing: {failures} failures ({commits} successful commits)",
                            pendingBatchCount, maxPendingBatches, Option.toNullable validatedEpoch, !progCommitFails, !progCommits)
                | Some committed when !progCommitFails <> 0 ->
                    log.Warning("Uncommitted {pendingBatches}/{maxPendingBatches} @ {validated} (committed: {committed}, {commits} commits, {failures} failures)",
                            pendingBatchCount, maxPendingBatches, Option.toNullable validatedEpoch, committed, !progCommits, !progCommitFails)
                | Some committed ->
                    log.Information("Uncommitted {pendingBatches}/{maxPendingBatches} @ {validated} (committed: {committed}, {commits} commits)",
                            pendingBatchCount, maxPendingBatches, Option.toNullable validatedEpoch, committed, !progCommits)
                progCommits := 0; progCommitFails := 0
            else
                log.Information("Uncommitted {pendingBatches}/{maxPendingBatches} @ {validated} (committed: {committed})",
                        pendingBatchCount, maxPendingBatches, Option.toNullable validatedEpoch, Option.toNullable comittedEpoch)
        member __.Handle : InternalMessage -> unit = function
            | Batch _ | ActivateSeries _ | CloseSeries _-> () // stats are managed via Added internal message in same cycle
            | ProgressResult (Choice1Of2 epoch) ->
                incr progCommits
                comittedEpoch <- Some epoch
            | ProgressResult (Choice2Of2 (_exn : exn)) ->
                incr progCommitFails
            | Added (streams,events) ->
                incr batchesPended
                streamsPended := !streamsPended + streams
                eventsPended := !eventsPended + events
        member __.HandleValidated(epoch, pendingBatches) = 
            validatedEpoch <- epoch
            pendingBatchCount <- pendingBatches
        member __.HandleCommitted epoch = 
            comittedEpoch <- epoch
        member __.TryDump((available,maxDop),streams : Streams,readingAhead,ready) =
            incr cycles
            if statsDue () then
                dumpStats (available,maxDop) (readingAhead,ready)
                streams.Dump(categorize,log)

    and [<NoComparison; NoEquality>] private InternalMessage =
        | Batch of seriesIndex: int * epoch: int64 * markCompleted: Async<unit> * items: StreamItem seq
        /// Result from updating of Progress to backing store - processed up to nominated `epoch` or threw `exn`
        | ProgressResult of Choice<int64,exn>
        /// Internal message for stats purposes
        | Added of steams: int * events: int
        | CloseSeries of seriesIndex: int
        | ActivateSeries of seriesIndex: int

    let tryRemove key (dict: Dictionary<_,_>) =
        match dict.TryGetValue key with
        | true, value ->
            dict.Remove key |> ignore
            Some value
        | false, _ -> None
    
    /// Holds batches away from Core processing to limit in-flight processing
    type Engine<'R>(log : ILogger, scheduler: Scheduling.Engine<'R>, maxRead, maxSubmissions, initialSeriesIndex, categorize, statsInterval : TimeSpan, ?pumpDelayMs) =
        let cts = new CancellationTokenSource()
        let pumpDelayMs = defaultArg pumpDelayMs 5
        let work = ConcurrentQueue<InternalMessage>() // Queue as need ordering semantically
        let readMax = new Sem(maxRead)
        let submissionsMax = new Sem(maxSubmissions)
        let streams = Streams()
        let stats = Stats(log, maxRead, categorize, statsInterval)
        let pending = Queue<_>()
        let readingAhead, ready = Dictionary<int,ResizeArray<_>>(), Dictionary<int,ResizeArray<_>>()
        let progressWriter = Progress.Writer<_>()
        let mutable activeSeries = initialSeriesIndex
        let mutable validatedPos = None

        let handle = function
            | Batch (seriesId, epoch, checkpoint, items) ->
                let batchInfo =
                    let items = Array.ofSeq items
                    streams.Merge items
                    let markCompleted () =
                        submissionsMax.Release()
                        readMax.Release()
                        validatedPos <- Some (epoch,checkpoint)
                    work.Enqueue(Added (HashSet(seq { for x in items -> x.stream }).Count,items.Length))
                    markCompleted, items
                if activeSeries = seriesId then pending.Enqueue batchInfo
                else
                    match readingAhead.TryGetValue seriesId with
                    | false, _ -> readingAhead.[seriesId] <- ResizeArray(Seq.singleton batchInfo)
                    | true,current -> current.Add(batchInfo)
            | CloseSeries seriesIndex ->
                if activeSeries = seriesIndex then
                    log.Information("Completed reading active series {activeSeries}; moving to next", activeSeries)
                    work.Enqueue <| ActivateSeries (activeSeries + 1)
                else
                    match readingAhead |> tryRemove seriesIndex with
                    | Some batchesRead ->
                        ready.[seriesIndex] <- batchesRead
                        log.Information("Completed reading {series}, marking {buffered} buffered items ready", seriesIndex, batchesRead.Count)
                    | None ->
                        ready.[seriesIndex] <- ResizeArray()
                        log.Information("Completed reading {series}, leaving empty batch list", seriesIndex)
            | ActivateSeries newActiveSeries ->
                activeSeries <- newActiveSeries
                let buffered =
                    match ready |> tryRemove newActiveSeries with
                    | Some completedChunkBatches ->
                        completedChunkBatches |> Seq.iter pending.Enqueue
                        work.Enqueue <| ActivateSeries (newActiveSeries + 1)
                        completedChunkBatches.Count
                    | None ->
                        match readingAhead |> tryRemove newActiveSeries with
                        | Some batchesReadToDate -> batchesReadToDate |> Seq.iter pending.Enqueue; batchesReadToDate.Count
                        | None -> 0
                log.Information("Moving to series {activeChunk}, releasing {buffered} buffered batches, {ready} others ready, {ahead} reading ahead",
                    newActiveSeries, buffered, ready.Count, readingAhead.Count)
            // These events are for stats purposes
            | Added _
            | ProgressResult _ -> ()

        member private __.Pump() = async {
            use _ = progressWriter.Result.Subscribe(ProgressResult >> work.Enqueue)
            Async.Start(progressWriter.Pump(), cts.Token)
            let presubmitInterval = expiredMs (1000L*10L)
            while not cts.IsCancellationRequested do
                try let mutable itemLimit = 4096
                    while itemLimit > 0 do
                        match work.TryDequeue() with
                        | true, x -> handle x; stats.Handle x; itemLimit <- itemLimit - 1
                        | false, _ -> itemLimit <- 0
                    // 1. Update any progress into the stats
                    stats.HandleValidated(Option.map fst validatedPos, fst submissionsMax.State)
                    validatedPos |> Option.iter progressWriter.Post
                    stats.HandleCommitted progressWriter.CommittedEpoch
                    // 2. Forward content for any active streams into processor immediately
                    if presubmitInterval () then
                        let relevantBufferedStreams = streams.Take(fun x -> true (*scheduler.AllStreams.Contains*))
                        scheduler.AddOpenStreamData(relevantBufferedStreams)
                    // 3. Submit to ingester until read queue, tranche limit or ingester limit exhausted
                    while pending.Count <> 0 && submissionsMax.HasCapacity do
                        // mark off a write as being in progress (there is a race if there are multiple Ingesters, but thats good)
                        do! submissionsMax.Await()
                        scheduler.Submit(pending.Dequeue())
                    // 4. Periodically emit status info
                    stats.TryDump(submissionsMax.State,streams,readingAhead,ready)
                    do! Async.Sleep pumpDelayMs
                with e -> log.Error(e,"Buffer thread exception") }

        /// Generalized; normal usage is via Ingester.Start, this is used by the `eqxsync` template to handle striped reading for bulk ingestion purposes
        static member Start<'R>(log, scheduler, maxRead, maxSubmissions, startingSeriesId, categorize, statsInterval) =
            let instance = new Engine<'R>(log, scheduler, maxRead, maxSubmissions, startingSeriesId, categorize, statsInterval = statsInterval)
            Async.Start <| instance.Pump()
            instance

        /// Awaits space in `read` to limit reading ahead - yields (used,maximum) counts from Read Semaphore for logging purposes
        member __.Submit(content : Message) = async {
            do! readMax.Await()
            match content with
            | Message.Batch (seriesId, epoch, markBatchCompleted, events) ->
                work.Enqueue <| Batch (seriesId, epoch, markBatchCompleted, events)
                // NB readMax.Release() is effected in the Batch handler's MarkCompleted()
            | Message.EndOfSeries seriesId ->
                work.Enqueue <| CloseSeries seriesId
                readMax.Release()
            return readMax.State }

        /// As range assignments get revoked, a user is expected to `Stop `the active processing thread for the Ingester before releasing references to it
        member __.Stop() = cts.Cancel()

type Ingester =

    /// Starts an Ingester that will submit up to `maxSubmissions` items at a time to the `scheduler`, blocking on Submits when more than `maxRead` batches have yet to complete processing 
    static member Start<'R>(log, scheduler, maxRead, maxSubmissions, categorize, ?statsInterval) =
        let singleSeriesIndex = 0
        let instance = Ingestion.Engine<'R>.Start(log, scheduler, maxRead, maxSubmissions, singleSeriesIndex, categorize, statsInterval = defaultArg statsInterval (TimeSpan.FromMinutes 1.))
        { new IIngester with
            member __.Submit(epoch, markCompleted, items) : Async<int*int> =
                instance.Submit(Ingestion.Message.Batch(singleSeriesIndex, epoch, markCompleted, items))
            member __.Stop() = __.Stop() }