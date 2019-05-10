module Equinox.Projection.State

open Serilog
open System.Collections.Generic

let arrayBytes (x:byte[]) = if x = null then 0 else x.Length
let inline eventSize (x : Equinox.Codec.IEvent<_>) = arrayBytes x.Data + arrayBytes x.Meta + x.EventType.Length + 16
let mb x = float x / 1024. / 1024.
let category (streamName : string) = streamName.Split([|'-'|],2).[0]

type [<NoComparison>] Span = { index: int64; events: Equinox.Codec.IEvent<byte[]>[] }
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
    let (|NNA|) xs = if xs = null then Array.empty else xs
    module Span =
        let (|End|) (x : Span) = x.index + if x.events = null then 0L else x.events.LongLength
        let trim min = function
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

    let inline optionCombine f (r1: int64 option) (r2: int64 option) =
        match r1, r2 with
        | Some x, Some y -> f x y |> Some
        | None, None -> None
        | None, x | x, None -> x
    let combine (s1: StreamState) (s2: StreamState) : StreamState =
        let writePos = optionCombine max s1.write s2.write
        let items = let (NNA q1, NNA q2) = s1.queue, s2.queue in Seq.append q1 q2
        { write = writePos; queue = Span.merge (defaultArg writePos 0L) items; isMalformed = s1.isMalformed || s2.isMalformed }

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
    let pending (requestedOrder : string seq) = seq {
        for s in requestedOrder do
            let state = states.[s]
            if state.IsReady && not (busy.Contains s) then
                yield state.write, { stream = s; span = state.queue.[0] }
        // [lazily] Slipstream in futher events that have been posted to streams which we've already visited
        for KeyValue(s,v) in states do
            if v.IsReady && not (busy.Contains s) then
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
    member __.Pending(byQueuedPriority : string seq) : (int64 option * StreamSpan) seq =
        pending byQueuedPriority
    member __.Dump(log : ILogger) =
        let mutable busyCount, busyB, ready, readyB, unprefixed, unprefixedB, malformed, malformedB, synced = 0, 0L, 0, 0L, 0, 0L, 0, 0L, 0
        let busyCats, readyCats, readyStreams, unprefixedStreams, malformedStreams = CatStats(), CatStats(), CatStats(), CatStats(), CatStats()
        let kb sz = (sz + 512L) / 1024L
        for KeyValue (stream,state) in states do
            match int64 state.Size with
            | 0L ->
                synced <- synced + 1
            | sz when busy.Contains stream ->
                busyCats.Ingest(category stream)
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
                readyCats.Ingest(category stream)
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