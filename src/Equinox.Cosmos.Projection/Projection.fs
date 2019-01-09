namespace Equinox.Cosmos.Projection

[<AutoOpen>]
module Infra =

    open System
    open System.Threading.Tasks

    type Async with
        // TODO share with Tool
        static member map (f:'a -> 'b) (a:Async<'a>) : Async<'b> = async.Bind(a, f >> async.Return)
        // TODO share with Tool
        static member choose a b = async {
            let! result = Async.Choice [|Async.map Some a ; Async.map Some b |]
            return Option.get result
        }
        static member internal chooseTasks2 (a:Task<'T>) (b:Task) : Async<Choice<'T * Task, Task<'T>>> = async {
            let ta = a :> Task
            let! i = Task.WhenAny( ta, b ) |> Async.AwaitTask
            if i = ta then return (Choice1Of2 (a.Result, b))
            elif i = b then return (Choice2Of2 (a))
            else return! failwith "unreachable" }

        /// Returns an async computation which runs the argument computation but raises an exception if it doesn't complete
        /// by the specified timeout.
        static member timeoutAfter (timeout:TimeSpan) (c:Async<'a>) = async {
            let! r = Async.StartChild(c, (int)timeout.TotalMilliseconds)
            return! r }

    open FSharp.Control

    module AsyncSeq =
      let bufferByTimeFold (timeMs:int) (flatten:'State -> 'T -> 'State) (initialState: 'State) (source:AsyncSeq<'T>) : AsyncSeq<'State> = asyncSeq {
        if (timeMs < 1) then invalidArg "timeMs" "must be positive"
        let mutable curState = initialState
        use ie = source.GetEnumerator()
        let rec loop (next:Task<'T option> option, waitFor:Task option) = asyncSeq {
          let! next =
            match next with
            | Some n -> async.Return n
            | None -> ie.MoveNext () |> Async.StartChildAsTask
          let waitFor =
            match waitFor with
            | Some w -> w
            | None -> Task.Delay timeMs
          let! res = Async.chooseTasks2 next waitFor
          match res with
          | Choice1Of2 (Some a,waitFor) ->
            curState <- flatten curState a
            yield! loop (None,Some waitFor)
          // reached the end of seq
          | Choice1Of2 (None,_) ->
              yield curState
          // time has expired
          | Choice2Of2 next ->
            yield curState
            curState <- initialState
            yield! loop (Some next, None) }
        yield! loop (None, None) }
        
    type internal Reactor<'a> = private {
      send : 'a -> unit
      events : AsyncSeq<'a>
    }

    [<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
    module internal Reactor =

      let mk<'a> : Reactor<'a> =
        let mb = MailboxProcessor<'a>.Start (fun _ -> async.Return())
        let s = AsyncSeq.replicateInfiniteAsync (mb.Receive())
        { send = mb.Post ; events = s }

      let send (r:Reactor<'a>) (a:'a) = r.send a

      let recv (r:Reactor<'a>) : AsyncSeq<'a> = r.events

module SaganChangefeedProcessor =

    open System
    open System.Linq

    open Microsoft.Azure.Documents
    open Microsoft.Azure.Documents.Client

    open FSharp.Control
    open Equinox.Store.Infrastructure

    type CosmosEndpoint = {
      uri : Uri
      authKey : string
      databaseName : string
      collectionName : string
    }

    type RangePosition = {
      RangeMin : int64
      RangeMax : int64
      LastLSN : int64
    }

    type ChangefeedPosition = RangePosition[]

    /// ChangefeedProcessor configuration
    type Config = {
      /// MaxItemCount fetched from a partition per batch
      BatchSize : int

      /// Interval between invocations of `progressHandler`
      ProgressInterval : TimeSpan

      /// Wait time for successive queries of a changefeed partition if its tail position is reached
      RetryDelay : TimeSpan

      /// Position in the Changefeed to begin processing from
      StartingPosition : StartingPosition

      /// Position in the Changefeed to stop processing at
      StoppingPosition : ChangefeedPosition option
    }

    /// ChangefeedProcessor starting position in the DocDB changefeed
    and StartingPosition =
      | Beginning
      | ChangefeedPosition of ChangefeedPosition

    type private State = {
      /// The client used to communicate with DocumentDB
      client : DocumentClient

      /// URI of the collection being processed
      collectionUri : Uri
    }

    [<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
    module RangePosition =
      /// Returns true if the range, r, covers the semi-open interval [min, max)
      let inline rangeCoversMinMax (r:RangePosition) min max =
        (r.RangeMin <= min) && (r.RangeMax >= max)

      /// Returns true if the range of the second argument, y, is fully contained within the first one.
      let inline fstCoversSnd (x:RangePosition) (y:RangePosition) =
        rangeCoversMinMax x y.RangeMin y.RangeMax

      /// Converts a cosmos range string from hex to int64
      let rangeToInt64 str =
        match Int64.TryParse(str, Globalization.NumberStyles.HexNumber, Globalization.CultureInfo.InvariantCulture) with
        | true, 255L when str.ToLower() = "ff" -> Int64.MaxValue    // max for last partition
        | true, i -> i
        | false, _ when str = "" -> 0L
        | false, _ -> 0L  // NOTE: I am not sure if this should be the default or if we should return an option instead

      /// Converts a cosmos logical sequence number (LSN) from string to int64
      let lsnToInt64 str =
        match Int64.TryParse str with
        | true, i -> i
        | false, _ -> 0L

    [<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
    module ChangefeedPosition =
      let fstSucceedsSnd (cfp1:ChangefeedPosition) (cfp2:ChangefeedPosition) =
        let successionViolationExists =
          seq { // test range covers
            for x in cfp1 do
              yield cfp2 |> Seq.exists (fun y -> (RangePosition.fstCoversSnd x y) && (x.LastLSN > y.LastLSN))
          }
          |> Seq.exists id // find any succession violations
        not successionViolationExists

      let tryPickLatest (cfp1:ChangefeedPosition) (cfp2:ChangefeedPosition) =
        if fstSucceedsSnd cfp1 cfp2 then Some cfp1
        elif fstSucceedsSnd cfp2 cfp1 then Some cfp2
        else None

      let tryGetPartitionByRange min max (cfp:ChangefeedPosition) =
        cfp |> Array.tryFind (fun rp -> RangePosition.rangeCoversMinMax rp min max)

    [<RequireQualifiedAccess>]
    /// A PartitionSelector is a function that filters a list of CosmosDB partitions into ones that a single processor will handle
    module PartitionSelectors =
      /// A PartitionSelector is a function that filters a list of CosmosDB partitions into ones that a single processor will handle
      type PartitionSelector = PartitionKeyRange[] -> PartitionKeyRange[]

      /// An identity partition selector. It returns all given partitions
      let allPartitions : PartitionSelector = id

      /// A partition selector that attempts to split the partitions across `totalProcessors`.
      /// The ChangefeedProcessor gets assigned every nth partition to process.
      let evenSplit totalProcessors n : PartitionSelector =
        fun allPartitions ->
          allPartitions
          |> Array.sortBy (fun pkr -> pkr.GetPropertyValue "minInclusive" |> RangePosition.rangeToInt64)
          |> Array.mapi (fun i pkr -> if i % totalProcessors = n then Some pkr else None)
          |> Array.choose id

    /// Returns the current set of partitions in docdb changefeed
    let private getPartitions (st:State) = async {
      // TODO: set FeedOptions properly. Needed for resuming from a changefeed position
      let! response = st.client.ReadPartitionKeyRangeFeedAsync st.collectionUri |> Async.AwaitTaskCorrect
      return response.ToArray()
    }

    /// Reads a partition of the changefeed and returns an AsyncSeq<Document[] * RangePosition>
    ///   - Document[] is a batch of documents read from changefeed
    ///   - RangePosition is a record containing the partition's key range and the last logical sequence number in the batch.
    /// This function attempts to read all documents in the semi-closed LSN range [start, end).
    let rec private readPartition (config:Config) (st:State) (pkr:PartitionKeyRange) =
      let rangeMin = pkr.GetPropertyValue "minInclusive" |> RangePosition.rangeToInt64
      let rangeMax = pkr.GetPropertyValue "maxExclusive" |> RangePosition.rangeToInt64

      let continuationToken : string =
        match config.StartingPosition with
        | Beginning -> null
        | ChangefeedPosition cfp ->
          cfp
          |> ChangefeedPosition.tryGetPartitionByRange rangeMin rangeMax
          |> Option.map (fun rp -> string rp.LastLSN)
          |> Option.defaultValue null   // docdb starts at the the beginning of a partition if null
      let stoppingPosition =
        config.StoppingPosition
        |> Option.bind (ChangefeedPosition.tryGetPartitionByRange rangeMin rangeMax)
        |> Option.map (fun rp -> rp.LastLSN)
      let cfo =
        ChangeFeedOptions(
          PartitionKeyRangeId = pkr.Id,
          MaxItemCount = Nullable config.BatchSize,
          StartFromBeginning = true,   // TODO: double check that this is ignored by docdb if RequestContinuation is set
          RequestContinuation = continuationToken)
      use query = st.client.CreateDocumentChangeFeedQuery(st.collectionUri, cfo)

      let rec readPartition (query:Linq.IDocumentQuery<Document>) (pkr:PartitionKeyRange) = asyncSeq {
        let! response = query.ExecuteNextAsync<Document>() |> Async.AwaitTask

        let rp : RangePosition = {
          RangeMin = rangeMin
          RangeMax = rangeMax
          LastLSN = response.ResponseContinuation.Replace("\"", "") |> RangePosition.lsnToInt64
        }
        if response <> null then
          if response.Count > 0 then
            yield (response.ToArray(), rp)
          if query.HasMoreResults then
            match stoppingPosition with
            | Some stopLSN when rp.LastLSN >= stopLSN -> ()   // TODO: this can stop after the stop position, but this is ok for now. Fix later.
            | _ -> yield! readPartition query pkr
          else
            do! Async.Sleep (int config.RetryDelay.TotalMilliseconds)
            yield! readPartition query pkr
      }

      let startLSN =
        if continuationToken = null then Int64.MinValue
        else int64 continuationToken

      match stoppingPosition with
      | Some stopLSN when startLSN >= stopLSN -> AsyncSeq.empty
      | _ -> readPartition query pkr

    /// Returns an async computation that runs a concurrent (per-docdb-partition) changefeed processor.
    /// - `handle`: is an asynchronous function that takes a batch of documents and returns a result
    /// - `progressHandler`: is an asynchronous function ('a * ChangefeedPosition) -> Async<unit>
    /// - `outputMerge` : is a merge function that merges the output result 'a -> 'a -> 'a
    ///    that is called periodically with a list of outputs that were produced by the handle function since the
    ///    last invocation and the current position of the changefeedprocessor.
    /// - `partitionSelector`: is a filtering function `PartitionKeyRange[] -> PartitionKeyRange` that is used to narrow down the list of partitions to process.
    let go (cosmos:CosmosEndpoint) (config:Config) (partitionSelector:PartitionSelectors.PartitionSelector)
        (handle:Document[]*RangePosition -> Async<'a>) (progressHandler:'a * ChangefeedPosition -> Async<Unit>) (outputMerge:'a*'a -> 'a) = async {
      use client = 
        let connPolicy = 
            let cp = ConnectionPolicy.Default
            cp.ConnectionMode <- ConnectionMode.Direct
            cp.ConnectionProtocol <- Protocol.Tcp
            cp.MaxConnectionLimit <- 1000
            cp
        new DocumentClient(cosmos.uri, cosmos.authKey, connPolicy, Nullable ConsistencyLevel.Session)
      let state = {
        client = client
        collectionUri = UriFactory.CreateDocumentCollectionUri(cosmos.databaseName, cosmos.collectionName)
      }

      // updates the given partition position in the given changefeed position
      let updateChangefeedPosition (cfp:ChangefeedPosition) (rp:RangePosition) =
        let index = cfp |> Array.tryFindIndex (function x -> x.RangeMin=rp.RangeMin && x.RangeMax=rp.RangeMax)
        match index with
        | Some i ->
          let newCfp = Array.copy cfp
          newCfp.[i] <- rp
          newCfp
        | None ->
          let newCfp = Array.zeroCreate (cfp.Length+1)
          for i in 0..cfp.Length-1 do
            newCfp.[i] <- cfp.[i]
          newCfp.[cfp.Length] <- rp
          newCfp

      // wraps around the user's merge function with option type for AsyncSeq scan
      let optionMerge (input:'a option*'a option) : 'a option =
        match input with
        | None, None -> None
        | Some a, None -> Some a
        | None, Some b -> Some b
        | Some a, Some b -> (a,b) |> outputMerge |> Some

      // wraps around user's progressHandler function to only excute when a exists
      let reactorProgressHandler ((a,cf): 'a option*ChangefeedPosition) : Async<Unit> = async {
        match a with
        | None -> ()
        | Some a -> do! progressHandler (a,cf) }

      // updates changefeed position and add the new element to the list of outputs
      let accumPartitionsPositions (merge: 'a option*'a option -> 'a option) (output:'a option, cfp:ChangefeedPosition) (handlerOutput: 'a, pp:RangePosition) =
        merge (output,(Some handlerOutput)) , (updateChangefeedPosition cfp pp)

      //basically always takes the latter one (renew state)
      let stateFlatten _ out = out

      // used to accumulate the output of all the user handle functions
      let progressReactor = Reactor.mk

      // wrap the document handler so that it takes and passes out partition position
      let handle ((_doc, pp) as input) = async {
        let! ret = handle input
        (ret, pp) |> Reactor.send progressReactor
      }

      let initialPosition = 
        match config.StartingPosition with
        | Beginning -> [||]
        | ChangefeedPosition cfp -> cfp

      let! progressTracker =
        progressReactor
        |> Reactor.recv
        |> AsyncSeq.scan (accumPartitionsPositions optionMerge) (None,initialPosition)
        |> AsyncSeq.bufferByTimeFold (int config.ProgressInterval.TotalMilliseconds) stateFlatten (None,initialPosition)
        |> AsyncSeq.iterAsync reactorProgressHandler
        |> Async.StartChild

      let! partitions = getPartitions state
      let workers =
        partitions
        |> partitionSelector
        |> Array.map ((fun pkr -> readPartition config state pkr) >> AsyncSeq.iterAsync handle)
        |> Async.Parallel
        |> Async.Ignore

      return! Async.choose progressTracker workers
    }

    /// Periodically queries DocDB for the latest positions and timestamp of all partitions in its changefeed.
    /// The `handler` function will be called periodically, once per `interval`, with an updated ChangefeedPosition and timestamp of last document
    let trackTailPosition (cosmos:CosmosEndpoint) (interval:TimeSpan) (handler:DateTime*(DateTime*RangePosition)[] -> Async<unit>) = async {
      use client = 
        let connPolicy = 
            let cp = ConnectionPolicy.Default
            cp.ConnectionMode <- ConnectionMode.Direct
            cp.ConnectionProtocol <- Protocol.Tcp
            cp.MaxConnectionLimit <- 1000
            cp
        new DocumentClient(cosmos.uri, cosmos.authKey, connPolicy, Nullable ConsistencyLevel.Session)
      let state = {
        client = client
        collectionUri = UriFactory.CreateDocumentCollectionUri(cosmos.databaseName, cosmos.collectionName)
      }

      let getRecentPosition (pkr:PartitionKeyRange) = async {
        let cfo = ChangeFeedOptions(PartitionKeyRangeId = pkr.Id, StartTime = Nullable (DateTime.Now.AddHours(1.)))
        use query = client.CreateDocumentChangeFeedQuery(state.collectionUri, cfo)
        let! response = query.ExecuteNextAsync<Document>() |> Async.AwaitTask
        let rp : RangePosition = {
          RangeMin = pkr.GetPropertyValue "minInclusive" |> RangePosition.rangeToInt64
          RangeMax = pkr.GetPropertyValue "maxExclusive" |> RangePosition.rangeToInt64
          LastLSN = response.ResponseContinuation.Replace("\"", "") |> RangePosition.lsnToInt64
        }
        let cfoForLastDoc = ChangeFeedOptions(PartitionKeyRangeId = pkr.Id, RequestContinuation = string (rp.LastLSN - 1L))
        use queryForLastDoc = client.CreateDocumentChangeFeedQuery(state.collectionUri, cfoForLastDoc)
        let! responseForLastDoc = queryForLastDoc.ExecuteNextAsync<Document>() |> Async.AwaitTask
        let lastDocument = 
            Seq.last <| responseForLastDoc.ToArray()
        return lastDocument.Timestamp, rp
      }

      let getRecentPosition pkr =
        getRecentPosition pkr
        |> Async.timeoutAfter (TimeSpan.FromSeconds(3.)) // times out after 3 seconds, shouldnt take more than 1

      let handler input =
        handler input
        |> Async.timeoutAfter (TimeSpan.FromTicks(interval.Ticks * 3L))

      let! partitions = getPartitions state   // NOTE: partitions will only be fetched once. Consider moving this inside the query function in case of a docdb partition split.
      let queryPartitions dateTime = async {
        let! changefeedPosition =
          partitions
          |> Array.map getRecentPosition
          |> Async.Parallel
        return (dateTime, changefeedPosition)
      }

      return!
        AsyncSeq.intervalMs (int interval.TotalMilliseconds)
        |> AsyncSeq.mapAsyncParallel queryPartitions
        |> AsyncSeq.iterAsync handler
    }

open Confluent.Kafka
open Confluent.Kafka.Legacy
open Equinox.Cosmos.Store
open FSharp.Control
open Microsoft.Azure.Documents
open Microsoft.Azure.Documents.Client
open Microsoft.Azure.Documents.Linq
open Newtonsoft.Json.Linq
open SaganChangefeedProcessor
open System
open System.IO
open System.Linq
open System.Text
open System.Threading

module Map =

  type MergeChoice<'left, 'right> = Choice<'left * 'right, 'left, 'right>

  let inline (|MergeBoth|MergeLeft|MergeRight|) mc = mc

  let inline MergeBoth (left, right) = Choice1Of3(left, right)

  let inline MergeLeft left = Choice2Of3(left)

  let inline MergeRight right = Choice3Of3(right)

  /// Maps over the second item of a tuple.
  let inline mapSnd f (a,b) = (a, f b)

  /// Gets the set of keys.
  let inline keys m = m |> Map.toSeq |> Seq.map fst |> Set.ofSeq

  /// Filters a map such that the resulting map only contains keys from the provided set.
  let onlyKeys (ks:'a Set) (m:Map<'a, 'b>) : Map<'a, 'b> =
    m |> Map.filter (fun k _ -> ks |> Set.contains k)

  /// Merges two maps using the specified function which receives the key, the value from the
  /// left map and the value from the right map and produces a value for the resulting map.
  let mergeWith (f:'a -> 'b option -> 'c option -> 'd option) (a:Map<'a, 'b>) (b:Map<'a, 'c>) : Map<'a, 'd> =
    Set.union (keys a) (keys b)
    |> Seq.choose (fun k -> f k (a |> Map.tryFind k) (b |> Map.tryFind k) |> Option.map (fun c -> k,c))
    |> Map.ofSeq

  /// Merges two maps using a function which receives either both values, the left or the right depending on existence.
  /// If the function returns None, the key is skipped.
  let mergeChoose (f:'a -> MergeChoice<'b, 'c> -> 'd option) (a:Map<'a, 'b>) (b:Map<'a, 'c>) : Map<'a, 'd> =
    Set.union (keys a) (keys b)
    |> Seq.choose (fun k ->
      match Map.tryFind k a, Map.tryFind k b with
      | Some b, Some c -> f k (MergeBoth (b,c)) |> Option.map (fun c -> k,c)
      | Some b, None   -> f k (MergeLeft b) |> Option.map (fun c -> k,c)
      | None,   Some c -> f k (MergeRight c) |> Option.map (fun c -> k,c)
      | None,   None   -> failwith "invalid state"
    )
    |> Map.ofSeq

  /// Merges two maps favoring values from the second.
  let merge (a:Map<'a, 'b>) (b:Map<'a, 'b>) =
    Set.union (keys a) (keys b)
    |> Seq.choose (fun k ->
      match (a |> Map.tryFind k), (b |> Map.tryFind k) with
      | _, Some b -> Some (k,b)
      | Some a, _ -> Some (k,a)
      | _ -> None
    )
    |> Map.ofSeq
    
module Array =
    /// Makes an array, its elements are calculated from the function and the elements of input arrays occuring at the same position in both array.
    /// The two arrays must have equal length.
    let zipWith (f:'a -> 'b -> 'c) (a:'a[]) (b:'b[]) : 'c[] =
      // TODO: optimize such that only one result array is allocated.
      Array.zip a b |> Array.map (fun (a,b) -> f a b)

module Extensions =
  
  type TopicName = string
  type Partition = int

  type KafkaConfig = {
    broker : string
    clientId : string
  }
  
  /// Gets the last offset in the offset index topic.
  let latestTopicOffset (kafkaConfig:KafkaConfig) topic = async {
    let! offsets = Consumer.offsetRange kafkaConfig.broker topic []
    return
      offsets
      |> Map.toSeq
      |> Seq.sortBy fst
      |> Seq.map(fun (p,(lead,lag)) -> lag)
      |> Seq.toArray
  }

module ProjectEvent =
  
  type Document with
    member document.Cast<'T>() =
        let parent = new Document()
        parent.SetPropertyValue("content", document)
        parent.GetPropertyValue<'T>("content")
  
  type IEquinoxEvent =
    inherit IIndexedEvent
    abstract member Stream : string
    abstract member TimeStamp : DateTimeOffset

  type ProjectEvent =
    | EquinoxEvent of IEquinoxEvent 
    | CustomEvent of JToken

[<RequireQualifiedAccess>]
module Utils =
  open ProjectEvent

  let WriteStringToStream (w:TextWriter) (value:string) =
    if String.IsNullOrEmpty value then ()
    else
      for i = 0 to value.Length - 1 do
        let c = value.[i]
        let ci = int c
        if ci >= 0 && ci <= 7 || ci = 11 || ci >= 14 && ci <= 31 then
          w.Write("\\u{0:x4}", ci) |> ignore
        else
          match c with
          | '\b' -> w.Write "\\b"
          | '\t' -> w.Write "\\t"
          | '\n' -> w.Write "\\n"
          | '\f' -> w.Write "\\f"
          | '\r' -> w.Write "\\r"
          | '"'  -> w.Write "\\\""
          | '\\' -> w.Write "\\\\"
          | _    -> w.Write c

  module ProjectEvent =

    let getPropertyRec(nestedPropertyName:string) (x:JToken) =
      let fieldNames = nestedPropertyName.Split('.') |> Array.toList
      let rec tryFindValue (names: string list) (jsonProperty : JToken) =
        match names with
        | head::tail ->
          if jsonProperty.HasValues then
              tryFindValue tail jsonProperty.[head]
          else jsonProperty.Value<string>()
        | [] -> jsonProperty.Value<string>()
      tryFindValue fieldNames x

    let fromDocument(d:Document) : ProjectEvent[] =
      // checks to see if the format is equinox event or just a generic event for pass through
      let isEquinoxEvent =
        [d.GetPropertyValue("p");d.GetPropertyValue("i");d.GetPropertyValue("n");d.GetPropertyValue("e")]
        |> Seq.forall(fun a -> a <> null)
      if isEquinoxEvent then
        let batch = d.Cast<Batch>()
        batch.e |> Array.mapi(fun offset x ->
          { new IEquinoxEvent with
              member __.Index = batch.i + int64 offset
              member __.IsUnfold = false
              member __.EventType = x.c
              member __.Data = x.d
              member __.Meta = x.m
              member __.TimeStamp = x.t
              member __.Stream = batch.p }
          |> ProjectEvent.EquinoxEvent )
      else
        let e =
          d.ToString()
          |> JToken.Parse
          |> ProjectEvent.CustomEvent
        [|e|]

module Log =
    open Serilog
    type Event =
        | CustomEmitted of key: string * topic: string
        | EquinoxEmitted of stream: string * eventType: string * number: int64 * topic: string
        | Messages of ProjectEvent.ProjectEvent[]
        | BatchRead of timestamp: obj * lastTimestamp: obj * size: obj * range: obj * continuationLSN: obj
        | ProducerMsg of LogMessage

    /// Attach a property to the log context to hold the metrics
    // Sidestep Log.ForContext converting to a string; see https://github.com/serilog/serilog/issues/1124
    open Serilog.Events
    let event (value : Event) (log : ILogger) =
        let enrich (e : LogEvent) = e.AddPropertyIfAbsent(LogEventProperty("prjEvent", ScalarValue value))
        log.ForContext({ new Serilog.Core.ILogEventEnricher with member __.Enrich(evt,_) = enrich evt })

module Route =
    open ProjectEvent

    /// An event selection predicate.
    type Predicate =
      | CategorySelector of string
      | EventTypeSelector of string
      | StreamSelector of string
      | Union of Predicate[]
      | Intersect of Predicate[]
      | All

    /// A projection.
    type Projection =
      {
        /// The name of the projection.
        name : string

        /// The topic projected to.
        topic : string

        partitionCount : int

        /// The selector predicate used to filter events
        /// from the incoming stream.
        predicate : Predicate

        /// the collection name that this projection projects from
        collection : string

        makeKeyName : string option

        /// Partition key path, eg conversation.id
        partitionKeyPath : string option
      } 

    /// A projector step.
    type Step =
      /// Emit a projected event into a topic.
      | Emit of ProjectEvent * topic:string * partitionKey:string option

    // Implementation -------------------------------------------------------------

    /// Determines whether a given event should be projected with respect to the
    /// specified projection.

    let private cond (e:ProjectEvent) predicate =
      match e with
      | EquinoxEvent event ->
        let rec condp = function
          | CategorySelector category -> category = event.Stream.Split('-').[0]
          | EventTypeSelector t -> t = event.EventType
          | StreamSelector s -> s = event.Stream
          | Union predicates -> Array.exists condp predicates
          | Intersect predicates -> Array.forall condp predicates
          | All -> true
        condp predicate
      | CustomEvent event ->
        true

    /// Projects the specified projections.
    /// @projections - the projection definitions.
    /// @checkpointStream - the checkpoint stream name.
    /// @events - the incoming event stream.
    let project (log: Serilog.ILogger) (projections:Projection[]) (events:ProjectEvent[]) : Step [] =
      [|
        for event in events do
          for p in projections do
            if (cond event p.predicate) then
              yield Emit(event, p.topic, p.partitionKeyPath)
              match event, p.partitionKeyPath with
              | CustomEvent t,Some key -> log |> Log.event (Log.CustomEmitted (Utils.ProjectEvent.getPropertyRec key t, p.topic)) |> fun l -> l.Information("Emitted")
              | EquinoxEvent t, None -> log |> Log.event (Log.EquinoxEmitted (t.Stream, t.EventType, t.Index, p.topic)) |> fun l -> l.Information("Emitted")
              | _,_ -> log.Warning "event is invalid"
      |]

[<AutoOpen>]
module Changefeed =
  type RangePosition with
    static member ToString (x : RangePosition): string =
      String.Format("[MinInc={0}, MaxExc={1}, LastLSN={2}]", x.RangeMin, x.RangeMax, x.LastLSN)

    static member FromJsonObject (jobj: JToken): RangePosition =
      let rangeMin = jobj.["MinInc"].Value<int64>()
      let rangeMax = jobj.["MaxExc"].Value<int64>()
      let lastLSN = jobj.["LastLSN"].Value<int64>()
      {RangeMin = rangeMin; RangeMax = rangeMax; LastLSN = lastLSN}

    member v.ToJson() = JObject(JProperty("MinInc", v.RangeMin),
                               JProperty("MaxExc", v.RangeMax),
                               JProperty("LastLSN", v.LastLSN))

  module ChangefeedPosition =
    let ToJson (cfp: ChangefeedPosition): JToken =
      JArray([| for p in cfp -> p.ToJson() |]) :> JToken

    let FromJson (jobj: JToken): RangePosition[] =
      [| for c in jobj.Children() -> RangePosition.FromJsonObject c |]

    let toString (cfp: ChangefeedPosition): string =
      cfp |> Array.map RangePosition.ToString |> String.concat ";"

module ProgressWriter =
    open Extensions
    open Route
    
    type ProgressWriterConfig = {
      Owner : string
      WritePeriodMs : int
      CosmosDBClient : DocumentClient
      CosmosDBDatabaseName : string
      CosmosDBCollectionName : string
      CosmosDBAuxCollectionName : string
      KafkaConfig : KafkaConfig
      Region : string
      IncrementGeneration : bool
      Projections : Projection [] }
    
    /// A record of the latest kafka offsets for all the partitions of a kafka projection topic
    type ProjectionPosition = {
      Projection: string
      Topic: string
      KafkaOffsets: int64[]
    } with
      static member FromJsonObject (jobj: JToken): ProjectionPosition =
        let projection = jobj.["Projection"].Value<string>()
        let topic = jobj.["Topic"].Value<string>()
        let kafkaOffsets = [| for c in jobj.["KafkaOffsets"].Children() -> c.Value<int64>() |]
        {Projection = projection; Topic = topic; KafkaOffsets = kafkaOffsets}

      member v.ToJson = JObject([JProperty("Projection", v.Projection);
                                 JProperty("Topic", v.Topic);
                                 JProperty("KafkaOffsets", v.KafkaOffsets)])

    /// The projector offset index entry maps a changefeed position (range*lsn)[] to the latest kafka (partition*offset)[]
    /// for all projections produced
    type OffsetIndexEntry = {
      Region: string
      Generation: int
      Epoch: int
      ChangefeedPosition: ChangefeedPosition
      ChangefeedPositionIndex: Map<string, int64>
      ProjectionsPositions: ProjectionPosition[]
      ProjectionsPositionsIndex: Map<string, int64>
    }
    with
      static member FromJsonObject (jobj: JToken): OffsetIndexEntry =
        let region = jobj.["Region"].Value<string>()
        let generation = jobj.["Generation"].Value<int>()
        let epoch = jobj.["Epoch"].Value<int>()
        let changeFeedPosition = ChangefeedPosition.FromJson jobj.["ChangefeedPosition"]
        let changeFeedPositionIndex = [
          for p in jobj.["ChangefeedPositionIndex"].Children<JProperty>() ->
            (p.Name, Int64.Parse(p.Value.ToString()))] |> Map.ofList
        let projectionsPositions = [|
          for c in jobj.["ProjectionsPositions"].Children() ->
            ProjectionPosition.FromJsonObject c |]
        let projectionsPositionsIndex = [
          for p in jobj.["ProjectionsPositionsIndex"].Children<JProperty>() ->
            (p.Name, Int64.Parse(p.Value.ToString()))] |> Map.ofList
        in
        {Region = region; Generation = generation;
        Epoch = epoch;
        ChangefeedPosition = changeFeedPosition;
        ChangefeedPositionIndex = changeFeedPositionIndex;
        ProjectionsPositions = projectionsPositions;
        ProjectionsPositionsIndex = projectionsPositionsIndex}

      static member FromJsonString (json: string): OffsetIndexEntry =
        JToken.Parse json |> OffsetIndexEntry.FromJsonObject

      member v.ToJson: JObject =
        JObject(
          [JProperty("Region", v.Region);
           JProperty("Generation", v.Generation);
           JProperty("Epoch", v.Epoch);
           JProperty("ChangefeedPosition", ChangefeedPosition.ToJson(v.ChangefeedPosition));
           JProperty("ChangefeedPositionIndex", JObject.FromObject(v.ChangefeedPositionIndex));
           JProperty("ProjectionsPositions", [for p in v.ProjectionsPositions -> p.ToJson]);
           JProperty("ProjectionsPositionsIndex", JObject.FromObject(v.ProjectionsPositionsIndex));           
           JProperty("DocType", "OffsetIndexEntry");
           JProperty("id", String.Format("offsetIndex-{0}-{1}", v.Region, v.Epoch));
          ])

    let getLatestProgressEntry (client: DocumentClient) (auxCollectionUri: Uri) (region: string) = async {
      let querySpec =
        let q =
          """
          SELECT TOP 1 * FROM c
          WHERE
                c.Region = @region
            AND c.DocType = "OffsetIndexEntry"
          ORDER BY
                c.Epoch DESC"""
        let p = SqlParameterCollection [| SqlParameter ("@region", region) |]
        SqlQuerySpec(q,p)

      use iter =
        client.CreateDocumentQuery<OffsetIndexEntry>(auxCollectionUri, querySpec).AsDocumentQuery()

      if iter.HasMoreResults then
        let! entry = iter.ExecuteNextAsync<Document>() |> Async.AwaitTask //used to be Marvel.Async.AwaitTaskCorrect
        if entry.Count > 0 then
          return Some (entry.First().ToString() |> JToken.Parse |> OffsetIndexEntry.FromJsonObject)
        else
          return None
      else
        return None
    }

    let kafkaOffsetsMerge ((o1,o2):Map<TopicName, int64[]> * Map<TopicName, int64[]>) : Map<TopicName, int64[]> =
        Map.mergeWith(
          fun _ a b ->
            match a,b with
            | Some a, Some b ->
              Some <| Array.zipWith max a b
            | None, Some b -> Some b
            | Some a, None -> Some a
            | None, None -> None
            ) o1 o2

    /// Returns an Async computation that in turns returns a function that marks the progress of the projector to be called by
    /// Sagan's changefeed processor.
    let create (config:ProgressWriterConfig) = async {
      // everything that's needed to get a lease
      let auxCollectionUri =
        (config.CosmosDBDatabaseName, config.CosmosDBAuxCollectionName)
        |> UriFactory.CreateDocumentCollectionUri

      let! latestEntry = getLatestProgressEntry config.CosmosDBClient auxCollectionUri config.Region

      let mutable epoch = match latestEntry with Some e -> e.Epoch + 1 | None -> 0

      // The generation of the current projector is incremented when the projector is reset
      let generation = match latestEntry with Some e -> (if config.IncrementGeneration then e.Generation + 1 else e.Generation) | None -> 0

      // The progressWriter function to be called by Sagan's ChangefeedProcessor
      let writer (kafkaOffsets:Map<TopicName, int64[]>, cfp:SaganChangefeedProcessor.ChangefeedPosition) = async {
          // QUESTION: what happens if a kafka partition is not written to?
          if kafkaOffsets.IsEmpty then return ()
          else
            let projectionPositions : ProjectionPosition[] =
              config.Projections
              |> Array.map (fun p ->
                { Projection = p.name
                  Topic = p.topic
                  KafkaOffsets = kafkaOffsets |> Map.find p.topic })

            // lookup indices
            let projectionsIndex =
              projectionPositions
              |> Array.collect (fun pp -> pp.KafkaOffsets |> Array.mapi (fun i offset -> (sprintf "%s_%d" pp.Projection i), offset))
              |> Map.ofArray

            /// Sequencing operator like Haskell's ($). Has better precedence than (<|) due to the
            /// first character used in the symbol.
            let (^) = (<|)

            let changefeedIndex =
              cfp
              |> Seq.map (fun pp -> (sprintf "cfp%d_%d" pp.RangeMin pp.RangeMax), pp.LastLSN)
              |> Map.ofSeq
              |> Map.add "length" (int64 ^ Microsoft.FSharp.Collections.Array.length cfp)


            let indexEntry =
              {
                Region = config.Region
                Epoch = epoch
                Generation = generation
                ChangefeedPosition = cfp |> Array.sortBy (fun rp -> rp.RangeMin)
                ProjectionsPositions = projectionPositions

                ChangefeedPositionIndex = changefeedIndex
                ProjectionsPositionsIndex = projectionsIndex
              }
            epoch <- epoch + 1
            let bytes = Encoding.UTF8.GetBytes(indexEntry.ToJson.ToString())
            use documentStream = new System.IO.MemoryStream(bytes)
            do!
              config.CosmosDBClient.CreateDocumentAsync(auxCollectionUri, Resource.LoadFrom<Document> documentStream)
              |> Async.AwaitTask
              |> Async.Ignore }

      return writer
    }

[<RequireQualifiedAccess>]
module ChangefeedProcessor =

  /// Projector configuration.
  type Config = {
      /// The identity of the upstream Equinox/Eventstore being consumed
      equinox : string

      /// Region in which this projector is running
      region : string

      /// Cosmos collection containing Equinox database to project
      cosmos : CosmosEndpoint

      /// Auxilliary collection where changefeed processor keeps tracks its progress
      auxCollection : CosmosEndpoint

      /// Where in the changefeed should the processor start
      startPosition : StartingPosition

      /// Changefeed position at which to stop processing
      endPosition : ChangefeedPosition option

      /// Number of documents to read from each Cosmos partition in each batch
      batchSize : int

      /// How often to run the progress handler function
      progressInterval : TimeSpan
    }

  and StartingPosition =
    | ResumePrevious
    | StartFromBeginning
    | ResetToChangefeedPosition of ChangefeedPosition


  /// Sets up a cosmos/equinox changefeed processor that calls the `handle` function when a new batch of event is received
  /// Handle takes in the latest changefeed position as well as the events read from changefeed
  /// latest changefeed position is obtained from the RangeLSN field embedded within the document response
  let run log (config:Config) (eventHandler:Document[] -> Async<'a>) progressHandler merge : Async<unit> = async {

    // Sagan changefeed processor configuration. Some logic is needed to determine the starting position
    let! cfConfig = async {
      let! sp = async {
        match config.startPosition with
        | StartFromBeginning ->
          return SaganChangefeedProcessor.StartingPosition.Beginning
        | ResetToChangefeedPosition cfp ->
          return SaganChangefeedProcessor.StartingPosition.ChangefeedPosition cfp
        | ResumePrevious ->
          return! async{
            use client = new DocumentClient(config.auxCollection.uri, config.auxCollection.authKey)
            let auxCollectionUri = UriFactory.CreateDocumentCollectionUri(config.auxCollection.databaseName, config.auxCollection.collectionName)
            let! entry = ProgressWriter.getLatestProgressEntry client auxCollectionUri config.region
            match entry with
            | Some e -> return SaganChangefeedProcessor.StartingPosition.ChangefeedPosition e.ChangefeedPosition
            | None -> return SaganChangefeedProcessor.StartingPosition.Beginning
          }
        }

      return
        { BatchSize = config.batchSize
          ProgressInterval = config.progressInterval
          StartingPosition = sp
          RetryDelay = TimeSpan.FromSeconds 1.
          StoppingPosition = config.endPosition }
    }

    let sw = Diagnostics.Stopwatch.StartNew()

    let handler log (docs:Document[], rangePosition:RangePosition) = async {
      let range = (rangePosition.RangeMin, rangePosition.RangeMax)
      let ts = (Seq.last docs).Timestamp
    
      log |> Log.event (Log.BatchRead(sw.ElapsedMilliseconds, ts, docs.Length, range, rangePosition.LastLSN)) |> fun l -> l.Information("Read {count} in {ms} ms", docs.Length, sw.ElapsedMilliseconds)
      sw.Restart()  // not accurate, but close enough
      
      return!
        docs
        |> eventHandler
    }

    return! SaganChangefeedProcessor.go config.cosmos cfConfig PartitionSelectors.allPartitions (handler log) progressHandler merge
  }

[<AutoOpen>]
module ArraySegmentExtensions =
    type Encoding with
        member x.GetString(data:ArraySegment<byte>) = x.GetString(data.Array, data.Offset, data.Count)

module Publish =
    open Extensions
    open ProjectEvent
    open Equinox.Store
    
    /// State for a projection topic.
    type TopicPublisher =
      {
        /// An async computation returning a partition-indexed array of the latest written offsets.
        offsets : int64[]

        /// The producer used to write messages into the topic.
        producer : Producer
      }

    /// Creates a Kafka producer message given an event.
    let private messageOfEvent (event:ProjectEvent) (partitionKeyPath:string option) =
      // if partitionKeyPath has value, means it is cosmos event, not equinox schema
      match event,partitionKeyPath with
      | CustomEvent t, Some partitionKeyPath ->
        let key = t |> Utils.ProjectEvent.getPropertyRec partitionKeyPath |> System.Text.Encoding.UTF8.GetBytes |> Binary.ofArray
        let value = t.ToString() |> System.Text.Encoding.UTF8.GetBytes |> Binary.ofArray
        ProducerMessage(value, key)

      | EquinoxEvent t, None ->
        let key = t.Stream |> System.Text.Encoding.UTF8.GetBytes |> Binary.ofArray
        let value = t.Data |> Binary.ofArray
        let count = value.Count
        let offset = value.Offset
        value.Array.[offset+count-1] <- byte(44)

        // Injects eqxheader into the message
        let body = [|
            ("et", t.EventType)
            ("id", t.Index.ToString())
            ("s", t.Stream)
            ("t", t.TimeStamp.ToString("o"))
            |]
        use ms = new MemoryStream()
        use w = new StreamWriter(ms)
        w.Write "\"~eqxheader\":{"
        body |>
          Array.iteri(fun i x ->
            let k,v = x
            if i > 0 then w.Write ","
            w.Write "\""
            Utils.WriteStringToStream w k
            w.Write "\":"
            w.Write "\""
            Utils.WriteStringToStream w v
            w.Write "\"") 
        w.Write "}}"
        w.Flush()
        let str = System.Text.Encoding.UTF8.GetString(ArraySegment<byte>(Array.concat [|value.Array; ms.ToArray()|]))
        ProducerMessage(ArraySegment<byte>(Array.concat [|value.Array; ms.ToArray()|]), key)
      | _,_ ->
        failwithf "messege encode exception| invalid event type | messageOfEvent in Publish.fs"

    /// Performs a projection step.
    /// For Emit it produces into the projected Kafka topic.
    let private performBatch (log: Serilog.ILogger) (topics:Map<TopicName, TopicPublisher>) (steps:Route.Step[]) : Async<Map<TopicName, TopicPublisher>> = async {
      let! rs =
        steps
        |> Seq.map (fun step ->
          match step with
          | Route.Emit (event,topic,partitionKey) -> topic,event,partitionKey)
        |> Seq.groupBy(fun (x,_,p) -> x,p)
        |> Seq.map (fun (topic,xs) -> topic, xs |> Seq.map(fun (_,x,_) -> x) |> Seq.toArray)
        |> Seq.map (fun ((topic,partitionKey),es)-> async {
          match topics |> Map.tryFind topic with
          | Some(pubTopic) ->
            let ms = es |> Seq.map (fun event -> messageOfEvent event partitionKey)

            let! rs = async {
              let rec retry retryCount = async {
                try
                  let t0 = DateTime.UtcNow
                  let! t, rs = Legacy.produceBatched pubTopic.producer topic ms |> Stopwatch.Time
                  let t1 = DateTime.UtcNow
                  log |> Log.event (Log.Messages es) |> fun l -> l.Information("Produced {count} messages in {ms} ms", es.Length, (let e = t.Elapsed in e.TotalMilliseconds))
                  return rs
                with ex ->
                  log.Error(ex,"producer_exception|topic={topic} retryCount={retryCount}", topic, retryCount)
                  if retryCount <= 0 then
                    return raise ex
                  else
                    return! retry (retryCount-1)
              }
              return! retry 3 } //retry 3 times for now

            // latest offset per partition
            let rs =
              rs
              |> Seq.map (fun r -> r.partition, r.offset.Value + int64 r.count)
              |> Seq.groupBy fst
              |> Seq.map (fun (p,xs) -> p, xs |> Seq.map snd |> Seq.max)
              |> Map.ofSeq
            return Some(topic, rs)
          | None ->
            // We skipped this topic during the prepare step so we
            // don't publish to it. Usually this means the topic
            // was missing or we were unable to retrieve metadata
            // for this topic on start.
            return None })
        |> Async.Parallel

      // update latest offsets
      let topics' =
        (topics, rs |> Seq.choose id |> Map.ofSeq)
        ||> Map.mergeChoose (fun _ -> function
            | Map.MergeBoth (pt,os') ->
              let latestOffsets' =
                pt.offsets
                |> Array.mapi (fun p o ->
                  match os' |> Map.tryFind p with
                  | Some o' -> max o o'
                  | None -> o)
              Some { pt with offsets = latestOffsets' }
            | Map.MergeLeft pt -> Some pt
            | Map.MergeRight _ -> None)

      return topics' }

    /// Prepares projection kafka topics.
    /// For each topic, it creates a Kafka producer fetches the latest offsets
    let prepareKafkaTopicsPublishers (log: Serilog.ILogger) (kafkaConfig:KafkaConfig) (topics:TopicName[]) = async {
      let prepareTopic (topicName:TopicName) = async {
        let! offsets = Extensions.latestTopicOffset kafkaConfig topicName

        // NOTE: batchLingerMs is required to keep this from spinning rapidly.
        let producer =
          Config.Producer.safe
          |> Config.bootstrapServers kafkaConfig.broker
          |> Config.clientId kafkaConfig.clientId
          |> Config.Producer.requiredAcks (Config.Producer.RequiredAcks.Some 1)
          |> Config.Producer.batchNumMessages 10000 // default 2000; 10k for higher throughput
          |> Config.Producer.lingerMs 1000 // default 300 ms; 1 sec for higher throughput
          |> Producer.create
          |> Producer.onLog (fun msg -> log |> Log.event (Log.ProducerMsg msg) |> fun l -> l.Warning("Producer: {msg}", msg.Message))
        let topicPublisher = {offsets = offsets; producer = producer}
        return (topicName, topicPublisher)}

      let! topicsPublishers =
        topics
        |> Array.map prepareTopic
        |> Async.Parallel

      return Map.ofArray topicsPublishers
    }

    /// Consumes a stream of projector steps and writes them into Kafka based
    /// on the specified configuration.
    let write log (topicsPublishers:Map<TopicName, TopicPublisher>) (steps:Route.Step []) : Async<Map<TopicName, int64[]>> = async {
      match steps.Length with
      | 0 -> return Map.empty<TopicName, int64[]>
      | _ ->
        let! res = steps |> performBatch log topicsPublishers
        return res |> Map.map(fun _ value -> value.offsets)
    }

module Projector =
    open Extensions
    open Route
    
    type Publisher =
      {
        equinox : string 
        databaseEndpoint: Uri 
        databaseAuth: string 
        collectionName : string 
        database : string 
        changefeedBatchSize : int 
        projections : Projection[]
        region: string
        kafkaBroker: string
        clientId: string
        startPositionStrategy: ChangefeedProcessor.StartingPosition
        progressInterval: float
      }

    let go (log: Serilog.ILogger) (pub:Publisher) = async {
      let! ct = Async.CancellationToken

      let region = pub.region
      let groupId = sprintf "%s-%s" pub.collectionName region
      let databaseEndpoint = pub.databaseEndpoint
      let databaseAuth = pub.databaseAuth
      let databaseName = pub.database
      let collectionName = pub.collectionName
      let auxCollectionName = collectionName + "-aux"
      let kafkaBroker = pub.kafkaBroker
      let clientId = pub.clientId
      let startPositionStrategy = pub.startPositionStrategy
      let progressInterval = pub.progressInterval

      let client = new DocumentClient (pub.databaseEndpoint, pub.databaseAuth)

      let endpoint = {
        uri = databaseEndpoint
        authKey = databaseAuth
        databaseName = databaseName
        collectionName = collectionName
      }

      let auxendpoint = {
        uri = databaseEndpoint
        authKey = databaseAuth
        databaseName = databaseName
        collectionName = auxCollectionName
      }

      let config : ChangefeedProcessor.Config = {
        batchSize = pub.changefeedBatchSize
        progressInterval = TimeSpan.FromSeconds progressInterval
        startPosition = startPositionStrategy
        endPosition = None
        cosmos = endpoint
        auxCollection = auxendpoint
        equinox = collectionName
        region = region
      }

      let filteredProjections = (pub.projections |> Array.filter(fun proj -> proj.collection = pub.collectionName))
      let kafkaConfig = {
        broker = kafkaBroker
        clientId = clientId
      }

      let! topicsPublishers =
        filteredProjections
        |> Array.map (fun p -> p.topic)
        |> Publish.prepareKafkaTopicsPublishers log kafkaConfig

      let count = ref 0
      async {
        while true do
          log.Information("Projected {i}", !count)
          do! Async.Sleep 10000 }
      |> (fun x -> Async.Start (x,ct))

      let handler log (events:Document[]) = async {
        Interlocked.Add (count, events.Length) |> ignore
        return!
          events
          |> Array.filter(fun d -> d.Id <> "-1")
          |> Array.map (fun d -> d |> Utils.ProjectEvent.fromDocument)
          |> Array.concat
          |> Route.project log filteredProjections
          |> Publish.write log topicsPublishers
      }

      log.Information "Equinox Projector Starting"

      let progressWriterConfig : ProgressWriter.ProgressWriterConfig = {
        Owner = groupId
        WritePeriodMs = 30000
        CosmosDBClient = client
        CosmosDBDatabaseName = databaseName
        CosmosDBCollectionName = collectionName
        CosmosDBAuxCollectionName = auxCollectionName
        KafkaConfig = kafkaConfig
        Region = region
        Projections = pub.projections
        IncrementGeneration = true  // FIXME
      }

      let! progressWriter = ProgressWriter.create progressWriterConfig
      return! ChangefeedProcessor.run log config (handler log) progressWriter ProgressWriter.kafkaOffsetsMerge
    }