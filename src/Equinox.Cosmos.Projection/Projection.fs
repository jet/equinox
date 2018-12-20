namespace Equinox.Cosmos.Projection

open Equinox.Cosmos.Store
open System
open System.IO
open System.Linq
open System.Text
open System.Threading
open Microsoft.Azure.Documents
open Microsoft.Azure.Documents.Client
open Microsoft.Azure.Documents.Linq
open FSharp.Control
open Newtonsoft.Json.Linq
open Sagan
open Sagan.ChangefeedProcessor
open Confluent.Kafka
open Confluent.Kafka.Legacy
//open Serilog

module Map =

  type MergeChoice<'left, 'right> = Choice<'left * 'right, 'left, 'right>

  let inline (|MergeBoth|MergeLeft|MergeRight|) mc = mc

  let inline MergeBoth (left, right) = Choice1Of3(left, right)

  let inline MergeLeft left = Choice2Of3(left)

  let inline MergeRight right = Choice3Of3(right)

  /// Maps over the first item of a tuple.
  let inline mapFst f (a,b) = (f a, b)

    /// Maps over the second item of a tuple.
  let inline mapSnd f (a,b) = (a, f b)

  [<System.Obsolete("use Map.add instead")>]
  let replace = Map.add

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

  /// Searches the map for the specified keys and if any are missing returns Choice2Of2 with
  /// all of the missing keys, otherwise returns the provided map.
  let missingKeys (keys:seq<'a>) (args:Map<'a, 'b>) : Choice<Map<'a, 'b>, 'a list> =
    keys |> Seq.fold (fun s k ->
      if args.ContainsKey k then s
      else
        match s with
        | Choice1Of2 _  -> Choice2Of2 [k]
        | Choice2Of2 ks -> k::ks |> Choice2Of2 )
      (Choice1Of2 args)

  /// Creates a multi-map from a sequence of key-value pairs.
  let ofSeqMultimap (f:'b seq -> 'c) : ('a * 'b) seq -> Map<'a, 'c> =
    Seq.groupBy fst >> Seq.map (mapSnd (Seq.map snd >> f)) >> Map.ofSeq

  /// Creates a multi-map from a sequence of key-value pairs.
  let inline ofSeqMultimapArray<'a, 'b when 'a : comparison> : ('a * 'b) seq -> Map<'a, 'b[]> =
    ofSeqMultimap Seq.toArray

  /// Creates a multi-map from a sequence of key-value pairs.
  let inline ofSeqMultimapList<'a, 'b when 'a : comparison> : ('a * 'b) seq -> Map<'a, 'b list> =
    ofSeqMultimap Seq.toList

  /// Adds a new key-value pair or updates an existing value.
  let addOrUpdate (k:'a) (f:'b option -> 'b) (m:Map<'a, 'b>) : Map<'a, 'b> =
    let b = m |> Map.tryFind k
    m |> Map.add k (f b)
    
module Array =
    /// Makes an array, its elements are calculated from the function and the elements of input arrays occuring at the same position in both array.
    /// The two arrays must have equal length.
    let zipWith (f:'a -> 'b -> 'c) (a:'a[]) (b:'b[]) : 'c[] =
      // TODO: optimize such that only one result array is allocated.
      Array.zip a b |> Array.map (fun (a,b) -> f a b)

module Option =
    /// Folds an option by applying f to Some otherwise returning the default value.
    let inline foldOr (f:'a -> 'b) (defaultValue:'b) = function Some a -> f a | None -> defaultValue

module Extensions =
  
  type TopicName = string
  type Partition = int

  type KafkaConfig = {
    broker : string
    clientId : string
  }
  
  module Binary =
    open System.Text
      
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

    let getUniqueId (e:ProjectEvent) =
      match e with
      | CustomEvent t -> t |> getPropertyRec "id"
      | EquinoxEvent t -> sprintf "%s-%O" t.Stream t.Index

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

    // Instrumentation ------------------------------------------------------------

    /// Indicates that the projector emitted an event for production into Kafka.
    type EventEmitted(event, topic, partitionKey) =
      member val event : ProjectEvent = event
      member val topic : string = topic
      member val partitionKey : string option = partitionKey
      override __.ToString() =
        match event,partitionKey with
        |  CustomEvent t,Some key ->
          sprintf "event_emitted|key=%s|topic=%s" (Utils.ProjectEvent.getPropertyRec key t) topic
        | EquinoxEvent t, None ->
          sprintf "event_emitted|stream=%s|type=%s|number=%i|topic=%s"
            (t.Stream) (t.EventType) (t.Index) topic
        | _,_ ->
          "event is invalid"

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

    // Public API -----------------------------------------------------------------

    /// Projects the specified projections.
    /// @projections - the projection definitions.
    /// @checkpointStream - the checkpoint stream name.
    /// @events - the incoming event stream.
    let project (projections:Projection[]) (events:ProjectEvent[]) : Step [] =
      [|
        for event in events do
          for p in projections do
            if (cond event p.predicate) then
              yield Emit(event, p.topic, p.partitionKeyPath)
              printf "%s" <| EventEmitted(event, p.topic, p.partitionKeyPath).ToString()
      |]
   
    //TODO: rename
    let filter (predicate:Predicate) (events:seq<ProjectEvent>) : seq<ProjectEvent> =
      events |> Seq.filter (fun e -> cond e predicate)

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

      let mutable epoch = latestEntry |> Option.foldOr (fun e -> e.Epoch + 1) 0

      // The generation of the current projector is incremented when the projector is reset
      let generation =
        latestEntry
        |> Option.foldOr (fun e -> if config.IncrementGeneration then e.Generation + 1 else e.Generation) 0

      // The progressWriter function to be called by Sagan's ChangefeedProcessor
      let writer (kafkaOffsets:Map<TopicName, int64[]>, cfp:Sagan.ChangefeedProcessor.ChangefeedPosition) = async {
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

  type BatchRead(timestamp, lastTimestamp, size, range, continuationLSN) =
    member val timestamp = timestamp with get
    member val lastTimestamp = lastTimestamp with get
    member val size = size with get
    member val range = range with get
    member val continuationLSN = continuationLSN with get

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
  let run (config:Config) (eventHandler:Document[] -> Async<'a>) progressHandler merge : Async<unit> = async {

    // Sagan changefeed processor configuration. Some logic is needed to determine the starting position
    let! cfConfig = async {
      let! sp = async {
        match config.startPosition with
        | StartFromBeginning ->
          return Sagan.ChangefeedProcessor.StartingPosition.Beginning
        | ResetToChangefeedPosition cfp ->
          return Sagan.ChangefeedProcessor.StartingPosition.ChangefeedPosition cfp
        | ResumePrevious ->
          return! async{
            use client = new DocumentClient(config.auxCollection.uri, config.auxCollection.authKey)
            let auxCollectionUri = UriFactory.CreateDocumentCollectionUri(config.auxCollection.databaseName, config.auxCollection.collectionName)
            let! entry = ProgressWriter.getLatestProgressEntry client auxCollectionUri config.region
            match entry with
            | Some e -> return Sagan.ChangefeedProcessor.StartingPosition.ChangefeedPosition e.ChangefeedPosition
            | None -> return Sagan.ChangefeedProcessor.StartingPosition.Beginning
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

    let handler (docs:Document[], rangePosition:RangePosition) = async {
      let range = (rangePosition.RangeMin, rangePosition.RangeMax)
      let ts = (Seq.last docs).Timestamp
      
      printf "%O" (BatchRead(sw.ElapsedMilliseconds, ts, docs.Length, range, rangePosition.LastLSN))
      sw.Restart()  // not accurate, but close enough
      
      return!
        docs
        |> eventHandler
    }

    return! Sagan.ChangefeedProcessor.go config.cosmos cfConfig PartitionSelectors.allPartitions handler progressHandler merge
  }

[<AutoOpen>]
module ArraySegmentExtensions =
    type Encoding with
        member x.GetString(data:ArraySegment<byte>) = x.GetString(data.Array, data.Offset, data.Count)

module Publish =
    open Extensions
    open ProjectEvent
    
    /// State for a projection topic.
    type TopicPublisher =
      {
        /// An async computation returning a partition-indexed array of the latest written offsets.
        offsets : int64[]

        /// The producer used to write messages into the topic.
        producer : Producer
      }

    type EventPublished(event:ProjectEvent) =
      member val event = event

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
    let private performBatch (topics:Map<TopicName, TopicPublisher>) (steps:Route.Step[]) : Async<Map<TopicName, TopicPublisher>> = async {
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
                  let! rs = Legacy.produceBatched pubTopic.producer topic ms
                  let t1 = DateTime.UtcNow
                  for e in es do
                    printf "%O" <| EventPublished(e)
                  //Probe.infof "Produced %O messages |Elapsed:%Oms" es.Length (t1-t0).TotalMilliseconds
                  return rs
                with ex ->
                  printf "producer_exception|topic=%s retryCount=%O error=%O" topic retryCount ex
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
    let prepareKafkaTopicsPublishers (kafkaConfig:KafkaConfig) (topics:TopicName[]) = async {
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
          |> Producer.onLog (fun logger ->
            let msg = sprintf "producer: %s" logger.Message
            printf "%s" msg)

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
    let write (topicsPublishers:Map<TopicName, TopicPublisher>) (steps:Route.Step []) : Async<Map<TopicName, int64[]>> = async {
      match steps.Length with
      | 0 -> return Map.empty<TopicName, int64[]>
      | _ ->
        let! res = steps |> performBatch topicsPublishers
        return res |> Map.map(fun _ value -> value.offsets)
    }

module Projector =
    open Extensions
    open Route
    
    type Publisher =
      {
        equinox : string // "equinox-test-ming"
        databaseEndpoint: Uri // Uri("https://marvel-batman-equinox.documents.azure.com:443/")
        databaseAuth: string // "9WaRihbtAImGeii2nixYwI0cWqMfI2NdXMAWCyHrtRrZdkRM1qRreG8M0f97FTRoQQwRZ4UP3E55Rf1Btog7Ew=="
        collectionName : string // "ming"
        database : string // "equinox-test"
        changefeedBatchSize : int // 100
        projections : Projection[]
      }

    let go (pub:Publisher) = async {
      let! ct = Async.CancellationToken

      let region = "eastus2"
      let groupId = sprintf "%s-%s" pub.collectionName region
      let databaseEndpoint = pub.databaseEndpoint
      let databaseAuth = pub.databaseAuth
      let databaseName = pub.database
      let collectionName = pub.collectionName
      let auxCollectionName = collectionName + "-aux"

      let client = new DocumentClient (pub.databaseEndpoint, pub.databaseAuth)

      let kafkaBroker = "shared.kafka.eastus2.qa.jet.network:9092"

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
        progressInterval = TimeSpan.FromSeconds 30.0
        startPosition = ChangefeedProcessor.StartingPosition.ResumePrevious
        endPosition = None
        cosmos = endpoint
        auxCollection = auxendpoint
        equinox = collectionName
        region = region
      }

      let filteredProjections = (pub.projections |> Array.filter(fun proj -> proj.collection = pub.collectionName))
      let kafkaConfig = {
        broker = kafkaBroker
        clientId = "projector"
      }

      let! topicsPublishers =
        filteredProjections
        |> Array.map (fun p -> p.topic)
        |> Publish.prepareKafkaTopicsPublishers kafkaConfig

      let count = ref 0
      async {
        while true do
          printf "%d" <| int(!count)
          do! Async.Sleep 10000 }
      |> (fun x -> Async.Start (x,ct))

      let handler (events:Document[]) = async {
        Interlocked.Add (count, events.Length) |> ignore
        return!
          events
         //|> Array.iter(fun e -> Probe.infof "Projecting Event sn=%A data=%A" e.Index (System.Text.Encoding.UTF8.GetString e.Data))
          |> Array.filter(fun d -> d.Id <> "-1")
          |> Array.map (fun d -> d |> Utils.ProjectEvent.fromDocument)
          |> Array.concat
          |> Route.project filteredProjections
          |> Publish.write topicsPublishers
      }

      printf "Equinox Projector Starting"

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
      return! ChangefeedProcessor.run config handler progressWriter ProgressWriter.kafkaOffsetsMerge
    }