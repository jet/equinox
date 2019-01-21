namespace Equinox.Projection.Kafka.Infrastructure

open Confluent.Kafka
open Serilog
open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks

[<AutoOpen>]
module private Helpers =

    let encoding = System.Text.Encoding.UTF8
    let mkSerializer() = new Confluent.Kafka.Serialization.StringSerializer(encoding)
    let mkDeserializer() = new Confluent.Kafka.Serialization.StringDeserializer(encoding)

    type Async with
        static member AwaitTaskCorrect (task : Task<'T>) : Async<'T> =
            Async.FromContinuations <| fun (k,ek,_) ->
                task.ContinueWith (fun (t:Task<'T>) ->
                    if t.IsFaulted then
                        let e = t.Exception
                        if e.InnerExceptions.Count = 1 then ek e.InnerExceptions.[0]
                        else ek e
                    elif t.IsCanceled then ek (TaskCanceledException("Task wrapped with Async has been cancelled."))
                    elif t.IsCompleted then k t.Result
                    else ek(Exception "invalid Task state!"))
                |> ignore

        static member AwaitTaskCorrect (task : Task) : Async<unit> =
            Async.FromContinuations <| fun (k,ek,_) ->
                task.ContinueWith (fun (t:Task) ->
                    if t.IsFaulted then
                        let e = t.Exception
                        if e.InnerExceptions.Count = 1 then ek e.InnerExceptions.[0]
                        else ek e
                    elif t.IsCanceled then ek (TaskCanceledException("Task wrapped with Async has been cancelled."))
                    elif t.IsCompleted then k ()
                    else ek(Exception "invalid Task state!"))
                |> ignore

// A small DSL for interfacing with Kafka configuration c.f. https://kafka.apache.org/documentation/#configuration
/// See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md for documentation on the implications of specfic settings
[<RequireQualifiedAccess>]
module Config =

    [<NoComparison; NoEquality>]
    type ConfigKey<'T> = Key of id:string * render:('T -> obj) with
        member k.Id = let (Key(id,_)) = k in id

        static member (==>) (Key(id, render) : ConfigKey<'T>, value : 'T) = 
            match render value with
            | null -> nullArg id
            | :? string as str when String.IsNullOrWhiteSpace str -> nullArg id
            | obj -> KeyValuePair(id, obj)

    let private mkKey id render = Key(id, render >> box)
    let private ms (t : TimeSpan) = int t.TotalMilliseconds
    let private validateBrokerUri (u:Uri) =
        if not u.IsAbsoluteUri then invalidArg "broker" "should be of 'host:port' format"
        if String.IsNullOrEmpty u.Authority then 
            // handle a corner case in which Uri instances are erroneously putting the hostname in the `scheme` field.
            if System.Text.RegularExpressions.Regex.IsMatch(string u, "^\S+:[0-9]+$") then string u
            else invalidArg "broker" "should be of 'host:port' format"

        else u.Authority

    (* shared keys applying to producers and consumers alike *)

    let broker              = mkKey "bootstrap.servers" validateBrokerUri
    let clientId            = mkKey "client.id" id<string>
    let logConnectionClose  = mkKey "log.connection.close" id<bool>
    let maxInFlight         = mkKey "max.in.flight.requests.per.connection" id<int>
    let rebalanceTimeout    = mkKey "rebalance.timeout.ms" ms
    let retries             = mkKey "retries" id<int>
    let retryBackoff        = mkKey "retry.backoff.ms" ms
    let socketKeepAlive     = mkKey "socket.keepalive.enable" id<bool>
    let statisticsInterval  = mkKey "statistics.interval.ms" ms

    /// Config keys applying to Producers
    module Producer =
        type Acks = Zero | One | All
        type Compression = None | GZip | Snappy | LZ4
        type Partitioner = Random | Consistent | ConsistentRandom

        let acks                = mkKey "acks" (function Zero -> 0 | One -> 1 | All -> -1)
        let batchNumMessages    = mkKey "batch.num.messages" id<int>
        let compression         = mkKey "compression.type" (function None -> "none" | GZip -> "gzip" | Snappy -> "snappy" | LZ4 -> "lz4")
        let linger              = mkKey "linger.ms" ms
        let maxInFlight         = mkKey "max.in.flight.requests.per.connection" id<int>
        let messageSendRetries  = mkKey "message.send.max.retries" id<int>
        let partitioner         = mkKey "partitioner" (function | Random -> "random" | Consistent -> "consistent" | ConsistentRandom -> "consistent_random")
        let queueBufferingMaxInterval = mkKey "queue.buffering.max.ms" ms
        let timeout             = mkKey "request.timeout.ms" ms

    /// Config keys applying to Consumers
    module Consumer =
        type AutoOffsetReset = Earliest | Latest | None

        let autoCommitInterval  = mkKey "auto.commit.interval.ms" ms
        let autoOffsetReset     = mkKey "auto.offset.reset" (function Earliest -> "earliest" | Latest -> "latest" | None -> "none")
        let topicConfig         = mkKey "default.topic.config" id : ConfigKey<seq<KeyValuePair<string, obj>>>
        let enableAutoCommit    = mkKey "enable.auto.commit" id<bool>
        let enableAutoOffsetStore = mkKey "enable.auto.offset.store" id<bool>
        let checkCrc            = mkKey "check.crcs" id<bool>
        let heartbeatInterval   = mkKey "heartbeat.interval.ms" ms
        let groupId             = mkKey "group.id" id<string>
        let fetchMaxBytes       = mkKey "fetch.message.max.bytes" id<int>
        let fetchMinBytes       = mkKey "fetch.min.bytes" id<int>
        let fetchMaxWait        = mkKey "fetch.wait.max.ms" ms
        let sessionTimeout      = mkKey "session.timeout.ms" ms

// NB we deliberately wrap all types exposed by Confluent.Kafka to avoid needing to reference librdkafka everywhere
[<Struct; NoComparison>]
type KafkaMessage internal (message : Message<string, string>) =
    member internal __.UnderlyingMessage = message
    member __.Topic = message.Topic
    member __.Partition = message.Partition
    member __.Offset = let o = message.Offset in o.Value
    member __.Key = message.Key
    member __.Value = message.Value

[<NoComparison>]
type KafkaProducerConfig(kvps, broker : Uri, compression : Config.Producer.Compression, acks : Config.Producer.Acks) =
    member val Kvps = kvps
    member val Acks = acks
    member val Broker = broker
    member val Compression = compression

    /// Creates a Kafka producer instance with supplied configuration
    static member Create
        (   clientId : string, broker : Uri, 
            /// Message compression. Defaults to 'none'.
            ?compression,
            /// Number of retries. Defaults to 60.
            ?retries,
            /// Backoff interval. Defaults to 1 second.
            ?retryBackoff,
            /// Statistics Interval. Defaults to no stats.
            ?statisticsInterval,
            /// Defaults to 10 ms.
            ?queueBufferingMaxInterval,
            /// Defaults to true.
            ?socketKeepAlive,
            /// Defaults to 200 milliseconds.
            ?linger,
            /// Defaults to 'One'
            ?acks,
            /// Defaults to 'consistent_random'.
            ?partitioner,
            /// Misc configuration parameter to be passed to the underlying CK producer.
            ?custom) =

        let compression = defaultArg compression Config.Producer.Compression.None
        let acks = defaultArg acks Config.Producer.Acks.One
        let config =
            [|  yield Config.clientId ==> clientId
                yield Config.broker ==> broker

                yield Config.socketKeepAlive ==> defaultArg socketKeepAlive true
                yield Config.retries ==> defaultArg retries 60
                yield Config.retryBackoff ==> defaultArg retryBackoff (TimeSpan.FromSeconds 1.)
                match statisticsInterval with Some t -> yield Config.statisticsInterval ==> t | None -> ()
                yield Config.Producer.linger ==> defaultArg linger (TimeSpan.FromMilliseconds 200.)
                yield Config.Producer.compression ==> compression 
                yield Config.Producer.partitioner ==> defaultArg partitioner Config.Producer.Partitioner.ConsistentRandom
                yield Config.Producer.acks ==> acks
                yield Config.Producer.queueBufferingMaxInterval ==> defaultArg queueBufferingMaxInterval (TimeSpan.FromMilliseconds 10.)

                // https://github.com/confluentinc/confluent-kafka-dotnet/issues/124#issuecomment-289727017
                yield Config.logConnectionClose ==> false

                match custom with None -> () | Some miscConfig -> yield! miscConfig |]
        KafkaProducerConfig(config, broker, compression, acks)

type KafkaProducer private (log: ILogger, producer : Producer<string, string>, topic : string) =
    let log = log.ForContext<KafkaProducer>()
    let d1 = producer.OnLog.Subscribe(fun m -> log.Information("{message} level={level} name={name} facility={facility}", m.Message, m.Level, m.Name, m.Facility))
    let d2 = producer.OnError.Subscribe(fun e -> log.Error("{reason} code={code} isBrokerError={isBrokerError}", e.Reason, e.Code, e.IsBrokerError))

    let tryWrap (msg : Message<string, string>) =
        if msg.Error.HasError then
            let errorMsg = sprintf "Error writing message topic=%s code=%O reason=%s" topic msg.Error.Code msg.Error.Reason
            Choice2Of2 (Exception errorMsg)
        else
            Choice1Of2 (KafkaMessage msg)

    /// https://github.com/edenhill/librdkafka/wiki/Statistics
    [<CLIEvent>]
    member __.OnStatistics = producer.OnStatistics
    member __.Topic = topic
    member __.Produce(key : string, value : string) = async {
        let! msg = producer.ProduceAsync(topic, key, value) |> Async.AwaitTaskCorrect
        match tryWrap msg with
        | Choice1Of2 msg -> return msg
        | Choice2Of2 e -> return raise e }

    /// Produces a batch of supplied key/value messages. Results are returned in order of writing.  
    member __.ProduceBatch
        (   keyValueBatch : seq<string * string>,
            /// Enable marshalling of data in returned messages. Defaults to false.
            ?marshalDeliveryReportData : bool) = async {
        match Seq.toArray keyValueBatch with
        | [||] -> return [||]
        | keyValueBatch ->

        let! ct = Async.CancellationToken

        let tcs = new TaskCompletionSource<KafkaMessage[]>()
        let numMessages = keyValueBatch.Length
        let results = Array.zeroCreate<KafkaMessage> numMessages
        let numCompleted = ref 0

        use _ = ct.Register(fun _ -> tcs.TrySetCanceled() |> ignore)

        let handler =
            { new IDeliveryHandler<string, string> with
                member __.MarshalData = defaultArg marshalDeliveryReportData false
                member __.HandleDeliveryReport m =
                    match tryWrap m with
                    | Choice2Of2 e ->
                        tcs.TrySetException e |> ignore

                    | Choice1Of2 m ->
                        let i = Interlocked.Increment numCompleted
                        results.[i - 1] <- m
                        if i = numMessages then tcs.TrySetResult results |> ignore }
        for key,value in keyValueBatch do
            producer.ProduceAsync(topic, key, value, blockIfQueueFull = true, deliveryHandler = handler)
        return! Async.AwaitTaskCorrect tcs.Task }

    interface IDisposable with member __.Dispose() = for d in [d1;d2;producer:>_] do d.Dispose()

    static member Create(log : ILogger, config : KafkaProducerConfig, topic : string) =
        if String.IsNullOrEmpty topic then nullArg "topic"
        log.Information("Starting Kafka Producer on topic={topic} broker={broker} compression={compression} acks={acks}",
                          topic, config.Broker, config.Compression, config.Acks)
        let producer = new Producer<string, string>(config.Kvps, mkSerializer(), mkSerializer())
        new KafkaProducer(log, producer, topic)
        
 module private ConsumerImpl =

    /// used for calculating approximate message size in bytes
    let getMessageSize (message : Message<string, string>) =
        let inline len (x:string) = match x with null -> 0 | x -> sizeof<char> * x.Length
        16 + len message.Key + len message.Value |> int64

    let getBatchOffset (batch : KafkaMessage[]) =
        let maxOffset = batch |> Array.maxBy (fun m -> m.Offset)
        maxOffset.UnderlyingMessage.TopicPartitionOffset

    let mkMessage (msg : Message<string, string>) =
        if msg.Error.HasError then
            failwithf "error consuming message topic=%s partition=%d offset=%O code=%O reason=%s"
                            msg.Topic msg.Partition msg.Offset msg.Error.Code msg.Error.Reason
        KafkaMessage(msg)

    type BlockingCollection<'T> with
        member bc.FillBuffer(buffer : 'T[], maxDelay : TimeSpan) : int =
            let cts = new CancellationTokenSource()
            do cts.CancelAfter maxDelay

            let n = buffer.Length
            let mutable i = 0
            let mutable t = Unchecked.defaultof<'T>

            while i < n && not cts.IsCancellationRequested do
                if bc.TryTake(&t, 5 (* ms *)) then
                    buffer.[i] <- t ; i <- i + 1
                    while i < n && not cts.IsCancellationRequested && bc.TryTake(&t) do 
                        buffer.[i] <- t ; i <- i + 1

            i

    type PartitionedBlockingCollection<'Key, 'Message when 'Key : equality>(?perPartitionCapacity : int) =
        let collections = new ConcurrentDictionary<'Key, Lazy<BlockingCollection<'Message>>>()
        let onPartitionAdded = new Event<'Key * BlockingCollection<'Message>>()

        let createCollection() =
            match perPartitionCapacity with
            | None -> new BlockingCollection<'Message>()
            | Some c -> new BlockingCollection<'Message>(boundedCapacity = c)

        [<CLIEvent>]
        member __.OnPartitionAdded = onPartitionAdded.Publish

        member __.Add (key : 'Key, message : 'Message) =
            let factory key = lazy(
                let coll = createCollection()
                onPartitionAdded.Trigger(key, coll)
                coll)

            let buffer = collections.GetOrAdd(key, factory)
            buffer.Value.Add message

        member __.Revoke(key : 'Key) =
            match collections.TryRemove key with
            | true, coll -> Task.Delay(10000).ContinueWith(fun _ -> coll.Value.CompleteAdding()) |> ignore
            | _ -> ()

    type InFlightMessageCounter(log: ILogger, minInFlightBytes : int64, maxInFlightBytes : int64) =
        do  if minInFlightBytes < 1L then invalidArg "minInFlightBytes" "must be positive value"
            if maxInFlightBytes < 1L then invalidArg "maxInFlightBytes" "must be positive value"
            if minInFlightBytes > maxInFlightBytes then invalidArg "maxInFlightBytes" "must be greater than minInFlightBytes"

        let mutable inFlightBytes = 0L

        member __.InFlightBytes = inFlightBytes
        member __.MinBytes = minInFlightBytes
        member __.MaxBytes = maxInFlightBytes

        member __.Add(numBytes : int64) = Interlocked.Add(&inFlightBytes, numBytes) |> ignore

        member __.AwaitThreshold() =
            if inFlightBytes > maxInFlightBytes then
                log.Warning("Consumer reached in-flight message threshold, breaking off polling, bytes={max}", inFlightBytes)
                while inFlightBytes > minInFlightBytes do Thread.Sleep 5
                log.Information "Consumer resuming polling"

    type Consumer<'Key, 'Value> with
        member c.StoreOffset(tpo : TopicPartitionOffset) =
            c.StoreOffsets[| TopicPartitionOffset(tpo.Topic, tpo.Partition, Offset(let o = tpo.Offset in o.Value + 1L)) |]
            |> ignore

        member c.RunPoll(pollTimeout : TimeSpan, counter : InFlightMessageCounter) =
            let cts = new CancellationTokenSource()
            let poll() = 
                while not cts.IsCancellationRequested do
                    counter.AwaitThreshold()
                    c.Poll(pollTimeout)

            let _ = Async.StartAsTask(async { poll() })
            { new IDisposable with member __.Dispose() = cts.Cancel() }

        member c.WithLogging(log: ILogger) =
            let fmtError (e : Error) = if e.HasError then sprintf " reason=%s code=%O isBrokerError=%b" e.Reason e.Code e.IsBrokerError else ""
            let fmtTopicPartitions (topicPartitions : seq<TopicPartition>) =
                topicPartitions 
                |> Seq.groupBy (fun p -> p.Topic)
                |> Seq.map (fun (t,ps) -> sprintf "topic=%s|partitions=[%s]" t (ps |> Seq.map (fun p -> string p.Partition) |> String.concat "; "))

            let fmtTopicPartitionOffsetErrors (topicPartitionOffsetErrors : seq<TopicPartitionOffsetError>) =
                topicPartitionOffsetErrors
                |> Seq.groupBy (fun p -> p.Topic)
                |> Seq.map (fun (t,ps) -> sprintf "topic=%s|offsets=[%s]" t (ps |> Seq.map (fun p -> sprintf "%d@%O%s" p.Partition p.Offset (fmtError p.Error)) |> String.concat "; "))
                    
            let d1 = c.OnLog.Subscribe(fun m -> log.Information("consumer_info|{message} level={level} name={name} facility={facility}", m.Message, m.Level, m.Name, m.Facility))
            let d2 = c.OnError.Subscribe(fun e -> log.Error("consumer_error|{error}", fmtError e))
            let d3 = c.OnPartitionsAssigned.Subscribe(fun tps -> for fmt in fmtTopicPartitions tps do log.Information("consumer_partitions_assigned|{topics}", fmt))
            let d4 = c.OnPartitionsRevoked.Subscribe(fun tps -> for fmt in fmtTopicPartitions tps do log.Information("consumer_partitions_revoked|{topics}", fmt))
            let d5 = c.OnPartitionEOF.Subscribe(fun tpo -> log.Verbose("consumer_partition_eof|topic={topic}|partition={partition}|offset={offset}", tpo.Topic, tpo.Partition, let o = tpo.Offset in o.Value))
            let d6 = c.OnOffsetsCommitted.Subscribe(fun cos -> for fmt in fmtTopicPartitionOffsetErrors cos.Offsets do log.Information("consumer_committed_offsets|{offsets}{oe}", fmt, fmtError cos.Error))
            { new IDisposable with member __.Dispose() = for d in [d1;d2;d3;d4;d5;d6] do d.Dispose() }

    let mkConsumer (kvps, topics : string seq) keyDeserializer valueDeserializer =
        let consumer = new Consumer<'Key, 'Value>(kvps, keyDeserializer, valueDeserializer)
        let _ = consumer.OnPartitionsAssigned.Subscribe(fun m -> consumer.Assign m)
        let _ = consumer.OnPartitionsRevoked.Subscribe(fun _ -> consumer.Unassign())
        consumer.Subscribe topics
        consumer

    let mkBatchedMessageConsumer (log: ILogger) (minInFlightBytes, maxInFlightBytes, maxBatchSize, maxBatchDelay, pollTimeout)
            (ct : CancellationToken) (consumer : Consumer<string, string>) (handler : KafkaMessage[] -> Async<unit>) = async {
        let tcs = new TaskCompletionSource<unit>()
        use cts = CancellationTokenSource.CreateLinkedTokenSource(ct)
        use _ = ct.Register(fun _ -> tcs.TrySetResult () |> ignore)

        use _ = consumer
        use _ = consumer.WithLogging(log)
        
        let counter = new InFlightMessageCounter(log, minInFlightBytes, maxInFlightBytes)
        let partitionedCollection = new PartitionedBlockingCollection<TopicPartition, Message<string, string>>()

        // starts a tail recursive loop which dequeues batches for a given partition buffer and schedules the user callback
        let consumePartition (_key : TopicPartition) (collection : BlockingCollection<Message<string, string>>) =
            let buffer = Array.zeroCreate maxBatchSize
            let nextBatch () =
                let count = collection.FillBuffer(buffer, maxBatchDelay)
                let batch = Array.init count (fun i -> mkMessage buffer.[i])
                Array.Clear(buffer, 0, count)
                batch

            let rec loop () = async {
                if not collection.IsCompleted then
                    try match nextBatch() with
                        | [||] -> ()
                        | batch ->
                            // run the handler function
                            do! handler batch

                            // store completed offsets
                            consumer.StoreOffset(getBatchOffset batch)

                            // decrement in-flight message counter
                            let batchSize = batch |> Array.sumBy (fun m -> getMessageSize m.UnderlyingMessage)
                            counter.Add -batchSize
                    with e ->
                        tcs.TrySetException e |> ignore
                        cts.Cancel()
                    do! loop() }

            Async.Start(loop(), cts.Token)

        use _ = partitionedCollection.OnPartitionAdded.Subscribe (fun (key,buffer) -> consumePartition key buffer)
        use _ = consumer.OnPartitionsRevoked.Subscribe (fun ps -> for p in ps do partitionedCollection.Revoke p)
        use _ = consumer.OnMessage.Subscribe (fun m -> counter.Add(getMessageSize m) ; partitionedCollection.Add(m.TopicPartition, m))

        use _ = consumer.RunPoll(pollTimeout, counter) // run the consumer

        // await for handler faults or external cancellation
        do! Async.AwaitTaskCorrect tcs.Task
    }

[<NoComparison>]
type KafkaConsumerConfig =
    {   clientId : string; broker : Uri; topics : string list; groupId : string
        autoOffsetReset : Config.Consumer.AutoOffsetReset
        offsetCommitInterval : TimeSpan
        fetchMinBytes : int; fetchMaxBytes : int
        retries : int; retryBackoff : TimeSpan
        statisticsInterval : TimeSpan option
        miscConfig : KeyValuePair<string, obj> list

        (* Local Batching configuration *)

        minInFlightBytes : int64; maxInFlightBytes : int64
        maxBatchSize : int; maxBatchDelay : TimeSpan
        pollTimeout : TimeSpan }
    member c.ToKeyValuePairs() =
        [|  yield Config.clientId ==> c.clientId
            yield Config.broker ==> c.broker
            yield Config.Consumer.groupId ==> c.groupId

            yield Config.Consumer.fetchMaxBytes ==> c.fetchMaxBytes
            yield Config.Consumer.fetchMinBytes ==> c.fetchMinBytes
            yield Config.retries ==> c.retries
            yield Config.retryBackoff ==> c.retryBackoff
            match c.statisticsInterval with None -> () | Some t -> yield Config.statisticsInterval ==> t

            yield Config.Consumer.enableAutoCommit ==> true
            yield Config.Consumer.enableAutoOffsetStore ==> false
            yield Config.Consumer.autoCommitInterval ==> c.offsetCommitInterval

            yield Config.Consumer.topicConfig ==> [ Config.Consumer.autoOffsetReset ==> Config.Consumer.AutoOffsetReset.Earliest ]

            // https://github.com/confluentinc/confluent-kafka-dotnet/issues/124#issuecomment-289727017
            yield Config.logConnectionClose ==> false

            yield! c.miscConfig |]

    /// Builds a Kafka Consumer Config suitable for KafkaConsumer.Start*
    static member Create
        (   /// Identify this consumer in logs etc
            clientId, broker, topics,
            /// Consumer group identifier.
            groupId,
            /// Default Earliest.
            ?autoOffsetReset,
            /// Default 100kB.
            ?fetchMaxBytes,
            /// Default 10B.
            ?fetchMinBytes,
            /// Default 60.
            ?retries,
            /// Default 1s.
            ?retryBackoff, ?statisticsInterval,
            /// Consumed offsets commit interval. Default 10s. (WAS 1s)
            ?offsetCommitInterval,
            /// Misc configuration parameters to pass to the underlying CK consumer.
            ?custom,

            (* Client side batching *)

            /// Poll timeout used by the CK consumer. Default 200ms.
            ?pollTimeout,
            /// Maximum number of messages to group per batch on consumer callbacks. Default 1000.
            ?maxBatchSize,
            /// Message batch linger time. Default 500ms.
            ?maxBatchDelay,
            /// Minimum total size of consumed messages in-memory for the consumer to attempt to fill. Default 16MiB.
            ?minInFlightBytes,
            /// Maximum total size of consumed messages in-memory before broker polling is throttled. Default 24MiB.
            ?maxInFlightBytes) =
        {   clientId = clientId; broker = broker; topics = match Seq.toList topics with [] -> invalidArg "topics" "must be non-empty collection" | ts -> ts
            groupId = groupId
            autoOffsetReset = defaultArg autoOffsetReset Config.Consumer.AutoOffsetReset.Earliest
            fetchMaxBytes = defaultArg fetchMaxBytes 100000
            fetchMinBytes = defaultArg fetchMinBytes 10 // TODO check if sane default
            retries = defaultArg retries 60
            retryBackoff = defaultArg retryBackoff (TimeSpan.FromSeconds 1.)
            statisticsInterval = statisticsInterval
            offsetCommitInterval = defaultArg offsetCommitInterval (TimeSpan.FromSeconds 10.)
            miscConfig = match custom with None -> [] | Some c -> Seq.toList c
            
            pollTimeout = defaultArg pollTimeout (TimeSpan.FromMilliseconds 200.)
            maxBatchSize = defaultArg maxBatchSize 1000
            maxBatchDelay = defaultArg maxBatchDelay (TimeSpan.FromMilliseconds 500.)
            minInFlightBytes = defaultArg minInFlightBytes (16L * 1024L * 1024L)
            maxInFlightBytes = defaultArg maxInFlightBytes (24L * 1024L * 1024L) }

type KafkaConsumer private (config : KafkaConsumerConfig, consumer : Consumer<string, string>, task : Task<unit>, cts : CancellationTokenSource) =

    /// https://github.com/edenhill/librdkafka/wiki/Statistics
    [<CLIEvent>]
    member __.OnStatistics = consumer.OnStatistics
    member __.Config = config
    member __.Status = task.Status
    /// Asynchronously awaits until consumer stops or is faulted
    member __.AwaitConsumer() = Async.AwaitTaskCorrect task
    member __.Stop() = cts.Cancel() ; task.Result

    interface IDisposable with member __.Dispose() = __.Stop()

    /// Starts a kafka consumer with provider configuration and batch message handler.
    /// Batches are grouped by topic partition. Batches belonging to the same topic partition will be scheduled sequentially and monotonically,
    /// however batches from different partitions can be run concurrently.
    static member Start (log : ILogger) (config : KafkaConsumerConfig) (partitionHandler : KafkaMessage[] -> Async<unit>) =
        if List.isEmpty config.topics then invalidArg "config" "must specify at least one topic"
        log.Information("Starting Kafka consumer on topics={topics} groupId={groupId} broker={broker} autoOffsetReset={autoOffsetRead} fetchMaxBytes={fetchMaxB} maxInFlightBytes={maxInFlightB} maxBatchSize={maxBatchB} maxBatchDelay={maxBatchDelay}",
                        config.topics, config.groupId, config.broker, config.autoOffsetReset, config.fetchMaxBytes,
                        config.maxInFlightBytes, config.maxBatchSize, config.maxBatchDelay)

        let cts = new CancellationTokenSource()
        let consumer = ConsumerImpl.mkConsumer (config.ToKeyValuePairs(), config.topics) (mkDeserializer()) (mkDeserializer())
        let cfg = config.minInFlightBytes, config.maxInFlightBytes, config.maxBatchSize, config.maxBatchDelay, config.pollTimeout
        let task = ConsumerImpl.mkBatchedMessageConsumer log cfg cts.Token consumer partitionHandler |> Async.StartAsTask

        new KafkaConsumer(config, consumer, task, cts)

    /// Starts a Kafka consumer instance that schedules handlers grouped by message key. Additionally accepts a global degreeOfParallelism parameter
    /// that controls the number of handlers running concurrently across partitions for the given consumer instance.
    static member StartByKey (log: ILogger) (config : KafkaConsumerConfig) (degreeOfParallelism : int) (keyHandler : KafkaMessage [] -> Async<unit>) =
        let semaphore = new SemaphoreSlim(degreeOfParallelism)
        let partitionHandler (messages : KafkaMessage[]) = async {
            return!
                messages
                |> Seq.groupBy (fun m -> m.Key)
                |> Seq.map (fun (_,gp) -> async { 
                    let! ct = Async.CancellationToken
                    let! _ = semaphore.WaitAsync ct |> Async.AwaitTaskCorrect
                    try do! keyHandler (Seq.toArray gp)
                    finally semaphore.Release() |> ignore })
                |> Async.Parallel
                |> Async.Ignore
        }

        KafkaConsumer.Start log config partitionHandler