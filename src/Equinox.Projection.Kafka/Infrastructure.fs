namespace Equinox.Projection.Kafka.Infrastructure

open System
open System.Collections.Generic
open System.Collections.Concurrent
open System.Threading
open System.Threading.Tasks

open NLog
open Confluent.Kafka

[<AutoOpen>]
module private Helpers =

    let logger = LogManager.GetLogger __SOURCE_FILE__
    let encoding = System.Text.Encoding.UTF8
    let mkSerializer() = new Confluent.Kafka.Serialization.StringSerializer(encoding)
    let mkDeserializer() = new Confluent.Kafka.Serialization.StringDeserializer(encoding)

    type Logger with
        static member Create name = LogManager.GetLogger name

        member inline ts.log (format, level) =
            let inline trace (message:string) = ts.Log(level, message)
            Printf.kprintf trace format

        member inline ts.log (message:string, level) = ts.Log(level, message)
        member inline ts.info format = ts.log (format, LogLevel.Info)
        member inline ts.warn format = ts.log (format, LogLevel.Warn)
        member inline ts.error format = ts.log (format, LogLevel.Error)
        member inline ts.verbose format = ts.log (format, LogLevel.Debug)
        member inline ts.trace format = ts.log (format, LogLevel.Trace)
        member inline ts.critical format = ts.log (format, LogLevel.Fatal)

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

[<RequireQualifiedAccess>]
type Acks =
    | Zero
    | One
    | All

[<RequireQualifiedAccess>]
type Compression =
    | None
    | GZip
    | Snappy
    | LZ4

[<RequireQualifiedAccess>]
type AutoOffsetReset =
    | Earliest
    | Latest
    | None

[<RequireQualifiedAccess>]
type Partitioner =
    | Random
    | Consistent
    | ConsistentRandom

type KafkaConfiguration = KeyValuePair<string, obj>

type ConfigKey<'T> = Key of id:string * render:('T -> obj)
with
    member k.Id = let (Key(id,_)) = k in id

    static member (==>) (Key(id, render) : ConfigKey<'T>, value : 'T) = 
        match render value with
        | null -> nullArg id
        | :? string as str when String.IsNullOrWhiteSpace str -> nullArg id
        | obj -> KeyValuePair(id, obj)

[<RequireQualifiedAccess>]
module Config =

    // A small DSL for interfacing with Kafka configuration
    // c.f. https://kafka.apache.org/documentation/#configuration

    let private mkKey id render = Key(id, render >> box)
    let private ms (t : TimeSpan) = int t.TotalMilliseconds
    let private validateBrokerUri (u:Uri) =
        if not u.IsAbsoluteUri then invalidArg "broker" "should be of 'host:port' format"
        if String.IsNullOrEmpty u.Authority then 
            // handle a corner case in which Uri instances are erroneously putting the hostname in the `scheme` field.
            if System.Text.RegularExpressions.Regex.IsMatch(string u, "^\S+:[0-9]+$") then string u
            else invalidArg "broker" "should be of 'host:port' format"

        else u.Authority

    // shared keys
    let broker = mkKey "bootstrap.servers" validateBrokerUri
    let clientId = mkKey "client.id" id<string>
    let maxInFlight = mkKey "max.in.flight.requests.per.connection" id<int>
    let logConnectionClose = mkKey "log.connection.close" id<bool>
    let apiVersionRequest = mkKey "api.version.request" id<bool>
    let brokerVersionFallback = mkKey "broker.version.fallback" id<string>
    let rebalanceTimeout = mkKey "rebalance.timeout.ms" ms

    // producers
    let linger = mkKey "linger.ms" ms
    let acks = mkKey "acks" (function Acks.Zero -> 0 | Acks.One -> 1 | Acks.All -> -1)
    let batchNumMessages = mkKey "batch.num.messages" id<int>
    let retries = mkKey "retries" id<int>
    let retryBackoff = mkKey "retry.backoff.ms" ms
    let timeout = mkKey "request.timeout.ms" ms
    let messageSendMaxRetries = mkKey "message.send.max.retries" id<int>
    let statisticsInterval = mkKey "statistics.interval.ms" ms
    let compression = 
        mkKey "compression.type"
            (function
            | Compression.None -> "none"
            | Compression.GZip -> "gzip"
            | Compression.Snappy -> "snappy"
            | Compression.LZ4 -> "lz4")

    let partitioner =
        mkKey "partitioner"
            (function
            | Partitioner.Random -> "random"
            | Partitioner.Consistent -> "consistent"
            | Partitioner.ConsistentRandom -> "consistent_random")

    // consumers
    let enableAutoCommit = mkKey "enable.auto.commit" id<bool>
    let enableAutoOffsetStore = mkKey "enable.auto.offset.store" id<bool>
    let autoCommitInterval = mkKey "auto.commit.interval.ms" ms
    let groupId = mkKey "group.id" id<string>
    let fetchMaxBytes = mkKey "fetch.message.max.bytes" id<int>
    let fetchMinBytes = mkKey "fetch.min.bytes" id<int>
    let fetchMaxWait = mkKey "fetch.wait.max.ms" ms
    let checkCrc = mkKey "check.crcs" id<bool>
    let heartbeatInterval = mkKey "heartbeat.interval.ms" ms
    let sessionTimeout = mkKey "session.timeout.ms" ms
    let topicConfig = mkKey "default.topic.config" id : ConfigKey<seq<KafkaConfiguration>>
    let socketKeepAlive = mkKey "socket.keepalive.enable" id<bool>
    let socketBlockingMaxInterval = mkKey "socket.blocking.max.ms" ms
    let queueBufferingMaxInterval = mkKey "queue.buffering.max.ms" ms
    let autoOffsetReset = 
        mkKey "auto.offset.reset"
            (function
            | AutoOffsetReset.Earliest -> "earliest"
            | AutoOffsetReset.Latest -> "latest"
            | AutoOffsetReset.None -> "none")


// NB we deliberately wrap all types exposed by Confluent.Kafka
// to avoid needing to reference librdkafka everywehere
[<Struct>]
type KafkaMessage internal (message : Message<string, string>) =
    member internal __.UnderlyingMessage = message
    member __.Topic = message.Topic
    member __.Partition = message.Partition
    member __.Offset = message.Offset.Value
    member __.Key = message.Key
    member __.Value = message.Value

type KafkaProducer private (producer : Producer<string, string>, topic : string) =

    let d1 = producer.OnLog.Subscribe (fun m -> logger.info "producer_info|%s level=%d name=%s facility=%s" m.Message m.Level m.Name m.Facility)
    let d2 = producer.OnError.Subscribe (fun e -> logger.error "producer_error|%s code=%O isBrokerError=%b" e.Reason e.Code e.IsBrokerError)

    let tryWrap (msg : Message<string, string>) =
        if msg.Error.HasError then
            let errorMsg = sprintf "Error writing message topic=%s code=%O reason=%s" topic msg.Error.Code msg.Error.Reason
            Choice2Of2(Exception errorMsg)
        else
            Choice1Of2(KafkaMessage msg)

    /// https://github.com/edenhill/librdkafka/wiki/Statistics
    [<CLIEvent>]
    member __.OnStatistics = producer.OnStatistics
    member __.Topic = topic
    member __.Produce(key : string, value : string) = async {
        let! msg = 
            producer.ProduceAsync(topic, key, value) 
            |> Async.AwaitTaskCorrect

        match tryWrap msg with
        | Choice1Of2 msg -> return msg
        | Choice2Of2 e -> return raise e
    }

    /// <summary>
    ///   Produces a batch of supplied key/value messages. Results are returned in order of writing.  
    /// </summary>
    /// <param name="keyValueBatch"></param>
    /// <param name="marshalDeliveryReportData">Enable marshalling of data in returned messages. Defaults to false.</param>
    member __.ProduceBatch(keyValueBatch : seq<string * string>, ?marshalDeliveryReportData : bool) = async {
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


        do for key,value in keyValueBatch do
            producer.ProduceAsync(topic, key, value, blockIfQueueFull = true, deliveryHandler = handler)


        return! Async.AwaitTaskCorrect tcs.Task
    }

    interface IDisposable with member __.Dispose() = for d in [d1;d2;producer:>_] do d.Dispose()

    /// <summary>
    ///     Creates a Kafka producer instance with supplied configuration
    /// </summary>
    /// <param name="broker"></param>
    /// <param name="topic"></param>
    /// <param name="compression">Message compression. Defaults to 'none'.</param>
    /// <param name="retries">Number of retries. Defaults to 60.</param>
    /// <param name="retryBackoff">Backoff interval. Defaults to 1 second.</param>
    /// <param name="statisticsInterval">Statistics Interval. Defaults to no stats.</param>
    /// <param name="queueBufferingMaxInterval">queueBufferingMaxInterval. Defaults to 10 ms.</param>
    /// <param name="socketBlockingMaxInterval">socketBlockingMaxInterval. Defaults to 1 ms.</param>
    /// <param name="socketKeepAlive">socketKeepAlive. Defaults to true.</param>
    /// <param name="linger">Linger time. Defaults to 200 milliseconds.</param>
    /// <param name="acks">Acks setting. Defaults to 'One'.</param>
    /// <param name="partitioner">Partitioner setting. Defaults to 'consistent_random'.</param>
    /// <param name="kafunkCompatibility">A magical flag that attempts to provide compatibility for Kafunk consumers. Defalts to false.</param>
    /// <param name="miscConfig">Misc configuration parameter to be passed to the underlying CK producer.</param>
    static member Create(clientId : string, broker : Uri, topic : string, 
                            ?compression, ?retries, ?retryBackoff, ?statisticsInterval,
                            ?queueBufferingMaxInterval, ?socketBlockingMaxInterval, ?socketKeepAlive,
                            ?linger, ?acks, ?partitioner, ?kafunkCompatibility : bool, ?miscConfig) =
        if String.IsNullOrEmpty topic then nullArg "topic"

        let linger = defaultArg linger (TimeSpan.FromMilliseconds 200.)
        let compression = defaultArg compression Compression.None
        let retries = defaultArg retries 60
        let retryBackoff = defaultArg retryBackoff (TimeSpan.FromSeconds 1.)
        let partitioner = defaultArg partitioner Partitioner.ConsistentRandom
        let acks = defaultArg acks Acks.One
        let miscConfig = defaultArg miscConfig [||]
        let kafunkCompatibility = defaultArg kafunkCompatibility false
        let socketKeepAlive = defaultArg socketKeepAlive true
        let queueBufferingMaxInterval = defaultArg queueBufferingMaxInterval (TimeSpan.FromMilliseconds 10.)
        let socketBlockingMaxInterval = defaultArg socketBlockingMaxInterval (TimeSpan.FromMilliseconds 1.)

        let config =
            [|
                yield Config.clientId ==> clientId
                yield Config.broker ==> broker

                match statisticsInterval with Some t -> yield Config.statisticsInterval ==> t | None -> ()
                yield Config.linger ==> linger
                yield Config.compression ==> compression
                yield Config.partitioner ==> partitioner
                yield Config.retries ==> retries
                yield Config.retryBackoff ==> retryBackoff
                yield Config.acks ==> acks

                yield Config.socketKeepAlive ==> socketKeepAlive
                yield Config.queueBufferingMaxInterval ==> queueBufferingMaxInterval
                yield Config.socketBlockingMaxInterval ==> socketBlockingMaxInterval

                if kafunkCompatibility then
                    yield Config.apiVersionRequest ==> false
                    yield Config.brokerVersionFallback ==> "0.9.0"

                // https://github.com/confluentinc/confluent-kafka-dotnet/issues/124#issuecomment-289727017
                yield Config.logConnectionClose ==> false

                yield! miscConfig
            |]

        logger.info "Starting Kafka Producer on topic=%s broker=%O compression=%O acks=%O" topic broker compression acks
        let producer = new Producer<string, string>(config, mkSerializer(), mkSerializer())
        new KafkaProducer(producer, topic)

type KafkaConsumerConfig =
    {
        /// Client identifier; should typically be superhero name
        clientId : string
        broker : Uri
        topics : string list
        /// Consumer group identifier
        groupId : string

        autoOffsetReset : AutoOffsetReset
        fetchMinBytes : int
        fetchMaxBytes : int
        retryBackoff : TimeSpan
        socketBlockingMaxInterval : TimeSpan
        retries : int
        statisticsInterval : TimeSpan option

        miscConfig : KafkaConfiguration list

        /// Poll timeout used by the Confluent.Kafka consumer
        pollTimeout : TimeSpan
        /// Maximum number of messages to group per batch on consumer callbacks
        maxBatchSize : int
        /// Message batch linger time
        maxBatchDelay : TimeSpan
        /// Minimum total size of consumed messages in-memory for the consumer to attempt to fill.
        minInFlightBytes : int64
        /// Maximum total size of consumed messages in-memory before broker polling is throttled.
        maxInFlightBytes : int64
        /// Consumed offsets commit interval
        offsetCommitInterval : TimeSpan
    }
with
    /// <summary>
    ///     Creates a Kafka consumer configuration object.
    /// </summary>
    /// <param name="clientId">Should typically be superhero name.</param>
    /// <param name="broker"></param>
    /// <param name="topics"></param>
    /// <param name="groupId">Consumer group id.</param>
    /// <param name="autoOffsetReset">Auto offset reset. Defaults to Earliest.</param>
    /// <param name="fetchMaxBytes">Fetch max bytes. Defaults to 100KB.</param>
    /// <param name="fetchMinBytes">Fetch min bytes. Defaults to 10B.</param>
    /// <param name="socketBlockingMaxInterval">Max socket blocking interval. Defaults to 1 millsecond.</param>
    /// <param name="retries">Max retries. Defaults to 60.</param>
    /// <param name="retryBackoff">Retry backoff interval. Defaults to 1 second.</param>
    /// <param name="pollTimeout">Poll timeout used by the CK consumer. Defaults to 200ms.</param>
    /// <param name="maxBatchSize">Max number of messages to use in batched consumers. Defaults to 1000.</param>
    /// <param name="maxBatchDelay">Max batch linger time. Defaults to 500 milliseconds.</param>
    /// <param name="offsetCommitInterval">Offset commit interval. Defaults to 1 second.</param>
    /// <param name="maxInFlightBytes">Minimum total size of consumed messages in-memory for the consumer to attempt to fill. Defaults to 16MiB.</param>
    /// <param name="maxInFlightBytes">Maximum total size of consumed messages in-memory before broker polling is throttled. Defaults to 24MiB.</param>
    /// <param name="miscConfiguration">Misc configuration parameters to pass to the underlying CK consumer.</param>
    static member Create(clientId, broker, topics, groupId,
                            ?autoOffsetReset, ?fetchMaxBytes, ?fetchMinBytes, ?socketBlockingMaxInterval,
                            ?retries, ?retryBackoff, ?statisticsInterval,
                            ?pollTimeout, ?maxBatchSize, ?maxBatchDelay,
                            ?offsetCommitInterval, ?minInFlightBytes, ?maxInFlightBytes,
                            ?miscConfiguration) =
        {
            clientId = clientId
            broker = broker
            topics = 
                match Seq.toList topics with
                | [] -> invalidArg "topics" "must be non-empty collection"
                | ts -> ts

            groupId = groupId

            miscConfig = match miscConfiguration with None -> [] | Some c -> Seq.toList c

            autoOffsetReset = defaultArg autoOffsetReset AutoOffsetReset.Earliest
            fetchMaxBytes = defaultArg fetchMaxBytes 100000
            fetchMinBytes = defaultArg fetchMinBytes 10 // TODO check if sane default
            retries = defaultArg retries 60
            retryBackoff = defaultArg retryBackoff (TimeSpan.FromSeconds 1.)
            statisticsInterval = statisticsInterval

            pollTimeout = defaultArg pollTimeout (TimeSpan.FromMilliseconds 200.)
            maxBatchSize = defaultArg maxBatchSize 1000
            maxBatchDelay = defaultArg maxBatchDelay (TimeSpan.FromSeconds 0.5)
            minInFlightBytes = defaultArg minInFlightBytes (16L * 1024L * 1024L)
            maxInFlightBytes = defaultArg maxInFlightBytes (24L * 1024L * 1024L)
            offsetCommitInterval = defaultArg offsetCommitInterval (TimeSpan.FromSeconds 10.)
            socketBlockingMaxInterval = defaultArg socketBlockingMaxInterval (TimeSpan.FromMilliseconds 1.)
        }

module private ConsumerImpl =

    type KafkaConsumerConfig with
        member c.ToKeyValuePairs() =
            [|
                yield Config.clientId ==> c.clientId
                yield Config.broker ==> c.broker
                yield Config.groupId ==> c.groupId

                yield Config.fetchMaxBytes ==> c.fetchMaxBytes
                yield Config.fetchMinBytes ==> c.fetchMinBytes
                yield Config.retries ==> c.retries
                yield Config.retryBackoff ==> c.retryBackoff
                match c.statisticsInterval with None -> () | Some t -> yield Config.statisticsInterval ==> t

                yield Config.enableAutoCommit ==> true
                yield Config.enableAutoOffsetStore ==> false
                yield Config.autoCommitInterval ==> c.offsetCommitInterval

                yield Config.topicConfig ==> 
                    [
                        Config.autoOffsetReset ==> AutoOffsetReset.Earliest
                    ]

                // https://github.com/confluentinc/confluent-kafka-dotnet/issues/124#issuecomment-289727017
                yield Config.logConnectionClose ==> false

                yield Config.socketBlockingMaxInterval ==> c.socketBlockingMaxInterval

                yield! c.miscConfig
            |]


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
                if bc.TryTake(&t, 5) then
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

    type InFlightMessageCounter(minInFlightBytes : int64, maxInFlightBytes : int64) =
        do 
            if minInFlightBytes < 1L then invalidArg "minInFlightBytes" "must be positive value"
            if maxInFlightBytes < 1L then invalidArg "maxInFlightBytes" "must be positive value"
            if minInFlightBytes > maxInFlightBytes then invalidArg "maxInFlightBytes" "must be greater than minInFlightBytes"

        let mutable inFlightBytes = 0L

        member __.InFlightBytes = inFlightBytes
        member __.MinInFlightBytes = minInFlightBytes
        member __.MaxInFlightBytes = maxInFlightBytes

        member __.Add(numBytes : int64) = Interlocked.Add(&inFlightBytes, numBytes) |> ignore

        member __.AwaitThreshold() =
            if inFlightBytes > maxInFlightBytes then
                logger.warn "Consumer reached in-flight message threshold, breaking off polling, bytes=%d" inFlightBytes
                while inFlightBytes > minInFlightBytes do Thread.Sleep 5
                logger.info "Consumer resuming polling"
            

    type Consumer<'Key, 'Value> with
        static member Create (config : KafkaConsumerConfig) keyDeserializer valueDeserializer =
            let kvps = config.ToKeyValuePairs()
            let consumer = new Consumer<'Key, 'Value>(kvps, keyDeserializer, valueDeserializer)
            let _ = consumer.OnPartitionsAssigned.Subscribe(fun m -> consumer.Assign m)
            let _ = consumer.OnPartitionsRevoked.Subscribe(fun _ -> consumer.Unassign())
            consumer.Subscribe config.topics
            consumer

        member c.StoreOffset(tpo : TopicPartitionOffset) =
            c.StoreOffsets[| TopicPartitionOffset(tpo.Topic, tpo.Partition, Offset(tpo.Offset.Value + 1L)) |]
            |> ignore

        member c.RunPoll(pollTimeout : TimeSpan, counter : InFlightMessageCounter) =
            let cts = new CancellationTokenSource()
            let poll() = 
                while not cts.IsCancellationRequested do
                    counter.AwaitThreshold()
                    c.Poll(pollTimeout)

            let _ = Async.StartAsTask(async { poll() })
            { new IDisposable with member __.Dispose() = cts.Cancel() }

        member c.WithLogging() =
            let fmtError (e : Error) = if e.HasError then sprintf " reason=%s code=%O isBrokerError=%b" e.Reason e.Code e.IsBrokerError else ""
            let fmtTopicPartitions (topicPartitions : seq<TopicPartition>) =
                topicPartitions 
                |> Seq.groupBy (fun p -> p.Topic)
                |> Seq.map (fun (t,ps) -> sprintf "topic=%s|partitions=[%s]" t (ps |> Seq.map (fun p -> string p.Partition) |> String.concat "; "))

            let fmtTopicPartitionOffsetErrors (topicPartitionOffsetErrors : seq<TopicPartitionOffsetError>) =
                topicPartitionOffsetErrors
                |> Seq.groupBy (fun p -> p.Topic)
                |> Seq.map (fun (t,ps) -> sprintf "topic=%s|offsets=[%s]" t (ps |> Seq.map (fun p -> sprintf "%d@%O%s" p.Partition p.Offset (fmtError p.Error)) |> String.concat "; "))
                    
            let d1 = c.OnLog.Subscribe (fun m -> logger.info "consumer_info|%s level=%d name=%s facility=%s" m.Message m.Level m.Name m.Facility)
            let d2 = c.OnError.Subscribe (fun e -> logger.error "consumer_error|%s" (fmtError e))
            let d3 = c.OnPartitionsAssigned.Subscribe (fun tps -> for fmt in fmtTopicPartitions tps do logger.info "consumer_partitions_assigned|%s" fmt)
            let d4 = c.OnPartitionsRevoked.Subscribe (fun tps -> for fmt in fmtTopicPartitions tps do logger.info "consumer_partitions_revoked|%s" fmt)
            let d5 = c.OnPartitionEOF.Subscribe(fun tpo -> logger.verbose "consumer_partition_eof|topic=%s|partition=%d|offset=%d" tpo.Topic tpo.Partition tpo.Offset.Value)
            let d6 = c.OnOffsetsCommitted.Subscribe (fun cos -> for fmt in fmtTopicPartitionOffsetErrors cos.Offsets do logger.info "consumer_committed_offsets|%s%s" fmt (fmtError cos.Error))
            { new IDisposable with member __.Dispose() = for d in [d1;d2;d3;d4;d5;d6] do d.Dispose() }
    

    let mkBatchedMessageConsumer (config : KafkaConsumerConfig) (ct : CancellationToken) (consumer : Consumer<string, string>) (handler : KafkaMessage[] -> Async<unit>) = async {
        let tcs = new TaskCompletionSource<unit>()
        use cts = CancellationTokenSource.CreateLinkedTokenSource(ct)
        use _ = ct.Register(fun _ -> tcs.TrySetResult () |> ignore)

        use _ = consumer
        use _ = consumer.WithLogging()
        
        let counter = new InFlightMessageCounter(config.minInFlightBytes, config.maxInFlightBytes)
        let partitionedCollection = new PartitionedBlockingCollection<TopicPartition, Message<string, string>>()

        // starts a tail recursive loop which dequeues batches for a given partition buffer and schedules the user callback
        let consumePartition (_key : TopicPartition) (collection : BlockingCollection<Message<string, string>>) =
            let buffer = Array.zeroCreate config.maxBatchSize
            let nextBatch () =
                let count = collection.FillBuffer(buffer, config.maxBatchDelay)
                let batch = Array.init count (fun i -> mkMessage buffer.[i])
                Array.Clear(buffer, 0, count)
                batch

            let rec loop () = async {
                if not collection.IsCompleted then
                    try
                        match nextBatch() with
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

                    do! loop()
            }

            Async.Start(loop(), cts.Token)

        use _ = partitionedCollection.OnPartitionAdded.Subscribe (fun (key,buffer) -> consumePartition key buffer)
        use _ = consumer.OnPartitionsRevoked.Subscribe (fun ps -> for p in ps do partitionedCollection.Revoke p)
        use _ = consumer.OnMessage.Subscribe (fun m -> counter.Add(getMessageSize m) ; partitionedCollection.Add(m.TopicPartition, m))

        use _ = consumer.RunPoll(config.pollTimeout, counter) // run the consumer

        // await for handler faults or external cancellation
        do! Async.AwaitTaskCorrect tcs.Task
    }

open ConsumerImpl

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
    /// Batches are grouped by topic partition. Batches belonging to the same topic partition
    /// will be scheduled sequentially and monotonically, however batches from different partitions
    /// can be run concurrently.
    static member Start (config : KafkaConsumerConfig) (partitionHandler : KafkaMessage[] -> Async<unit>) =
        if List.isEmpty config.topics then invalidArg "config" "must specify at least one topic"
        logger.info "Starting Kafka consumer on topics=%A groupId=%s broker=%O autoOffsetReset=%O fetchMaxBytes=%d maxInFlightBytes=%d maxBatchSize=%d maxBatchDelay=%O" 
                        config.topics config.groupId config.broker config.autoOffsetReset config.fetchMaxBytes config.maxInFlightBytes config.maxBatchSize config.maxBatchDelay

        let cts = new CancellationTokenSource()
        let consumer = Consumer<string, string>.Create config (mkDeserializer()) (mkDeserializer())
        let task = 
            ConsumerImpl.mkBatchedMessageConsumer config cts.Token consumer partitionHandler
            |> Async.StartAsTask

        new KafkaConsumer(config, consumer, task, cts)


type KafkaConsumer with

    /// Starts a Kafka consumer instance which schedules handlers grouped by message key. Additionally accepts a global degreeOfParallelism parameter
    /// which controls the number of handlers running concurrently across partitions for the given consumer instance.
    static member StartByKey (config : KafkaConsumerConfig) (degreeOfParallelism : int) (keyHandler : KafkaMessage [] -> Async<unit>) =
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

        KafkaConsumer.Start config partitionHandler