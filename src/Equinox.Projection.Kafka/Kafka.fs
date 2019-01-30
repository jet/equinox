namespace Equinox.Projection.Kafka

open Confluent.Kafka
open Serilog
open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks

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
            | obj -> KeyValuePair(id, string obj)

    let private mkKey id render = Key(id, render >> box)
    let private ms (t : TimeSpan) = int t.TotalMilliseconds
    let validateBrokerUri (u:Uri) =
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
        let messageSendRetries  = mkKey "message.send.max.retries" id<int>
        let partitioner         = mkKey "partitioner" (function | Random -> "random" | Consistent -> "consistent" | ConsistentRandom -> "consistent_random")
        let queueBufferingMaxInterval = mkKey "queue.buffering.max.ms" ms
        let retries             = mkKey "retries" id<int>
        let retryBackoff        = mkKey "retry.backoff.ms" ms
        let timeout             = mkKey "request.timeout.ms" ms

// NB we deliberately wrap all types exposed by Confluent.Kafka to avoid needing to reference librdkafka everywhere
[<NoComparison>]
type KafkaDeliveredMessage internal (message : DeliveryReportResult<string,string>) =
    member internal __.UnderlyingMessage = message
    static member TryWrap (msg : DeliveryReportResult<string,string>) =
        if msg.Error.IsError then
            let errorMsg = sprintf "Error on message topic=%s code=%O reason=%s" msg.Topic msg.Error.Code msg.Error.Reason
            Choice2Of2 (Exception errorMsg)
        else
            Choice1Of2 (KafkaDeliveredMessage msg)
    static member WrapOrThrow msg = match KafkaDeliveredMessage.TryWrap msg with Choice2Of2 e -> raise e | Choice1Of2 m -> m
    member __.Topic = message.Topic
    member __.Partition : int = let p = message.Partition in p.Value
    member __.Offset : int64 = let o = message.Offset in o.Value
    member __.Key : string = message.Key
    member __.Value : string = message.Value

// NB we deliberately wrap all types exposed by Confluent.Kafka to avoid needing to reference librdkafka everywhere
[<NoComparison>]
type KafkaConsumerMessage internal (message : ConsumeResult<string,string>) =
    member internal __.UnderlyingMessage = message
    member __.Topic = message.Topic
    member __.Partition : int = let p = message.Partition in p.Value
    member __.Offset : int64 = let o = message.Offset in o.Value
    member __.Key : string = message.Key
    member __.Value : string = message.Value

[<NoComparison>]
type KafkaProducerConfig(kvps, broker : Uri, compression : Config.Producer.Compression, acks : Config.Producer.Acks) =
    member val Kvps : seq<KeyValuePair<string,string>> = kvps
    member val Acks = acks
    member val Broker = broker
    member val Compression = compression

    /// Creates a Kafka producer instance with supplied configuration
    static member Create
        (   clientId : string, broker : Uri, 
            /// Message compression. Defaults to 'none'.
            ?compression,
            /// Maximum in-flight requests. Default 1000000.
            /// Set to 1 to prevent potential reordering of writes should a batch fail and then succeed in a subsequent retry
            ?maxInFlight,
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
                yield Config.maxInFlight ==> defaultArg maxInFlight 1000000
                yield Config.socketKeepAlive ==> defaultArg socketKeepAlive true
                yield Config.Producer.retries ==> defaultArg retries 60
                yield Config.Producer.retryBackoff ==> defaultArg retryBackoff (TimeSpan.FromSeconds 1.)
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

    /// https://github.com/edenhill/librdkafka/wiki/Statistics
    [<CLIEvent>]
    member __.OnStatistics = producer.OnStatistics
    member __.Topic = topic

    /// Produces a batch of supplied key/value messages. Results are returned in order of writing.  
    member __.ProduceBatch(keyValueBatch : (string * string)[]) = async {
        if Array.isEmpty keyValueBatch then return [||] else

        let! ct = Async.CancellationToken

        let tcs = new TaskCompletionSource<KafkaDeliveredMessage[]>()
        let numMessages = keyValueBatch.Length
        let results = Array.zeroCreate<KafkaDeliveredMessage> numMessages
        let numCompleted = ref 0

        use _ = ct.Register(fun _ -> tcs.TrySetCanceled() |> ignore)

        let handler (m : DeliveryReportResult<string,string>) =
            match KafkaDeliveredMessage.TryWrap m with
            | Choice2Of2 e ->
                tcs.TrySetException e |> ignore
            | Choice1Of2 m ->
                let i = Interlocked.Increment numCompleted
                results.[i - 1] <- m
                if i = numMessages then tcs.TrySetResult results |> ignore 
        for key,value in keyValueBatch do
            producer.BeginProduce(topic, Message<_,_>(Key=key, Value=value), deliveryHandler = handler)
        producer.Flush(ct)
        log.Information("Produced {count}",!numCompleted)
        return! Async.AwaitTaskCorrect tcs.Task }

    interface IDisposable with member __.Dispose() = for d in [d1;d2;producer:>_] do d.Dispose()

    static member Create(log : ILogger, config : KafkaProducerConfig, topic : string) =
        if String.IsNullOrEmpty topic then nullArg "topic"
        log.Information("Starting Kafka Producer on topic={topic} broker={broker} compression={compression} acks={acks}",
                          topic, config.Broker, config.Compression, config.Acks)
        let producer = new Producer<string, string>(config.Kvps)
        new KafkaProducer(log, producer, topic)
        
 module private ConsumerImpl =

    /// used for calculating approximate message size in bytes
    let getMessageSize (message : ConsumeResult<string, string>) =
        let inline len (x:string) = match x with null -> 0 | x -> sizeof<char> * x.Length
        16 + len message.Key + len message.Value |> int64

    let getBatchOffset (batch : KafkaConsumerMessage[]) : TopicPartitionOffset =
        let maxOffset = batch |> Array.maxBy (fun m -> m.Offset)
        maxOffset.UnderlyingMessage.TopicPartitionOffset

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
        member c.StoreOffset(tpo : TopicPartitionOffset) : unit =
            c.StoreOffsets[| TopicPartitionOffset(tpo.Topic, tpo.Partition, Offset(let o = tpo.Offset in o.Value + 1L)) |]

        member c.RunPoll(log : ILogger, counter : InFlightMessageCounter, onMessage: ConsumeResult<'Key,'Value> -> unit) =
            let cts = new CancellationTokenSource()

            let poll() = 
                while not cts.IsCancellationRequested do
                    counter.AwaitThreshold()
                    try let message = c.Consume(cts.Token) // NB don't use TimeSpan overload unless you want GPFs on1.0.0-beta2
                        if message <> null then
                            onMessage message
                    with :? ConsumeException as e -> log.Warning(e, "Consume exception")

            let _ = Async.StartAsTask(async { poll() })
            { new IDisposable with member __.Dispose() = cts.Cancel() }

        member c.WithLogging(log: ILogger) =
            let d1 = c.OnLog.Subscribe(fun m ->
                log.Information("consumer_info|{message} level={level} name={name} facility={facility}", m.Message, m.Level, m.Name, m.Facility))
            let d2 = c.OnError.Subscribe(fun e ->
                log.Error("consumer_error| reason={reason} code={code} broker={isBrokerError}", e.Reason, e.Code, e.IsBrokerError))
            let fmtTopicPartitions (topicPartitions : seq<TopicPartition>) =
                topicPartitions 
                |> Seq.groupBy (fun p -> p.Topic)
                |> Seq.map (fun (t,ps) -> t, [| for p in ps -> let p = p.Partition in p.Value |])
            let d3 = c.OnPartitionsAssigned.Subscribe(fun tps ->
                for topic,partitions in fmtTopicPartitions tps do log.Information("consumer_partitions_assigned|{topic:l} {partitions}", topic, partitions))
            let d4 = c.OnPartitionsRevoked.Subscribe(fun tps ->
                for topic,partitions in fmtTopicPartitions tps do log.Information("consumer_partitions_revoked|{topic:l} {partitions}", topic, partitions))
            let d5 = c.OnPartitionEOF.Subscribe(fun tpo ->
                log.Verbose("consumer_partition_eof|topic={topic}|partition={partition}|offset={offset}", tpo.Topic, tpo.Partition, let o = tpo.Offset in o.Value))
            let fmtError (e : Error) = if e.IsError then sprintf " reason=%s code=%O isBrokerError=%b" e.Reason e.Code e.IsBrokerError else ""
            let fmtTopicPartitionOffsetErrors (topicPartitionOffsetErrors : seq<TopicPartitionOffsetError>) =
                topicPartitionOffsetErrors
                |> Seq.groupBy (fun p -> p.Topic)
                |> Seq.map (fun (t,ps) ->
                    let o = (ps |> Seq.map (fun p -> sprintf "%d@%O%s" (let p = p.Partition in p.Value) p.Offset (fmtError p.Error)) |> String.concat "; ")
                    sprintf "topic=%s|offsets=[%s]" t o)
            let d6 = c.OnOffsetsCommitted.Subscribe(fun cos ->
                for fmt in fmtTopicPartitionOffsetErrors cos.Offsets do log.Information("consumer_committed_offsets|{offsets}{oe}", fmt, fmtError cos.Error))
            { new IDisposable with member __.Dispose() = for d in [d1;d2;d3;d4;d5;d6] do d.Dispose() }

    let mkConsumer (kvps : seq<KeyValuePair<string,string>>, topics : string seq)  =
        let consumer = new Consumer<'Key, 'Value>(kvps)
        let _ = consumer.OnPartitionsAssigned.Subscribe(fun m -> consumer.Assign m)
        let _ = consumer.OnPartitionsRevoked.Subscribe(fun _ -> consumer.Unassign())
        consumer.Subscribe topics
        consumer

    let mkBatchedMessageConsumer (log: ILogger) (minInFlightBytes, maxInFlightBytes, maxBatchSize, maxBatchDelay)
            (ct : CancellationToken) (consumer : Consumer<string, string>) (handler : KafkaConsumerMessage[] -> Async<unit>) = async {
        let tcs = new TaskCompletionSource<unit>()
        use cts = CancellationTokenSource.CreateLinkedTokenSource(ct)
        use _ = ct.Register(fun _ -> tcs.TrySetResult () |> ignore)

        use _ = consumer
        use _ = consumer.WithLogging(log)
        
        let counter = new InFlightMessageCounter(log, minInFlightBytes, maxInFlightBytes)
        let partitionedCollection = new PartitionedBlockingCollection<TopicPartition, ConsumeResult<string, string>>()

        // starts a tail recursive loop which dequeues batches for a given partition buffer and schedules the user callback
        let consumePartition (_key : TopicPartition) (collection : BlockingCollection<ConsumeResult<string, string>>) =
            let buffer = Array.zeroCreate maxBatchSize
            let nextBatch () =
                let count = collection.FillBuffer(buffer, maxBatchDelay)
                if count <> 0 then log.Information("Consuming {count}", count)
                let batch = Array.init count (fun i -> KafkaConsumerMessage(buffer.[i]))
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

        // run the consumer
        use _ = consumer.RunPoll(log, counter, (fun m -> counter.Add(getMessageSize m) ; partitionedCollection.Add(m.TopicPartition, m)))

        // await for handler faults or external cancellation
        do! Async.AwaitTaskCorrect tcs.Task
    }

[<NoComparison>]
type KafkaConsumerConfig =
    {   conf: ConsumerConfig
        topics: string list

        (* Local Batching configuration *)

        minInFlightBytes : int64; maxInFlightBytes : int64
        maxBatchSize : int; maxBatchDelay : TimeSpan }

    /// Builds a Kafka Consumer Config suitable for KafkaConsumer.Start*
    static member Create
        (   /// Identify this consumer in logs etc
            clientId, broker : Uri, topics,
            /// Consumer group identifier.
            groupId,
            /// Default Earliest.
            ?autoOffsetReset,
            /// Default 100kB.
            ?fetchMaxBytes,
            /// Default 10B.
            ?fetchMinBytes,
            ?statisticsInterval,
            /// Consumed offsets commit interval. Default 10s. (WAS 1s)
            ?offsetCommitInterval,

            (* Client side batching *)

            /// Maximum number of messages to group per batch on consumer callbacks. Default 1000.
            ?maxBatchSize,
            /// Message batch linger time. Default 500ms.
            ?maxBatchDelay,
            /// Minimum total size of consumed messages in-memory for the consumer to attempt to fill. Default 16MiB.
            ?minInFlightBytes,
            /// Maximum total size of consumed messages in-memory before broker polling is throttled. Default 24MiB.
            ?maxInFlightBytes) =
        let conf =
            ConsumerConfig(
                ClientId=clientId, BootstrapServers=Config.validateBrokerUri broker, GroupId=groupId,
                AutoOffsetReset = Nullable (defaultArg autoOffsetReset AutoOffsetResetType.Earliest),
                FetchMaxBytes = Nullable (defaultArg fetchMaxBytes 100000),
                MessageMaxBytes = Nullable (defaultArg fetchMaxBytes 100000),
                FetchMinBytes = Nullable (defaultArg fetchMinBytes 10),  // TODO check if sane default
                EnableAutoCommit = Nullable true,
                AutoCommitIntervalMs = Nullable (match offsetCommitInterval with Some (i: TimeSpan) -> int i.TotalMilliseconds | None -> 1000*10),
                EnableAutoOffsetStore = Nullable false,
                LogConnectionClose = Nullable false) // https://github.com/confluentinc/confluent-kafka-dotnet/issues/124#issuecomment-289727017
        statisticsInterval |> Option.iter (fun (i : TimeSpan) -> conf.StatisticsIntervalMs <- Nullable (int i.TotalMilliseconds))
        {   conf = conf; topics = match Seq.toList topics with [] -> invalidArg "topics" "must be non-empty collection" | ts -> ts
            maxBatchSize = defaultArg maxBatchSize 1000
            maxBatchDelay = defaultArg maxBatchDelay (TimeSpan.FromMilliseconds 500.)
            minInFlightBytes = defaultArg minInFlightBytes (16L * 1024L * 1024L)
            maxInFlightBytes = defaultArg maxInFlightBytes (24L * 1024L * 1024L) }

type KafkaConsumer private (consumer : Consumer<string, string>, task : Task<unit>, cts : CancellationTokenSource) =

    /// https://github.com/edenhill/librdkafka/wiki/Statistics
    [<CLIEvent>]
    member __.OnStatistics = consumer.OnStatistics
    member __.Status = task.Status
    /// Asynchronously awaits until consumer stops or is faulted
    member __.AwaitConsumer() = Async.AwaitTaskCorrect task
    member __.Stop() = cts.Cancel() ; task.Result

    interface IDisposable with member __.Dispose() = __.Stop()

    /// Starts a kafka consumer with provider configuration and batch message handler.
    /// Batches are grouped by topic partition. Batches belonging to the same topic partition will be scheduled sequentially and monotonically,
    /// however batches from different partitions can be run concurrently.
    static member Start (log : ILogger) (config : KafkaConsumerConfig) (partitionHandler : KafkaConsumerMessage[] -> Async<unit>) =
        if List.isEmpty config.topics then invalidArg "config" "must specify at least one topic"
        log.Information("Starting Kafka consumer on broker={broker} topics={topics} groupId={groupId} " (*autoOffsetReset={autoOffsetReset}*) + "fetchMaxBytes={fetchMaxB} maxInFlightBytes={maxInFlightB} maxBatchSize={maxBatchB} maxBatchDelay={maxBatchDelay}",
            config.conf.BootstrapServers, config.topics, config.conf.GroupId, (*config.conf.AutoOffsetReset.Value,*) config.conf.FetchMaxBytes,
            config.maxInFlightBytes, config.maxBatchSize, config.maxBatchDelay)

        let cts = new CancellationTokenSource()
        let consumer = ConsumerImpl.mkConsumer (config.conf, config.topics)
        let cfg = config.minInFlightBytes, config.maxInFlightBytes, config.maxBatchSize, config.maxBatchDelay
        let task = ConsumerImpl.mkBatchedMessageConsumer log cfg cts.Token consumer partitionHandler |> Async.StartAsTask
        new KafkaConsumer(consumer, task, cts)

    /// Starts a Kafka consumer instance that schedules handlers grouped by message key. Additionally accepts a global degreeOfParallelism parameter
    /// that controls the number of handlers running concurrently across partitions for the given consumer instance.
    static member StartByKey (log: ILogger) (config : KafkaConsumerConfig) (degreeOfParallelism : int) (keyHandler : KafkaConsumerMessage [] -> Async<unit>) =
        let semaphore = new SemaphoreSlim(degreeOfParallelism)
        let partitionHandler (messages : KafkaConsumerMessage[]) = async {
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