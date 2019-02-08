namespace Equinox.Projection.Kafka

open Confluent.Kafka
open Serilog
open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks

module private Config =
    let validateBrokerUri (u:Uri) =
        if not u.IsAbsoluteUri then invalidArg "broker" "should be of 'host:port' format"
        if String.IsNullOrEmpty u.Authority then 
            // handle a corner case in which Uri instances are erroneously putting the hostname in the `scheme` field.
            if System.Text.RegularExpressions.Regex.IsMatch(string u, "^\S+:[0-9]+$") then string u
            else invalidArg "broker" "should be of 'host:port' format"

        else u.Authority

type Compression = Uncompressed | GZip | Snappy | LZ4 // as soon as CK provides such an Enum, this can go

/// See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md for documentation on the implications of specfic settings
[<NoComparison>]
type KafkaProducerConfig private (conf, cfgs, broker : Uri, compression : Compression, acks : Acks) =
    member val Conf : ProducerConfig = conf
    member val Acks = acks
    member val Broker = broker
    member val Compression = compression
    member __.Kvps = Seq.append conf cfgs

    /// Creates a Kafka producer instance with supplied configuration
    static member Create
        (   clientId : string, broker : Uri, acks : Acks,
            /// Message compression. Defaults to Uncompressed/'none'.
            ?compression,
            /// Maximum in-flight requests; <> 1 implies potential reordering of writes should a batch fail and then succeed in a subsequent retry. Defaults to 1.
            ?maxInFlight,
            /// Number of retries. Defaults to 60.
            ?retries,
            /// Backoff interval. Defaults to 1 second.
            ?retryBackoff,
            /// Statistics Interval. Defaults to no stats.
            ?statisticsInterval,
            /// Defaults to true.
            ?socketKeepAlive,
            /// Defaults to 10 ms.
            ?linger : TimeSpan,
            /// Defaults to 'consistent_random'.
            ?partitioner,
            /// Misc configuration parameter to be passed to the underlying CK producer.
            ?custom) =
        let compression = defaultArg compression Uncompressed
        let cfgs = seq {
            yield KeyValuePair("compression.codec", match compression with Uncompressed -> "none" | GZip -> "gzip" | Snappy -> "snappy" | LZ4 -> "lz4")
            match custom with None -> () | Some miscConfig -> yield! miscConfig  }
        let c =
            ProducerConfig(
                ClientId = clientId, BootstrapServers = Config.validateBrokerUri broker,
                RetryBackoffMs = Nullable (match retryBackoff with Some (t : TimeSpan) -> int t.TotalMilliseconds | None -> 1000),
                MessageSendMaxRetries = Nullable (defaultArg retries 60),
                Acks = Nullable acks,
                LingerMs = Nullable (match linger with Some t -> int t.TotalMilliseconds | None -> 10),
                SocketKeepaliveEnable = Nullable (defaultArg socketKeepAlive true),
                Partitioner = Nullable (defaultArg partitioner Partitioner.ConsistentRandom),
                MaxInFlight = Nullable (defaultArg maxInFlight 1000000),
                LogConnectionClose = Nullable false) // https://github.com/confluentinc/confluent-kafka-dotnet/issues/124#issuecomment-289727017
        statisticsInterval |> Option.iter (fun (i : TimeSpan) -> c.StatisticsIntervalMs <- Nullable (int i.TotalMilliseconds))
        KafkaProducerConfig(c, cfgs, broker, compression, acks)

type KafkaProducer private (log: ILogger, producer : Producer<string, string>, topic : string) =
    member __.Topic = topic

    interface IDisposable with member __.Dispose() = for d in [(*d1;d2;*)producer:>IDisposable] do d.Dispose()

    /// Produces a batch of supplied key/value messages. Results are returned in order of writing.  
    member __.ProduceBatch(keyValueBatch : (string * string)[]) = async {
        if Array.isEmpty keyValueBatch then return [||] else

        let! ct = Async.CancellationToken

        let tcs = new TaskCompletionSource<DeliveryReport<_,_>[]>()
        let numMessages = keyValueBatch.Length
        let results = Array.zeroCreate<DeliveryReport<_,_>> numMessages
        let numCompleted = ref 0

        use _ = ct.Register(fun _ -> tcs.TrySetCanceled() |> ignore)

        let handler (m : DeliveryReport<string,string>) =
            if m.Error.IsError then
                let errorMsg = exn (sprintf "Error on message topic=%s code=%O reason=%s" m.Topic m.Error.Code m.Error.Reason)
                tcs.TrySetException errorMsg |> ignore
            else
                let i = Interlocked.Increment numCompleted
                results.[i - 1] <- m
                if i = numMessages then tcs.TrySetResult results |> ignore 
        for key,value in keyValueBatch do
            producer.BeginProduce(topic, Message<_,_>(Key=key, Value=value), deliveryHandler = handler)
        producer.Flush(ct)
        log.Debug("Produced {count}",!numCompleted)
        return! Async.AwaitTaskCorrect tcs.Task }

    static member Create(log : ILogger, config : KafkaProducerConfig, topic : string) =
        if String.IsNullOrEmpty topic then nullArg "topic"
        log.Information("Producing... {broker} / {topic} compression={compression} acks={acks}", config.Broker, topic, config.Compression, config.Acks)
        let producer =
            ProducerBuilder<string, string>(config.Kvps)
                .SetLogHandler(fun _p m -> log.Information("{message} level={level} name={name} facility={facility}", m.Message, m.Level, m.Name, m.Facility))
                .SetErrorHandler(fun _p e -> log.Error("{reason} code={code} isBrokerError={isBrokerError}", e.Reason, e.Code, e.IsBrokerError))
                .Build()
        new KafkaProducer(log, producer, topic)
 
 type ConsumerBufferingConfig = { minInFlightBytes : int64; maxInFlightBytes : int64; maxBatchSize : int; maxBatchDelay : TimeSpan }
   
 module private ConsumerImpl =
    /// guesstimate approximate message size in bytes
    let approximateMessageBytes (message : ConsumeResult<string, string>) =
        let inline len (x:string) = match x with null -> 0 | x -> sizeof<char> * x.Length
        16 + len message.Key + len message.Value |> int64

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

        member __.Add(numBytes : int64) = Interlocked.Add(&inFlightBytes, numBytes) |> ignore

        member __.AwaitThreshold() =
            if inFlightBytes > maxInFlightBytes then
                log.Warning("Consumer reached in-flight message threshold, breaking off polling, bytes={max}", inFlightBytes)
                while inFlightBytes > minInFlightBytes do Thread.Sleep 5
                log.Information "Consumer resuming polling"

    let mkBatchedMessageConsumer (log: ILogger) (buf : ConsumerBufferingConfig) (ct : CancellationToken) (consumer : Consumer<string, string>)
            (partitionedCollection: PartitionedBlockingCollection<TopicPartition, ConsumeResult<string, string>>)
            (handler : ConsumeResult<string,string>[] -> Async<unit>) = async {
        let tcs = new TaskCompletionSource<unit>()
        use cts = CancellationTokenSource.CreateLinkedTokenSource(ct)
        use _ = ct.Register(fun _ -> tcs.TrySetResult () |> ignore)

        use _ = consumer
        
        let counter = new InFlightMessageCounter(log, buf.minInFlightBytes, buf.maxInFlightBytes)

        // starts a tail recursive loop that dequeues batches for a given partition buffer and schedules the user callback
        let consumePartition (collection : BlockingCollection<ConsumeResult<string, string>>) =
            let buffer = Array.zeroCreate buf.maxBatchSize
            let nextBatch () =
                let count = collection.FillBuffer(buffer, buf.maxBatchDelay)
                if count <> 0 then log.Debug("Consuming {count}", count)
                let batch = Array.init count (fun i -> buffer.[i])
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
                            let tpo =
                                let maxOffset = batch |> Array.maxBy (fun m -> let o = m.Offset in o.Value)
                                let raw = maxOffset.TopicPartitionOffset
                                TopicPartitionOffset(raw.Topic, raw.Partition, Offset(let o = raw.Offset in o.Value + 1L))
                            consumer.StoreOffsets[| tpo |]

                            // decrement in-flight message counter
                            let batchSize = batch |> Array.sumBy approximateMessageBytes
                            counter.Add -batchSize
                    with e ->
                        tcs.TrySetException e |> ignore
                        cts.Cancel()
                    do! loop() }

            Async.Start(loop(), cts.Token)

        use _ = partitionedCollection.OnPartitionAdded.Subscribe (fun (_key,buffer) -> consumePartition buffer)

        // run the consumer
        let ct = cts.Token
        try while not ct.IsCancellationRequested do
                counter.AwaitThreshold()
                try let message = consumer.Consume(cts.Token) // NB don't use TimeSpan overload unless you want AVEs on1.0.0-beta2
                    if message <> null then
                        counter.Add(approximateMessageBytes message)
                        partitionedCollection.Add(message.TopicPartition, message)
                with| :? ConsumeException as e -> log.Warning(e, "Consume exception")
                    | :? System.OperationCanceledException -> log.Warning("Consumer:{name} | Consuming cancelled", consumer.Name)
        finally
            consumer.Close()
        
        // await for handler faults or external cancellation
        do! Async.AwaitTaskCorrect tcs.Task
    }

/// See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md for documentation on the implications of specfic settings
[<NoComparison>]
type KafkaConsumerConfig = private { conf: ConsumerConfig; custom: seq<KeyValuePair<string,string>>; topics: string list; buffering: ConsumerBufferingConfig } with
    member __.Kvps = Seq.append __.conf __.custom
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
            /// Misc configuration parameter to be passed to the underlying CK consumer.
            ?custom,

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
                AutoOffsetReset = Nullable (defaultArg autoOffsetReset AutoOffsetReset.Earliest),
                FetchMaxBytes = Nullable (defaultArg fetchMaxBytes 100000),
                MessageMaxBytes = Nullable (defaultArg fetchMaxBytes 100000),
                FetchMinBytes = Nullable (defaultArg fetchMinBytes 10),  // TODO check if sane default
                EnableAutoCommit = Nullable true,
                AutoCommitIntervalMs = Nullable (match offsetCommitInterval with Some (i: TimeSpan) -> int i.TotalMilliseconds | None -> 1000*10),
                EnableAutoOffsetStore = Nullable false,
                LogConnectionClose = Nullable false) // https://github.com/confluentinc/confluent-kafka-dotnet/issues/124#issuecomment-289727017
        statisticsInterval |> Option.iter (fun (i : TimeSpan) -> conf.StatisticsIntervalMs <- Nullable (int i.TotalMilliseconds))
        {   conf = conf; custom = match custom with None -> Seq.empty | Some c -> List.toSeq c
            topics = match Seq.toList topics with [] -> invalidArg "topics" "must be non-empty collection" | ts -> ts
            buffering = {
                maxBatchSize = defaultArg maxBatchSize 1000
                maxBatchDelay = defaultArg maxBatchDelay (TimeSpan.FromMilliseconds 500.)
                minInFlightBytes = defaultArg minInFlightBytes (16L * 1024L * 1024L)
                maxInFlightBytes = defaultArg maxInFlightBytes (24L * 1024L * 1024L) } }

type KafkaConsumer private (log : ILogger, consumer : Consumer<string, string>, task : Task<unit>, cts : CancellationTokenSource) =

    /// Asynchronously awaits until consumer stops or is faulted
    member __.AwaitCompletion() = Async.AwaitTaskCorrect task
    interface IDisposable with member __.Dispose() = __.Stop()

    member __.Status = task.Status
    member __.Stop() =  
        log.Information("Consuming ... Stopping {name}", consumer.Name)
        cts.Cancel();  

    /// Starts a kafka consumer with provider configuration and batch message handler.
    /// Batches are grouped by topic partition. Batches belonging to the same topic partition will be scheduled sequentially and monotonically,
    /// however batches from different partitions can be run concurrently.
    static member Start (log : ILogger) (config : KafkaConsumerConfig) (partitionHandler : ConsumeResult<string,string>[] -> Async<unit>) =
        if List.isEmpty config.topics then invalidArg "config" "must specify at least one topic"
        log.Information("Consuming... {broker} {topics} {groupId}" (*autoOffsetReset={autoOffsetReset}*) + " fetchMaxBytes={fetchMaxB} maxInFlightBytes={maxInFlightB} maxBatchSize={maxBatchB} maxBatchDelay={maxBatchDelay}s",
            config.conf.BootstrapServers, config.topics, config.conf.GroupId, (*config.conf.AutoOffsetReset.Value,*) config.conf.FetchMaxBytes,
            config.buffering.maxInFlightBytes, config.buffering.maxBatchSize, (let t = config.buffering.maxBatchDelay in t.TotalSeconds))

        let partitionedCollection = new ConsumerImpl.PartitionedBlockingCollection<TopicPartition, ConsumeResult<string, string>>()
        let consumer =
            ConsumerBuilder<_,_>(config.Kvps)
                .SetLogHandler(fun _c m -> log.Information("consumer_info|{message} level={level} name={name} facility={facility}", m.Message, m.Level, m.Name, m.Facility))
                .SetErrorHandler(fun _c e -> log.Error("Consuming... Error reason={reason} code={code} broker={isBrokerError}", e.Reason, e.Code, e.IsBrokerError))
                .SetRebalanceHandler(fun _c m ->
                    for topic,partitions in m.Partitions |> Seq.groupBy (fun p -> p.Topic) |> Seq.map (fun (t,ps) -> t, [| for p in ps -> let p = p.Partition in p.Value |]) do
                        if m.IsAssignment then log.Information("Consuming... Assigned {topic:l} {partitions}", topic, partitions)
                        else log.Information("Consuming... Revoked {topic:l} {partitions}", topic, partitions)
                    if m.IsRevocation then m.Partitions |> Seq.iter partitionedCollection.Revoke)
                //let d5 = c.OnPartitionEOF.Subscribe(fun tpo ->
                //    log.Verbose("consumer_partition_eof|topic={topic}|partition={partition}|offset={offset}", tpo.Topic, tpo.Partition, let o = tpo.Offset in o.Value))
                .SetOffsetsCommittedHandler(fun _c cos ->
                    for t,ps in cos.Offsets |> Seq.groupBy (fun p -> p.Topic) do
                        let o = [for p in ps -> let pp = p.Partition in pp.Value, let o = p.Offset in if o.IsSpecial then box (string o) else box o.Value(*, fmtError p.Error*)]
                        let e = cos.Error
                        if not e.IsError then log.Information("Consuming... Committed {topic} {@offsets}", t, o)
                        else log.Warning("Consuming... Committed {topic} {@offsets} reason={error} code={code} isBrokerError={isBrokerError}", t, o, e.Reason, e.Code, e.IsBrokerError))
                .Build()
        consumer.Subscribe config.topics
        let cts = new CancellationTokenSource()
        let task = ConsumerImpl.mkBatchedMessageConsumer log config.buffering cts.Token consumer partitionedCollection partitionHandler |> Async.StartAsTask
        new KafkaConsumer(log, consumer, task, cts)

    /// Starts a Kafka consumer instance that schedules handlers grouped by message key. Additionally accepts a global degreeOfParallelism parameter
    /// that controls the number of handlers running concurrently across partitions for the given consumer instance.
    static member StartByKey (log: ILogger) (config : KafkaConsumerConfig) (degreeOfParallelism : int) (keyHandler : ConsumeResult<_,_> [] -> Async<unit>) =
        let semaphore = new SemaphoreSlim(degreeOfParallelism)
        let partitionHandler (messages : ConsumeResult<_,_>[]) = async {
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