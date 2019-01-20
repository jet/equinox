namespace Equinox.Projection.Kafka.Infrastructure.Tests

open Newtonsoft.Json
open System
open System.Collections.Concurrent
open System.ComponentModel
open System.Threading
open System.Threading.Tasks
open Xunit

[<AutoOpen>]
[<EditorBrowsable(EditorBrowsableState.Never)>]
module Helpers =

    let getTestBroker() = 
        match Environment.GetEnvironmentVariable "KAFORK_TEST_BROKER" with
        | x when String.IsNullOrEmpty x -> invalidOp "missing environment variable 'KAFORK_TEST_BROKER'"
        | x -> Uri x

    let newId() = System.Guid.NewGuid().ToString("N")

    type Assert with
        static member ThrowsAsync<'ExpectedExn when 'ExpectedExn :> exn> (workflow : Async<unit>) : unit =
            Assert.ThrowsAsync<'ExpectedExn>(workflow) |> ignore

    [<AbstractClass>]
    type AsyncBuilderAbstract() =
        member __.Zero() = async.Zero()
        member __.Return t = async.Return t
        member __.ReturnFrom t = async.ReturnFrom t
        member __.Bind(f,g) = async.Bind(f,g)
        member __.Combine(f,g) = async.Combine(f,g)
        member __.Delay f = async.Delay f
        member __.While(c,b) = async.While(c,b)
        member __.For(xs,b) = async.For(xs,b)
        member __.Using(d,b) = async.Using(d,b)
        member __.TryWith(b,e) = async.TryWith(b,e)
        member __.TryFinally(b,f) = async.TryFinally(b,f)


    type Async with
        static member ParallelThrottled degreeOfParallelism jobs = async {
            let s = new SemaphoreSlim(degreeOfParallelism)
            return!
                jobs
                |> Seq.map (fun j -> async {
                    let! ct = Async.CancellationToken
                    do! s.WaitAsync ct |> Async.AwaitTask
                    try return! j
                    finally s.Release() |> ignore })
                |> Async.Parallel
        }

    type TaskBuilder(?ct : CancellationToken) =
        inherit AsyncBuilderAbstract()
        member __.Run f : Task<'T> = Async.StartAsTask(f, ?cancellationToken = ct)

    type UnitTaskBuilder() =
        inherit AsyncBuilderAbstract()
        member __.Run f : Task = Async.StartAsTask f :> _

    /// Async builder variation that automatically runs top-level expression as task
    let task = new TaskBuilder()

    /// Async builder variation that automatically runs top-level expression as untyped task
    let utask = new UnitTaskBuilder()

    type KafkaConsumer with
        member c.StopAfter(delay : TimeSpan) =
            Task.Delay(delay).ContinueWith(fun (_:Task) -> c.Stop()) |> ignore

    type TestMessage = { producerId : int ; messageId : int }
    type ConsumedTestMessage = { consumerId : int ; message : KafkaMessage ; payload : TestMessage }
    type ConsumerCallback = KafkaConsumer -> ConsumedTestMessage [] -> Async<unit>

    let runProducers (broker : Uri) (topic : string) (numProducers : int) (messagesPerProducer : int) = async {
        let runProducer (producerId : int) = async {
            use producer = KafkaProducer.Create("panther", broker, topic)

            let! results =
                [1 .. messagesPerProducer]
                |> Seq.map (fun msgId ->
                    let key = string msgId
                    let value = JsonConvert.SerializeObject { producerId = producerId ; messageId = msgId }
                    key, value)

                |> Seq.chunkBySize 100
                |> Seq.map producer.ProduceBatch
                |> Async.ParallelThrottled 7

            return Array.concat results
        }

        return! Async.Parallel [for i in 1 .. numProducers -> runProducer i]
    }

    let runConsumers (config : KafkaConsumerConfig) (numConsumers : int) (timeout : TimeSpan option) (handler : ConsumerCallback) = async {
        let mkConsumer (consumerId : int) = async {
            let deserialize (msg : KafkaMessage) = { consumerId = consumerId ; message = msg ; payload = JsonConvert.DeserializeObject<_> msg.Value }

            // need to pass the consumer instance to the handler callback
            // do a bit of cyclic dependency fixups
            let consumerCell = ref None
            let rec getConsumer() =
                // avoid potential race conditions by polling
                match !consumerCell with
                | None -> Thread.SpinWait 20; getConsumer()
                | Some c -> c

            let consumer = KafkaConsumer.Start config (fun batch -> handler (getConsumer()) (Array.map deserialize batch))

            consumerCell := Some consumer

            timeout |> Option.iter (fun t -> consumer.StopAfter t)

            do! consumer.AwaitConsumer()
        }

        do! Async.Parallel [for i in 1 .. numConsumers -> mkConsumer i] |> Async.Ignore
    }


module Tests =

    [<Test; Category("IntegrationTest")>]
    let ``ConfluentKafka producer-consumer basic roundtrip`` () = utask {
        let broker = getTestBroker()

        let numProducers = 10
        let numConsumers = 10
        let messagesPerProducer = 1000

        let topic = newId() // dev kafka topics are created and truncated automatically
        let groupId = newId()

        let consumedBatches = new ConcurrentBag<ConsumedTestMessage[]>()
        let consumerCallback (consumer:KafkaConsumer) batch = async {
            do consumedBatches.Add batch
            let messageCount = consumedBatches |> Seq.sumBy Array.length
            // signal cancellation if consumed items reaches expected size
            if messageCount >= numProducers * messagesPerProducer then
                consumer.Stop()
        }

        // Section: run the test
        let producers = runProducers broker topic numProducers messagesPerProducer |> Async.Ignore

        let config = KafkaConsumerConfig.Create("panther", broker, [topic], groupId)
        let consumers = runConsumers config numConsumers None consumerCallback

        do!
            [ producers ; consumers ]
            |> Async.Parallel
            |> Async.Ignore

        // Section: assertion checks
        let ``consumed batches should be non-empty`` =
            consumedBatches
            |> Seq.forall (not << Array.isEmpty)

        Assert.IsTrue(``consumed batches should be non-empty``, "consumed batches should all be non-empty")

        let ``batches should be grouped by partition`` =
            consumedBatches
            |> Seq.map (fun batch -> batch |> Seq.distinctBy (fun b -> b.message.Partition) |> Seq.length)
            |> Seq.forall (fun numKeys -> numKeys = 1)
        
        Assert.IsTrue(``batches should be grouped by partition``, "batches should be grouped by partition")

        let allMessages =
            consumedBatches
            |> Seq.concat
            |> Seq.toArray

        let ``all message keys should have expected value`` =
            allMessages |> Array.forall (fun msg -> int msg.message.Key = msg.payload.messageId)

        Assert.IsTrue(``all message keys should have expected value``, "all message keys should have expected value")

        let ``should have consumed all expected messages`` =
            allMessages
            |> Array.groupBy (fun msg -> msg.payload.producerId)
            |> Array.map (fun (_, gp) -> gp |> Array.distinctBy (fun msg -> msg.payload.messageId))
            |> Array.forall (fun gp -> gp.Length = messagesPerProducer)

        Assert.IsTrue(``should have consumed all expected messages``, "should have consumed all expected messages")
    }

    [<Test; Category("IntegrationTest")>]
    let ``ConfluentKafka consumer should have expected exception semantics`` () = utask {
        let broker = getTestBroker()

        let topic = newId() // dev kafka topics are created and truncated automatically
        let groupId = newId()

        let! _ = runProducers broker topic 1 10 // populate the topic with a few messages

        let config = KafkaConsumerConfig.Create("panther", broker, [topic], groupId)
        runConsumers config 1 None (fun _ _ -> raise <| IndexOutOfRangeException())
        |> Assert.ThrowsAsync<IndexOutOfRangeException>
    }

    [<Test; Category("IntegrationTest")>]
    let ``Given a topic different consumer group ids should be consuming the same message set`` () = utask {
        let broker = getTestBroker()
        let numMessages = 10

        let topic = newId() // dev kafka topics are created and truncated automatically

        let! _ = runProducers broker topic 1 numMessages // populate the topic with a few messages

        let messageCount = ref 0
        let groupId1 = newId()
        let config = KafkaConsumerConfig.Create("panther", broker, [topic], groupId1)
        do! runConsumers config 1 None 
                (fun c b -> async { if Interlocked.Add(messageCount, b.Length) >= numMessages then c.Stop() })

        Assert.AreEqual(numMessages, !messageCount)

        let messageCount = ref 0
        let groupId2 = newId()
        let config = KafkaConsumerConfig.Create("panther", broker, [topic], groupId2)
        do! runConsumers config 1 None
                (fun c b -> async { if Interlocked.Add(messageCount, b.Length) >= numMessages then c.Stop() })

        Assert.AreEqual(numMessages, !messageCount)
    }

    [<Test; Category("IntegrationTest")>]
    let ``Spawning a new consumer with same consumer group id should not receive new messages`` () = utask {
        let broker = getTestBroker()

        let numMessages = 10
        let topic = newId() // dev kafka topics are created and truncated automatically
        let groupId = newId()
        let config = KafkaConsumerConfig.Create("panther", broker, [topic], groupId)

        let! _ = runProducers broker topic 1 numMessages // populate the topic with a few messages

        // expected to read 10 messages from the first consumer
        let messageCount = ref 0
        do! runConsumers config 1 None
                (fun c b -> async {
                    if Interlocked.Add(messageCount, b.Length) >= numMessages then 
                        c.StopAfter(TimeSpan.FromSeconds 1.) }) // cancel after 1 second to allow offsets to be stored

        Assert.AreEqual(numMessages, !messageCount)

        // expected to read no messages from the subsequent consumer
        let messageCount = ref 0
        do! runConsumers config 1 (Some (TimeSpan.FromSeconds 10.)) 
                (fun c b -> async { 
                    if Interlocked.Add(messageCount, b.Length) >= numMessages then c.Stop() })

        Assert.AreEqual(0, !messageCount)
    }

    [<Test; Category("IntegrationTest")>]
    let ``Commited offsets should not result in missing messages`` () = utask {
        let broker = getTestBroker()

        let numMessages = 10
        let topic = newId() // dev kafka topics are created and truncated automatically
        let groupId = newId()
        let config = KafkaConsumerConfig.Create("panther", broker, [topic], groupId)

        let! _ = runProducers broker topic 1 numMessages // populate the topic with a few messages

        // expected to read 10 messages from the first consumer
        let messageCount = ref 0
        do! runConsumers config 1 None
                (fun c b -> async {
                    if Interlocked.Add(messageCount, b.Length) >= numMessages then 
                        c.StopAfter(TimeSpan.FromSeconds 1.) }) // cancel after 1 second to allow offsets to be committed)

        Assert.AreEqual(numMessages, !messageCount)

        let! _ = runProducers broker topic 1 numMessages // produce more messages

        // expected to read 10 messages from the subsequent consumer,
        // this is to verify there are no off-by-one errors in how offsets are committed
        let messageCount = ref 0
        do! runConsumers config 1 None
                (fun c b -> async {
                    if Interlocked.Add(messageCount, b.Length) >= numMessages then 
                        c.StopAfter(TimeSpan.FromSeconds 1.) }) // cancel after 1 second to allow offsets to be committed)

        Assert.AreEqual(numMessages, !messageCount)
    }

    [<Test; Category("IntegrationTest")>]
    let ``Consumers should never schedule two batches of the same partition concurrently`` () = utask {
        // writes 2000 messages down a topic with a shuffled partition key
        // then attempts to consume the topic, while verifying that per-partition batches
        // are never scheduled for concurrent execution. also checks that batches are
        // monotonic w.r.t. offsets
        let broker = getTestBroker()

        let numMessages = 2000
        let maxBatchSize = 5
        let topic = newId() // dev kafka topics are created and truncated automatically
        let groupId = newId()
        let config = KafkaConsumerConfig.Create("panther", broker, [topic], groupId, maxBatchSize = maxBatchSize)

        // Produce messages in the topic
        do! runProducers broker topic 1 numMessages |> Async.Ignore

        let globalMessageCount = ref 0

        let getPartitionOffset = 
            let state = new ConcurrentDictionary<int, int64 ref>()
            fun partition -> state.GetOrAdd(partition, fun _ -> ref -1L)

        let getBatchPartitionCount =
            let state = new ConcurrentDictionary<int, int ref>()
            fun partition -> state.GetOrAdd(partition, fun _ -> ref 0)

        do! runConsumers config 1 None
                (fun c b -> async {
                    let partition = b.[0].message.Partition

                    // check batch sizes are bounded by maxBatchSize
                    Assert.LessOrEqual(b.Length, maxBatchSize, "batch sizes should never exceed maxBatchSize")

                    // check per-partition handlers are serialized
                    let concurrentBatchCell = getBatchPartitionCount partition
                    let concurrentBatches = Interlocked.Increment concurrentBatchCell
                    Assert.AreEqual(1, concurrentBatches, "partitions should never schedule more than one batch concurrently")

                    // check for message monotonicity
                    let offset = getPartitionOffset partition
                    for msg in b do
                        Assert.Greater(msg.message.Offset, !offset, "offset for partition should be monotonic")
                        offset := msg.message.Offset

                    do! Async.Sleep 100

                    let _ = Interlocked.Decrement concurrentBatchCell

                    if Interlocked.Add(globalMessageCount, b.Length) >= numMessages then c.Stop() })


        Assert.AreEqual(numMessages, !globalMessageCount)
    }