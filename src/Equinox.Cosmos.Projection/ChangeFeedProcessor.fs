namespace Equinox.Cosmos.Projection

open Equinox.Cosmos.Projection
open Equinox.Store.Infrastructure
open Microsoft.Azure.Documents
open Microsoft.Azure.Documents.Client
open Microsoft.Azure.Documents.ChangeFeedProcessor
open Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing
open Serilog
open System
open System.Collections.Generic

/// Provides F#-friendly wrapping to compose a ChangeFeedObserver from functions
type ChangeFeedObserver =
    static member Create
      ( /// Base logger context; will be decorated with a `partitionKeyRangeId` property when passed to `assign` and `processBatch`
        log: ILogger,
        /// Callback responsible for accepting a set of documents, the `ctx` argument enables one to checkpoint up to the end of the batch passed
        /// NB emitting an exception will not trigger a retry, and no progress writing will take place without explicit calls to `ctx.CheckpointAsync`
        processBatch : ILogger -> IChangeFeedObserverContext -> IReadOnlyList<Document> -> Async<unit>,
        /// Callback triggered when a lease is won and the observer is being spun up (0 or more `processBatch` calls will follow)
        ?assign: ILogger -> unit,
        /// Callback triggered when a lease is revoked and the observer about to be `Dispose`d
        ?revoke: ILogger -> unit,
        /// Function called when this Observer is being destroyed due to the revocation of a lease (triggered after `revoke`)
        ?dispose: unit -> unit) =
        let mutable log = log
        { new IChangeFeedObserver with
            member __.OpenAsync ctx = UnitTaskBuilder() {
                log <- log.ForContext("partitionKeyRangeId",ctx.PartitionKeyRangeId)
                log.Information("Range {partitionKeyRangeId} Assigned", ctx.PartitionKeyRangeId)
                assign |> Option.iter (fun f -> f log) }
            member __.ProcessChangesAsync(ctx, docs, ct) = (UnitTaskBuilder ct) {
                try do! processBatch log ctx docs
                with e ->
                    log.Warning(e, "Range {partitionKeyRangeId} Handler Threw", ctx.PartitionKeyRangeId)
                    do! Async.Raise e }
            member __.CloseAsync (ctx, reason) = UnitTaskBuilder() {
                log.Information("Range {partitionKeyRangeId} Revoked {reason}", ctx.PartitionKeyRangeId, reason)
                revoke |> Option.iter (fun f -> f log) } 
          interface IDisposable with
            member __.Dispose() =
                match dispose with
                | Some f -> f ()
                | None -> () }

type ChangeFeedObserverFactory =
    static member FromFunction (f : unit -> #IChangeFeedObserver) =
        { new IChangeFeedObserverFactory with member __.CreateObserver () = f () :> _ }

type CosmosCollectionId = { database: string; collection: string }

//// Wraps the [Azure CosmosDb ChangeFeedProcessor library](https://github.com/Azure/azure-documentdb-changefeedprocessor-dotnet)
type ChangeFeedProcessor =
    static member Start
        (   log : ILogger, discovery: Equinox.Cosmos.Discovery, connectionPolicy : ConnectionPolicy, source : CosmosCollectionId,
            /// The aux, non-partitioned collection holding the partition leases.
            // Aux coll should always read from the write region to keep the number of write conflicts to minimum when the sdk
            // updates the leases. Since the non-write region(s) might lag behind due to us using non-strong consistency, during
            // failover we are likely to reprocess some messages, but that's okay since processing has to be idempotent in any case
            aux : CosmosCollectionId,
            createObserver : unit -> IChangeFeedObserver,
            ?leaseOwnerId : string,
            /// Used to specify an endpoint/account key for the aux collection, where that varies from that of the source collection. Default: use `discovery`
            ?auxDiscovery : Equinox.Cosmos.Discovery,
            /// Identifier to disambiguate multiple independent feed processor positions (akin to a 'consumer group')
            ?leasePrefix : string,
            /// (NB Only applies if this is the first time this leasePrefix is presented)
            /// Specify `true` to request starting of projection from the present write position.
            /// Default: false (projecting all events from start beforehand)
            ?forceSkipExistingEvents : bool,
            /// Limit on items to take in a batch when querying for changes (in addition to 4MB response size limit). Default 1000
            /// NB While a larger limit may deliver better throughput, it's important to balance that .concern with the risk of throttling
            ?cfBatchSize : int, 
            /// Frequency to check for partitions without a processor. Default 1s
            ?leaseAcquireInterval : TimeSpan,
            /// Frequency to renew leases held by processors under our control. Default 3s
            ?leaseRenewInterval : TimeSpan,
            /// Duration to take lease when acquired/renewed. Default 10s
            ?leaseTtl : TimeSpan,
            /// Delay before re-polling a partitition after backlog has been drained
            ?feedPollDelay : TimeSpan,
            /// Continuously fed per-partion lag information until parent Async completes
            /// callback should Async.Sleep until next update is desired
            ?reportLagAndAwaitNextEstimation) = async {

        let leaseOwnerId = defaultArg leaseOwnerId (ChangeFeedProcessor.mkLeaseOwnerIdForProcess())
        let cfBatchSize = defaultArg cfBatchSize 1000
        let feedPollDelay = defaultArg feedPollDelay (TimeSpan.FromSeconds 1.)
        let leaseAcquireInterval = defaultArg leaseAcquireInterval (TimeSpan.FromSeconds 1.)
        let leaseRenewInterval = defaultArg leaseRenewInterval (TimeSpan.FromSeconds 3.)
        let leaseTtl = defaultArg leaseTtl (TimeSpan.FromSeconds 10.)

        let inline s (x : TimeSpan) = x.TotalSeconds
        log.Information("Processing Lease acquire {leaseAcquireIntervalS:n0}s ttl {ttlS:n0}s renew {renewS:n0}s feedPollDelay {feedPollDelayS:n0}s",
            s leaseAcquireInterval, s leaseTtl, s leaseRenewInterval, s feedPollDelay)

        let builder =
            let feedProcessorOptions = 
                ChangeFeedProcessorOptions(
                    StartFromBeginning = not (defaultArg forceSkipExistingEvents false),
                    MaxItemCount = Nullable cfBatchSize,
                    LeaseAcquireInterval = leaseAcquireInterval, LeaseExpirationInterval = leaseTtl, LeaseRenewInterval = leaseRenewInterval,
                    FeedPollDelay = feedPollDelay)
            // As of CFP 2.2.5, the default behavior does not afford any useful characteristics when the processing is erroring:-
            // a) progress gets written regardless of whether the handler completes with an Exception or not
            // b) no retries happen while the processing is online
            // ... as a result the checkpointing logic is turned off.
            // NB for lag reporting to work correctly, it is of course still important that the writing take place, and that it be written via the CFP lib
            feedProcessorOptions.CheckpointFrequency.ExplicitCheckpoint <- true
            leasePrefix |> Option.iter (fun lp -> feedProcessorOptions.LeasePrefix <- lp + ":")
            let mk (Equinox.Cosmos.Discovery.UriAndKey (u,k)) d c =
                DocumentCollectionInfo(Uri = u, DatabaseName = d, CollectionName = c, MasterKey = k, ConnectionPolicy = connectionPolicy)
            ChangeFeedProcessorBuilder()
                .WithHostName(leaseOwnerId)
                .WithFeedCollection(mk discovery source.database source.collection)
                .WithLeaseCollection(mk (defaultArg auxDiscovery discovery) aux.database aux.collection)
                .WithProcessorOptions(feedProcessorOptions)
        match reportLagAndAwaitNextEstimation with
        | None -> ()
        | Some lagMonitorCallback ->
            let! estimator = builder.BuildEstimatorAsync() |> Async.AwaitTaskCorrect
            let rec emitLagMetrics () = async {
                let! remainingWork = estimator.GetEstimatedRemainingWorkPerPartitionAsync() |> Async.AwaitTaskCorrect
                do! lagMonitorCallback <| List.ofSeq (seq { for r in remainingWork -> int (r.PartitionKeyRangeId.Trim[|'"'|]),r.RemainingWork } |> Seq.sortBy fst)
                return! emitLagMetrics () }
            let! _ = Async.StartChild(emitLagMetrics ()) in ()
        let! processor = builder.WithObserverFactory(ChangeFeedObserverFactory.FromFunction createObserver).BuildAsync() |> Async.AwaitTaskCorrect
        do! processor.StartAsync() |> Async.AwaitTaskCorrect
        return processor }
    static member private mkLeaseOwnerIdForProcess() =
        // If k>1 processes share an owner id, then they will compete for same partitions.
        // In that scenario, redundant processing happen on assigned partitions, but checkpoint will process on only 1 consumer.
        // Including the processId should eliminate the possibility that a broken process manager causes k>1 scenario to happen.
        // The only downside is that upon redeploy, lease experation / TTL would have to be observed before a consumer can pick it up.
        let processName = System.Reflection.Assembly.GetEntryAssembly().GetName().Name
        let processId = System.Diagnostics.Process.GetCurrentProcess().Id
        let hostName = System.Environment.MachineName
        sprintf "%s-%s-%d" hostName processName processId