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

type ChangeFeedObserver() =
    static member Create(log: ILogger, onChange : IChangeFeedObserverContext -> IReadOnlyList<Document> -> Async<unit>, ?dispose: unit -> unit) =
        { new IChangeFeedObserver with
            member __.ProcessChangesAsync(ctx, docs, ct) = (UnitTaskBuilder ct) {
                try do! onChange ctx docs
                with e ->
                    log.Warning(e, "Range {partitionKeyRangeId} Handler Threw", ctx.PartitionKeyRangeId)
                    do! Async.Raise e }
            member __.OpenAsync ctx = UnitTaskBuilder() {
                log.Information("Range {partitionKeyRangeId} Assigned", ctx.PartitionKeyRangeId) }
            member __.CloseAsync (ctx, reason) = UnitTaskBuilder() {
                log.Information("Range {partitionKeyRangeId} Revoked {reason}", ctx.PartitionKeyRangeId, reason) } 
          interface IDisposable with
            member __.Dispose() =
                match dispose with Some f -> f () | None -> () }

type ChangeFeedObserverFactory =
    static member FromFunction (f : unit -> #IChangeFeedObserver) =
        { new IChangeFeedObserverFactory with member __.CreateObserver () = f () :> _ }

type CosmosCollection = { database: string; collection: string }

/// Wraps the [Azure CosmosDb ChangeFeedProcessor library](https://github.com/Azure/azure-documentdb-changefeedprocessor-dotnet)
type ChangeFeedProcessor =
    static member Start
        (   log : ILogger, endpoint : Uri, accountKey : string, connectionPolicy : ConnectionPolicy, source : CosmosCollection,
            /// The aux, non-partitioned collection holding the partition leases.
            // Aux coll should always read from the write region to keep the number of write conflicts to minimum when the sdk
            // updates the leases. Since the non-write region(s) might lag behind due to us using non-strong consistency, during
            // failover we are likely to reprocess some messages, but that's okay since we are idempotent.
            aux : CosmosCollection,
            createObserver : unit -> IChangeFeedObserver,
            ?leaseOwnerId : string,
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
        log.Information("Processing Lease acquireS={leaseAcquireIntervalS:n0} ttlS={ttlS:n0} renewS={renewS:n0} feedPollDelayS={feedPollDelayS:n0}",
            s leaseAcquireInterval, s leaseTtl, s leaseRenewInterval, s feedPollDelay)

        let builder =
            let feedProcessorOptions = 
                ChangeFeedProcessorOptions(
                    StartFromBeginning = not (defaultArg forceSkipExistingEvents false),
                    MaxItemCount = Nullable cfBatchSize,
                    LeaseAcquireInterval = leaseAcquireInterval, LeaseExpirationInterval = leaseTtl, LeaseRenewInterval = leaseRenewInterval,
                    FeedPollDelay = feedPollDelay)
            leasePrefix |> Option.iter (fun lp -> feedProcessorOptions.LeasePrefix <- lp + ":")
            let mk d c = DocumentCollectionInfo(Uri = endpoint, DatabaseName = d, CollectionName = c, MasterKey = accountKey, ConnectionPolicy = connectionPolicy)
            ChangeFeedProcessorBuilder()
                .WithHostName(leaseOwnerId)
                .WithFeedCollection(mk source.database source.collection)
                .WithLeaseCollection(mk aux.database aux.collection)
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