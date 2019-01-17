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

[<RequireQualifiedAccess>]
module Parse =
    type Document with
        member document.Cast<'T>() =
            let tmp = new Document()
            tmp.SetPropertyValue("content", document)
            tmp.GetPropertyValue<'T>("content")
    /// Determines whether this document represents an index page [and hence should not be expected to contain any events]
    let isIndex (d : Document) = d.Id = "-1"
    type IEvent =
        inherit Equinox.Cosmos.Store.IIndexedEvent
            abstract member Stream : string
            abstract member TimeStamp : DateTimeOffset
    /// Infers whether the document represents a valid Event-Batch
    let enumEvents (d : Document) = seq {
        if not (isIndex d)
           && d.GetPropertyValue("p") <> null && d.GetPropertyValue("i") <> null
           && d.GetPropertyValue("n") <> null && d.GetPropertyValue("e") <> null then
            let batch = d.Cast<Equinox.Cosmos.Store.Batch>()
            yield! batch.e |> Seq.mapi (fun offset x ->
                { new IEvent with
                      member __.Index = batch.i + int64 offset
                      member __.IsUnfold = false
                      member __.EventType = x.c
                      member __.Data = x.d
                      member __.Meta = x.m
                      member __.TimeStamp = x.t
                      member __.Stream = batch.p } ) }

[<AutoOpen>]
module Wrappers =
    type IChangeFeedObserver with
        static member Create(log: ILogger, onChange : IChangeFeedObserverContext -> IReadOnlyList<Document> -> Async<unit>) =
            { new IChangeFeedObserver with
                member __.ProcessChangesAsync(ctx, docs, ct) = (UnitTaskBuilder ct) {
                    try do! onChange ctx docs
                    with e ->
                        log.Warning(e, "Range {partitionKeyRangeId} Handler Threw", ctx.PartitionKeyRangeId)
                        do! Async.Raise e }
                member __.OpenAsync ctx = UnitTaskBuilder() {
                    log.Information("Range {partitionKeyRangeId} Assigned", ctx.PartitionKeyRangeId) }
                member __.CloseAsync (ctx, reason) = UnitTaskBuilder() {
                    log.Information("Range {partitionKeyRangeId} Closed {reason}", ctx.PartitionKeyRangeId, reason) } }

    type IChangeFeedObserverFactory with
        static member FromFunction (f : unit -> #IChangeFeedObserver) =
            { new IChangeFeedObserverFactory with member __.CreateObserver () = f () :> _ }

type CosmosCollection = { database: string; collection: string }

/// Wraps the [Azure CosmosDb ChangeFeedProcessor library](https://github.com/Azure/azure-documentdb-changefeedprocessor-dotnet)
type ChangeFeedProcessor =
    static member Start
        (   log : ILogger, endpoint : Uri, accountKey : string, connectionPolicy : ConnectionPolicy, source : CosmosCollection,
            // Aux coll should always read from the write region to keep the number of write conflicts to minimum when the sdk
            // updates the leases. Since the non-write region(s) might lag behind due to us using non-strong consistency, during
            // failover we are likely to reprocess some messages, but that's okay since we are idempotent.
            // The aux, non-partitioned collection holding the partition leases.
            aux : CosmosCollection,
            changeFeedObserver : IChangeFeedObserver,
            ?leaseOwnerId : string,
            /// Identifier to disambiguate multiple independent feed processor positions
            ?leasePrefix : string,
            /// (Only if this is the first time this leasePrefix is presnted) Request starting of projection from this position (as opposed to projecting all events from start beforehand). Default: false.
            ?forceSkipExistingEvents : bool,
            /// Limit on items to take in a batch when querying for changes. Default 1000
            ?cfBatchSize : int, 
            /// Frequency to check for partitions without a processor. Default 1s
            ?leaseAcquireInterval : TimeSpan,
            /// Frequency to renew leases held by processors under our control. Default 3s
            ?leaseRenewInterval : TimeSpan,
            /// Duration to take lease when acquired/renewed. Default 10s
            ?leaseTtl : TimeSpan,
            /// Delay before re-polling a partitition after backlog has been drained
            ?feedPollDelay : TimeSpan,
            /// Frequency to dump lag stats
            ?lagMonitorInterval: TimeSpan) = async {

        let leaseOwnerId = defaultArg leaseOwnerId (ChangeFeedProcessor.mkLeaseOwnerIdForProcess())
        let cfBatchSize = defaultArg cfBatchSize 1000
        let feedPollDelay = defaultArg feedPollDelay (TimeSpan.FromSeconds 1.)
        let leaseAcquireInterval = defaultArg leaseAcquireInterval (TimeSpan.FromSeconds 1.)
        let leaseRenewInterval = defaultArg leaseRenewInterval (TimeSpan.FromSeconds 3.)
        let leaseTtl = defaultArg leaseTtl (TimeSpan.FromSeconds 10.)

        let inline s (x : TimeSpan) = x.TotalSeconds
        log.Information("Processing Lease acquireS={leaseAcquireIntervalS:n0} ttlS={ttlS:n0} renewS={renewS:n0} feedPollDelayS={feedPollDelayS:n0}",
            s leaseAcquireInterval, s leaseTtl, s leaseRenewInterval, s feedPollDelay)

        let mk d c = DocumentCollectionInfo(Uri = endpoint, DatabaseName = d, CollectionName = c, MasterKey = accountKey, ConnectionPolicy = connectionPolicy)
        let monitoredColl = mk source.database source.collection
        let auxColl = mk aux.database aux.collection

        let feedProcessorOptions = 
            ChangeFeedProcessorOptions(
                StartFromBeginning = not (defaultArg forceSkipExistingEvents false),
                MaxItemCount = Nullable cfBatchSize,
                LeaseAcquireInterval = leaseAcquireInterval, LeaseExpirationInterval = leaseTtl, LeaseRenewInterval = leaseRenewInterval,
                FeedPollDelay = feedPollDelay)

        leasePrefix |> Option.iter (fun lp -> feedProcessorOptions.LeasePrefix <- lp + "/")

        let factory = IChangeFeedObserverFactory.FromFunction (fun () -> changeFeedObserver)
       
        let! processor =
            ChangeFeedProcessorBuilder()
                .WithHostName(leaseOwnerId)
                .WithFeedCollection(monitoredColl)
                .WithLeaseCollection(auxColl)
                .WithProcessorOptions(feedProcessorOptions)
                .WithObserverFactory(factory)
                .BuildAsync() |> Async.AwaitTaskCorrect
        do! processor.StartAsync() |> Async.AwaitTaskCorrect

        match lagMonitorInterval with
        | None -> ()
        | Some lagMonitorInterval ->
            let! estimator =
                ChangeFeedProcessorBuilder()
                    .WithHostName(leaseOwnerId)
                    .WithFeedCollection(monitoredColl)
                    .WithLeaseCollection(auxColl)                    
                    .BuildEstimatorAsync() |> Async.AwaitTaskCorrect
            let accountName = endpoint.Host // .Split('.').[0]
            let rec emitLagMetrics () = async {
                let! remainingWork = estimator.GetEstimatedRemainingWorkPerPartitionAsync() |> Async.AwaitTaskCorrect
                let logLevel = if remainingWork |> Seq.exists (fun x -> x.RemainingWork <> 0L) then Events.LogEventLevel.Information else Events.LogEventLevel.Debug
                log.Write(logLevel, "Work outstanding {@workRemaining} database={database} collection={collection} account={account}",
                    remainingWork, source.database, source.collection, accountName)
                do! Async.Sleep lagMonitorInterval
                return! emitLagMetrics ()
            }
            let! _ = Async.StartChild(emitLagMetrics ()) in ()

        return processor }
    static member private mkLeaseOwnerIdForProcess() =
        // If k>1 processes share owner id, then they will compete for same partitions.
        // In that scenario, redundant processing happen on assigned partitions, but checkpoint will process on only 1 consumer.
        // Including the processId should eliminate the possibility that a broken process manager causes k>1 scenario to happen.
        // The only downside is that upon redeploy, lease ttl would have to be observed before a consumer can pick it up.
        let processName = System.Reflection.Assembly.GetEntryAssembly().GetName().Name
        let processId = System.Diagnostics.Process.GetCurrentProcess().Id
        let hostName = System.Environment.MachineName
        sprintf "%s-%s-%d" hostName processName processId