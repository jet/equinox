namespace Equinox.CosmosStore.Linq

open Equinox.Core
open FSharp.Control // taskSeq
open Serilog
open System
open System.ComponentModel

module internal Internal =
    open Microsoft.Azure.Cosmos
    let inline miB x = float x / 1024. / 1024.
    module Query =
        let [<EditorBrowsable(EditorBrowsableState.Never)>] enum__ (iterator: FeedIterator<'T>) = taskSeq {
            while iterator.HasMoreResults do
                let! response = iterator.ReadNextAsync()
                let m = response.Diagnostics.GetQueryMetrics().CumulativeMetrics
                yield struct (response.Diagnostics.GetClientElapsedTime(), response.RequestCharge, response.Resource,
                              int m.RetrievedDocumentCount, int m.RetrievedDocumentSize, int m.OutputDocumentSize) }
        let enum_<'T> (log: ILogger) (container: Container) (action: string) cat logLevel (iterator: FeedIterator<'T>) = taskSeq {
            let startTicks = System.Diagnostics.Stopwatch.GetTimestamp()
            use _ = iterator
            let mutable responses, items, totalRtt, totalRu, totalRdc, totalRds, totalOds = 0, 0, TimeSpan.Zero, 0., 0, 0, 0
            try for rtt, rc, response, rdc, rds, ods in enum__ iterator do
                    responses <- responses + 1
                    totalRdc <- totalRdc + rdc
                    totalRds <- totalRds + rds
                    totalOds <- totalOds + ods
                    totalRu <- totalRu + rc
                    totalRtt <- totalRtt + rtt
                    for item in response do
                        items <- items + 1
                        yield item
            finally
                let interval = StopwatchInterval(startTicks, System.Diagnostics.Stopwatch.GetTimestamp())
                log.Write(logLevel, "EqxCosmos {action:l} {count} ({trips}r {totalRtt:f0}ms; {rdc}i {rds:f2}>{ods:f2} MiB) {rc:f2} RU {lat:f0} ms",
                                action, items, responses, totalRtt.TotalMilliseconds, totalRdc, miB totalRds, miB totalOds, totalRu, interval.ElapsedMilliseconds) }
        let exec__<'R> (log: ILogger) (container: Container) cat logLevel (queryDefinition: QueryDefinition): TaskSeq<'R> =
            if log.IsEnabled logLevel then log.Write(logLevel, "CosmosStoreQuery.run {cat} {query}", cat, queryDefinition.QueryText)
            container.GetItemQueryIterator<'R> queryDefinition |> enum_ log container "Query" cat logLevel
        /// Execute a query, hydrating as 'R
        let exec<'R> (log: ILogger) (container: Container) logLevel (queryDefinition: QueryDefinition): TaskSeq<'R> =
            exec__<'R> log container "%" logLevel queryDefinition
