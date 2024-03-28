namespace Equinox.CosmosStore.Linq

open FSharp.Control
open Serilog
open System
open System.Linq
open System.Linq.Expressions

module Internal =

    open Microsoft.Azure.Cosmos
    let private taskEnum (iterator: FeedIterator<'T>) = taskSeq {
        while iterator.HasMoreResults do
            let! response = iterator.ReadNextAsync()
            let m = response.Diagnostics.GetQueryMetrics().CumulativeMetrics
            yield struct (response.Diagnostics.GetClientElapsedTime(), response.RequestCharge, response.Resource,
                          int m.RetrievedDocumentCount, int m.RetrievedDocumentSize, int m.OutputDocumentSize) }
    let inline miB x = float x / 1024. / 1024.
    let enum<'T, 'R> (desc: string) (container: Container) (parse: 'T -> 'R) (queryDefinition: QueryDefinition) = taskSeq {
        if Log.IsEnabled Serilog.Events.LogEventLevel.Debug then Log.Debug("CosmosStoreQuery.enum {desc} {query}", desc, queryDefinition.QueryText)
        let sw = System.Diagnostics.Stopwatch.StartNew()
        let iterator = container.GetItemQueryIterator<'T>(queryDefinition)
        let mutable responses, items, totalRtt, totalRu, totalRdc, totalRds, totalOds = 0, 0, TimeSpan.Zero, 0., 0, 0, 0
        try for rtt, rc, response, rdc, rds, ods in taskEnum iterator do
                responses <- responses + 1
                totalRdc <- totalRdc + rdc
                totalRds <- totalRds + rds
                totalOds <- totalOds + ods
                totalRu <- totalRu + rc
                totalRtt <- totalRtt + rtt
                for item in response do
                    items <- items + 1
                    yield parse item
        finally Log.Information("CosmosStoreQuery.enum {desc} {count} ({trips}r {totalRtt:f0}ms; {rdc}i {rds:f2}>{ods:f2} MiB) {rc} RU {latency} ms",
                                desc, items,  responses, totalRtt.TotalMilliseconds, totalRdc, miB totalRds, miB totalOds, totalRu, sw.ElapsedMilliseconds) }

    open Microsoft.Azure.Cosmos.Linq
    let tryScalar<'T, 'R> desc container (query: IQueryable<'T>) (parse: 'T -> 'R): Async<'R option> =
        query.Take(1).ToQueryDefinition() |> enum<'T, 'R> desc container parse |> TaskSeq.tryHead |> Async.AwaitTask
    let page<'T0, 'T1, 'R> desc container (pageSize: int) pageIndex (query: IQueryable<'T0>) (parse: 'T1 -> 'R): Async<'R[]> =
        query.Skip(pageIndex * pageSize).Take(pageSize).ToQueryDefinition() |> enum desc container parse |> TaskSeq.toArrayAsync |> Async.AwaitTask
    let count (desc: string) (query: IQueryable<'T>): System.Threading.Tasks.Task<int> = task {
        if Log.IsEnabled Serilog.Events.LogEventLevel.Debug then Log.Debug("CosmosStoreQuery.count {desc} {query}", desc, query.ToQueryDefinition().QueryText)
        let sw = System.Diagnostics.Stopwatch.StartNew()
        let! (r: Response<int>) = query.CountAsync()
        let m = r.Diagnostics.GetQueryMetrics().CumulativeMetrics
        Log.Information("CosmosStoreQuery.count {desc} {count} ({rdc}i {rds:f2}>{ods:f2} MiB) {rc} RU {latency} ms",
                        desc, r.Resource, m.RetrievedDocumentCount, miB m.RetrievedDocumentSize, miB m.OutputDocumentSize, r.RequestCharge, sw.ElapsedMilliseconds)
        return r.Resource }

module Expressions =

    module Expression =
        let replace find replace =
            { new ExpressionVisitor() with
                override _.Visit node =
                    if node = find then replace
                    else base.Visit node }
        // https://stackoverflow.com/a/8829845/11635
        let compose (selector: Expression<Func<'T, 'I>>) (predicate: Expression<Func<'I, bool>>) =
            let param = Expression.Parameter(typeof<'T>, "x")
            let prop = (replace selector.Parameters[0] param).Visit(selector.Body)
            let body = (replace predicate.Parameters[0] prop).Visit(predicate.Body)
            Expression.Lambda<Func<'T, bool>>(body, param)

    type IQueryable<'T> with
        member source.OrderBy(indexSelector: Expression<System.Func<'T, 'I>>, propertyName: string, descending) =
            let indexSortProperty = Expression.PropertyOrField(indexSelector.Body, propertyName)
            let keySelector = Expression.Lambda(indexSortProperty, indexSelector.Parameters[0])
            let call = Expression.Call(
                typeof<Queryable>,
                (if descending then "OrderByDescending" else "OrderBy"),
                [| typeof<'T>; indexSortProperty.Type |],
                source.Expression,
                keySelector)
            source.Provider.CreateQuery<'T>(call)

type Query<'T, 'P0, 'P1, 'R>(container, description, query: IQueryable<'T>, render: Expressions.Expression<Func<'T, 'P0>>, hydrate: 'P1 -> 'R) =
    member _.CountAsync() = query |> Internal.count description
    member _.HydratePage(pageSize, pageIndex): Async<'R[]> =
        let items = query.Select(render)
        Internal.page<'P0, 'P1, 'R> description container pageSize pageIndex items hydrate
    member x.ItemsAndCount(pageSize, pageIndex) = async {
        let countQ = x.CountAsync() // start in parallel
        let! items = x.HydratePage(pageSize, pageIndex)
        let! count = countQ |> Async.AwaitTask
        return items, count }

module Index =

    [<NoComparison; NoEquality>]
    type Item<'I> =
        {   p: string
            _etag: string
            u: Unfold<'I> ResizeArray }
    and [<NoComparison; NoEquality>]
        Unfold<'I> =
        {   c: string
            d: 'I }

    let inline prefix categoryName = $"%s{categoryName}-"
    let queryCategory<'I> (container: Microsoft.Azure.Cosmos.Container) categoryName: IQueryable<Item<'I>> =
        let prefix = prefix categoryName
        container.GetItemLinqQueryable<Item<'I>>().Where(fun d -> d.p.StartsWith(prefix))
    let queryIndex<'I> (container: Microsoft.Azure.Cosmos.Container) categoryName caseName: IQueryable<Item<'I>> =
        let prefix = prefix categoryName
        container.GetItemLinqQueryable<Item<'I>>().Where(fun d -> d.p.StartsWith(prefix) && d.u[0].c = caseName)

    let tryStreamName desc container (query: IQueryable<Item<'I>>): Async<FsCodec.StreamName option> =
        Internal.tryScalar desc container (query.Select(fun x -> x.p)) FsCodec.StreamName.Internal.trust

    // We want to generate a projection statement of the shape: VALUE {"sn": root["p"], "snap": root["u"][0].["d"]}
    // However the Cosmos SDK does not support F# (or C#) records yet https://github.com/Azure/azure-cosmos-dotnet-v3/issues/3728
    // F#'s LINQ support cannot translate parameterless constructor invocations in a Lambda well;
    //  the best native workaround without Expression Manipulation is/was https://stackoverflow.com/a/78206722/11635
    // In C#, you can generate an Expression that works with the Cosmos SDK via `.Select(x => new { sn = x.p, snap = x.u[0].d })`
    // This hack is based on https://stackoverflow.com/a/73506241/11635
    type SnAndSnap<'I>() =
        member val sn: FsCodec.StreamName = Unchecked.defaultof<_> with get, set
        [<System.Text.Json.Serialization.JsonConverter(typeof<Equinox.CosmosStore.Core.JsonCompressedBase64Converter>)>]
        member val snap: 'I = Unchecked.defaultof<_> with get, set
        static member FromIndexQuery(snapExpression: Expression<System.Func<Item<'I>, 'I>>) =
            let param = Expression.Parameter(typeof<Item<'I>>, "x")
            let targetType = typeof<SnAndSnap<'I>>
            let snMember = targetType.GetMember(nameof Unchecked.defaultof<SnAndSnap<'I>>.sn)[0]
            let snapMember = targetType.GetMember(nameof Unchecked.defaultof<SnAndSnap<'I>>.snap)[0]
            Expression.Lambda<System.Func<Item<'I>, SnAndSnap<'I>>>(
                Expression.MemberInit(
                    Expression.New(targetType.GetConstructor [||]),
                    [|  Expression.Bind(snMember, Expression.PropertyOrField(param, nameof Unchecked.defaultof<Item<'I>>.p)) :> MemberBinding
                        Expression.Bind(snapMember, (Expressions.Expression.replace snapExpression.Parameters[0] param).Visit(snapExpression.Body)) |]),
                [| param |])

/// Enables querying based on an Index stored
[<NoComparison; NoEquality>]
type IndexContext<'I>(container, categoryName, caseName) =

    member val Description = $"{categoryName}/{caseName}" with get, set

    /// Fetches a base Queryable that's filtered based on the `categoryName` and `caseName`
    /// NOTE this is relatively expensive to compute a Count on, compared to `CategoryQueryable`
    member _.Queryable(): IQueryable<Index.Item<'I>> = Index.queryIndex<'I> container categoryName caseName

    /// Fetches a base Queryable that's filtered only on the `categoryName`
    member _.CategoryQueryable(): IQueryable<Index.Item<'I>> = Index.queryCategory<'I> container categoryName

    /// Runs the query; yields the StreamName from the TOP 1 Item matching the criteria
    member x.TryStreamNameWhere(criteria: Expression<Func<Index.Item<'I>, bool>>): Async<FsCodec.StreamName option> =
        Index.tryStreamName x.Description container (x.Queryable().Where(criteria))

    /// Query the items, `Select()`ing as type `P` per the `render` function. Items are parsed from the results via the `hydrate` function
    member x.Query<'P0, 'P1, 'R>(query: IQueryable<Index.Item<'I>>, render: Expression<Func<Index.Item<'I>, 'P0>>, hydrate: 'P1 -> 'R) =
        Query<Index.Item<'I>, 'P0, 'P1, 'R>(container, x.Description, query, render, hydrate)

    /// Query the items, grabbing the Stream name and the Snapshot; The StreamName and the (Decompressed if applicable) Snapshot are passed to `hydrate`
    member x.QueryStreamNameAndSnapshot(query, renderSnapshot, hydrate) =
        x.Query<Index.SnAndSnap<'I>, Index.SnAndSnap<System.Text.Json.JsonElement>, 'R>(query, Index.SnAndSnap<'I>.FromIndexQuery renderSnapshot, hydrate)
