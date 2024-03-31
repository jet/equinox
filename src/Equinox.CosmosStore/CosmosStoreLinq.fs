namespace Equinox.CosmosStore.Linq

open Equinox.Core.Infrastructure
open FSharp.Control // taskSeq
open Serilog
open System
open System.ComponentModel
open System.Linq
open System.Linq.Expressions

/// Generic Expression Tree manipulation helpers / Cosmos SDK LINQ support incompleteness workarounds
type [<AbstractClass; Sealed>] QueryExtensions =
    static member Replace(find, replace) = // https://stackoverflow.com/a/8829845/11635
        { new ExpressionVisitor() with
            override _.Visit node =
                if node = find then replace
                else base.Visit node }
    static member Compose(selector: Expression<Func<'T, 'I>>, predicate: Expression<Func<'I, bool>>) =
        let param = Expression.Parameter(typeof<'T>, "x")
        let prop = QueryExtensions.Replace(selector.Parameters[0], param).Visit(selector.Body)
        let body = QueryExtensions.Replace(predicate.Parameters[0], prop).Visit(predicate.Body)
        Expression.Lambda<Func<'T, bool>>(body, param)
    [<System.Runtime.CompilerServices.Extension>]
    static member Where(source: IQueryable<'T>, indexSelector: Expression<Func<'T, 'I>>, indexPredicate: Expression<Func<'I, bool>>): IQueryable<'T> =
        source.Where(QueryExtensions.Compose(indexSelector, indexPredicate))
    [<System.Runtime.CompilerServices.Extension>]
    static member OrderBy(source: IQueryable<'T>, indexSelector: Expression<Func<'T, 'I>>, propertyName: string, descending) =
        let indexSortProperty = Expression.PropertyOrField(indexSelector.Body, propertyName)
        let keySelector = Expression.Lambda(indexSortProperty, indexSelector.Parameters[0])
        let call = Expression.Call(
            typeof<Queryable>,
            (if descending then "OrderByDescending" else "OrderBy"),
            [| typeof<'T>; indexSortProperty.Type |],
            source.Expression,
            keySelector)
        source.Provider.CreateQuery<'T>(call)

module Internal =
    open Microsoft.Azure.Cosmos
    open Microsoft.Azure.Cosmos.Linq
    let inline miB x = float x / 1024. / 1024.
    module Query =
        /// Generates an `OFFSET skip LIMIT take` Cosmos SQL query
        /// NOTE: such a query gets more expensive the more your Skip traverses, so use with care
        /// NOTE: (continuation tokens are the key to more linear costs)
        let offsetLimit (skip: int, take: int) (query: IQueryable<'T>) =
            query.Skip(skip).Take(take)
        let [<EditorBrowsable(EditorBrowsableState.Never)>] enum_ (iterator: FeedIterator<'T>) = taskSeq {
            while iterator.HasMoreResults do
                let! response = iterator.ReadNextAsync()
                let m = response.Diagnostics.GetQueryMetrics().CumulativeMetrics
                yield struct (response.Diagnostics.GetClientElapsedTime(), response.RequestCharge, response.Resource,
                              int m.RetrievedDocumentCount, int m.RetrievedDocumentSize, int m.OutputDocumentSize) }
        let toAsyncEnum<'T> (desc: string) (iterator: FeedIterator<'T>) = taskSeq {
            let sw = System.Diagnostics.Stopwatch.StartNew()
            use _ = iterator
            let mutable responses, items, totalRtt, totalRu, totalRdc, totalRds, totalOds = 0, 0, TimeSpan.Zero, 0., 0, 0, 0
            try for rtt, rc, response, rdc, rds, ods in enum_ iterator do
                    responses <- responses + 1
                    totalRdc <- totalRdc + rdc
                    totalRds <- totalRds + rds
                    totalOds <- totalOds + ods
                    totalRu <- totalRu + rc
                    totalRtt <- totalRtt + rtt
                    for item in response do
                        items <- items + 1
                        yield item
            finally Log.Information("CosmosStoreQuery.enum {desc} {count} ({trips}r {totalRtt:f0}ms; {rdc}i {rds:f2}>{ods:f2} MiB) {rc} RU {latency} ms",
                                    desc, items,  responses, totalRtt.TotalMilliseconds, totalRdc, miB totalRds, miB totalOds, totalRu, sw.ElapsedMilliseconds) }
        /// Runs a query that renders 'T, Hydrating the results as 'P (can be the same types but e.g. you might want to map an object to a JsonElement etc)
        let enum<'T, 'P> desc (container: Container) (query: IQueryable<'T>): TaskSeq<'P> =
            let queryDefinition = query.ToQueryDefinition()
            if Log.IsEnabled Serilog.Events.LogEventLevel.Debug then Log.Debug("CosmosStoreQuery.query {desc} {query}", desc, queryDefinition.QueryText)
            container.GetItemQueryIterator<'P>(queryDefinition) |> toAsyncEnum<'P> desc
    module AggregateOp =
        /// Runs one of the typical Cosmos SDK extensions, e.g. CountAsync, logging the costs
        let exec (desc: string) (query: IQueryable<'T>) run render: System.Threading.Tasks.Task<'R> = task {
            if Log.IsEnabled Serilog.Events.LogEventLevel.Debug then Log.Debug("CosmosStoreQuery.exec {desc} {query}", desc, query.ToQueryDefinition().QueryText)
            let sw = System.Diagnostics.Stopwatch.StartNew()
            let! (rsp: Response<'R>) = run query
            let res = rsp.Resource
            let summary = render res
            let m = rsp.Diagnostics.GetQueryMetrics().CumulativeMetrics
            Log.Information("CosmosStoreQuery.count {res} {desc} {count} ({rdc}i {rds:f2}>{ods:f2} MiB) {rc} RU {latency} ms",
                            desc, summary, m.RetrievedDocumentCount, miB m.RetrievedDocumentSize, miB m.OutputDocumentSize, rsp.RequestCharge, sw.ElapsedMilliseconds)
            return res }
        /// Runs query.CountAsync, with instrumentation equivalent to what query provides
        let countAsync desc (query: IQueryable<'T>) ct =
            exec desc query (_.CountAsync(ct)) id<int>
    module Scalar =
        /// Generates a TOP 1 SQL query
        let top1 (query: IQueryable<'T>) =
            query.Take(1)
        /// Handles a query that's expected to yield 0 or 1 result item
        let tryHeadAsync<'T, 'R> desc (container: Container) (query: IQueryable<'T>) (_ct: CancellationToken): Task<'R option> =
            let queryDefinition = (top1 query).ToQueryDefinition()
            if Log.IsEnabled Serilog.Events.LogEventLevel.Debug then Log.Debug("CosmosStoreQuery.tryScalar {desc} {query}", desc, queryDefinition.QueryText)
            container.GetItemQueryIterator<'R>(queryDefinition) |> Query.toAsyncEnum desc |> TaskSeq.tryHead
    type Projection<'T, 'M>(query, description, container, enum: IQueryable<'T> -> TaskSeq<'M>) =
        static member Create<'P>(q, d, c, hydrate: 'P -> 'M) = Projection<'T, 'M>(q, d, c, Query.enum<'T, 'P> d c >> TaskSeq.map hydrate)
        member _.Enum: TaskSeq<'M> = query |> enum
        member x.EnumPage(skip, take): TaskSeq<'M> = query |> Query.offsetLimit (skip, take) |> enum
        member _.CountAsync: CancellationToken -> Task<int> = query |> AggregateOp.countAsync description
        [<EditorBrowsable(EditorBrowsableState.Never)>] member val Query: IQueryable<'T> = query
        [<EditorBrowsable(EditorBrowsableState.Never)>] member val Description: string = description
        [<EditorBrowsable(EditorBrowsableState.Never)>] member val Container: Container = container

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
    static member CreateItemQueryLambda<'T>(snExpression: Expression -> MemberExpression, snapExpression: Expression<Func<'T, 'I>>) =
        let param = Expression.Parameter(typeof<'T>, "x")
        let targetType = typeof<SnAndSnap<'I>>
        let snMember = targetType.GetMember(nameof Unchecked.defaultof<SnAndSnap<'I>>.sn)[0]
        let snapMember = targetType.GetMember(nameof Unchecked.defaultof<SnAndSnap<'I>>.snap)[0]
        Expression.Lambda<Func<'T, SnAndSnap<'I>>>(
            Expression.MemberInit(
                Expression.New(targetType.GetConstructor [||]),
                [|  Expression.Bind(snMember, snExpression param) :> MemberBinding
                    Expression.Bind(snapMember, QueryExtensions.Replace(snapExpression.Parameters[0], param).Visit(snapExpression.Body)) |]),
            [| param |])

/// Helpers for Querying and Projecting results based on relevant aspects of Equinox.CosmosStore's storage schema
module Index =

    [<NoComparison; NoEquality>]
    type Item<'I> =
        {   p: string
            _etag: string
            u: Unfold<'I> ResizeArray }
    and [<NoComparison; NoEquality>] Unfold<'I> =
        {   c: string
            d: 'I }

    let inline prefix categoryName = $"%s{categoryName}-"
    /// The cheapest search basis; the categoryName is a prefix of the `p` partition field
    /// Depending on how much more selective the caseName is, `byCaseName` may be a better choice
    /// (but e.g. if the ration is 1:1 then no point having additional criteria)
    let byCategoryNameOnly<'I> (container: Microsoft.Azure.Cosmos.Container) categoryName: IQueryable<Item<'I>> =
        let prefix = prefix categoryName
        container.GetItemLinqQueryable<Item<'I>>().Where(fun d -> d.p.StartsWith(prefix))
    // Searches based on the prefix of the `p` field, but also checking the `c` of the relevant unfold is correct
    // A good idea if that'll be significantly cheaper due to better selectivity
    let byCaseName<'I> (container: Microsoft.Azure.Cosmos.Container) categoryName caseName: IQueryable<Item<'I>> =
        let prefix = prefix categoryName
        container.GetItemLinqQueryable<Item<'I>>().Where(fun d -> d.p.StartsWith(prefix) && d.u[0].c = caseName)

    /// Returns the StreamName (from the `p` field) for a 0/1 item query; only the TOP 1 item is returned
    let tryGetStreamNameAsync description container (query: IQueryable<Item<'I>>) ct =
        Internal.Scalar.tryHeadAsync<string, FsCodec.StreamName> description container (query.Select(fun x -> x.p)) ct

    /// Query the items, returning the Stream name and the Snapshot as a JsonElement (Decompressed if applicable)
    let projectStreamNameAndSnapshot<'I> snapExpression: Expression<Func<Item<'I>, SnAndSnap<'I>>> =
        // a very ugly workaround for not being able to write query.Select<Item<'I>,Internal.SnAndSnap<'I>>(fun x -> { p = x.p; snap = x.u[0].d })
        let pExpression item = Expression.PropertyOrField(item, nameof Unchecked.defaultof<Item<'I>>.p)
        SnAndSnap.CreateItemQueryLambda(pExpression, snapExpression)

/// Represents a query projecting information values from an Index and/or Snapshots with a view to rendering the items and/or a count
type Query<'T, 'M>(inner: Internal.Projection<'T, 'M>) =
    member _.Enum: TaskSeq<'M> = inner.Enum
    member _.EnumPage(skip, take): TaskSeq<'M> = inner.EnumPage(skip, take)
    member _.CountAsync(ct: CancellationToken): Task<int> = inner.CountAsync ct
    member _.Count(): Async<int> = inner.CountAsync |> Async.call
    [<EditorBrowsable(EditorBrowsableState.Never)>] member val Inner = inner

/// Enables querying based on uncompressed Indexed values stored as secondary unfolds alongside the snapshot
[<NoComparison; NoEquality>]
type IndexContext<'I>(container, categoryName, caseName) =

    member val Description = $"{categoryName}/{caseName}" with get, set
    member val Container = container

    /// Fetches a base Queryable that's filtered based on the `categoryName` and `caseName`
    /// NOTE this is relatively expensive to compute a Count on, compared to `CategoryQueryable`
    member _.ByCaseName(): IQueryable<Index.Item<'I>> =
        Index.byCaseName<'I> container categoryName caseName

    /// Fetches a base Queryable that's filtered only on the `categoryName`
    member _.ByCategory(): IQueryable<Index.Item<'I>> =
        Index.byCategoryNameOnly<'I> container categoryName

    /// Runs the query; yields the StreamName from the TOP 1 Item matching the criteria
    member x.TryGetStreamNameWhereAsync(criteria: Expressions.Expression<Func<Index.Item<'I>, bool>>, ct) =
        Index.tryGetStreamNameAsync x.Description container (x.ByCategory().Where criteria) ct

    /// Runs the query; yields the StreamName from the TOP 1 Item matching the criteria
    member x.TryGetStreamNameWhere(criteria: Expressions.Expression<Func<Index.Item<'I>, bool>>): Async<FsCodec.StreamName option> =
        (fun ct -> x.TryGetStreamNameWhereAsync(criteria, ct)) |> Async.call

    /// Query the items, grabbing the Stream name and the Snapshot; The StreamName and the (Decompressed if applicable) Snapshot are passed to `hydrate`
    member x.QueryStreamNameAndSnapshot(query: IQueryable<Index.Item<'I>>, selectBody: Expression<Func<Index.Item<'I>, 'I>>,
                                        hydrate: SnAndSnap<System.Text.Json.JsonElement> -> 'M) =
        Internal.Projection.Create(query.Select(Index.projectStreamNameAndSnapshot<'I> selectBody), x.Description, container, hydrate) |> Query
