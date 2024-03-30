namespace Equinox.CosmosStore.Linq

open Equinox.Core.Infrastructure
open FSharp.Control
open Serilog
open System
open System.ComponentModel
open System.Linq
open System.Linq.Expressions

module Internal =

    open Microsoft.Azure.Cosmos
    [<EditorBrowsable(EditorBrowsableState.Never)>] // In case of emergency, use this, but log an issue so we can understand why
    let enum_ (iterator: FeedIterator<'T>) = taskSeq {
        while iterator.HasMoreResults do
            let! response = iterator.ReadNextAsync()
            let m = response.Diagnostics.GetQueryMetrics().CumulativeMetrics
            yield struct (response.Diagnostics.GetClientElapsedTime(), response.RequestCharge, response.Resource,
                          int m.RetrievedDocumentCount, int m.RetrievedDocumentSize, int m.OutputDocumentSize) }
    let inline miB x = float x / 1024. / 1024.
    let taskEnum<'T> (desc: string) (iterator: FeedIterator<'T>) = taskSeq {
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

    (* Query preparation *)

    /// Generates a TOP 1 SQL query
    let top1 (query: IQueryable<'T>) =
        query.Take(1)
    /// Generates an `OFFSET skip LIMIT take` Cosmos SQL query
    /// NOTE: such a query gets more expensive the more your Skip traverses, so use with care
    /// NOTE: (continuation tokens are the key to more linear costs)
    let offsetLimit (skip: int, take: int) (query: IQueryable<'T>) =
        query.Skip(skip).Take(take)

    (* IAsyncEnumerable aka TaskSeq wrapping *)

    open Microsoft.Azure.Cosmos.Linq
    /// Runs a query that renders 'T, Hydrating the results as 'R (can be the same types but e.g. you might want to map an object to a JsonElement etc)
    let enum<'T, 'R> desc (container: Container) (query: IQueryable<'T>): TaskSeq<'R> =
        let queryDefinition = query.ToQueryDefinition()
        if Log.IsEnabled Serilog.Events.LogEventLevel.Debug then Log.Debug("CosmosStoreQuery.query {desc} {query}", desc, queryDefinition.QueryText)
        container.GetItemQueryIterator<'R>(queryDefinition) |> taskEnum desc

    (* Scalar call dispatch *)

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
    /// Run's query.CountAsync, with instrumentation equivalent to what query provides
    let countAsync desc (query: IQueryable<'T>) ct =
        exec desc query (_.CountAsync(ct)) id<int>

    let tryHeadAsync<'T, 'R> desc (container: Container) (query: IQueryable<'T>) (_ct: CancellationToken) =
        let queryDefinition = (top1 query).ToQueryDefinition()
        if Log.IsEnabled Serilog.Events.LogEventLevel.Debug then Log.Debug("CosmosStoreQuery.tryScalar {desc} {query}", desc, queryDefinition.QueryText)
        container.GetItemQueryIterator<'R>(queryDefinition) |> taskEnum desc |> TaskSeq.tryHead

    /// Encapsulates an Indexed query expression
    type Queryable<'T, 'P, 'R>(container, description, query: IQueryable<'T>, render: Expressions.Expression<Func<'T, 'P>>) =
        member _.Enum<'M>(query, hydrate: 'R -> 'M): TaskSeq<'M> = enum<'P, 'R> description container query |> TaskSeq.map hydrate
        member _.CountAsync ct: System.Threading.Tasks.Task<int> =
            countAsync description query ct
        member _.Count(): Async<int> =
            countAsync description query System.Threading.CancellationToken.None |> Async.AwaitTask
        member x.Fetch<'M>(hydrate): TaskSeq<'M> =
            x.Enum<'M>(query.Select render, hydrate)
        member x.FetchPage<'M>(skip, take, hydrate): TaskSeq<'M> =
            x.Enum<'M>(query.Select render |> offsetLimit (skip, take), hydrate)
        (* In case of emergency, use these. but log an issue so we can understand why *)
        [<EditorBrowsable(EditorBrowsableState.Never)>] member val Container = container
        [<EditorBrowsable(EditorBrowsableState.Never)>] member val Description = description
        [<EditorBrowsable(EditorBrowsableState.Never)>] member val Query = query
        [<EditorBrowsable(EditorBrowsableState.Never)>] member val Render = render

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

type Queryable<'T, 'P, 'R, 'M>(inner: Internal.Queryable<'T, 'P, 'R>, hydrate: 'R -> 'M) =
    member val Inner = inner
    member _.CountAsync = inner.CountAsync
    member _.Count(): Async<int> = inner.Count()
    member _.Fetch(): TaskSeq<'M> = inner.Fetch hydrate
    member _.FetchPage(skip, take): TaskSeq<'M> = inner.FetchPage(skip, take, hydrate)

module Queryable =

   let map<'T, 'P, 'R, 'M> (hydrate: 'R -> 'M) (inner: Internal.Queryable<'T, 'P, 'R>) = Queryable<'T, 'P, 'R, 'M>(inner, hydrate)

/// Helpers for Querying and Projecting results based on relevant aspects of Equinox.CosmosStore's storage schema
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
    let byCategoryNameOnly<'I> (container: Microsoft.Azure.Cosmos.Container) categoryName: IQueryable<Item<'I>> =
        let prefix = prefix categoryName
        container.GetItemLinqQueryable<Item<'I>>().Where(fun d -> d.p.StartsWith(prefix))
    let byCaseName<'I> (container: Microsoft.Azure.Cosmos.Container) categoryName caseName: IQueryable<Item<'I>> =
        let prefix = prefix categoryName
        container.GetItemLinqQueryable<Item<'I>>().Where(fun d -> d.p.StartsWith(prefix) && d.u[0].c = caseName)

    let tryGetStreamNameAsync description container (query: IQueryable<Item<'I>>) =
        Internal.tryHeadAsync<string, FsCodec.StreamName> description container (query.Select(fun x -> x.p))

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

    (* In case of emergency a) use this b) log an issue as to why you had to; can't think of a good reason *)
    [<EditorBrowsable(EditorBrowsableState.Never)>]
    let streamNameAndSnapshot_<'I, 'R> description container renderSnapshot (query: IQueryable<Item<'I>>) =
        Internal.Queryable<Item<'I>, SnAndSnap<'I>, SnAndSnap<'R>>(container, description, query, SnAndSnap<'I>.FromIndexQuery renderSnapshot)

    /// Query the items, returning the Stream name and the Snapshot as a JsonElement (Decompressed if applicable)
    let streamNameAndSnapshot<'I> description container renderSnapshot (query: IQueryable<Item<'I>>) =
        streamNameAndSnapshot_<'I, System.Text.Json.JsonElement> description container renderSnapshot query

/// Enables querying based on an Index stored
[<NoComparison; NoEquality>]
type IndexContext<'I>(container, categoryName, caseName) =

    member val Description = $"{categoryName}/{caseName}" with get, set

    /// Fetches a base Queryable that's filtered based on the `categoryName` and `caseName`
    /// NOTE this is relatively expensive to compute a Count on, compared to `CategoryQueryable`
    member _.ByCaseName(): IQueryable<Index.Item<'I>> =
        Index.byCaseName<'I> container categoryName caseName

    /// Fetches a base Queryable that's filtered only on the `categoryName`
    member _.ByCategory(): IQueryable<Index.Item<'I>> =
        Index.byCategoryNameOnly<'I> container categoryName

    /// Runs the query; yields the StreamName from the TOP 1 Item matching the criteria
    member x.TryGetStreamNameWhereAsync(criteria: Expression<Func<Index.Item<'I>, bool>>, ct) =
        Index.tryGetStreamNameAsync x.Description container (x.ByCategory().Where(criteria)) ct

    /// Runs the query; yields the StreamName from the TOP 1 Item matching the criteria
    member x.TryGetStreamNameWhere(criteria: Expression<Func<Index.Item<'I>, bool>>): Async<FsCodec.StreamName option> =
        (fun ct -> x.TryGetStreamNameWhereAsync(criteria, ct)) |> Async.call

    /// Query the items, grabbing the Stream name and the Snapshot; The StreamName and the (Decompressed if applicable) Snapshot are passed to `hydrate`
    member x.QueryStreamNameAndSnapshot(query, renderSnapshot, hydrate) =
        Index.streamNameAndSnapshot<'I> x.Description container renderSnapshot query
        |> Queryable.map hydrate
