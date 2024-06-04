namespace Equinox.CosmosStore.Linq

open Equinox.Core
open Equinox.CosmosStore.Core // Log, JsonCompressedBase64Converter
open FSharp.Control // taskSeq
open Serilog
open System
open System.ComponentModel
open System.Linq
open System.Linq.Expressions
open System.Runtime.CompilerServices

/// Generic Expression Tree manipulation helpers / Cosmos SDK LINQ support incompleteness workarounds
type [<AbstractClass; Sealed>] QueryExtensions =
    static member Replace(find, replace) =
        { new ExpressionVisitor() with
            override _.Visit node =
                if node = find then replace
                else base.Visit node }
    [<Extension>]
    static member Replace(x: Expression, find, replace) = QueryExtensions.Replace(find, replace).Visit(x)
    [<Extension>] // https://stackoverflow.com/a/8829845/11635
    static member Compose(selector: Expression<Func<'T, 'I>>, projector: Expression<Func<'I, 'R>>): Expression<Func<'T, 'R>> =
        let param = Expression.Parameter(typeof<'T>, "x")
        let prop = selector.Body.Replace(selector.Parameters[0], param)
        let body = projector.Body.Replace(projector.Parameters[0], prop)
        Expression.Lambda<Func<'T, 'R>>(body, param)
    [<Extension>]
    static member OrderBy(source: IQueryable<'T>, indexSelector: Expression<Func<'T, 'I>>, keySelector: Expression<Func<'I, 'U>>, descending) =
        QueryExtensions.OrderByLambda<'T>(source, indexSelector.Compose keySelector, descending)
    [<Extension>] // https://stackoverflow.com/a/233505/11635
    static member OrderByPropertyName(source: IQueryable<'T>, indexSelector: Expression<Func<'T, 'I>>, propertyName: string, descending) =
        let indexProperty = Expression.PropertyOrField(indexSelector.Body, propertyName)
        let delegateType = typedefof<Func<_,_>>.MakeGenericType(typeof<'T>, indexProperty.Type)
        let keySelector = Expression.Lambda(delegateType, indexProperty, indexSelector.Parameters[0])
        QueryExtensions.OrderByLambda(source, keySelector, descending)
    // NOTE not an extension method as OrderByPropertyName and OrderBy represent the as-strongly-typed-as-possible top level use cases
    // NOTE no support for a `comparison` arg is warranted as CosmosDB only handles direct scalar prop expressions, https://stackoverflow.com/a/69268191/11635
    static member OrderByLambda<'T>(source: IQueryable<'T>, keySelector: LambdaExpression, descending) =
        let call = Expression.Call(
            typeof<Queryable>,
            (if descending then "OrderByDescending" else "OrderBy"),
            [| typeof<'T>; keySelector.ReturnType |],
            [| source.Expression; keySelector |])
        source.Provider.CreateQuery<'T>(call) :?> IOrderedQueryable<'T>

/// Predicate manipulation helpers
type [<AbstractClass; Sealed>] Predicate =
    /// F# maps `fun` expressions to Expression trees, only when the target is a `member` arg
    /// See https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/query/linq-to-sql for the list of supported constructs
    static member Create<'T> expr: Expression<Func<'T, bool>> = expr
    [<Extension>] // https://stackoverflow.com/a/22569086/11635
    static member And<'T>(l: Expression<Func<'T, bool>>, r: Expression<Func<'T, bool>>) =
        let rBody = r.Body.Replace(r.Parameters[0], l.Parameters[0])
        Expression.Lambda<Func<'T, bool>>(Expression.AndAlso(l.Body, rBody), l.Parameters)
    [<Extension>] // https://stackoverflow.com/a/22569086/11635
    static member Or<'T>(l: Expression<Func<'T, bool>>, r: Expression<Func<'T, bool>>) =
        let rBody = r.Body.Replace(r.Parameters[0], l.Parameters[0])
        Expression.Lambda<Func<'T, bool>>(Expression.OrElse(l.Body, rBody), l.Parameters)
    [<Extension>]
    static member Where(source: IQueryable<'T>, indexSelector: Expression<Func<'T, 'I>>, indexPredicate: Expression<Func<'I, bool>>): IQueryable<'T> =
        source.Where(indexSelector.Compose indexPredicate)

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
        /// Runs a query that can be hydrated as 'T
        let enum<'T> (log: ILogger) (container: Container) cat (iterator: FeedIterator<'T>) = taskSeq {
            let startTicks = System.Diagnostics.Stopwatch.GetTimestamp()
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
            finally
                let interval = StopwatchInterval(startTicks, System.Diagnostics.Stopwatch.GetTimestamp())
                let log = let evt = Log.Metric.Index { database = container.Database.Id; container = container.Id; stream = cat + FsCodec.StreamName.Category.SeparatorStr
                                                       interval = interval; bytes = totalOds; count = items; ru = totalRu } in log |> Log.event evt
                log.Information("EqxCosmos {action:l} {count} ({trips}r {totalRtt:f0}ms; {rdc}i {rds:f2}>{ods:f2} MiB) {rc:f2} RU {lat:n0} ms",
                                "Index", items, responses, totalRtt.TotalMilliseconds, totalRdc, miB totalRds, miB totalOds, totalRu, interval.ElapsedMilliseconds) }
        /// Runs a query that renders 'T, Hydrating the results as 'P (can be the same types but e.g. you might want to map an object to a JsonElement etc)
        let enumAs<'T, 'P> (log: ILogger) (container: Container) cat logLevel (query: IQueryable<'T>): TaskSeq<'P> =
            let queryDefinition = query.ToQueryDefinition()
            if log.IsEnabled logLevel then log.Write(logLevel, "CosmosStoreQuery.query {cat} {query}", cat, queryDefinition.QueryText)
            container.GetItemQueryIterator<'P> queryDefinition |> enum log container cat
    module AggregateOp =
        /// Runs one of the typical Cosmos SDK extensions, e.g. CountAsync, logging the costs
        let [<EditorBrowsable(EditorBrowsableState.Never)>] exec (log: ILogger) (container: Container) (op: string) (cat: string) (query: IQueryable<'T>) run render: System.Threading.Tasks.Task<'R> = task {
            let startTicks = System.Diagnostics.Stopwatch.GetTimestamp()
            let! (rsp: Response<'R>) = run query
            let res = rsp.Resource
            let summary = render res
            let m = rsp.Diagnostics.GetQueryMetrics().CumulativeMetrics
            let interval = StopwatchInterval(startTicks, System.Diagnostics.Stopwatch.GetTimestamp())
            let totalOds, totalRu = m.OutputDocumentSize, rsp.RequestCharge
            let log = let evt = Log.Metric.Index { database = container.Database.Id; container = container.Id; stream = cat + FsCodec.StreamName.Category.SeparatorStr
                                                   interval = interval; bytes = int totalOds; count = -1; ru = totalRu } in log |> Log.event evt
            log.Information("EqxCosmos {action:l} {cat} {count} ({rdc}i {rds:f2}>{ods:f2} MiB) {rc} RU {lat:n0} ms",
                            op, cat, summary, m.RetrievedDocumentCount, miB m.RetrievedDocumentSize, miB totalOds, totalRu, interval.ElapsedMilliseconds)
            return res }
        /// Runs query.CountAsync, with instrumentation equivalent to what query provides
        let countAsync (log: ILogger) container cat logLevel (query: IQueryable<'T>) ct =
            if log.IsEnabled logLevel then log.Write(logLevel, "CosmosStoreQuery.count {cat} {query}", cat, query.ToQueryDefinition().QueryText)
            exec log container "count" cat query (_.CountAsync(ct)) id<int>
    module Scalar =
        /// Generates a TOP 1 SQL query
        let top1 (query: IQueryable<'T>) =
            query.Take(1)
        /// Handles a query that's expected to yield 0 or 1 result item
        let tryHeadAsync<'T, 'R> (log: ILogger) (container: Container) cat logLevel (query: IQueryable<'T>) (_ct: CancellationToken): Task<'R option> =
            let queryDefinition = (top1 query).ToQueryDefinition()
            if log.IsEnabled logLevel then log.Write(logLevel, "CosmosStoreQuery.tryScalar {cat} {query}", queryDefinition.QueryText)
            container.GetItemQueryIterator<'R> queryDefinition |> Query.enum log container cat |> TaskSeq.tryHead
    type Projection<'T, 'M>(query, category, container, enum: IQueryable<'T> -> TaskSeq<'M>, count: IQueryable<'T> -> CancellationToken -> Task<int>) =
        static member Create<'P>(q, cat, c, log, hydrate: 'P -> 'M, logLevel) =
             Projection<'T, 'M>(q, cat, c, Query.enumAs<'T, 'P> log c cat logLevel >> TaskSeq.map hydrate, AggregateOp.countAsync log c cat logLevel)
        member _.Enum: TaskSeq<'M> = query |> enum
        member x.EnumPage(skip, take): TaskSeq<'M> = query |> Query.offsetLimit (skip, take) |> enum
        member _.CountAsync: CancellationToken -> Task<int> = query |> count
        [<EditorBrowsable(EditorBrowsableState.Never)>] member val Query: IQueryable<'T> = query
        [<EditorBrowsable(EditorBrowsableState.Never)>] member val Category: string = category
        [<EditorBrowsable(EditorBrowsableState.Never)>] member val Container: Container = container

// We want to generate a projection statement of the shape: VALUE {"sn": root["p"], "d": root["u"][0].["d"], "D": root["u"][0].["D"]}
// However the Cosmos SDK does not support F# (or C#) records yet https://github.com/Azure/azure-cosmos-dotnet-v3/issues/3728
// F#'s LINQ support cannot translate parameterless constructor invocations in a Lambda well;
//  the best native workaround without Expression Manipulation is/was https://stackoverflow.com/a/78206722/11635
// In C#, you can generate an Expression that works with the Cosmos SDK via `.Select(x => new { sn = x.p, d = x.u[0].d, D = x.u[0].D })`
// This hack is based on https://stackoverflow.com/a/73506241/11635
type SnAndSnap() =
    member val sn: FsCodec.StreamName = Unchecked.defaultof<_> with get, set
    member val d: System.Text.Json.JsonElement = Unchecked.defaultof<_> with get, set
    member val D: int = Unchecked.defaultof<_> with get, set
    static member CreateItemQueryLambda<'T, 'U>(
            snExpression: Expression -> MemberExpression,
            uExpression: Expression<Func<'T, 'U>>) =
        let param = Expression.Parameter(typeof<'T>, "x")
        let targetType = typeof<SnAndSnap>
        let snMember = targetType.GetMember(nameof Unchecked.defaultof<SnAndSnap>.sn)[0]
        let dMember = targetType.GetMember(nameof Unchecked.defaultof<SnAndSnap>.d)[0]
        let formatMember = targetType.GetMember(nameof Unchecked.defaultof<SnAndSnap>.D)[0]
        Expression.Lambda<Func<'T, SnAndSnap>>(
            Expression.MemberInit(
                Expression.New(targetType.GetConstructor [||]),
                [|  Expression.Bind(snMember, snExpression param) :> MemberBinding
                    Expression.Bind(dMember, uExpression.Body.Replace(uExpression.Parameters[0], param))
                    Expression.Bind(formatMember, uExpression.Body.Replace(uExpression.Parameters[0], param)) |]),
            [| param |])

/// Represents a query projecting information values from an Index and/or Snapshots with a view to rendering the items and/or a count
type Query<'T, 'M>(inner: Internal.Projection<'T, 'M>) =
    member _.Enum: TaskSeq<'M> = inner.Enum
    member _.EnumPage(skip, take): TaskSeq<'M> = inner.EnumPage(skip, take)
    member _.CountAsync(ct: CancellationToken): Task<int> = inner.CountAsync ct
    member _.Count(): Async<int> = inner.CountAsync |> Async.call
    [<EditorBrowsable(EditorBrowsableState.Never)>] member val Inner = inner

/// Helpers for Querying and Projecting results based on relevant aspects of Equinox.CosmosStore's storage schema
module Index =

    [<NoComparison; NoEquality>]
    type Item =
        {   p: string
            _etag: string
            u: Unfold ResizeArray }
    and [<NoComparison; NoEquality>] Unfold =
        {   c: string
            d: System.Text.Json.JsonElement
            D: int }

    let inline prefix categoryName = $"%s{categoryName}-"
    /// The cheapest search basis; the categoryName is a prefix of the `p` partition field
    /// Depending on how much more selective the caseName is, `byCaseName` may be a better choice
    /// (but e.g. if the ration is 1:1 then no point having additional criteria)
    let byCategoryNameOnly<'I> (container: Microsoft.Azure.Cosmos.Container) categoryName: IQueryable<Item> =
        let prefix = prefix categoryName
        container.GetItemLinqQueryable<Item>().Where(fun d -> d.p.StartsWith(prefix))
    // Searches based on the prefix of the `p` field, but also checking the `c` of the relevant unfold is correct
    // A good idea if that'll be significantly cheaper due to better selectivity
    let byCaseName<'I> (container: Microsoft.Azure.Cosmos.Container) categoryName caseName: IQueryable<Item> =
        let prefix = prefix categoryName
        container.GetItemLinqQueryable<Item>().Where(fun d -> d.p.StartsWith(prefix) && d.u[0].c = caseName)

    /// Returns the StreamName (from the `p` field) for a 0/1 item query; only the TOP 1 item is returned
    let tryGetStreamNameAsync log cat logLevel container (query: IQueryable<Item>) ct =
        Internal.Scalar.tryHeadAsync<string, FsCodec.StreamName> log cat logLevel container (query.Select(fun x -> x.p)) ct

    // /// Query the items, returning the Stream name and the Snapshot as a JsonElement (Decompressed if applicable)
    // let projectStreamNameAndSnapshot<'I> uExpression: Expression<Func<Item, SnAndSnap>> =
    //     // a very ugly workaround for not being able to write query.Select<Item,Internal.SnAndSnap>(fun x -> { p = x.p; d = x.u[0].d; D = x.u[0].D })
    //     let pExpression item = Expression.PropertyOrField(item, nameof Unchecked.defaultof<Item>.p)
    //     let uItem name item = Expression.PropertyOrField(uExpression, name)
    //     SnAndSnap.CreateItemQueryLambda(pExpression, uExpression, uItem (nameof Unchecked.defaultof<Unfold>.d), uItem (nameof Unchecked.defaultof<Unfold>.D))

    let createSnAndSnapshotQuery<'M> log container cat logLevel (hydrate: SnAndSnap -> 'M) (query: IQueryable<SnAndSnap>) =
        Internal.Projection.Create(query, cat, container, log, hydrate, logLevel) |> Query<SnAndSnap, 'M>

/// Enables querying based on uncompressed Indexed values stored as secondary unfolds alongside the snapshot
[<NoComparison; NoEquality>]
type IndexContext<'I>(container, categoryName, caseName, log, [<O; D null>]?queryLogLevel) =

    let queryLogLevel = defaultArg queryLogLevel Serilog.Events.LogEventLevel.Debug
    member val Log = log
    member val Description = $"{categoryName}/{caseName}" with get, set
    member val Container = container

    /// Helper to make F# consumption code more terse (the F# compiler generates Expression trees only when a function is passed to a `member`)
    /// Example: `i.Predicate(fun e -> e.name = name)`
    /// See https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/query/linq-to-sql for the list of supported constructs
    member _.Predicate expr: Expression<Func<'I, bool>> = expr

    /// Fetches a base Queryable that's filtered based on the `categoryName` and `caseName`
    /// NOTE this is relatively expensive to compute a Count on, compared to `CategoryQueryable`
    member _.ByCaseName(): IQueryable<Index.Item> =
        Index.byCaseName<'I> container categoryName caseName

    /// Fetches a base Queryable that's filtered only on the `categoryName`
    member _.ByCategory(): IQueryable<Index.Item> =
        Index.byCategoryNameOnly container categoryName

    /// Runs the query; yields the StreamName from the TOP 1 Item matching the criteria
    member x.TryGetStreamNameWhereAsync(criteria: Expressions.Expression<Func<Index.Item, bool>>, ct, [<O; D null>] ?logLevel) =
        let logLevel = defaultArg logLevel queryLogLevel
        Index.tryGetStreamNameAsync x.Log container categoryName logLevel (x.ByCategory().Where criteria) ct

    /// Runs the query; yields the StreamName from the TOP 1 Item matching the criteria
    member x.TryGetStreamNameWhere(criteria: Expressions.Expression<Func<Index.Item, bool>>): Async<FsCodec.StreamName option> =
        (fun ct -> x.TryGetStreamNameWhereAsync(criteria, ct)) |> Async.call

    // /// Query the items, grabbing the Stream name and the Snapshot; The StreamName and the (Decompressed if applicable) Snapshot are passed to `hydrate`
    // member x.QueryStreamNameAndSnapshot(query: IQueryable<Index.Item>, selectBody: Expression<Func<Index.Item, Index.Unfold>>,
    //                                     hydrate: SnAndSnap -> 'M,
    //                                     [<O; D null>] ?logLevel): Query<SnAndSnap, 'M> =
    //     let logLevel = defaultArg logLevel queryLogLevel
    //     query.Select(Index.projectStreamNameAndSnapshot selectBody)
    //     |> Index.createSnAndSnapshotQuery x.Log container categoryName logLevel hydrate
