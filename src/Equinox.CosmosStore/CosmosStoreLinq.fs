namespace Equinox.CosmosStore.Linq

open Equinox.Core
open Equinox.CosmosStore.Core // Log
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
    [<Extension>]
    static member InlineParam(f: Expression<Func<'T, 'I>>, expr) = f.Body.Replace(f.Parameters[0], expr)
    [<Extension>] // https://stackoverflow.com/a/8829845/11635
    static member Compose(selector: Expression<Func<'T, 'I>>, projector: Expression<Func<'I, 'R>>): Expression<Func<'T, 'R>> =
        let param = Expression.Parameter(typeof<'T>, "x")
        let body = projector.InlineParam(selector.InlineParam param)
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

type [<NoComparison; NoEquality>] IProjection<'T> =
    abstract ToAsyncEnumerable: unit -> TaskSeq<'T>
    abstract Page: skip: int * take: int -> TaskSeq<'T>
    abstract CountAsync: ct: CancellationToken -> Task<int>

type [<AbstractClass; Sealed>] ProjectionExtensions =
    [<Extension>] static member Count(x: IProjection<'M>): Async<int> = x.CountAsync |> Async.call
    [<Extension>] static member TryHead(x: TaskSeq<'T>) = TaskSeq.tryHead x |> Async.AwaitTask
    [<Extension>] static member TryHead(x: IProjection<'T>) = x.ToAsyncEnumerable().TryHead()
    [<Extension>] static member All(x: TaskSeq<'T>) = TaskSeq.toArrayAsync x |> Async.AwaitTask
    [<Extension>] static member All(x: IProjection<'T>) = x.ToAsyncEnumerable().All()

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
                let log = if cat = null then log else
                          let evt = Log.Metric.Index { database = container.Database.Id; container = container.Id; stream = cat + FsCodec.StreamName.Category.SeparatorStr
                                                       interval = interval; bytes = totalOds; count = items; ru = totalRu } in log |> Log.event evt
                log.Write(logLevel, "EqxCosmos {action:l} {count} {cat} ({trips}r {totalRtt:f0}ms; {rdc}i {rds:f2}>{ods:f2} MiB) {rc:f2} RU {lat:f0} ms",
                                action, items, cat, responses, totalRtt.TotalMilliseconds, totalRdc, miB totalRds, miB totalOds, totalRu, interval.ElapsedMilliseconds) }
        /// Runs a query that can be hydrated directly as the projected type
        let enum log container cat = enum_ log container "Index" cat Events.LogEventLevel.Information
        let exec__<'R> (log: ILogger) (container: Container) cat logLevel (queryDefinition: QueryDefinition): TaskSeq<'R> =
            if log.IsEnabled logLevel then log.Write(logLevel, "CosmosStoreQuery.run {cat} {query}", cat, queryDefinition.QueryText)
            container.GetItemQueryIterator<'R> queryDefinition |> enum_ log container "Query" cat logLevel
        /// Runs a query that renders 'T, Hydrating the results as 'P (can be the same types but e.g. you might want to map an object to a JsonElement etc.)
        let enumAs<'T, 'P> (log: ILogger) (container: Container) cat logLevel (query: IQueryable<'T>): TaskSeq<'P> =
            let queryDefinition = query.ToQueryDefinition()
            exec__<'P> log container cat logLevel queryDefinition
        /// Execute a query, hydrating as 'R
        let exec<'R> (log: ILogger) (container: Container) logLevel (queryDefinition: QueryDefinition): TaskSeq<'R> =
            exec__<'R> log container "%" logLevel queryDefinition
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
            log.Information("EqxCosmos {action:l} {count} {cat} ({rdc}i {rds:f2}>{ods:f2} MiB) {rc} RU {lat:f0} ms",
                            op, summary, cat, m.RetrievedDocumentCount, miB m.RetrievedDocumentSize, miB totalOds, totalRu, interval.ElapsedMilliseconds)
            return res }
        /// Runs query.CountAsync, with instrumentation equivalent to what query provides
        let countAsync (log: ILogger) container cat logLevel (query: IQueryable<'T>) ct =
            if log.IsEnabled logLevel then log.Write(logLevel, "CosmosStoreQuery.count {cat} {query}", cat, query.ToQueryDefinition().QueryText)
            exec log container "Count" cat query (_.CountAsync(ct)) id<int>
    module Scalar =
        /// Generates a TOP 1 SQL query
        let top1 (query: IQueryable<'T>) =
            query.Take(1)
        /// Handles a query that's expected to yield 0 or 1 result item
        let tryHeadAsync<'T, 'R> (log: ILogger) (container: Container) cat logLevel (query: IQueryable<'T>) (_ct: CancellationToken): Task<'R option> =
            let queryDefinition = (top1 query).ToQueryDefinition()
            if log.IsEnabled logLevel then log.Write(logLevel, "CosmosStoreQuery.tryScalar {cat} {query}", cat, queryDefinition.QueryText)
            container.GetItemQueryIterator<'R> queryDefinition |> Query.enum_ log container "Scalar" cat logLevel |> TaskSeq.tryHead
    type Projection<'T, 'M>(query, category, container, enum: IQueryable<'T> -> TaskSeq<'M>, count: IQueryable<'T> -> CancellationToken -> Task<int>) =
        static member Create<'P>(q, cat, c, log, hydrate: 'P -> 'M, logLevel) =
             Projection<'T, 'M>(q, cat, c, Query.enumAs<'T, 'P> log c cat logLevel >> TaskSeq.map hydrate, AggregateOp.countAsync log c cat logLevel)
        interface IProjection<'M> with
            member _.ToAsyncEnumerable(): TaskSeq<'M> = query |> enum
            member _.Page(skip, take): TaskSeq<'M> = query |> Query.offsetLimit (skip, take) |> enum
            member _.CountAsync cancellationToken: Task<int> = count query cancellationToken
        [<EditorBrowsable(EditorBrowsableState.Never)>] member val Query: IQueryable<'T> = query
        [<EditorBrowsable(EditorBrowsableState.Never)>] member val Category: string = category
        [<EditorBrowsable(EditorBrowsableState.Never)>] member val Container: Container = container

/// Helpers for Querying Indices and Projecting Snapshot data based on well-known aspects of Equinox.CosmosStore's storage schema
module Index =

    [<NoComparison; NoEquality>]
    type Item<'I> =
        {   p: string
            _etag: string
            u: Unfold<'I> ResizeArray } // Arrays do not bind correctly in Cosmos LINQ
    and [<NoComparison; NoEquality>] Unfold<'I> =
        {   c: string
            d: 'I // For an index, this is the uncompressed JSON data; we're generating a LINQ query using this field's type, 'I
            [<System.Text.Json.Serialization.JsonPropertyName "d"; EditorBrowsable(EditorBrowsableState.Never)>]
            data: System.Text.Json.JsonElement // The raw data representing the encoded snapshot
            [<System.Text.Json.Serialization.JsonPropertyName "D"; EditorBrowsable(EditorBrowsableState.Never)>]
            format: Nullable<int> } // The (optional) encoding associated with that snapshot

    let inline prefix categoryName = $"%s{categoryName}-"
    /// The cheapest search basis; the categoryName is a prefix of the `p` partition field
    /// Depending on how much more selective the caseName is, `byCaseName` may be a better choice
    /// (but e.g. if the ration is 1:1 then no point having additional criteria)
    let queryCategory<'I> (container: Microsoft.Azure.Cosmos.Container) categoryName: IQueryable<Item<'I>> =
        let prefix = prefix categoryName
        container.GetItemLinqQueryable<Item<'I>>().Where(fun d -> d.p.StartsWith(prefix))

    // We want to generate a projection statement of the shape: VALUE {"sn": root["p"], "d": root["u"][0].["d"], "D": root["u"][0].["D"]}
    // However the Cosmos SDK does not support F# (or C#) records yet https://github.com/Azure/azure-cosmos-dotnet-v3/issues/3728
    // F#'s LINQ support cannot translate parameterless constructor invocations in a Lambda well;
    //  the best native workaround without Expression Manipulation is/was https://stackoverflow.com/a/78206722/11635
    // In C#, you can generate an Expression that works with the Cosmos SDK via `.Select(x => new { sn = x.p, d = x.u[0].d, D = x.u[0].D })`
    // This hack is based on https://stackoverflow.com/a/73506241/11635
    type SnAndSnap() =
        member val sn: FsCodec.StreamName = Unchecked.defaultof<_> with get, set
        member val D: Nullable<int> = Unchecked.defaultof<_> with get, set
        member val d: System.Text.Json.JsonElement = Unchecked.defaultof<_> with get, set
        member x.EncodedBody = EncodedBody.ofUnfoldBody (x.D.GetValueOrDefault 0, x.d)
        static member CreateItemQueryLambda<'T, 'U>(
                snExpression: Expression -> MemberExpression,
                uExpression: Expression<Func<'T, 'U>>,
                formatExpression: Expression<Func<'U, Nullable<int>>>,
                dataExpression: Expression<Func<'U, System.Text.Json.JsonElement>>) =
            let param = Expression.Parameter(typeof<'T>, "x")
            let targetType = typeof<SnAndSnap>
            let bind (name, expr) = Expression.Bind(targetType.GetMember(name)[0], expr) :> MemberBinding
            let body =
                Expression.MemberInit(
                    Expression.New(targetType.GetConstructor [||]),
                    [|  bind (nameof Unchecked.defaultof<SnAndSnap>.sn, snExpression param)
                        bind (nameof Unchecked.defaultof<SnAndSnap>.D,  uExpression.Compose(formatExpression).InlineParam(param))
                        bind (nameof Unchecked.defaultof<SnAndSnap>.d,  uExpression.Compose(dataExpression).InlineParam(param)) |])
            Expression.Lambda<Func<'T, SnAndSnap>>(body, [| param |])
        // a very ugly workaround for not being able to write query.Select<Item<'I>, SnAndSnap>(fun x -> { p = x.p; D = x.u[0].D; d = x.u[0].d })
        static member Project snapshotUnfoldExpression: Expression<Func<Item<'I>, SnAndSnap>> =
            let pExpression item = Expression.PropertyOrField(item, nameof Unchecked.defaultof<Item<'I>>.p)
            SnAndSnap.CreateItemQueryLambda<Item<'I>, Unfold<'I>>(pExpression, snapshotUnfoldExpression, (fun x -> x.format), (fun x -> x.data))

    /// Enables querying based on uncompressed Indexed values stored as secondary unfolds alongside the snapshot
    [<NoComparison; NoEquality>]
    type Context<'I>(container, categoryName, log, [<O; D null>]?queryLogLevel) =

        member val Container = container
        member val CategoryName = categoryName
        member val Log = log
        member val QueryLogLevel = defaultArg queryLogLevel Serilog.Events.LogEventLevel.Debug

        /// Fetches a base Queryable that's filtered only on the <c>categoryName</c>
        member _.Query(): IQueryable<Item<'I>> =
            queryCategory<'I> container categoryName

        /// Runs the query; yields the TOP 1 result, deserialized as the specified Model type
        member x.TryScalarAsync<'T, 'M>(query: IQueryable<'T>, ct, [<O; D null>] ?logLevel): Task<'M option> =
            let logLevel = defaultArg logLevel x.QueryLogLevel
            Internal.Scalar.tryHeadAsync<'T, 'M> log container categoryName logLevel query ct

        /// Runs the query; yields the StreamName from the TOP 1 result
        member x.TryGetStreamNameAsync(query: IQueryable<Item<'I>>, ct, [<O; D null>] ?logLevel): Task<FsCodec.StreamName option> =
            x.TryScalarAsync<string, FsCodec.StreamName>(query.Select(fun x -> x.p), ct, ?logLevel = logLevel)

        /// Runs the query; yields the StreamName from the TOP 1 result
        member x.TryGetStreamName(query: IQueryable<Item<'I>>): Async<FsCodec.StreamName option> =
            (fun ct -> x.TryGetStreamNameAsync(query, ct)) |> Async.call

        /// Query the items, projecting as T1, which gets bound to T2, which is then rendered as T
        member x.Project<'T1, 'T2, 'T>(query: IQueryable<'T1>, render: 'T2 -> 'T, [<O; D null>] ?logLevel) =
            let logLevel = defaultArg logLevel x.QueryLogLevel
            Internal.Projection.Create<'T2>(query, categoryName, container, log, render, logLevel) :> IProjection<'T>
        /// Query the items, projecting as T1, which gets bound to T2, which is then rendered as T
        member x.Project<'T1, 'T2, 'T>(query: IQueryable<Item<'I>>, projection: Expression<Func<Item<'I>, 'T1>>, render: 'T2 -> 'T, [<O; D null>] ?logLevel) =
            x.Project<'T1, 'T2, 'T>(query.Select projection, render, ?logLevel = logLevel)

        /// Query the items, grabbing the Stream name, snapshot and encoding from the snapshot identified by `selectSnapshotUnfold`, mapping to a result via `hydrate`
        member x.ProjectStreamNameAndSnapshot<'T>(query: IQueryable<Item<'I>>, selectSnapshotUnfold: Expression<Func<Item<'I>, Unfold<'I>>>, render: SnAndSnap -> 'T, [<O; D null>] ?logLevel) =
            x.Project<SnAndSnap, SnAndSnap, 'T>(query, SnAndSnap.Project<'I> selectSnapshotUnfold, render, ?logLevel = logLevel)

        /// Runs the query, rendering from the StreamName of each result
        member x.ProjectStreamName<'T>(query: IQueryable<Item<'I>>, render: FsCodec.StreamName -> 'T, [<O; D null>] ?logLevel) =
            x.Project<string, FsCodec.StreamName, 'T>(query, (fun x -> x.p), render, ?logLevel = logLevel)
