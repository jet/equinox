module Equinox.Cosmos.CosmosManager

open System
open Equinox.Cosmos
open Equinox.EventStore.Infrastructure
open Microsoft.Azure.Documents
open Microsoft.Azure.Documents.Client

let configCosmos connStr dbName collName ru auxRu = async {
    let uri, key =
        match connStr,connStr with
        | Strings.RegexGroup "AccountEndpoint=(.+?);" uri, Strings.RegexGroup "AccountKey=(.+?);" key ->
            System.Uri(uri), key
        | _ -> failwithf "Invalid DocumentDB connection string: %s" connStr
    let client =
        let connPolicy =
          let cp = ConnectionPolicy.Default
          cp.ConnectionMode <- ConnectionMode.Direct
          cp.MaxConnectionLimit <- 200
          cp.RetryOptions <- RetryOptions(MaxRetryAttemptsOnThrottledRequests = 2, MaxRetryWaitTimeInSeconds = 10)
          cp
        new DocumentClient(uri, key, connPolicy, Nullable ConsistencyLevel.Session)

    let createDatabase (client:DocumentClient) =
        let dbRequestOptions =
            let o = RequestOptions ()
            o.ConsistencyLevel <- Nullable<ConsistencyLevel>(ConsistencyLevel.Session)
            o
        client.CreateDatabaseIfNotExistsAsync(Database(Id=dbName), options = dbRequestOptions)
        |> Async.AwaitTaskCorrect
        |> Async.map (fun response -> Client.UriFactory.CreateDatabaseUri (response.Resource.Id))

    let createCollection (client: DocumentClient) (dbUri: Uri) =
        let pkd = PartitionKeyDefinition()
        pkd.Paths.Add("/k")
        let coll = DocumentCollection(Id = collName, PartitionKey = pkd)

        coll.IndexingPolicy.IndexingMode <- IndexingMode.Consistent
        coll.IndexingPolicy.Automatic <- true
        coll.IndexingPolicy.IncludedPaths.Add(new IncludedPath (Path="/s/?"))
        coll.IndexingPolicy.IncludedPaths.Add(new IncludedPath (Path="/k/?"))
        coll.IndexingPolicy.IncludedPaths.Add(new IncludedPath (Path="/sn/?"))
        coll.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath (Path="/*"))
        client.CreateDocumentCollectionIfNotExistsAsync(dbUri, coll, RequestOptions(OfferThroughput=Nullable ru))
        |> Async.AwaitTaskCorrect
        |> Async.map (fun response -> Client.UriFactory.CreateDocumentCollectionUri (dbName, response.Resource.Id))

    let createStoreSproc (client: IDocumentClient) (collectionUri: Uri) =
        let f ="""
            function multidocInsert (docs) {
            var response = getContext().getResponse();
            var collection = getContext().getCollection();
            var collectionLink = collection.getSelfLink();

            if (!docs) throw new Error("Array of events is undefined or null.");

            for (i=0; i<docs.length; i++) {
            collection.createDocument(collectionLink, docs[i]);
            }

            response.setBody(true);
            }"""
        let batchSproc = new StoredProcedure(Id = "AtomicMultiDocInsert", Body = f)

        client.CreateStoredProcedureAsync(collectionUri, batchSproc)
        |> Async.AwaitTaskCorrect
        |> Async.map (fun r -> Client.UriFactory.CreateStoredProcedureUri(dbName, collName, r.Resource.Id))

    let createAux (client: DocumentClient) (dbUri: Uri) =
        let auxCollectionName = sprintf "%s-aux" collName
        let auxColl = DocumentCollection(Id = auxCollectionName)
        auxColl.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath(Path="/ChangefeedPosition/*"))
        auxColl.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath(Path="/ProjectionsPositions/*"))
        auxColl.IndexingPolicy.IncludedPaths.Add(new IncludedPath (Path="/*"))
        auxColl.IndexingPolicy.IndexingMode <- IndexingMode.Lazy
        auxColl.DefaultTimeToLive <- Nullable(365 * 60 * 60 * 24)
        client.CreateDocumentCollectionIfNotExistsAsync(dbUri, auxColl, RequestOptions(OfferThroughput=Nullable auxRu))
        |> Async.AwaitTaskCorrect
        |> Async.map (fun response -> Client.UriFactory.CreateDocumentCollectionUri (dbName, response.Resource.Id))        
    let! dbUri = createDatabase client
    do! (createCollection client dbUri) |> Async.bind (createStoreSproc client) |> Async.Ignore
    do! createAux client dbUri |> Async.Ignore
}
