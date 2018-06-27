// How to spin up a CosmosDB emulator locally:
// https://docs.microsoft.com/en-us/azure/cosmos-db/local-emulator#running-on-docker
// Then run the script below
#r "bin/Release/FSharp.Control.AsyncSeq.dll"
#r "bin/Release/Newtonsoft.Json.dll"
#r "bin/Release/Microsoft.Azure.Documents.Client.dll"
#load "Infrastructure.fs"

open System
open Equinox.Cosmos
open Microsoft.Azure.Documents
open Microsoft.Azure.Documents.Client

let URI = Uri "https://localhost:8081"
let KEY = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=="
let DBNAME = "test"
let COLLNAME = "test"
let RU = 5000
let AUXRU = 400

let client =
    let connPolicy =
      let cp = ConnectionPolicy.Default
      cp.ConnectionMode <- ConnectionMode.Direct
      cp.MaxConnectionLimit <- 200
      cp.RetryOptions <- RetryOptions(MaxRetryAttemptsOnThrottledRequests = 2, MaxRetryWaitTimeInSeconds = 10)
      cp
    new DocumentClient(URI, KEY, connPolicy, Nullable ConsistencyLevel.Session)

let createDatabase (client:DocumentClient)=
    let dbRequestOptions =
        let o = RequestOptions ()
        o.ConsistencyLevel <- Nullable<ConsistencyLevel>(ConsistencyLevel.ConsistentPrefix)
        o
    client.CreateDatabaseIfNotExistsAsync(Database(Id=DBNAME), options = dbRequestOptions)
    |> Async.AwaitTaskCorrect
    |> Async.map (fun response -> Client.UriFactory.CreateDatabaseUri (response.Resource.Id))

let createCollection (client: DocumentClient) (dbUri: Uri) =
    let pkd = PartitionKeyDefinition()
    pkd.Paths.Add("/k")
    let coll = DocumentCollection(Id = COLLNAME, PartitionKey = pkd)

    coll.IndexingPolicy.IndexingMode <- IndexingMode.Consistent
    coll.IndexingPolicy.Automatic <- true
    coll.IndexingPolicy.IncludedPaths.Add(new IncludedPath (Path="/s/?"))
    coll.IndexingPolicy.IncludedPaths.Add(new IncludedPath (Path="/k/?"))
    coll.IndexingPolicy.IncludedPaths.Add(new IncludedPath (Path="/sn/?"))
    coll.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath (Path="/*"))
    client.CreateDocumentCollectionIfNotExistsAsync(dbUri, coll, RequestOptions(OfferThroughput=Nullable RU))
    |> Async.AwaitTaskCorrect
    |> Async.map (fun response -> Client.UriFactory.CreateDocumentCollectionUri (DBNAME, response.Resource.Id))

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
    |> Async.map (fun r -> Client.UriFactory.CreateStoredProcedureUri(DBNAME, COLLNAME, r.Resource.Id))

let createAux (client: DocumentClient) (dbUri: Uri) =
    let auxCollectionName = sprintf "%s-aux" COLLNAME
    let auxColl = DocumentCollection(Id = auxCollectionName)
    auxColl.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath(Path="/ChangefeedPosition/*"))
    auxColl.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath(Path="/ProjectionsPositions/*"))
    auxColl.IndexingPolicy.IncludedPaths.Add(new IncludedPath (Path="/*"))
    auxColl.IndexingPolicy.IndexingMode <- IndexingMode.Lazy
    auxColl.DefaultTimeToLive <- Nullable(365 * 60 * 60 * 24)
    client.CreateDocumentCollectionIfNotExistsAsync(dbUri, auxColl, RequestOptions(OfferThroughput=Nullable AUXRU))
    |> Async.AwaitTaskCorrect
    |> Async.map (fun response -> Client.UriFactory.CreateDocumentCollectionUri (DBNAME, response.Resource.Id))

let go = async {
    let! dbUri = createDatabase client
    do! (createCollection client dbUri) |> Async.bind (createStoreSproc client) |> Async.Ignore
    do! createAux client dbUri |> Async.Ignore
}

go |> Async.RunSynchronously