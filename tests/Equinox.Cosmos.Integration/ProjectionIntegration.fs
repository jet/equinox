module ProjectionIntegration

open Equinox.Cosmos.Integration
open Equinox.Cosmos.Projection
open Equinox.Cosmos.Projection.Projector
open Equinox.Cosmos.Projection.Route
open Swensen.Unquote

type Tests(testOutputHelper) =
    inherit TestsWithLogCapture(testOutputHelper)
    let log = base.Log

    [<AutoData(SkipIfRequestedViaEnvironmentVariable="EQUINOX_INTEGRATION_SKIP_COSMOS")>]
    let projector () = Async.RunSynchronously <| async {
        let! conn = connectToSpecifiedCosmosOrSimulator log
        let predicate = Predicate.All
                                  //EventTypeSelector "<c>"
                                  //CategorySelector "<first token before - in p>"
                                  //StreamSelector "<p>"

        let projection = {
            name = "test"
            topic = "xray-telemetry"
            predicate = predicate
            collection = "michael"
            partitionKeyPath = None
          } 

        let pub : Projector.Config = {
            region = "eastus2"
            conn = conn
            dbName = "equinox-test"
            collectionName = "michael"
            auxCollectionName = "michael-aux"
            changefeedBatchSize = 100
            projections = [|projection|]
            kafkaBroker = "<redacted>"
            kafkaClientId = "projector"
            startPositionStrategy = StartingPosition.ResumePrevious
            progressInterval = System.TimeSpan.FromSeconds 30.
        }

        let! res = Projector.go log pub
        printf "%O" res
        test <@ 1 = 1 @>
    }