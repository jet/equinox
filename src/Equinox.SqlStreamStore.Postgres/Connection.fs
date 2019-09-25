namespace Equinox.SqlStreamStore.Postgres

open System
open Equinox
open Equinox.Core

type Connector (connectionString: string, [<O; D(null)>]?schema: string, [<O; D(null)>]?readRetryPolicy, [<O; D(null)>]?writeRetryPolicy) =
    let createStreamStore = 
        fun () -> async {
            let storeSettings = SqlStreamStore.PostgresStreamStoreSettings(connectionString)

            match schema with
            | Some schema when schema |> String.IsNullOrWhiteSpace |> not ->
                storeSettings.Schema <- schema
            | _ -> ()
        
            let store = new SqlStreamStore.PostgresStreamStore(storeSettings)

            do! store.CreateSchemaIfNotExists() |> Async.AwaitTask

            return store :> SqlStreamStore.IStreamStore
        }
        
    let connector = Equinox.SqlStreamStore.Connector(createStreamStore, ?readRetryPolicy=readRetryPolicy, ?writeRetryPolicy=writeRetryPolicy)

    member __.Connect () = connector.Connect ()

    member __.Establish () = connector.Establish ()