namespace Equinox.SqlStreamStore.MySql

open Equinox
open Equinox.Core

type Connector (connectionString: string, [<O; D(null)>]?readRetryPolicy, [<O; D(null)>]?writeRetryPolicy) =
    let createStreamStore = 
        fun () -> async {
            let storeSettings = SqlStreamStore.MySqlStreamStoreSettings(connectionString)
        
            let store = new SqlStreamStore.MySqlStreamStore(storeSettings)

            do! store.CreateSchemaIfNotExists() |> Async.AwaitTask

            return store :> SqlStreamStore.IStreamStore
        }
        
    let connector = Equinox.SqlStreamStore.Connector(createStreamStore, ?readRetryPolicy=readRetryPolicy, ?writeRetryPolicy=writeRetryPolicy)

    member __.Connect () = connector.Connect ()

    member __.Establish () = connector.Establish ()