namespace Equinox.SqlStreamStore.Postgres

open Equinox
open Equinox.Store

type Connector (connectionString: string(*, [<O; D(null)>]?schema: string*), [<O; D(null)>]?readRetryPolicy, [<O; D(null)>]?writeRetryPolicy) =
    let createStreamStore = 
        fun () -> async {
            let storeSettings = SqlStreamStore.PostgresStreamStoreSettings(connectionString)
        
            let store = new SqlStreamStore.PostgresStreamStore(storeSettings)

            do! store.CreateSchemaIfNotExists() |> Async.AwaitTask

            return store :> SqlStreamStore.IStreamStore
        }
        
    let connector = Equinox.SqlStreamStore.Connector(createStreamStore, ?readRetryPolicy=readRetryPolicy, ?writeRetryPolicy=writeRetryPolicy)

    member __.Connect () = connector.Connect ()

    member __.Establish () = connector.Establish ()