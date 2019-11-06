namespace Equinox.SqlStreamStore.MySql

open Equinox.Core
open SqlStreamStore

type Connector
    (    connectionString: string,
         [<O; D(null)>]?readRetryPolicy, [<O; D(null)>]?writeRetryPolicy,
         /// <c>true</c> to auto-create the schema upon connection
         [<O; D(null)>]?autoCreate) =
    inherit Equinox.SqlStreamStore.ConnectorBase(?readRetryPolicy=readRetryPolicy,?writeRetryPolicy=writeRetryPolicy)

    let settings = MySqlStreamStoreSettings(connectionString)
    let store = new MySqlStreamStore(settings)

    override __.Connect() = async {
        if autoCreate = Some true then do! store.CreateSchemaIfNotExists() |> Async.AwaitTaskCorrect
        return store :> IStreamStore
    }