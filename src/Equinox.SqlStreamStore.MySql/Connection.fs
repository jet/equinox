namespace Equinox.SqlStreamStore.MySql

open Equinox.Core
open SqlStreamStore

type Connector(connectionString: string, [<O; D(null)>] ?readRetryPolicy, [<O; D(null)>] ?writeRetryPolicy, [<O; D(null)>] ?autoCreate)
/// <c>true</c> to auto-create the schema upon connection
 =
    inherit Equinox.SqlStreamStore.ConnectorBase(?readRetryPolicy = readRetryPolicy, ?writeRetryPolicy = writeRetryPolicy)

    let settings = MySqlStreamStoreSettings(connectionString)
    let store = new MySqlStreamStore(settings)

    override _.Connect() = async {
        if autoCreate = Some true then
            do! store.CreateSchemaIfNotExists() |> Async.AwaitTaskCorrect

        return store :> IStreamStore
    }
