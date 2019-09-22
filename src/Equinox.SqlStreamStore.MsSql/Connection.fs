namespace Equinox.SqlStreamStore.MsSql

open Equinox
open Equinox.Store
open Equinox.SqlStreamStore

type Connector (connectionString: string(*, [<O; D(null)>]?schema: string*), [<O; D(null)>]?readRetryPolicy, [<O; D(null)>]?writeRetryPolicy) =
    let storeSettings = SqlStreamStore.MsSqlStreamStoreV3Settings(connectionString)

    let store = new SqlStreamStore.MsSqlStreamStoreV3(storeSettings)

    member __.Connect () = async {
        do! store.CreateSchemaIfNotExists() |> Async.AwaitTask

        return (store :> SqlStreamStore.IStreamStore)
    }

    member __.Establish () = async {
        let! conn = __.Connect()
        return Connection(conn, ?readRetryPolicy=readRetryPolicy, ?writeRetryPolicy=writeRetryPolicy) 
    }