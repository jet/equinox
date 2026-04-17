[<AutoOpen>]
module internal
#if STORE_EVENTSTOREDB
    Equinox.EventStoreDb.Tracing
#endif
#if STORE_EVENTSTORE_LEGACY
    Equinox.EventStore.Tracing
#endif
#if STORE_MESSAGEDB
    Equinox.MessageDb.Tracing
#endif
#if !STORE_EVENTSTOREDB && !STORE_EVENTSTORE_LEGACY && !STORE_MESSAGEDB
    Equinox.SqlStreamStore.Tracing
#endif

open System.Diagnostics
#if STORE_MESSAGEDB
open Equinox.MessageDb.Core
#endif

let [<Literal>] private SourceName =
#if STORE_EVENTSTOREDB
    "Equinox.EventStoreDb"
#endif
#if STORE_EVENTSTORE_LEGACY
    "Equinox.EventStore"
#endif
#if STORE_MESSAGEDB
    "Equinox.MessageDb"
#endif
#if !STORE_EVENTSTOREDB && !STORE_EVENTSTORE_LEGACY && !STORE_MESSAGEDB
    "Equinox.SqlStreamStore"
#endif

let source = new ActivitySource(SourceName)

[<AbstractClass; Sealed; System.Runtime.CompilerServices.Extension>]
type ActivityExtensions private () =

#if STORE_MESSAGEDB
    [<System.Runtime.CompilerServices.Extension>]
    static member AddExpectedVersion(act: Activity, version) =
        match version with StreamVersion v -> act.AddTag("eqx.expected_version", v) | Any -> act
#else
    [<System.Runtime.CompilerServices.Extension>]
    static member AddExpectedVersion(act: Activity, version: int64) =
        act.AddTag("eqx.expected_version", version)
#endif

    [<System.Runtime.CompilerServices.Extension>]
    static member AddLastVersion(act: Activity, version: int64) =
        act.AddTag("eqx.last_version", version)

    [<System.Runtime.CompilerServices.Extension>]
    static member AddBatches(act: Activity, batches: int) =
        act.AddTag("eqx.batches", batches)

#if STORE_MESSAGEDB
    [<System.Runtime.CompilerServices.Extension>]
    static member AddBatchSize(act: Activity, size: int64) =
        act.AddTag("eqx.batch_size", size)

    [<System.Runtime.CompilerServices.Extension>]
    static member AddStartPosition(act: Activity, pos: int64) =
        act.AddTag("eqx.start_position", pos)

    [<System.Runtime.CompilerServices.Extension>]
    static member AddLoadMethod(act: Activity, method: string) =
        act.AddTag("eqx.load_method", method)
#else
    [<System.Runtime.CompilerServices.Extension>]
    static member AddDirection(act: Activity, direction: string) =
        act.AddTag("eqx.direction", direction)
#endif

    [<System.Runtime.CompilerServices.Extension>]
    static member RecordConflict(act: Activity) =
        act.AddTag("eqx.conflict", true).SetStatus(ActivityStatusCode.Error, "WrongExpectedVersion")

/// Emits a short-lived child Activity from the store-specific ActivitySource, enabling test capture via ActivityListener
let emitStoreOp (name: string) =
    let a: Activity = source.StartActivity(name)
    if a <> null then a.Dispose()
