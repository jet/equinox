# ADR-0001: Replace Serilog Metric Capture with OpenTelemetry in Store Adapters

## Status

Accepted

## Context

Equinox store adapter packages (`Equinox.EventStore`, `Equinox.EventStoreDb`, `Equinox.SqlStreamStore`, and the `SqlStreamStore.*` database-specific packages) currently use Serilog as the primary mechanism for both structured logging and operational metric capture. The integration tests (`StoreIntegration.fs`, shared across all affected stores via conditional compilation) extract per-operation metrics by intercepting Serilog `LogEvent` emissions through a custom `ILogEventSink` (`LogCaptureBuffer`), then pattern-matching on `Log.Metric` discriminated union values embedded as `ScalarValue` properties.

This approach has several drawbacks:

1. **Tight coupling to Serilog internals** — Test assertions depend on the internal shape of Serilog `LogEvent.Properties`, `ScalarValue`, and `ILogEventSink`. This couples test infrastructure to a specific logging library's implementation details.

2. **No standard telemetry integration** — Modern .NET observability is built on `System.Diagnostics.Activity` and the OpenTelemetry ecosystem. Consumers using OTel collectors, Jaeger, Zipkin, or other APM tools cannot benefit from store-level operation tracing without custom bridging.

3. **Inconsistency across stores** — `Equinox.MessageDb` has already adopted `System.Diagnostics.Activity`-based tracing via `Activity.Current` tags (using the core `Equinox` `ActivitySource`), while the other stores have not. The integration test for MessageDb already sets up a `TracerProvider` with OTLP export.

4. **Public API surface** — `Equinox.EventStore` exposes a `Logger` discriminated union (`SerilogVerbose | SerilogNormal | CustomVerbose | CustomNormal`) and a `SerilogAdapter` type as public API, creating a hard dependency on `Serilog.ILogger` at the connector level. The other stores do not expose Serilog in their public APIs but use it extensively internally.

The core `Equinox` package will continue to depend on Serilog for the `ICategory.Load/Sync` interface and `Category.Stream` — this ADR does not propose changing that. Store packages will continue to receive `Serilog.ILogger` through these interfaces for human-readable structured logging.

## Decision

We will adopt the following changes, following and extending the pattern established by `Equinox.MessageDb`:

### 1. Add per-store `ActivitySource` via shared `Tracing.fs`

A single `Tracing.fs` file (physically in `src/Equinox.EventStoreDb/`) is linked into EventStore and SqlStreamStore packages via `<Compile Include>`, following the same shared-file-with-`#if`-conditionals pattern used by `StoreIntegration.fs` and `Infrastructure.fs` in the test projects. The existing `DefineConstants` from the test projects (`STORE_EVENTSTOREDB`, `STORE_EVENTSTORE_LEGACY`) are reused — the same constants are added to the corresponding src `.fsproj` files. `SqlStreamStore` uses the `#else` fallback and requires no new constant.

The shared file defines:
- A per-store `ActivitySource` selected via `#if STORE_EVENTSTOREDB` / `#elif STORE_EVENTSTORE_LEGACY` / `#else` blocks (e.g., `new ActivitySource("Equinox.EventStoreDb")`)
- Shared `ActivityExtensions` for domain-relevant Activity tags (shared across all stores)
- The `#else` branch provides the `Equinox.SqlStreamStore` namespace and ActivitySource name

`Equinox.MessageDb`'s existing `Tracing.fs` remains separate — it references `Equinox.MessageDb.Core.StreamVersion`, a MessageDb-specific type.

### 2. Instrument store operations with child Activities

Each discrete store operation (read slice, read batch, write, read-last) starts a child Activity from the store's `ActivitySource`. Tags carry operational metadata:
- `eqx.expected_version`, `eqx.last_version`, `eqx.count`, `eqx.bytes`
- `eqx.conflict`, `eqx.event_types` (on write conflict)
- `eqx.direction` (forward/backward for reads)
- `eqx.batch_size`, `eqx.batches`, `eqx.start_position`

These child Activities sit beneath the parent `Load`/`Sync` activities created by `Equinox.Core.Tracing.source` in `Category.fs`. Additionally, aggregate metrics are added to `Activity.Current` (the parent) via `IncMetric`, following the MessageDb pattern.

### 3. Replace test metric capture with OTel Activity capture

A new shared test helper (`ActivityCapture`) uses `System.Diagnostics.ActivityListener` to capture completed Activities from `Equinox.*` sources. The existing `EsAct` discriminated union is retained for assertion readability, but values are derived from Activity operation names and tags rather than Serilog `LogEvent` properties.

### 4. Remove Serilog-specific public APIs

- **`Equinox.EventStore`**: Remove the `Logger` DU, `SerilogAdapter` type, and `?log: Logger` parameter from `EventStoreConnector`. Users needing to configure the EventStore client logger can use the existing `?custom: ConnectionSettingsBuilder -> ConnectionSettingsBuilder` parameter.
- **`Equinox.EventStoreDb`**, **`Equinox.SqlStreamStore`**: No Serilog-specific public APIs exist; no removals needed.

### 5. Provide OTel-to-Serilog bridge helper

A shared helper (`OtelToSerilogBridge`) listens to Activity completions and emits equivalent Serilog log writes, allowing consumers migrating from Serilog-based metric consumption to see familiar output.

### 6. Per-metric equivalence tests (Serilog ↔ OTel)

For each `Log.Metric` case in each store (WriteSuccess, WriteConflict, Slice, Batch, ReadLast, etc.), add integration tests that perform dual capture — both Serilog `LogEvent` via `LogCaptureBuffer` and OTel `Activity` via `ActivityCapture` — and assert:
- Both yield the same `EsAct` classification
- Key metric values match (stream name, event count, bytes, direction, conflict status, version)
- The `OtelToSerilogBridge` produces equivalent output

This provides per-event proof that every Serilog-derived assertion in `StoreIntegration.fs` has an equivalent OTel-derived assertion.

### 7. Comment out `PackageValidationBaselineVersion`

Since removing public API surface is a breaking change, `PackageValidationBaselineVersion` is commented out in the `.fsproj` files of all affected packages until a new baseline is established.

### 8. Internal logging retained

The `Log` module (including `Measurement`, `Metric`, `MetricEvent`, `InternalMetrics`, `prop`, `event`, `propEventData`, `propResolvedEvents`) remains in each store package. Internal `log.Information(...)` calls continue to emit structured Serilog log events. The `ILogger` flowing from `ICategory.Load/Sync` is still used. We are adding OTel tracing as a parallel telemetry path, not removing Serilog logging.

## Consequences

### Positive

- **Standard observability**: Store operations appear as proper OTel spans in APM tools, with parent-child relationships (Decider → Load/Sync → ReadBatch/Write).
- **Consistent tracing across stores**: All stores (MessageDb, EventStoreDb, EventStore, SqlStreamStore) follow the same Activity-based pattern.
- **Decoupled test assertions**: Integration tests no longer depend on Serilog internals for metric validation.
- **Reduced public API surface**: Removing the `Logger` DU from `Equinox.EventStore` simplifies the connector API.

### Negative

- **Breaking change**: Removing `Logger` type from `Equinox.EventStore` requires consumers using `SerilogVerbose`/`SerilogNormal` to migrate to the `?custom` parameter or remove the logger argument. This necessitates a major or minor version bump with appropriate migration guidance.
- **Dual telemetry paths**: Both Serilog logging and OTel tracing coexist in the store code, adding some code volume. This is mitigated by the fact that they serve different purposes (human-readable logs vs. structured traces).
- **Package validation disabled**: Commenting out `PackageValidationBaselineVersion` temporarily removes automated API compatibility checking until a new baseline is set.

### Neutral

- **No impact on CosmosStore/DynamoStore**: These stores are not affected by this change. They may adopt the same pattern independently in the future.
- **TestOutput.fs unchanged**: The shared test output helper (used by all test projects) continues to use Serilog for rendering test output to xUnit `ITestOutputHelper`. This is orthogonal to the metric capture mechanism.
- **`Log.InternalMetrics.Stats.LogSink`**: The existing Serilog-based metric aggregation helper remains available for consumers. An OTel-based equivalent may be added separately.

## References

- `src/Equinox.MessageDb/Tracing.fs` — Established pattern for store-specific Activity extensions
- `src/Equinox/Tracing.fs` — Core `ActivitySource("Equinox")` and shared `ActivityExtensions`
- `src/Equinox/Category.fs` — Parent `Load`/`Sync` Activity creation
- `tests/Equinox.EventStoreDb.Integration/StoreIntegration.fs` — Shared integration tests with conditional compilation
- [CNCF OpenTelemetry .NET documentation](https://opentelemetry.io/docs/languages/dotnet/)
- [System.Diagnostics.Activity API](https://learn.microsoft.com/en-us/dotnet/api/system.diagnostics.activity)

## Implementation Notes

### F# preprocessor limitations

F# does not support `#elif` for namespace declarations. The shared `Tracing.fs` uses `[<AutoOpen>] module internal` with separate `#if`/`#endif` blocks (not `#elif`) to select the namespace.

### Test isolation via AsyncLocal

`ActivityListener` registered via `ActivitySource.AddActivityListener()` is process-global — unlike Serilog's per-logger `ILogEventSink`, ALL listeners receive ALL activities from matching sources. When xUnit runs test classes in parallel, activities from one class's tests bleed into another's captures.

The solution uses a single static `ActivityListener` per type with `AsyncLocal<ResizeArray<string>>` dispatch: the listener callback reads `AsyncLocal.Value` to find the current test's ops list, providing per-async-context isolation without process-level contention.

### SqlStreamStore unconditional ProjectReference

The `Equinox.SqlStreamStore.Postgres`, `.MsSql`, and `.MySql` wrapper projects previously used conditional references (`ProjectReference` in Debug, `PackageReference` in Release) to `Equinox.SqlStreamStore`. This was changed to unconditional `ProjectReference` to ensure locally-built `Tracing.fs` changes are always picked up. The previous pattern loaded the NuGet-published version in Release mode, which did not contain the new tracing code.
