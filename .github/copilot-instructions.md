# Copilot Instructions for Equinox

## Project Overview

Equinox is a set of low-dependency F# libraries for event-sourced processing against stream-based stores.
It is **not a framework**; consumers compose the libraries to fit their needs.

Related repositories:
- [FsCodec](https://github.com/jet/FsCodec) — encoding/decoding events
- [Propulsion](https://github.com/jet/propulsion) — projections and reactions
- [dotnet-templates](https://github.com/jet/dotnet-templates) — starter project templates

## Repository Layout

| Path | Contents |
|------|----------|
| `src/Equinox` | Core library: `Decider`, `Cache`, optimistic concurrency |
| `src/Equinox.Core` | Shared utilities: `TaskCell`, `Batcher` |
| `src/Equinox.CosmosStore` | Azure Cosmos DB adapter |
| `src/Equinox.DynamoStore` | Amazon DynamoDB adapter |
| `src/Equinox.EventStoreDb` | EventStoreDB (gRPC) adapter |
| `src/Equinox.MessageDb` | MessageDB (Postgres) adapter |
| `src/Equinox.MemoryStore` | In-memory store for testing |
| `src/Equinox.EventStore` | Legacy EventStore TCP adapter (deprecated) |
| `src/Equinox.SqlStreamStore*` | SqlStreamStore adapters (unmaintained upstream) |
| `src/Equinox.*.Prometheus` | Prometheus metrics sinks |
| `tests/` | Unit and integration tests (xUnit v3, MTP v2) |
| `samples/` | Tutorial scripts, TodoBackend, sample store |
| `tools/Equinox.Tool` | CLI tool for provisioning and benchmarking |

## Language and Style

- The codebase is **F#**. Follow existing conventions when editing.
- 4-space indentation, LF line endings, trim trailing whitespace (see `.editorconfig`).
- `TreatWarningsAsErrors` is enabled globally — all warnings are errors.
- Prefer minimal dependencies; Equinox intentionally keeps its dependency footprint small.
- Do not add comments unless they match the existing style or explain something non-obvious.

## Building and Testing

- **SDK**: .NET 10 (see `global.json` for exact version; `rollForward: latestMajor`).
- **Test runner**: Microsoft Testing Platform v2 (configured in `global.json` `"test"` section).
- **Build**:
  ```sh
  dotnet build -c Release
  ```
- **Test** (CI style):
  ```sh
  dotnet test -c Release --report-xunit-trx --ignore-exit-code 8 --results-directory tests/trx
  ```
  Exit code 8 means all tests in an assembly were skipped (expected in CI without Docker stores).
- **Build + test via MSBuild project**:
  ```sh
  dotnet build build.proj
  ```
- Integration test skip flags (set to `true` to skip):
  - `EQUINOX_INTEGRATION_SKIP_EVENTSTORE`
  - `EQUINOX_INTEGRATION_SKIP_COSMOS`
- CI runs on Azure Pipelines across Windows, Linux, and macOS.

## Key Design Patterns

- **Decider pattern**: Domain logic lives in `decide`/`interpret` functions that take state and return events.
  The `Decider<'event, 'state>` type manages the optimistic concurrency loop.
- **`IEventCodec`**: All serialization is expressed via `FsCodec.IEventCodec`. Use `FsCodec.SystemTextJson.Codec` or `FsCodec.NewtonsoftJson.Codec`.
- **Access Strategies**: Each store adapter provides strategies for snapshots/unfolds
  (e.g., `RollingState`, `Snapshot`, `Tip with Unfolds` for CosmosStore).
- **Caching**: `Equinox.Cache` wraps `System.Runtime.Caching.MemoryCache` to minimize store round-trips.
- **Store agnosticism**: Domain code should never reference a specific store package.
  Store wiring is the responsibility of the application composition root.

## PR and Contribution Guidelines

- See `CONTRIBUTING.md` — new features start at −100 points; keep changes focused.
- Ensure tests pass before submitting. Do not remove or weaken existing tests.
- Use `CHANGELOG.md` conventions when documenting user-facing changes.
