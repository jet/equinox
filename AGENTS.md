# AGENTS.md

## Repository Overview

Equinox is a set of low-dependency F# libraries for event-sourced processing against stream-based stores.
It is **not a framework**; consumers compose the libraries to fit their needs.
Equinox handles Snapshots, Caching, and Optimistic Concurrency Control.
Projections/Subscriptions are handled by [Propulsion](https://github.com/jet/propulsion).

## Project Structure

- `src/` — Core libraries (all target `net6.0`, all packable as NuGet packages):
  - `Equinox` — Core Decider, Stream, Cache abstractions
  - `Equinox.Core` — Internal helpers (TaskCell, Internal)
  - `Equinox.CosmosStore` — Azure Cosmos DB store implementation
  - `Equinox.DynamoStore` — Amazon DynamoDB store implementation
  - `Equinox.EventStore` — EventStore (TCP) store implementation
  - `Equinox.EventStoreDb` — EventStoreDB (gRPC) store implementation
  - `Equinox.MemoryStore` — In-memory store for testing
  - `Equinox.MessageDb` — MessageDB (PostgreSQL) store implementation
  - `Equinox.SqlStreamStore` — SqlStreamStore base implementation
  - `Equinox.SqlStreamStore.MsSql` / `.MySql` / `.Postgres` — SQL-specific connectors
  - `Equinox.CosmosStore.Prometheus` / `Equinox.DynamoStore.Prometheus` — Prometheus metrics
- `tests/` — Test projects (all target `net10.0`, xUnit v3 with MTP v2):
  - `Equinox.Tests`, `Equinox.Core.Tests` — Unit tests
  - `Equinox.*.Integration` — Integration tests (most require external stores)
- `samples/` — Sample applications:
  - `Store/Domain` — Sample domain models (referenced by tests and tools)
  - `Store/Domain.Tests` — Sample domain tests
  - `Store/Integration` — Sample integration tests
  - `Infrastructure` — Shared wiring for all stores
  - `TodoBackend` — Todo sample domain
  - `Tutorial` — F# tutorial scripts
  - `Web` — ASP.NET Core sample web app
- `tools/` — CLI tooling:
  - `Equinox.Tool` — `eqx` CLI (benchmarks, provisioning)
  - `Equinox.Tools.TestHarness` — Load test helpers

## Build System

- **Solution**: `Equinox.sln` contains all projects
- **SDK**: .NET 10 SDK (see `global.json`; `rollForward: latestMajor`)
- **Test runner**: Microsoft Testing Platform v2 (`global.json` → `"runner": "Microsoft.Testing.Platform"`)
- **Versioning**: [MinVer](https://github.com/adamralph/minver) computes versions from git tags
- **Package validation**: `Microsoft.DotNet.PackageValidation` SDK validates API compatibility
- **Reproducible builds**: `DotNet.ReproducibleBuilds` package (imports SourceLink for GitHub, GitLab, AzureRepos, Bitbucket)
- **CI**: Azure Pipelines (`azure-pipelines.yml`) — builds on Windows, Linux, macOS

### Key Build Commands

```bash
# Build everything (from clean):
dotnet build

# Run tests (MTP v2; exit code 8 = all tests in an assembly skipped):
dotnet test --report-xunit-trx --ignore-exit-code 8

# Build in Release (CI generates NuGet packages automatically):
dotnet build -c Release
```

### Build Conventions

- `src/` and `tools/` projects are packable (`IsPackable=true` via `Directory.Build.props`)
- `GeneratePackageOnBuild` is enabled only when `CI=true` (set in `Directory.Build.targets`)
- Warnings are errors (`TreatWarningsAsErrors=true`, `WarningLevel=5`)
- `DisableImplicitFSharpCoreReference=true` — libraries pin `FSharp.Core 6.0.7` explicitly
- Tests and tools use the SDK-bundled FSharp.Core (implicit reference)
- Debug builds use `ProjectReference` between src projects; Release builds use NuGet `PackageReference` (conditional references in `.fsproj` files)
- Several src projects share source files across project boundaries via `<Compile Include="..\OtherProject\File.fs" />` — this is intentional for shipping independent packages

### Integration Test Skip Flags

- `EQUINOX_INTEGRATION_SKIP_EVENTSTORE=true` — skip EventStore/EventStoreDB tests
- `EQUINOX_INTEGRATION_SKIP_COSMOS=true` — skip CosmosDB tests
- SQL and MessageDB integration tests have no separate skip flags

## Coding Conventions

- **Language**: F# throughout (no C# projects)
- **Style**: Concise, functional F# — modules over classes, pattern matching, computation expressions
- **Naming**: PascalCase for public API, camelCase for local bindings
- **Documentation**: XML doc comments on public APIs; `GenerateDocumentationFile` enabled in CI for packable projects
- **Error handling**: Prefer `Result`/`Option` types; exceptions for truly exceptional cases
- **Async**: `task { }` computation expressions (not `async { }` for hot paths)
- **Testing**: xUnit v3 with FsCheck for property-based tests, Unquote for assertions
- **Logging**: Serilog throughout; metrics via `Serilog.Events.LogEvent` Properties
