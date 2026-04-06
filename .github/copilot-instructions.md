# Copilot Instructions for Equinox

## What is Equinox?

Equinox is a set of low-dependency F# libraries for event-sourced stream processing. It is not a framework — consumers compose the libraries. It handles Snapshots, Caching, and Optimistic Concurrency Control against multiple store backends.

## Key Patterns

- **Decider pattern**: The central abstraction. A `Decider` loads events, folds them into state, and transacts new events with optimistic concurrency.
- **Conditional project references**: `src/` libraries use `ProjectReference` in Debug and `PackageReference` in Release. Never change this pattern without understanding the packaging implications.
- **Shared source files**: Several projects include `.fs` files from sibling projects (e.g., `Infrastructure.fs`, `Internal.fs`). This is intentional — each NuGet package ships independently.
- **Store-agnostic domain code**: Domain logic in `samples/Store/Domain` has zero store dependencies. Store wiring happens in `samples/Infrastructure`.

## Build & Test

```bash
dotnet build                  # Build all projects
dotnet test                   # Run tests (MTP v2)
./build.ps1 -s                # Build + test, skipping store-dependent tests
dotnet build -c Release       # Release build (generates packages when CI=true)
```

- Tests use xUnit v3 with Microsoft Testing Platform v2.
- MTP v2 exit code 8 means all tests in an assembly were skipped (not a failure).
- Use `--report-xunit-trx` for TRX test result output.
- Integration tests require running store instances and are skipped via `EQUINOX_INTEGRATION_SKIP_EVENTSTORE` / `EQUINOX_INTEGRATION_SKIP_COSMOS` environment variables.

## Code Style

- Write idiomatic F#: prefer modules, pattern matching, computation expressions.
- Use `task { }` for async hot paths (not `async { }`).
- Follow existing naming: PascalCase for public API, camelCase for locals.
- All projects have `TreatWarningsAsErrors=true` and `WarningLevel=5`.
- Libraries pin `FSharp.Core 6.0.7` explicitly (`DisableImplicitFSharpCoreReference=true`).
- Use Unquote (`=!`, `<>!`) for test assertions and FsCheck for property-based tests.
- Keep the code clean and readable — this is reference code for event sourcing in F#.

## Important Files

- `Directory.Build.props` — Shared build properties (warnings, packaging, authors)
- `Directory.Build.targets` — Version computation (MinVer), conditional packaging
- `global.json` — SDK version and test runner configuration
- `azure-pipelines.yml` — CI pipeline definition
- `build.ps1` — Local build script with store provisioning
