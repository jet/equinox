# Agents

## Code Review

### `@code-review`

Performs code review on pull requests, focusing on correctness, style, and adherence to project conventions.

#### Instructions

- This is an F# event-sourcing library. Evaluate changes in the context of functional programming best practices.
- Verify that new code follows the existing style: 4-space indentation, LF line endings, no trailing whitespace.
- `TreatWarningsAsErrors` is enabled; flag any code that would produce warnings.
- Ensure domain logic remains store-agnostic — store-specific code belongs in the respective adapter project, not in `Equinox` or `Equinox.Core`.
- Confirm that `FsCodec.IEventCodec` is used for all serialization concerns; no direct serializer coupling.
- Check that new dependencies are truly necessary; this project intentionally minimizes its dependency footprint.
- Verify tests are not removed or weakened without justification.
- For integration tests, ensure appropriate skip flags (`EQUINOX_INTEGRATION_SKIP_EVENTSTORE`, `EQUINOX_INTEGRATION_SKIP_COSMOS`) are respected.
- CI-only pack behavior is enabled when either `CI=true` or Azure Pipelines sets `TF_BUILD`; packaged projects under `src/` and `tools/` opt into documentation generation and package validation from their local `Directory.Build.props`.
- Build: `dotnet build -c Release`
- Test: `dotnet test -c Release --report-xunit-trx --ignore-exit-code 8 --results-directory tests/trx`
  - Exit code 8 is expected when store-dependent test assemblies have all tests skipped.
