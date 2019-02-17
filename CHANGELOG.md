# Changelog

The repo is versioned based on [SemVer 2.0](https://semver.org/spec/v2.0.0.html) using the tiny-but-mighty [MinVer](https://github.com/adamralph/minver) from [@adamralph](https://github.com/adamralph). [See here](https://github.com/adamralph/minver#how-it-works) for more information on how it works.

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
The `Unreleased` section name is replaced by the expected version of next release. A stable version's log contains all changes between that version and the previous stable version (can duplicate the prereleases logs).

_NB at the present time, this project does not adhere strictly to Semantic Versioning - small binary-breaking changes may occur without a change in Major at the until this notice is removed (it will be!)._

## [Unreleased]

### Added

- `Equinox.Projection.Kafka` consumer metrics emission, see [#94](https://github.com/jet/equinox/pull/94) @michaelliao5

### Changed

- Target `EventStore.Client 5.*` to avail of bugfixes and single client codebase. Interoperable with `4.*` servers
- Replace stateful `Context` API as primary Handler API with `Transact`; Compatible `Accumulator` remains to facilitate porting [#97](https://github.com/jet/equinox/pull/97)

### Removed
- Moved internal `Stream` helpers from `Equinox` to instead be inlined into Store impls [#97](https://github.com/jet/equinox/pull/97)

### Fixed

<a name="1.1.0-preview1"></a>
## [1.1.0-preview1] - 2019-02-08

### Added

- `Equinox.Cosmos` projection facilities, see [#87](https://github.com/jet/equinox/pull/87)

<a name="1.0.4"></a>
## [1.0.4] - 2019-02-08

### Changed

- Rename `Equinox.CatId` to `Equinox.AggregateId` (HT @lfr for prompting the change)
- Make `Newtonsoft.Json` dependency consistently `>= 11.0.2`
- Make `Microsoft.Azure.DocumentDB` dependency consistently `>= 2.0.0` (was temporarily 1.x on `net461` to fit in with constraints of a downstream)
- Refactor `Equinox.Cosmos` wiring to expose `EqxConnector.ConnectionPolicy`
- Use [`FSharp.UMX`](https://github.com/fsprojects/FSharp.UMX) from @alfonsogarciacaro and @eiriktsarpalis in tests/samples [#89](https://github.com/jet/equinox/pull/89) (HT @chinwobble in [#83](https://github.com/jet/equinox/pull/83) for prompting the change)

### Fixed

- Improve CosmosDb connection string parse error message 5b1b56bd94350ef47bd84ddbbb5b028e45fbb462
- change from `licenseUrl` to `license` in `.nupkg`
- Make `xUnit` dependency consistently `2.4.0`

<a name="1.0.3"></a>
## [1.0.3] - 2019-01-19

(See release notes in https://github.com/jet/equinox/releases and/or can someone please add it!)

[Unreleased]: https://github.com/jet/equinox/compare/1.1.0-preview1...HEAD
[1.1.0-preview1]: https://github.com/jet/equinox/compare/1.0.4...1.1.0-preview1
[1.0.4]: https://github.com/jet/equinox/compare/1.0.3...1.0.4
[1.0.3]: https://github.com/jet/equinox/compare/e28991d8005a2257594ac5cf5b764b76fdca7823...1.0.3