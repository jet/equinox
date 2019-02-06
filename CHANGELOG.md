# Changelog

The repo is versioned based on [SemVer 2.0](https://semver.org/spec/v2.0.0.html) using the tiny-but-mighty [MinVer](https://github.com/adamralph/minver) from [@adamralph](https://github.com/adamralph). [See here](https://github.com/adamralph/minver#how-it-works) for more information on how it works.

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
The `Unreleased` section name is replaced by the expected version of next release. A stable version's log contains all changes between that version and the previous stable version (can duplicate the prereleases logs).

_NB at the present time, this project does not adhere strictly to Semantic Versioning - small binary-breaking changes may occur without a change in Major at the until this notice is removed (it will be!)._

## [Unreleased] - (tentatively `1.1.0`)

### Added

`Equinox.Cosmos` projection facilities, see [#87](https://github.com/jet/equinox/pull/87)

### Changed
### Removed
### Fixed

<a name="1.0.4"></a>
## [1.0.4] - TBA, planning 2019-02-06

### Changed

- Rename `Equinox.CatId` to `Equinox.AggregateId` (HT @lfr for prompting the change)
- Use [`FSharp.UMX`](https://github.com/fsprojects/FSharp.UMX) from @alfonsogarciacaro and @eiriktsarpalis in tests/samples [#89](https://github.com/jet/equinox/pull/89) (HT @chinwobble in [#83](https://github.com/jet/equinox/pull/83) for prompting the change)
- Make `Newtonsoft.Json` dependency consistently `>= 11.0.2`
- Refactor `Equinox.Cosmos` wiring to expose `EqxConnector.ConnectionPolicy`

### Fixed

- Make `xUnit` dependency consistently `2.4.0`
- Improve CosmosDb connection string parse error message 5b1b56bd94350ef47bd84ddbbb5b028e45fbb462
- change from `licenseUrl` to `license` in `.nupkg`

<a name="1.0.3"></a>
## [1.0.3] - 2019-01-19

(See release notes in https://github.com/jet/equinox/releases and/or can someone please add it!)

[Unreleased]: https://github.com/jet/equinox/compare/1.0.4...HEAD
[1.0.4]: https://github.com/jet/equinox/compare/1.0.3...1.0.4
[1.0.3]: https://github.com/jet/equinox/compare/e28991d8005a2257594ac5cf5b764b76fdca7823...1.0.3