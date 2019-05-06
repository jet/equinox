# Changelog

The repo is versioned based on [SemVer 2.0](https://semver.org/spec/v2.0.0.html) using the tiny-but-mighty [MinVer](https://github.com/adamralph/minver) from [@adamralph](https://github.com/adamralph). [See here](https://github.com/adamralph/minver#how-it-works) for more information on how it works.

All notable changes to this project will be documented in this file. The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

The `Unreleased` section name is replaced by the expected version of next release. A stable version's log contains all changes between that version and the previous stable version (can duplicate the prereleases logs).

## [Unreleased]

### Added
### Changed
### Removed
### Fixed

<a name="2.0.0"></a>
<a name="2.0.0-preview6"></a>
## [2.0.0-preview6] - 2019-05-06

### Added

- Added `Equinox.Projection` state management: `StreamState`/`StreamStates`  [#128](https://github.com/jet/equinox/pull/128)
- Added `Equinox.Projection` pipeline: `Progress`/`Scheduling`/`Projector`/Ingestion`/`Ingester` [#128](https://github.com/jet/equinox/pull/128)
- Added `Equinox.Cosmos.Store.Log.Metrics.dump` as a placeholder stats mechanism [#126](https://github.com/jet/equinox/pull/126)
- Added `Equinox.Projection.StreamItem` as canonical representation of a raw item from a feed in Projections [#125](https://github.com/jet/equinox/pull/125)

### Changed

- [re]Introduced `Equinox.Projection.Codec` to house Codec-specific aspects of the shared projection presently in `Equinox.Projection` [#125](https://github.com/jet/equinox/pull/125)
- `ChangeFeedObserver`: Made `assign` and `revoke` extensibility points in builder `async` [#124](https://github.com/jet/equinox/pull/124)
- `ChangeFeedObserver`: Renamed `ChangeFeedObserver`'s `processBatch` to `ingest` and documented role of `IChangeFeedObserverContext.Checkpoint` in more detail [#124](https://github.com/jet/equinox/pull/124)
- Targeted `Jet.ConfluentKafka.FSharp` v `1.0.0-rc6` in `eqx` tool

<a name="2.0.0-preview5"></a>
## [2.0.0-preview5] - 2019-04-12

### Added

- exposed `assign` and `revoke` extensibility points in `ChangeFeedObserver` builder [#119](https://github.com/jet/equinox/pull/119)

### Changed

- switched ChangeFeedProcessor checkpointing to be _explicit_ (was automatic) based on requirements of [`equinox-sync` template PR #19](https://github.com/jet/dotnet-templates/pull/19) [#119](https://github.com/jet/equinox/pull/119)

### Fixed

- Added `partitionRangeId` context to `ChangeFeedObserver` logging [#119](https://github.com/jet/equinox/pull/119)

<a name="2.0.0-preview4"></a>
## [2.0.0-preview4] - 2019-04-03

### Added

- Added database-level RU-provisioning support (i.e., `eqx init`'s `-D` switch) [#120](https://github.com/jet/equinox/issues/120)
- Implemented ability to amend RU allocations where database/collection already exists [#112](https://github.com/jet/equinox/issues/112)

### Changed

- Targeted `Jet.ConfluentKafka.FSharp` v `1.0.0-rc2` in `eqx` tool
- Removed special casing of `Tip` batches from `Equinox.Cosmos.Projection` in preparation for transparent integration of [#110](https://github.com/jet/equinox/pull/110) without necessitating updating of projectors and related systems [#119](https://github.com/jet/equinox/pull/119)

<a name="2.0.0-preview3"></a>
## [2.0.0-preview3] - 2019-03-27

### Fixed

- Reading `null` from Equinox.Cosmos and then writing that to Kafka yielded invalid json [#18](https://github.com/jet/equinox/issues/118)

<a name="2.0.0-preview2"></a>
## [2.0.0-preview2] - 2019-03-23

### Added

- `Cosmos.Projection.ChangeFeedProcessor`: Support management of an `aux` collection in account other than the one being read from by adding `auxAccountKey` param [#115](https://github.com/jet/equinox/pull/115)
- Support ETL scenarios by enabling the event creation `Timestamp` to be [read and] written by supplying it in `Equinox.Codec.IEvent` [#116](https://github.com/jet/equinox/issues/116)

### Changed

- Rename `Equinox.Projection.Codec` NuGet to `Equinox.Projection` (no code changes)
- Renamespace and separate `Equinox.Codec` to `Equinox.Codec.NewtonsoftJson` and `Equinox.Codec.Custom` (in preparation for [#113](https://github.com/jet/equinox/issues/113)) HT @szer
- Uses MinVer 1.0.0 [internally] to compute package versions (was `rc.1`, `beta.4` along the way)

### Removed

- Remove `maxEventsPerSlice`/`maxTipEvents` pending [#109](https://github.com/jet/equinox/issues/109)

<a name="2.0.0-preview1"></a>
## [2.0.0-preview1] - 2019-03-07

### Added

- Provide capability to access `Metadata` and `EventNumber`/`Index` re [#102](https://github.com/jet/equinox/issues/102)

### Changed

- Make `caching` non-optional in `CosmosStreamResolver`; add `NoCaching` cache mode for `Equinox.Cosmos` [#104](https://github.com/jet/equinox/issues/104) @jakzale
- Reorder `caching` and `access` in `GesStreamResolver` to match `CosmosStreamResolver` [#107](https://github.com/jet/equinox/issues/107)
- Renamespaced and separated `Equinox.Codec` APIs to separate `Newtonsoft.Json` and custom `encode`/`tryDecode` approaches [#102](https://github.com/jet/equinox/issues/102) (in preparation for [#79](https://github.com/jet/equinox/issues/79))

### Removed

- Extract `Equinox.Projection.Kafka` and its integration tests to become [`Jet.ConfluentKafka.FSharp` v `1.0.0-preview1`](https://github.com/jet/Jet.ConfluentKafka.FSharp/tree/v1); retargeted `Equinox.Tool` to use same.

### Fixed

- Add Writing empty event list guard for `Equinox.Cosmos` [#105](https://github.com/jet/equinox/issues/105)
- Disable support for non-record Event payloads in `Equinox.Codec` [#103](https://github.com/jet/equinox/issues/103)

<a name="1.1.0-preview2"></a>
## [1.1.0-preview2] - 2019-02-20

### Added

- `Equinox.Projection.Kafka` consumer metrics emission, see [#94](https://github.com/jet/equinox/pull/94) @michaelliao5
- Add `samples/Tutorial` with `.fsx` files (see also related docs)
- Overloads to reduce need to use `CosmosCollections` @jakzale

### Changed

- Target `EventStore.Client 5.*` to avail of bugfixes and single client codebase. Interoperable with `4.*` servers
- Updated to `"Microsoft.SourceLink.GitHub" Version="1.0.0-beta2-18618-05"`
- Replace stateful `Context` API as primary Handler API with `Transact`; Compatible `Accumulator` remains to facilitate porting [#97](https://github.com/jet/equinox/pull/97) @eiriktsarpalis
- Replace `Handler` with `Stream`
- Replace `Equinox.Cosmos.Eqx*` with `Cosmos*` @jakzale
- Replace `Equinox.MemoryStore.Mem*` with `Memory*`

### Removed
- Moved internal `Stream` helpers from `Equinox` to instead be inlined into Store impls [#97](https://github.com/jet/equinox/pull/97) @eiriktsarpalis
- De-emphasized `Handler` in sample aggregates @jakzale

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
- Refactor `Equinox.Cosmos` wiring to expose `CosmosConnector.ConnectionPolicy`
- Use [`FSharp.UMX`](https://github.com/fsprojects/FSharp.UMX) from @alfonsogarciacaro and @eiriktsarpalis in tests/samples [#89](https://github.com/jet/equinox/pull/89) (HT @chinwobble in [#83](https://github.com/jet/equinox/pull/83) for prompting the change)

### Fixed

- Improve CosmosDb connection string parse error message 5b1b56bd94350ef47bd84ddbbb5b028e45fbb462
- change from `licenseUrl` to `license` in `.nupkg`
- Make `xUnit` dependency consistently `2.4.0`

<a name="1.0.3"></a>
## [1.0.3] - 2019-01-19

(For information pertaining to earlier releases, see release notes in https://github.com/jet/equinox/releases and/or can someone please add it!)

[Unreleased]: https://github.com/jet/equinox/compare/2.0.0-preview6...HEAD
[2.0.0-preview6]: https://github.com/jet/equinox/compare/2.0.0-preview5...2.0.0-preview6
[2.0.0-preview5]: https://github.com/jet/equinox/compare/2.0.0-preview4...2.0.0-preview5
[2.0.0-preview4]: https://github.com/jet/equinox/compare/2.0.0-preview3...2.0.0-preview4
[2.0.0-preview3]: https://github.com/jet/equinox/compare/2.0.0-preview2...2.0.0-preview3
[2.0.0-preview2]: https://github.com/jet/equinox/compare/2.0.0-preview1...2.0.0-preview2
[2.0.0-preview1]: https://github.com/jet/equinox/compare/1.1.0-preview2...2.0.0-preview1
[1.1.0-preview2]: https://github.com/jet/equinox/compare/1.1.0-preview1...1.1.0-preview2
[1.1.0-preview1]: https://github.com/jet/equinox/compare/1.0.4...1.1.0-preview1
[1.0.4]: https://github.com/jet/equinox/compare/1.0.3...1.0.4
[1.0.3]: https://github.com/jet/equinox/compare/e28991d8005a2257594ac5cf5b764b76fdca7823...1.0.3