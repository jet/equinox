# Changelog

The repo is versioned based on [SemVer 2.0](https://semver.org/spec/v2.0.0.html) using the tiny-but-mighty [MinVer](https://github.com/adamralph/minver) from [@adamralph](https://github.com/adamralph). [See here](https://github.com/adamralph/minver#how-it-works) for more information on how it works.

All notable changes to this project will be documented in this file. The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

The `Unreleased` section name is replaced by the expected version of next release. A stable version's log contains all changes between that version and the previous stable version (can duplicate the prereleases logs).

## [Unreleased]

### Added

- `eqx dump` perf + logging improvements
- `eqx dump -P` turns off JSON pretty printing
- `Cosmos`: `Tip 200` now logs received `n` and `_etag` values

### Changed

- Update to `Microsoft.SourceLink.GitHub` v `1.0.0`
- Samples etc target `Argu` v `6.0.0`
- `eqx dump`'s `-J` switch now turns off JSON rendering
- `eqx -vc dump` now renders Store `.Information` logs
- Samples consistently use `module Fold`, `(Events.ForX streamId)`, removed `(|Stream|)`  [#174](https://github.com/jet/equinox/pull/174) 

### Removed
### Fixed

- `eqx`: Reinstated writing of missing commandline argument messages to console
- `Cosmos`: Fix `null` Data handling exception when log level <= `Debug`

<a name="2.0.0"></a>
<a name="2.0.0-rc8"></a>
## [2.0.0-rc8] - 2019-11-14

### Added

- `SqlStreamStore`: Full support for Microsoft Sql Server, MySQL and Postgres using [SqlStreamStore](https://github.com/SQLStreamStore/SQLStreamStore) [#168](https://github.com/jet/equinox/pull/168) :pray: [@rajivhost](https://github.com/rajivhost) 
- `Cosmos`: Exposed a `Connector.CreateClient` for interop with V2 ChangeFeedProcessor and `Propulsion.Cosmos` [#171](https://github.com/jet/equinox/pull/171) 
- `Cosmos`: Codified `AccessStrategy.RollingState` [#178](https://github.com/jet/equinox/pull/178) :pray: [@jgardella](https://github.com/jgardella)
- `Cosmos`: Added `eqx stats` command to count streams/docs/events in a CosmosDb Container re [#127](https://github.com/jet/equinox/issues/127) [#176](https://github.com/jet/equinox/pull/176) 
- `MemoryStore`: Supports custom Codec logic (can use `FsCodec.Box.Codec` as default) [#173](https://github.com/jet/equinox/pull/173) 
- `eqx dump [store]`: Show event data from store [#177](https://github.com/jet/equinox/pull/177)

### Changed

- Targeted `Destructurama.FSharp` v `1.1.1-dev-00033` [dotnet-templates#36](https://github.com/jet/dotnet-templates/issues/36)
- Targeted `FsCodec` v `1.2.1`
- `Cosmos`: renamed `Connector`'s `maxRetryAttemptsOnThrottledRequests` and `maxRetryWaitTimeInSeconds` to maxRetryAttemptsOnRateLimitedRequests` and `maxRetryWaitTimeOnRateLimitedRequests` and changed latter to `TimeSpan` to match V3 SDK [#171](https://github.com/jet/equinox/pull/171) 
- `AccessStrategy`: consistent naming for Union cases and arguments; rewrote xmldoc [#178](https://github.com/jet/equinox/pull/178) :pray: [@jgardella](https://github.com/jgardella)

### Removed

- `Resolver.ResolveEx` [#172](https://github.com/jet/equinox/pull/172)

### Fixed

- `Cosmos`: fixed accidentally swapped `count` and `bytes` metrics field values

<a name="2.0.0-rc7"></a>
## [2.0.0-rc7] - 2019-10-17

### Added

- `.Cosmos`: ability to inhibit server certificate validation via `Connector`'s `bypassCertificateValidation` option [#170](https://github.com/jet/equinox/pull/170) :pray: [@Kelvin4702](https://github.com/Kelvin4702)
- store-neutral `ICache`; centralized implementation in `Equinox.Core` [#161](https://github.com/jet/equinox/pull/161) :pray: [@DSilence](https://github.com/DSilence)
- `ResolveOption.AllowStale`, maximizing use of OCC for `Stream.Transact`, enabling stale reads (in the face of multiple writers) for `Stream.Query` [#167](https://github.com/jet/equinox/pull/167)
- Ability to (optionally) pass a `'Context` when creating a `Stream`, in order to be able to interop with FsCodec's `CorrelationId` and `CausationId` fields (as added in [FsCodec#22](https://github.com/jet/FsCodec/pulls/22)) [#169](https://github.com/jet/equinox/pull/169)

### Changed

- Updated minimum `Microsoft.Azure.DocumentDb[.Core]` dep from `2.0.0` to `2.2.0` (required for [#170](https://github.com/jet/equinox/pull/170))
- Updated `FsCodec` to `1.0.0` to pick up final name changes [#162](https://github.com/jet/equinox/pull/162)
- Replaced `TargetId.AggregateIdEmpty` with `ResolveOption.AssumeEmpty` [#163](https://github.com/jet/equinox/pull/163)
- Extracted `Equinox.Core` module [#164](https://github.com/jet/equinox/pull/164)
- Used `Transact` name consistently in `Accumulator` (follow-up to [#97](https://github.com/jet/equinox/pull/97)) [#166](https://github.com/jet/equinox/pull/166)
- Changed all curried Methods to tupled
- `.EventStore` now caches written values [#167](https://github.com/jet/equinox/pull/167)
- (internal) Updated `MinVer` to `2.0.0`

<a name="2.0.0-rc6"></a>
## [2.0.0-rc6] - 2019-09-07

### Added

- EventStore: expose `Index` when decoding events via `IIndexedEvent` [#158](https://github.com/jet/equinox/pull/158)
- `AsAt.fsx`: Illustrates usage of `IndexedEvent.Index` and snapshots in `.EventStore` vs `.Cosmos` [#159](https://github.com/jet/equinox/pull/159)

### Changed

- Updated to `FsCodec 1.0.0-rc2` to enable `TryDecode` to see `IIndexedEvent` without casting [#158](https://github.com/jet/equinox/pull/157)

<a name="2.0.0-rc5"></a>
## [2.0.0-rc5] - 2019-08-30

### Changed

- Extracted `Equinox.Codec` to external project `FsCodec`, with Json.net support in `FsCodec.NewtonsoftJson` [#156](https://github.com/jet/equinox/pull/156)

<a name="2.0.0-rc4"></a>
## [2.0.0-rc4] - 2019-08-26

### Added

- `Equinox.Stream.QueryEx`: exposes the stream's version in order to support versioned summary projections [#152](https://github.com/jet/equinox/pull/152)
- `Equinox.Cosmos.Context`: added overload requiring only (Connection, databaseId, containerId), enabling `Gateway` and `BatchingPolicy` concepts to be skipped over in single-Aggregate microservices where they are less relevant [#153](https://github.com/jet/equinox/pull/153) @jakzale

<a name="2.0.0-rc3"></a>
## [2.0.0-rc3] - 2019-08-20

### Added

- `Cosmos`: Add `AccessStrategy.RollingUnfolds`, leveraging `_etag`-contingent writes to allow state management without needing to write >=1 event per update [#146](https://github.com/jet/equinox/pull/146)

### Changed

- Updated README.md to refer to `propulsion init` and `propulsion project` (formerly `eqx` `initAux` and `project`) [jet/propulsion#17](https://github.com/jet/propulsion/pull/17)
- `Equinox.Cosmos` now uses `Container` in preference to `Collection`, in alignment with the `Microsoft.Azure.Cosmos` SDK's standardized naming, _and other minor changes, see PR for details_ [#149](https://github.com/jet/equinox/pull/149)
- `EQUINOX_COSMOS_COLLECTION` environment variable argument for `eqx` tool is now `EQUINOX_COSMOS_CONTAINER` [#143](https://github.com/jet/equinox/pull/143)
- `eqx project` now uses environment variables `PROPULSION_KAFKA_*` instead of `EQUINOX_*` [#143](https://github.com/jet/equinox/pull/143)
- renamed `Equinox.DeprecatedRawName` -> `StreamName` [#150](https://github.com/jet/equinox/pull/150)

### Removed

- `eqx project` - `ChangeFeedProcessor` and Kafka support - All projection management logic now lives in the `Propulsion` libraries [#138](https://github.com/jet/equinox/pull/138)
- `eqx initAux` - now `propulsion init` (jet/propulsion#17)[https://github.com/jet/propulsion/pull/17]

<a name="2.0.0-rc2"></a>
## [2.0.0-rc2] - 2019-07-01

### Changed

- `eqx initAux` now sets Partition Key to `/id` as recommended for new `aux` collections [#142](https://github.com/jet/equinox/pull/142)

<a name="2.0.0-rc1"></a>
## [2.0.0-rc1] - 2019-06-14

### Added

- `Equinox.Codec` now uses `System.Buffers.ArrayPool` to reduce allocations when encoding/decoding union objects using json.net [#140](https://github.com/jet/equinox/pull/140)
- `Equinox.Codec` now uses `RecyclableMemoryStreamManager` to reduce allocations wrt `MemoryStream` objects when encoding union objects using json.net [#139](https://github.com/jet/equinox/pull/139)

### Changed

- `TypeShape` dependency in `Equinox.Codec` now `7.*` (was `6.*`)
- `Equinox.Tool` now depends on `Propulsion.Kafka` v `>= 1.0.1`
- `Equinox.Cosmos.Projection` is only an internal dependency of the `Equinox.Tool`
- `Equinox.Cosmos`: Top level `CosmosStore` is now called `Context` for consistency [#140](https://github.com/jet/equinox/pull/140)
- `Equinox.EventStore`: Top level `GesGateway` is now called `Context` for consistency [#140](https://github.com/jet/equinox/pull/140)
- Builders etc. in `Equinox.Cosmos`, `Equinox.EventStore` and `Equinox.MemoryStore` no longer have disambiguating `Cosmos`, `Ges` and `Memory` prefixes [#140](https://github.com/jet/equinox/pull/140)

### Removed

- `Equinox.Projection` has moved to `Propulsion`
- `Equinox.Projection.Codec` has moved to `Propulsion.Kafka`
- `Equinox.Cosmos.Projection` is now maintained in/as `Propulsion.Cosmos` (it's still in the repo for now, pending resolution of [PR #138](https://github.com/jet/equinox/pull/138) and [`Propulsion` issue #6](https://github.com/jet/propulsion/issues/6))

### Fixed

- `EqxCosmos Sync: Conflict writing {eventTypes}` message now trims to max 5 items

<a name="2.0.0-preview9"></a>
## [2.0.0-preview9] - 2019-06-01

### Added

- `Equinox.EventStore.GesGateway.Sync` API including adding `actualVersion` to `ConflictUnknown` Result DU case for [`dotnet new eqxsync` template `EventStoreSink` impl](https://github.com/jet/dotnet-templates/pull/23) [#133](https://github.com/jet/equinox/pull/133)
- `Equinox.EventStore` `Stats` for consistency with CosmosDb equivalent [#133](https://github.com/jet/equinox/pull/133)

### Changed

- Targeted `Jet.ConfluentKafka.FSharp` v `1.0.0-rc12` in `eqx` tool
- `Equinox.Tool`: Switched `IndexingMode` to `Automatic=false,IndexingMode=None`, remove `DefaultTimeToLive` from `aux` collections [#134](https://github.com/jet/equinox/pull/134)
- `Equinox.Cosmos.Projection`: Tidy `assign` and `init`, signatures; provided mechanism to inhibit logging [#137](https://github.com/jet/equinox/pull/137)

### Removed

- `Equinox.Projection` logic - Projection management logic now lives in the `Propulsion` libraries (`StreamItem` is the sole remaining item in the `Equinox.Projection` library).

### Fixed

- `Equinox.EventStore`: Fixed `PreferSlave` bug for `Discovery.Uri` mode [#135](https://github.com/jet/equinox/pull/135)
- `Equinox.EventStore`: Fixed `PreferSlave` and `Random` modes for gossip-based `Discovery` modes [#135](https://github.com/jet/equinox/pull/135) [@asetda](https://github.com/asetda)

<a name="2.0.0-preview8"></a>
## [2.0.0-preview8] - 2019-05-16

### Changed

- Replace flat single-item `Equinox.Projection.Codec.RenderedEvent` with `RenderedSpan` to match incoming Projection enhancements [#131](https://github.com/jet/equinox/pull/131)
- `Equinox.Projection` performance and interface extraction work via [`dotnet new eqxsync` template](https://github.com/jet/dotnet-templates/pull/16) [#132](https://github.com/jet/equinox/pull/132)

<a name="2.0.0-preview7"></a>
## [2.0.0-preview7] - 2019-05-14

### Changed

- `Equinox.Projection` performance work based on [`dotnet new eqxsync` template](https://github.com/jet/dotnet-templates/pull/16) [#130](https://github.com/jet/equinox/pull/130)
- Reorganized `Equinox.Projection` to be a single-file concern [#129](https://github.com/jet/equinox/pull/129)

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

[Unreleased]: https://github.com/jet/equinox/compare/2.0.0-rc8...HEAD
[2.0.0-rc8]: https://github.com/jet/equinox/compare/2.0.0-rc7...2.0.0-rc8
[2.0.0-rc7]: https://github.com/jet/equinox/compare/2.0.0-rc6...2.0.0-rc7
[2.0.0-rc6]: https://github.com/jet/equinox/compare/2.0.0-rc5...2.0.0-rc6
[2.0.0-rc5]: https://github.com/jet/equinox/compare/2.0.0-rc4...2.0.0-rc5
[2.0.0-rc4]: https://github.com/jet/equinox/compare/2.0.0-rc3...2.0.0-rc4
[2.0.0-rc3]: https://github.com/jet/equinox/compare/2.0.0-rc2...2.0.0-rc3
[2.0.0-rc2]: https://github.com/jet/equinox/compare/2.0.0-rc1...2.0.0-rc2
[2.0.0-rc1]: https://github.com/jet/equinox/compare/2.0.0-preview9...2.0.0-rc1
[2.0.0-preview9]: https://github.com/jet/equinox/compare/2.0.0-preview8...2.0.0-preview9
[2.0.0-preview8]: https://github.com/jet/equinox/compare/2.0.0-preview7...2.0.0-preview8
[2.0.0-preview7]: https://github.com/jet/equinox/compare/2.0.0-preview6...2.0.0-preview7
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