# Changelog

The repo is versioned based on [SemVer 2.0](https://semver.org/spec/v2.0.0.html) using the tiny-but-mighty [MinVer](https://github.com/adamralph/minver) from [@adamralph](https://github.com/adamralph). [See here](https://github.com/adamralph/minver#how-it-works) for more information on how it works.

All notable changes to this project will be documented in this file. The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

The `Unreleased` section name is replaced by the expected version of next release. A stable version's log contains all changes between that version and the previous stable version (can duplicate the prereleases logs).

## [Unreleased]

### Added

- `Equinox`: `Decider.Transact(interpret : 'state -> Async<'event list>)` [#314](https://github.com/jet/equinox/pull/314)

### Changed

- `eqx`/`Equinox.Tool`: Flip `-P` option to opt _in_ to pretty printing [#313](https://github.com/jet/equinox/pull/313)
- `Equinox`: rename `Decider.TransactAsync` to `Transact` [#314](https://github.com/jet/equinox/pull/314)
- `Equinox`: Merge `ResolveOption` and `XXXStoreCategory.FromMemento` as `LoadOption` [#308](https://github.com/jet/equinox/pull/308)
- `Equinox`: Merge `XXXStoreCategory.Resolve(sn, ?ResolveOption)` and `XXXStoreCategory.FromMemento` as option `LoadOption` parameter on all `Transact` and `Query` methods [#308](https://github.com/jet/equinox/pull/308)
- `CosmosStore`: Require `Microsoft.Azure.Cosmos` v `3.0.25` [#310](https://github.com/jet/equinox/pull/310)
- `CosmosStore`: Switch to natively using `JsonElement` event bodies [#305](https://github.com/jet/equinox/pull/305) :pray: [@ylibrach](https://github.com/ylibrach)
- `CosmosStore`: Switch to natively using `System.Text.Json` for serialization of all `Microsoft.Azure.Cosmos` round-trips [#305](https://github.com/jet/equinox/pull/305) :pray: [@ylibrach](https://github.com/ylibrach)
- `CosmosStore`: Only log `bytes` when log level is `Debug` [#305](https://github.com/jet/equinox/pull/305)
- `SqlStreamStore`.*: Target `SqlStreamStore` v `1.2.0` (`Postgres` remains at `1.2.0-beta.8` as that's the last released version) [#227](https://github.com/jet/equinox/pull/227) :pray: [@rajivhost](https://github.com/rajivhost)
- Update all non-Client dependencies except `FSharp.Core`, `FSharp.Control.AsyncSeq` [#310](https://github.com/jet/equinox/pull/310)

### Removed
 
- Remove explicit `net461` handling; minimum target now `netstandard 2.1` / `net6.0` [#310](https://github.com/jet/equinox/pull/310)

### Fixed

- `EventStore/SqlStreamStore`: rename `Equinox.XXXStore.Log.Event` -> `Metric` to match `CosmosStore` [#311](https://github.com/jet/equinox/pull/311)
- `SqlStreamStore`: Fix `Metric` key to be `ssEvt` (was `esEvt`) [#311](https://github.com/jet/equinox/pull/311)

<a name="3.0.6"></a>
## [3.0.6] - 2022-01-19

### Changed

- `CosmosStore`: Cleanup `Microsoft.Azure.Cosmos` calls [#303](https://github.com/jet/equinox/pull/303)

### Removed

- Removed Grafana spec now that canonical is in `dotnet-templates` [#304](https://github.com/jet/equinox/pull/304)

### Fixed

- `CosmosStore`: Fix fallback detection when all events pruned from Tip [#306](https://github.com/jet/equinox/pull/306)

<a name="3.0.5"></a>
## [3.0.5] - 2021-11-18

### Added

- Stores: Expose `.Log.PropertyTag` Literals to enable log filtering  [#298](https://github.com/jet/equinox/pull/298)
- `Equinox.Tool`: Add support for [autoscaling throughput](https://docs.microsoft.com/en-us/azure/cosmos-db/provision-throughput-autoscale) of Cosmos containers and databases [#302](https://github.com/jet/equinox/pull/302) :pray: [@belcher-rok](https://github.com/belcher-rok)

### Fixed

- `MemoryStore`: Fixed incorrect `Version` computation for `TransactEx` post-State [#296](https://github.com/jet/equinox/pull/296)

<a name="3.0.4"></a>
## [3.0.4] - 2021-10-15

### Fixed

- `CosmosStore`: Fixed event retrieval bug for `Reload` for both `Query` with cached value and resync cases [#295](https://github.com/jet/equinox/pull/295) :pray: [@ragiano215](https://github.com/ragiano215)

<a name="3.0.3"></a>
## [3.0.3] - 2021-09-16

### Changed

- `EventStore`: Add `customize` hook to `Connector` [#286](https://github.com/jet/equinox/pull/286) (cherry-pick from `2.6.0`)

<a name="3.0.2"></a>
## [3.0.2] - 2021-09-13

### Fixed

- Fix CI to stop emitting builds with incorrect `AssemblyVersion 1.0.0.0` (updated MinVer to `2.5.0`)
- Update global.json to use SDK version `5.0.200`

<a name="3.0.1"></a>
## [3.0.1] - 2021-06-11

### Added

- `CosmosStore.CosmosStoreConnector`: Wrapper for `CosmosClientFactory` enabling cleaner consumer code [#292](https://github.com/jet/equinox/issues/292)

### Fixed

- `CosmosStore`: Reinstate default connection mode as `Direct` per [#281](https://github.com/jet/equinox/issues/281) [#287](https://github.com/jet/equinox/issues/287)

<a name="3.0.0"></a>
## [3.0.0] - 2021-05-13

### Changed

- `CosmosStore.Prometheus.LogSink`: Aligned signature with Equivalent Propulsion one :pray: [@deviousasti](https://github.com/deviousasti)

<a name="3.0.0-beta.4"></a>
## [3.0.0-beta.4] - 2021-04-28

### Added

- `CosmosStore.Prometheus.LogSink`: Generalized `app` tag to arbitrary custom tags as per `Cosmos` [#287](https://github.com/jet/equinox/issues/287)

<a name="3.0.0-beta.3"></a>
## [3.0.0-beta.3] - 2021-04-06

### Added

- `CosmosStore.CosmosStoreClient`: Merge of `CosmosStore.CosmosStoreConnector` and `CosmosStoreConnection` [#280](https://github.com/jet/equinox/pull/280)
- `CosmosStore.Discovery.Endpoint`: Enables parsing of Endpoint Uri from connection string [#284](https://github.com/jet/equinox/pull/284) [#280](https://github.com/jet/equinox/pull/280)

### Changed

- `CosmosStore`: Add stream name to "EqxCosmos Tip" log entries

### Fixed

- `Equinox.Tool`: Fix incorrect `Azure.Cosmos.Direct` reference [#280](https://github.com/jet/equinox/pull/208)

<a name="3.0.0-beta.2"></a>
## [3.0.0-beta.2] - 2021-03-12

### Added

- `CosmosStore.CosmosStoreConnector`: Uses the CosmosDB SDK 3.17 `CreateAndInitialize` to correctly connect and initialize a CosmosClient for an application [#279](https://github.com/jet/equinox/pull/279)

### Changed

- `CosmosStore`: Default Connection Mode now `Direct` to align with V3 SDK (previous default was `Gateway` to match V2 SDK) [#281](https://github.com/jet/equinox/pull/281)
- `CosmosStore`: Target `Microsoft.Azure.Cosmos` v `3.17.0` [#278](https://github.com/jet/equinox/pull/278)

### Fixed

- `CosmosStore`: `Dispose()` as required by updated CosmosDB SDK `FeedIterator` interface [#278](https://github.com/jet/equinox/pull/278)

<a name="3.0.0-beta.1"></a>
## [3.0.0-beta.1] - 2021-02-24

### Added
 
 - `Equinox.CosmosStore`: Forked from `Equinox.Cosmos` to add significant new features (see Removed section for deprecation policy info).
   - Added support for accumulating events in Tip, i.e. being able to operate with a single document per stream until the events (if any) overflow a defined size threshold [#251](https://github.com/jet/equinox/pull/251) see also [#110](https://github.com/jet/equinox/pull/110)
   - Added Secondary store fallback for Event loading, enabling streams to be hot-migrated (archived to a secondary/clone, then pruned from the primary/active) between Primary and Secondary stores [#247](https://github.com/jet/equinox/pull/247), [#259](https://github.com/jet/equinox/pull/259)
   - Added support for pruning events within the Tip document [#258](https://github.com/jet/equinox/pull/258) see also [#233](https://github.com/jet/equinox/pull/233)
   - Added Prometheus integration package [#266](https://github.com/jet/equinox/pull/266) see also [#267](https://github.com/jet/equinox/pull/267)
   - target `Microsoft.Azure.Cosmos` v `3.9.0` (instead of `Microsoft.Azure.DocumentDB`[`.Core`] v 2.x) [#144](https://github.com/jet/equinox/pull/144)
   - Replaced `BatchingPolicy`, `RetryPolicy` with `TipOptions`, `QueryOptions` to better align with Cosmos SDK V4 [#253](https://github.com/jet/equinox/pull/253)
   - Reorganized `QueryRetryPolicy` to handle `IAsyncEnumerable` coming in Cosmos SDK V4 [#246](https://github.com/jet/equinox/pull/246) :pray: [@ylibrach](https://github.com/ylibrach)
   - Remove exceptions from 304/404 paths when reading Tip [#257](https://github.com/jet/equinox/pull/257)
   - Removed [warmup call](https://github.com/Azure/azure-cosmos-dotnet-v3/issues/1436)
   - Rename `Equinox.Cosmos` DLL and namespace to `Equinox.CosmosStore` [#243](https://github.com/jet/equinox/pull/243)
        - Rename `Equinox.Cosmos.Store` -> `Equinox.CosmosStore.Core` 
        - `Core` sub-namespace
            - Rename `Equinox.Cosmos.Core.Context` -> `Equinox.CosmosStore.Core.EventsContext`
            - Change `Equinox.Cosmos.Core.Connection` -> `Equinox.CosmosStore.Core.RetryPolicy`
            - Rename `Equinox.Cosmos.Core.Gateway` -> `Equinox.CosmosStore.Core.StoreClient`
        - Rename `Equinox.Cosmos.Containers` -> `Equinox.CosmosStore.CosmosStoreConnection`
        - Rename `Equinox.Cosmos.Context` -> `Equinox.CosmosStore.CosmosStoreContext`
        - Rename `Equinox.Cosmos.Resolver` -> `Equinox.CosmosStore.CosmosStoreCategory`
        - Rename `Equinox.Cosmos.Connector` -> `Equinox.CosmosStore.CosmosStoreClientFactory`

### Changed

- `Equinox`: Rename `type Stream` to `Decider` [#272](https://github.com/jet/equinox/pull/272) :pray: [@thinkbeforecoding](https://github.com/thinkbeforecoding)

- `Equinox.Core`: Simplify `AsyncCacheCell` [#229](https://github.com/jet/equinox/pull/229)

- `Equinox.EventStore`: target `EventStore.Client` v `20.6` (instead of v `5.0.x`) [#224](https://github.com/jet/equinox/pull/224)

- _All Stores_: `FSharp.Control.AsyncSeq` v `2.0.23`

- `Equinox.Tool`: Target `FSharp.Core` v `4.7.1`
- `Equinox.Tool`: Add `<RollForward>Major</RollForward>` [#270](https://github.com/jet/equinox/pull/270)

- _All Stores_: Standardise naming of top level structure: `<StoreName>Connection` (wraps the relevant `*Client` for that store), `Context`, `Category` [#276](https://github.com/jet/equinox/pull/276)
    - Rename `Resolver` -> `<StoreName>Category`
    - Rename `Context` -> `<StoreName>Context`

- Update AzDO CI/CD to use `windows-latest`
- Update to `3.1.101` SDK
- Remove `module Commands` convention from examples
- Remove separated `Backend` project from examples (support for architecturally separating all domain logic from Equinox and Domain Service logic from Concrete Stores remains)
- Retarget `netcoreapp2.1` sample apps to `netcoreapp3.1` with `SystemTextJson`
- Retarget Todobackend to `aspnetcore` v `3.1`
- Revise semantics of Cart Sample Command handling

### Removed

- `Equinox.Cosmos`: for now, there is no intention to release a `3.x` version; instead `Equinox.CosmosStore` maintains forward compatibility of the `2.x` data format but moves to `Microsoft.Azure.Cosmos`. Version `2.x` releases (which continue to depend on `Microsoft.Azure.DocumentDb.Core` are published from the https://github.com/jet/equinox/tree/v2 branch

### Fixed

- `Equinox`: change `createAttemptsExhaustedException` to allow any `exn`-derived `type` [#275](https://github.com/jet/equinox/pull/275)

<a name="2.6.0"></a>
## [2.6.0] - 2021-04-28

### Added

- `Cosmos.Discovery.Endpoint`: Extracts Uri for diagnostic purposes [#284](https://github.com/jet/equinox/issues/284)
- `Cosmos.Prometheus.LogSink`: Generalized `app` tag to arbitrary custom tags [#287](https://github.com/jet/equinox/issues/287)

### Changed

- `EventStore`: Add `customize` hook to `Connector` [#286](https://github.com/jet/equinox/pull/286)
- `EventStore`: Pin `EventStore.Client` to `[5.0.11,6)` [#286](https://github.com/jet/equinox/pull/286)
- `EventStore`: Added [`SetAutoCompatibilityMode("auto")`](https://www.eventstore.com/blog/5.0.11tcp-client-release-notes-0-0) to default wireup [#286](https://github.com/jet/equinox/pull/286)

<a name="2.5.1"></a>
## [2.5.1] - 2021-03-12

### Changed

- `ISyncContext.Version`: documented value as being `0`-based, rather than whatever a given store happens to use internally (which happens to align with the native version representation in `Equinox.Cosmos`) [#282](https://github.com/jet/equinox/pull/282)
- `MemoryStore` / `SqlStreamStore` / `EventStore`: aligned implementations to represent `Version` in a store-neutral manner per the documentation change [#282](https://github.com/jet/equinox/pull/282)

### Removed
### Fixed

- `Cosmos` / `ISyncContext.Version`: fixed erroneous `0` value when re-reading with caching but without snapshots in `Cosmos` store [#282](https://github.com/jet/equinox/pull/282)

<a name="2.5.0"></a>
## [2.5.0] - 2021-02-24

### Added

- `Stream.TransactEx`: extended signature, replacing `TransactAsyncEx`. Provides the `ISyncContext` both before and after the `decide` so pre-flight checks can be performed (as `master` [#263](https://github.com/jet/equinox/pull/263)) [#277](https://github.com/jet/equinox/pull/277)

### Removed

- `Stream.TransactAsyncEx` (as `master` [#263](https://github.com/jet/equinox/pull/263)) [#277](https://github.com/jet/equinox/pull/277)

### Fixed

- `Cosmos.Prometheus`: Correct namespace (was erroneously `Equinox.CosmosStore.Prometheus`) [#271](https://github.com/jet/equinox/pull/271)

<a name="2.4.0"></a>
## [2.4.0] - 2020-12-03

### Added

- `Cosmos.Prometheus`: Prometheus integration package as `master` [#266](https://github.com/jet/equinox/pull/266) [#267](https://github.com/jet/equinox/pull/267)

### Fixed

- `MemoryStore`: Serialize `Committed` events to guarantee consumption in event `Index` order re [#265](https://github.com/jet/equinox/issues/265) [#269](https://github.com/jet/equinox/pull/269) :pray: [@fnipo](https://github.com/fnipo)
- `Cosmos`: Fix defaulting for `compressUnfolds` in C# [#261](https://github.com/jet/equinox/pull/261)

<a name="2.3.0"></a>
## [2.3.0] - 2020-11-04

### Added

- `CachingStrategy.FixedTimeSpan`: Support for fixed caching periods [#255](https://github.com/jet/equinox/pull/255)

### Changed

- `Cosmos`: Finalize `Prune` API to align with `master` [#258](https://github.com/jet/equinox/pull/258)

<a name="2.3.0-rc2"></a>
## [2.3.0-rc2] - 2020-10-02

### Added

- `Cosmos`: Support Serverless Account Mode in `eqx init`; default RU/s to 400 if unspecified [#244](https://github.com/jet/equinox/pull/244) :pray: [@OmnipotentOwl](https://github.com/OmnipotentOwl)
- `Cosmos`: Added ability to turn off compression of Unfolds [#249](https://github.com/jet/equinox/pull/249) :pray: [@ylibrach](https://github.com/ylibrach)

### Changed

- `Cosmos`: Reorganize Sync log message text, merge with Sync Conflict message [#241](https://github.com/jet/equinox/pull/241)
- `Cosmos`: Converge Stored Procedure Impl with `tip-isa-batch` impl from V3 (minor Request Charges cost reduction) [#242](https://github.com/jet/equinox/pull/242)

<a name="2.3.0-rc1"></a>
## [2.3.0-rc1] - 2020-08-31

### Added

- `Cosmos`: `Prune` API to delete events from the head of a stream [#233](https://github.com/jet/equinox/pull/233)

### Changed

- `Cosmos`: Tweaked connecting log message

<a name="2.2.0"></a>
## [2.2.0] - 2020-08-04

### Added

- Add `AsyncBatchingGate` [#228](https://github.com/jet/equinox/pull/228)

### Fixed

- `EventStore`: Pin `EventStore.Client` to `[5.0.1,6)` to avoid conflicting changes in `EventStore.Client` v `20.06` [#223](https://github.com/jet/equinox/pull/223)

<a name="2.1.0"></a>
## [2.1.0] - 2020-05-22

### Added

- `eqx dump -b`, enabling overriding of Max Events per Batch
- `MemoryStore`: Add `Committed` event to enable simulating Change Feeds in integration tests re [#205](https://github.com/jet/equinox/issues/205) [#221](https://github.com/jet/equinox/pull/221)

### Changed

- `MemoryStore`: Target `FsCodec` v `2.0.0` [#219](https://github.com/jet/equinox/pull/219)

<a name="2.0.2"></a>
## [2.0.2] - 2020-05-10

- _Rebuilt version of `2.0.1` with new VM image in order to release complete set of nupkg files vs only releasing MsSql_

<a name="2.0.1"></a>
## [2.0.1] - 2020-03-25 - unlisted due to incomplete package set

### Fixed

- `SqlStreamStore.MsSql`: Initial Append when stream empty on MsSql was perpetually failing [#209](https://github.com/jet/equinox/pull/209) :pray: [@Kimserey](https://github.com/kimserey)

<a name="2.0.0"></a>
## [2.0.0] - 2020-02-19

### Added

- `Stream.TransactAsyncEx`, exposing the `Core.ISyncContext` at conclusion of the sync operation, affording the ability to examine the post-state `Version` etc. (This paves the way for exposing [`SessionToken`](https://github.com/jet/equinox/issues/192) at a later point without a breaking change) [#194](https://github.com/jet/equinox/pull/194)

### Changed

- `Stream.QueryEx` to supply `Core.ISyncContext` in lieu of only exposing `Version` (to align with `TransactAsyncEx`) [#194](https://github.com/jet/equinox/pull/194)
- Target `FsCodec` v `2.0.0`

<a name="2.0.0-rc9"></a>
## [2.0.0-rc9] - 2020-01-31

### Added

- `eqx dump` perf + logging improvements
- `eqx dump -P` turns off JSON pretty printing
- `Cosmos`: `Tip 200` now logs received `n` and `_etag` values
- `Cosmos`: Unfolds now write and return `t` (creation `DateTimeOffset.UtcNow`)
- `EventStore`: Add missing optional parameters for `Connector`: `gossipTimeout` and `clientConnectionTimeout` [#186](https://github.com/jet/equinox/pull/186) :pray: [@AndrewRublyov](https://github.com/AndrewRublyov)

### Changed

- `SqlStreamStore`.*: Target `SqlStreamStore` v `1.2.0-beta.8`
- Target `FsCodec` v `2.0.0-rc3`
- Target `Microsoft.SourceLink.GitHub`, `Microsoft.NETFramework.ReferenceAssemblies` v `1.0.0`
- Samples etc target `Argu` v `6.0.0`
- `eqx dump`'s `-J` switch now turns off JSON rendering
- `eqx -C dump` now renders Store `.Information` logs
- Samples consistently use `module Fold`, `(Events.ForX streamId)`, removed `(|Stream|)` [#174](https://github.com/jet/equinox/pull/174)

### Removed

- `Accumulator` [#184](https://github.com/jet/equinox/pull/184)
- `Target` (now uses `FsCodec.StreamName`) [#189](https://github.com/jet/equinox/pull/189)

### Fixed

- `eqx`: Reinstated writing of missing commandline argument messages to console
- `Cosmos`: Fix `null` Data handling exception when log level <= `Debug`

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

[Unreleased]: https://github.com/jet/equinox/compare/3.0.6...HEAD
[3.0.6]: https://github.com/jet/equinox/compare/3.0.5...3.0.6
[3.0.5]: https://github.com/jet/equinox/compare/3.0.4...3.0.5
[3.0.4]: https://github.com/jet/equinox/compare/3.0.3...3.0.4
[3.0.3]: https://github.com/jet/equinox/compare/3.0.2...3.0.3
[3.0.2]: https://github.com/jet/equinox/compare/3.0.1...3.0.2
[3.0.1]: https://github.com/jet/equinox/compare/3.0.0...3.0.1
[3.0.0]: https://github.com/jet/equinox/compare/3.0.0-beta.4...3.0.0
[3.0.0-beta.4]: https://github.com/jet/equinox/compare/3.0.0-beta.3...3.0.0-beta.4
[3.0.0-beta.3]: https://github.com/jet/equinox/compare/3.0.0-beta.2...3.0.0-beta.3
[3.0.0-beta.2]: https://github.com/jet/equinox/compare/3.0.0-beta.1...3.0.0-beta.2
[3.0.0-beta.1]: https://github.com/jet/equinox/compare/2.6.0...3.0.0-beta.1
[2.6.0]: https://github.com/jet/equinox/compare/2.5.1...2.6.0
[2.5.1]: https://github.com/jet/equinox/compare/2.5.0...2.5.1
[2.5.0]: https://github.com/jet/equinox/compare/2.4.0...2.5.0
[2.4.0]: https://github.com/jet/equinox/compare/2.3.0...2.4.0
[2.3.0]: https://github.com/jet/equinox/compare/2.3.0-rc2...2.3.0
[2.3.0-rc2]: https://github.com/jet/equinox/compare/2.3.0-rc1...2.3.0-rc2
[2.3.0-rc1]: https://github.com/jet/equinox/compare/2.2.0...2.3.0-rc1
[2.2.0]: https://github.com/jet/equinox/compare/2.1.0...2.2.0
[2.1.0]: https://github.com/jet/equinox/compare/2.0.2...2.1.0
[2.0.2]: https://github.com/jet/equinox/compare/2.0.1...2.0.2
[2.0.1]: https://github.com/jet/equinox/compare/2.0.0...2.0.1
[2.0.0]: https://github.com/jet/equinox/compare/2.0.0-rc9...2.0.0
[2.0.0-rc9]: https://github.com/jet/equinox/compare/2.0.0-rc8...2.0.0-rc9
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
