Equinox
=======
A lightweight set of infrastructure, examples and tests providing a consistent approach to Event-sourced Decision processing in F# against multiple ordered stream stores.

Features
--------
- Domain tests can be written directly against the models without any need to involve Equinox.
- Encoding of events via `Equinox.UnionCodec` provides for pluggable encoding events based on either:
    - Proving a hardcoed pair of `encode` and `tryDecode` functions
    - Using a versionable convention-based approach using `Typeshape`'s `UnionContractEncoder` under the covers, providing for serializer-agnostic schema evolution with minimal boilerplate
- Independent of the stored used, Equinox provides for caching using the .NET `MemoryCache` to minimize roundtrips, latency and bandwidth / request charges costs by maintaining the folded state without any explicit code within the Domain Model
- Logging is both high performance and pluggable (using [Serilog](https://github.com/serilog/serilog) to your hosting context (we feed log info to  Splunk atm and feed metrics embedded in the LogEvent Properties to Prometheus; see relevant tests for examples)
- (Azure CosmosDb-specific, WIP) Snapshotting support: Command processing can by optimized by employing a snapshot document which maintains a) (optionally) a rendition of the folded state b) (optionally) batches of events to fold into the state in a
	- no additional roundtrips to the store needed at either the Load or Sync points in the flow
	- when coupled with ther cache, a typical read is a point read with an etag, costing 1 RU
	- A snapshot isa Document, but not an Event
	- snapshot events can safely be deleted; they'll get regenerated in the course of normal processing
	- A given snapshot will typically only contain a single version of the snapshot
- (Mainly for EventStore) Compaction support: Command processing can by optimized by employing in-stream 'compaction' events in service of the following ends:
	- no additional roundtrips to the store needed at either the Load or Sync points in the flow
	- support, (via `UnionContractEncoder`) for the maintenance of multiple co-existing snapshot schemas in a given stream (A snapshot isa Event)
	- compaction events typically do not get deleted in EventStore, but pruning can make sense in CosmosDb
- Extracted from working software; currently used for all data storage within Jet's API gateway and Cart processing.
- Significant test coverage for core facilities, and per Storage system.

Elements
--------
Elements are delivered as multitargeted Nuget packages targeting `net461` (F# 3.1+) and `netstandard2.0` (F# 4.5+) profiles; each of the constituent elements is designed to be easily swappable as dictated by the task at hand. Each of the components can be inlined or customized easily:-

- `Equinox.Handler` (Nuget: `Equinox`, depends on `Serilog` (but no specific Serilog sinks, i.e. you can forward to `NLog` etc)): Store-agnostic Decision flow runner that manages the optimistic concurrency protocol
- `Equinox.Codec` (Nuget: `Equinox.Codec`, depends on `TypeShape`, (optionally) `Newtonsoft.Json >= 11.0.2` but can support any serializer): [a scheme for the serializing Events modelled as an F# Discriminated Union with the following capabilities](https://eiriktsarpalis.wordpress.com/2018/10/30/a-contract-pattern-for-schemaless-datastores/):
	- independent of any specific serializer
	- allows tagging of Discriminated Union cases in a versionable manner with low-dependency `DataMember(Name=` tags using [TypeShape](https://github.com/eiriktsarpalis/TypeShape)'s [`UnionContractEncoder`](https://github.com/eiriktsarpalis/TypeShape/blob/master/tests/TypeShape.Tests/UnionContractTests.fs)
- `Equinox.Cosmos` (Nuget: `Equinox.Cosmos`, depends on `System.Runtime.Caching`, `FSharp.Control.AsyncSeq`, `TypeShape`, ): Production-strength Azure CosmosDb Adapter with integrated transactional snapshotting facilitating optimal read performance in terms of latency and RU costs, instrumented to the degree necessitated by Jet's production monitoring requirements.
- `Equinox.EventStore` (Nuget: `Equinox.EventStore`, depends on `EventStore.Client[Api.NetCore] >= 4`, `System.Runtime.Caching`, `FSharp.Control.AsyncSeq`, `TypeShape`): Production-strength [EventStore](http://geteventstore.com) Adapter instrumented to the degree necessitated by Jet's production monitoring requirements
- `Equinox.MemoryStore` (Nuget: `Equinox.MemoryStore`): In-memory store for integration testing/performance baselining
- `Samples/Store` (in this repo): Example domain types reflecting examples of how one applies Equinox to a diverse set of stream-based models
- `Equinox.Bench` (in this repo): Scenario runner that facilitates running representative load tests composed of transactions in `Samples/Store` against each backend store; this allows perf tuning and measurement in terms of both latency and transaction charge aspects.

CONTRIBUTING
------------
Please raise GitHub issues for any questions so others can benefit from the discussion.

BUILDING
--------

## build and run

Run, including running the tests that assume you've got a local EventStore and pointers to a CosmosDb database and collection prepared (see #PROVISIONING):

`./build.ps1`

## build, skipping tests that require a Store instance

`./build.ps1 -sc -se`

## build, skipping all tests

`./build -a "/t:build"`

## run CosmosDb benchmark (when provisioned)

```& .\benchmarks\Equinox.Bench\bin\Release\net461\Equinox.Bench.dll cosmos -s $env:EQUINOX_COSMOS_CONNECTION -d test -c $env:EQUINOX_COSMOS_COLLECTION run
& dotnet .\benchmarks\Equinox.Bench\bin\Release\netcoreapp2.1\Equinox.Bench.dll cosmos -s $env:EQUINOX_COSMOS_CONNECTION -d test -c $env:EQUINOX_COSMOS_COLLECTION run
```

## run EventStore benchmark (when provisioned)

```
& .\benchmarks\Equinox.Bench\bin\Release\net461\Equinox.Bench.exe es run
& dotnet .\benchmarks\Equinox.Bench\bin\Release\netcoreapp2.1\Equinox.Bench.dll es run
```

PROVISIONING
------------

## COSMOSDB (when not using -sc)

```
$env:EQUINOX_COSMOS_CONNECTION="AccountEndpoint=https://....;AccountKey=....=;"
$env:EQUINOX_COSMOS_DATABASE=test
$env:EQUINOX_COSMOS_COLLECTION=$env:USERNAME

benchmarks/Equinox.Bench/bin/Release/net461/Equinox.Bench cosmos -s $env:EQUINOX_COSMOS_CONNECTION -d test -c $env:EQUINOX_COSMOS_COLLECTION provision -ru 10000
```

## DEPROVISIONING COSMOSDB

(same command as for provisioningwith `-ru 0`)

## PROVISIONING EVENTSTORE (when not using -se)

For EventStore, run a local instance with config as follows:-

```
# requires admin privilege
cinst eventstore-oss -y # where cinst is an invocation of the Chocolatey Package Installer on Windows
# run as a single-node cluster to allow connection logic to use cluster mode as for a commercial cluster
& $env:ProgramData\chocolatey\bin\EventStore.ClusterNode.exe --gossip-on-single-node --discover-via-dns 0 --ext-http-port=30778
```

## DEPROVISIONING EVENTSTORE DATA

```
# requires admin privilege
del C:\ProgramData\chocolatey\lib\eventstore-oss\tools\data
```
