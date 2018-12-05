# Equinox

Equinox provides a unified programming model for Command handling against event-sourced stream stores.

Current supported backends are:
- [EventStore](https://eventstore.org/) - this codebase itself has been in production since 2017 (commit history reflects usage), with elements dating back to 2016.
- [Azure Cosmos DB](https://docs.microsoft.com/en-us/azure/cosmos-db/) (See [`cosmos` branch](https://github.com/jet/equinox/tree/cosmos) - will converge with `master` when the time is right).
- (For integration test purposes only) Volatile in-memory store.

The underlying patterns have their roots in the [DDD-CQRS-ES](https://groups.google.com/forum/#!forum/dddcqrs) community, and the hard work and generosity of countless folks there presenting, explaining, writing and hacking over the years. It would be unfair to single out even a small number of people despite the immense credit that is due.

While the implementations are distilled from code from [`Jet.com` systems dating all the way back to 2013](http://gorodinski.com/blog/2013/02/17/domain-driven-design-with-fsharp-and-eventstore/), the abstractions in the API design are informed significantly by work, discussions and documentation and countless hours invested with no expectation of any reward from many previous systems, [frameworks](https://github.com/NEventStore), [samples](https://github.com/thinkbeforecoding/FsUno.Prod), [forks of samples](https://github.com/bartelink/FunDomain) and the outstanding continuous work of the :raised_hands: [EventStore](https://github.com/eventstore) founders, team and community over the years.

# Features

- Designed to be non-invasive to application code; Domain tests can be written directly against the models without any need to use Equinox assemblies or constructs as part of writing those tests.
- Encoding of events via `Equinox.UnionCodec` provides for pluggable encoding events based on either:
    - Providing an explicitly coded pair of `encode` and `tryDecode` functions
    - Using a [versionable convention-based approach (using `Typeshape`'s `UnionContractEncoder` under the covers)](https://eiriktsarpalis.wordpress.com/2018/10/30/a-contract-pattern-for-schemaless-datastores/), providing for serializer-agnostic schema evolution with minimal boilerplate
- Independent of the store used, Equinox provides for caching using the .NET `MemoryCache` to minimize roundtrips, latency and bandwidth / request charges costs by maintaining the folded state without any explicit code within the Domain Model
- Logging is both high performance and pluggable (using [Serilog](https://github.com/serilog/serilog) to your hosting context (we feed log info to  Splunk atm and feed metrics embedded in the LogEvent Properties to Prometheus; see relevant tests for examples)
- Extracted from working software; currently used for all data storage within Jet's API gateway and Cart processing.
- Significant test coverage for core facilities, and per Storage system.
- **`Equinox.EventStore` Transactionally-consistent Rolling Snapshots**: Command processing can be optimized by employing in-stream 'compaction' events in service of the following ends:
    - no additional roundtrips to the store needed at either the Load or Sync points in the flow
    - support, (via `UnionContractEncoder`) for the maintenance of multiple co-existing compaction schemas in a given stream (A snapshot isa Event) 
    - compaction events typically do not get deleted (consistent with how EventStore works), although it is safe to do so in concept
    - NB while this works well, and can deliver excellent performance (especially when allied with the Cache), [it's not a panacea, as noted in this excellent EventStore article on the topic](https://eventstore.org/docs/event-sourcing-basics/rolling-snapshots/index.html)
- **Azure CosmosDb Indexed mode**: Using `Equinox.Cosmos`, command processing can be optimized through an index document in the same partition as the event documents that maintains:
  a) compacted rendition(s) of the folded state
  b) (optionally) events since those snapshots have last been updated
  
  This yields many of the benefits of rolling snapshots while reducing latency, RU provisioning requirement, and Request Charges:-
	- no additional roundtrips to the store needed at either the Load or Sync points in the flow
	- when coupled with the cache, a typical read is a point read with an etag, costing 1 RU
	- The index isa DocDb Document, but _not_ an Event
	- The index can safely be deleted at any time; it'll get regenerated in the course of normal processing

# Elements

The Equinox components within this repository are delivered as a series of multi-targeted Nuget packages targeting `net461` (F# 3.1+) and `netstandard2.0` (F# 4.5+) profiles; each of the constituent elements is designed to be easily swappable as dictated by the task at hand. Each of the components can be inlined or customized easily:-

- `Equinox.Handler` (Nuget: `Equinox`, depends on `Serilog` (but no specific Serilog sinks, i.e. you can forward to `NLog` etc)): Store-agnostic Decision flow runner that manages the optimistic concurrency protocol
- `Equinox.Codec` (Nuget: `Equinox.Codec`, depends on `TypeShape`, (optionally) `Newtonsoft.Json >= 11.0.2` but can support any serializer): [a scheme for the serializing Events modelled as an F# Discriminated Union with the following capabilities](https://eiriktsarpalis.wordpress.com/2018/10/30/a-contract-pattern-for-schemaless-datastores/):
	- independent of any specific serializer
	- allows tagging of Discriminated Union cases in a versionable manner with low-dependency `DataMember(Name=` tags using [TypeShape](https://github.com/eiriktsarpalis/TypeShape)'s [`UnionContractEncoder`](https://github.com/eiriktsarpalis/TypeShape/blob/master/tests/TypeShape.Tests/UnionContractTests.fs)
- `Equinox.Cosmos` (Nuget: `Equinox.Cosmos`, depends on `System.Runtime.Caching`, `FSharp.Control.AsyncSeq`, `TypeShape`, ): Production-strength Azure CosmosDb Adapter with integrated transactional snapshotting facilitating optimal read performance in terms of latency and RU costs, instrumented to the degree necessitated by Jet's production monitoring requirements.
- `Equinox.EventStore` (Nuget: `Equinox.EventStore`, depends on `EventStore.Client[Api.NetCore] >= 4`, `System.Runtime.Caching`, `FSharp.Control.AsyncSeq`, `TypeShape`): Production-strength [EventStore](http://geteventstore.com) Adapter instrumented to the degree necessitated by Jet's production monitoring requirements
- `Equinox.MemoryStore` (Nuget: `Equinox.MemoryStore`): In-memory store for integration testing/performance baselining
- `samples/Store` (in this repo): Example domain types reflecting examples of how one applies Equinox to a diverse set of stream-based models
- `Equinox.Cli` (in this repo): General purpose tool incorporating a scenario runner that facilitates running representative load tests composed of transactions in `samples/Store` against each backend store; this allows perf tuning and measurement in terms of both latency and transaction charge aspects.

# CONTRIBUTING

Please raise GitHub issues for any questions so others can benefit from the discussion.

The aim in the medium term (and the hope from the inception of this work) is to run Equinox as a proper Open Source project at the point where there is enough time for maintainers to do that properly.

We are getting very close to that point and are extremely excited by that. But we're not there yet; this is intentionally a soft launch.

For now, the core focus of work here will be on converging the `cosmos` branch, which will bring changes, clarifications, simplifications and features, which all need to be integrated into the production systems built on it, before we can consider broader based additive changes and/or significantly increasing the API surface area.

For these reasons, the barrier for contributions will unfortunately be inordinately high in the short term:
- bugfixes with good test coverage are always welcome - PRs yield MyGet-hosted NuGets and in general we'll seek to move them to NuGet prerelease and then NuGet release packages with relatively short timelines.
- minor improvements / tweaks, subject to discussing in a GitHub issue first to see if it fits, but no promises at this time, even if the ideas are fantastic and necessary :sob:
- tests, examples and scenarios are always welcome; Equinox is intended to address a very broad base of usage patterns; Please note that the emphasis will always be (in order) 1) providing advice on how to achieve your aims without changing Equinox 2) how to open up an appropriate extension point in Equinox 3) (when all else fails), add to the complexity of the system by adding API surface area or logic.
- we will likely punt on non-IO perf improvements until such point as Cosmos support is converged into `master`
- Naming is hard; there is definitely room for improvement. There likely will be a set of controlled deprecations, switching to names, and then removing the old ones. However, PRs other than for discussion purposes probably don't make sense right now.

# BUILDING

## build and run

Run, including running the tests that assume you've got a local EventStore and pointers to a CosmosDb database and collection prepared (see PROVISIONING):

	./build.ps1

## build, skipping tests that require a Store instance

	./build -s

## build, skipping all tests

	dotnet pack build.proj

## build, skip EventStore tests

	./build -se
## build, skip EventStore tests, skip auto-provisioning + de-provisioning Cosmos

	./build -se -scp

# BENCHMARKS

A key facility of this repo is beoing able to run load tests, either in process against a nominated store, or via HTTP to a nominated Web app. The following tests are implemented at present:

- `Favorite` - Simulate a very enthusiastic user that favorites things once per Second - triggering an ever-growing state which can only work efficiently if you:
    - apply a snapshotting scheme (although being unbounded, it will eventually hit the store's limits - 4MB/event for EventStore, 3MB/document for CosmosDb)
    - apply caching on CosmosDb (so re-reading and transporting the snapshots is eliminated from the RU/bandwidth/latency costs)
- `SaveForLater` - Simulate a happy shopper that saves 3 items per second, and empties the Save For Later list whenever it is full (when it hits 50 items)
    - Snapshotting helps a lot
    - Caching is not as essential as it is for the `Favorite` test

## Run EventStore benchmark (when provisioned)

	& .\cli\Equinox.Cli\bin\Release\net461\Equinox.Cli.exe run es
	& dotnet run -f netcoreapp2.1 -p cli/equinox.cli -- run es

## run CosmosDb benchmark (when provisioned)

    $env:EQUINOX_COSMOS_CONNECTION="AccountEndpoint=https://....;AccountKey=....=;"
    $env:EQUINOX_COSMOS_DATABASE="equinox-test"
    $env:EQUINOX_COSMOS_COLLECTION="equinox-test"

    cli/Equinox.cli/bin/Release/net461/Equinox.Cli run `
        cosmos -s $env:EQUINOX_COSMOS_CONNECTION -d $env:EQUINOX_COSMOS_DATABASE -c $env:EQUINOX_COSMOS_COLLECTION
    dotnet run -f netcoreapp2.1 -p cli/equinox.cli -- run `
        cosmos -s $env:EQUINOX_COSMOS_CONNECTION -d $env:EQUINOX_COSMOS_DATABASE -c $env:EQUINOX_COSMOS_COLLECTION

## run Web benchmark

The CLI can drive the Store/Web ASP.NET Core app. Doing so requires starting a web process with an appropriate store (Cosmos in this example, but can be `memory`/omitted, `es` etc as in the other examples)

### in Window 1

    & dotnet run -c Release -f netcoreapp2.1 -p samples/Store/Web -- -C -U cosmos

### in Window 2

    & dotnet run -c Release -f netcoreapp2.1 -p cli/Equinox.Cli -- run -t saveforlater -f 200 web

# PROVISIONING

## Provisioning EventStore (when not using -s or -se)

For EventStore, the tests assume a running local instance configured as follows to replicate as much as possible the external appearance of a Production EventStore Commercial cluster :-

	# requires admin privilege
	cinst eventstore-oss -y # where cinst is an invocation of the Chocolatey Package Installer on Windows
	# run as a single-node cluster to allow connection logic to use cluster mode as for a commercial cluster
	& $env:ProgramData\chocolatey\bin\EventStore.ClusterNode.exe --gossip-on-single-node --discover-via-dns 0 --ext-http-port=30778

## CosmosDb (when not using -sc)

```
dotnet run -f netcoreapp2.1 -p cli/equinox.cli -- init -ru 10000 `
    cosmos -s $env:EQUINOX_COSMOS_CONNECTION -d $env:EQUINOX_COSMOS_DATABASE -c $env:EQUINOX_COSMOS_COLLECTION
```

# DEPROVISIONING

## Deprovisioning (aka nuking) EventStore data resulting from tests to reset baseline

While EventStore rarely shows any negative effects from repeated load test runs, it can be useful for various reasons to drop all the data generated by the load tests by casting it to the winds:-

	# requires admin privilege
	rm $env:ProgramData\chocolatey\lib\eventstore-oss\tools\data

## Deprovisioning CosmosDb

The above provisioning step provisions RUs in DocDB for the collection, which add up quickly. *When finished running any test, it's critical to drop the RU allocations back down again via some mechanism*. 

* Kill the collection and/or database
* Use the portal to change the allocation