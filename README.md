# Equinox

Equinox provides a unified programming model for event sourced processing against diverse stream-based stores.

Current supported backends are:

- [EventStore](https://eventstore.org/) - this codebase itself has been in production since 2017 (commit history reflects usage), with elements dating back to 2016.
- [Azure Cosmos DB](https://docs.microsoft.com/en-us/azure/cosmos-db) - contains code dating back to 2016, however [the storage model](https://github.com/jet/equinox/wiki/Cosmos-Storage-Model) was arrived at based on intensive benchmarking squash-merged in [#42](https://github.com/jet/equinox/pull/42).
- (For integration test purposes only) Volatile in-memory store.

The underlying patterns have their roots in the [DDD-CQRS-ES](https://groups.google.com/forum/#!forum/dddcqrs) community, and the hard work and generosity of countless folks there presenting, explaining, writing and hacking over the years. It would be unfair to single out even a small number of people despite the immense credit that is due.

While the implementations are distilled from code from [`Jet.com` systems dating all the way back to 2013](http://gorodinski.com/blog/2013/02/17/domain-driven-design-with-fsharp-and-eventstore/), the abstractions in the API design are informed significantly by work, discussions and documentation and countless hours invested with no expectation of any reward from many previous systems, [frameworks](https://github.com/NEventStore), [samples](https://github.com/thinkbeforecoding/FsUno.Prod), [forks of samples](https://github.com/bartelink/FunDomain) and the outstanding continuous work of the :raised_hands: [EventStore](https://github.com/eventstore) founders, team and community over the years.

_If you're looking to learn more about and/or discuss Event Sourcing and it's myriad benefits, tradeoffs and pitfalls as you apply it to your Domain, look no further than the DDD-CQRS-ES Slack. There's a thriving 2000+ strong [community on Slack](https://t.co/MRxpx0rLH2) you'll get patient and impartial world class advice from 24x7._

## Features

- Designed not to invade application code; Domain tests can be written directly against your models without any need to use Equinox assemblies or constructs as part of writing those tests.
- Encoding of events via `Equinox.UnionCodec` provides for pluggable encoding of events based on either:
  - Using a [versionable convention-based approach (using `Typeshape`'s `UnionContractEncoder` under the covers)](https://eiriktsarpalis.wordpress.com/2018/10/30/a-contract-pattern-for-schemaless-datastores/), providing for serializer-agnostic schema evolution with minimal boilerplate
  - optionally using an explicitly coded pair of `encode` and `tryDecode` functions for when you need to customize
- Independent of the store used, Equinox provides for caching using the .NET `MemoryCache` to minimize roundtrips, latency and bandwidth / Request Charges by maintaining the folded state, without any explicit code within the Domain Model
- Logging is both high performance and pluggable (using [Serilog](https://github.com/serilog/serilog) to your hosting context (we feed log info to Splunk and the metrics embedded in the LogEvent Properties to Prometheus; see relevant tests for examples)
- Extracted from working software; currently used for all data storage within Jet's API gateway and Cart processing.
- Significant test coverage for core facilities, and per Storage system.
- **`Equinox.EventStore` Transactionally-consistent Rolling Snapshots**: Command processing can be optimized by employing in-stream 'compaction' events in service of the following ends:
  - no additional roundtrips to the store needed at either the Load or Sync points in the flow
  - support, (via `UnionContractEncoder`) for the maintenance of multiple co-existing compaction schemas in a given stream (A snapshot isa Event) 
  - compaction events typically do not get deleted (consistent with how EventStore works), although it is safe to do so in concept
  - NB while this works well, and can deliver excellent performance (especially when allied with the Cache), [it's not a panacea, as noted in this excellent EventStore article on the topic](https://eventstore.org/docs/event-sourcing-basics/rolling-snapshots/index.html)
- **`Equinox.Cosmos` 'Tip with Unfolds' schema**: In contrast to `Equinox.EventStore`'s `Access.RollingSnapshots`, when using `Equinox.Cosmos`, optimized command processing is managed via the `Tip`; a document per stream with a well-known identity enabling syncs via point-reads by virtue of the fact that the document maintains:
  a) the present Position of the stream - i.e. the index at which the next events will be appended
  b) (compressed) [_unfolds_]((https://github.com/jet/equinox/wiki/Cosmos-Storage-Model)
  c) (optionally) events since those unfolded events ([presently removed](https://github.com/jet/equinox/pull/58), but [should return](https://github.com/jet/equinox/wiki/Roadmap))
  
  This yields many of the benefits of the in-stream Rolling Snapshots approach while reducing latency, RU provisioning requirement, and Request Charges:-
  - Writes never need to do queries or touch event documents in any way
  - when coupled with the cache, a typical read is a point read [with `IfNoneMatch` on an etag], costing 1.0 RU if in-date [to get the `302 Not Found` response] (when the stream is empty, a `404 NotFound` response pertains, costing 1.0 RU)
  - no additional roundtrips to the store needed at either the Load or Sync points in the flow

  It should be noted that from a querying perspective, the `Tip` shares the same structure as `Batch` documents (a potential future extension would be to carry some events in the `Tip` as [some interim versions of the implementation once did](https://github.com/jet/equinox/pull/58)) 

## Elements

The Equinox components within this repository are delivered as a series of multi-targeted Nuget packages targeting `net461` (F# 3.1+) and `netstandard2.0` (F# 4.5+) profiles; each of the constituent elements is designed to be easily swappable as dictated by the task at hand. Each of the components can be inlined or customized easily:-

- `Equinox.Handler` (Nuget: `Equinox`, depends on `Serilog` (but no specific Serilog sinks, i.e. you can forward to `NLog` etc)): Store-agnostic decision flow runner that manages the optimistic concurrency protocol
- `Equinox.Codec` (Nuget: `Equinox.Codec`, depends on `TypeShape`, (`Newtonsoft.Json` `>= 10.0.3` on `net461`, `>= 11.0.2` on `netstandard2.0` but can support any serializer): [a scheme for the serializing Events modelled as an F# Discriminated Union with the following capabilities](https://eiriktsarpalis.wordpress.com/2018/10/30/a-contract-pattern-for-schemaless-datastores/):
  - independent of any specific serializer
  - allows tagging of Discriminated Union cases in a versionable manner with low-dependency `DataMember(Name=` tags using [TypeShape](https://github.com/eiriktsarpalis/TypeShape)'s [`UnionContractEncoder`](https://github.com/eiriktsarpalis/TypeShape/blob/master/tests/TypeShape.Tests/UnionContractTests.fs)
- `Equinox.Cosmos` (Nuget: `Equinox.Cosmos`, depends on `System.Runtime.Caching`, `FSharp.Control.AsyncSeq`, `TypeShape`, `Microsoft.Azure.DocumentDb[.Core]`): Production-strength Azure CosmosDb Adapter with integrated transactionally-consistent snapshotting, facilitating optimal read performance in terms of latency and RU costs, instrumented to the degree necessitated by Jet's production monitoring requirements.
- `Equinox.EventStore` (Nuget: `Equinox.EventStore`, depends on `EventStore.Client[Api.NetCore] >= 4`, `System.Runtime.Caching`, `FSharp.Control.AsyncSeq`, `TypeShape`): Production-strength [EventStore](https://eventstore.org/) Adapter instrumented to the degree necessitated by Jet's production monitoring requirements
- `Equinox.MemoryStore` (Nuget: `Equinox.MemoryStore`): In-memory store for integration testing/performance baselining/providing out-of-the-box zero dependency storage for examples.
- `samples/Store` (in this repo): Example domain types reflecting examples of how one applies Equinox to a diverse set of stream-based models
- `samples/TodoBackend` (in this repo): Standard https://todobackend.com compliant backend
- `Equinox.Cli` (in this repo): General purpose tool incorporating a benchmakr scenario runner, facilitating running representative load tests composed of transactions in `samples/Store` and `samples/TodoBackend` against any nominated store; this allows perf tuning and measurement in terms of both latency and transaction charge aspects.

## CONTRIBUTING

Please raise GitHub issues for any questions so others can benefit from the discussion.

The aim in the medium term (and the hope from the inception of this work) is to run Equinox as a proper Open Source project at the point where there is enough time for maintainers to do that properly.

We are getting very close to that point and are extremely excited by that. But we're not there yet; this is intentionally a soft launch.

Unfortunately, in the interim, the barrier for contributions will unfortunately be inordinately high:
- bugfixes with good test coverage are always welcome - PRs yield [MyGet-hosted NuGets](https://www.myget.org/F/jet/api/v3/index.json); in general we'll seek to move them to NuGet prerelease and then NuGet release packages with relatively short timelines.
- minor improvements / tweaks, subject to discussing in a GitHub issue first to see if it fits, but no promises at this time, even if the ideas are fantastic and necessary :sob:
- tests, examples and scenarios are always welcome; Equinox is intended to address a very broad base of usage patterns; Please note that the emphasis will always be (in order)
  1. providing advice on how to achieve your aims without changing Equinox
  2. how to open up an appropriate extension point in Equinox
  3. (when all else fails), add to the complexity of the system by adding API surface area or logic.
- Naming is hard; there is definitely room for improvement. There may at some point be a set of controlled deprecations, switching to names, and then removing the old ones. However it should be noted that widespread renaming is not on the cards due to the number of downstream systems that would be affected.

## BUILDING

### build and run

Run, including running the tests that assume you've got a local EventStore and pointers to a CosmosDb database and collection prepared (see PROVISIONING):

    ./build.ps1

### build, skipping tests that require a Store instance

    ./build -s

### build, skipping all tests

    dotnet pack build.proj

### build, skip EventStore tests

    ./build -se

### build, skip EventStore tests, skip auto-provisioning + de-provisioning Cosmos

    ./build -se -scp

## SAMPLES

The `samples/` folder contains various examples, with the complementary goals of:

- being a starting point to see how one might consume the libraries.
- acting as [Consumer Driven Contracts](https://martinfowler.com/articles/consumerDrivenContracts.html) to validate and pin API designs.
- providing outline (not official and complete) guidance as to things that are valid to do in an application consuming Equinox components.
- to validate that each specific Storage implementation can fulfill the needs of each of the example Services/Aggregates/Applications. (_unfortunately this concern makes a lot of the DI wiring more complex than a real application should be; it's definitely not a goal for every Equinox app to be able to switch between backends, even though that's very much possible to achieve._)

### [TODOBACKEND, see samples/TodoBackend](/samples/TodoBackend)

The repo contains a vanilla ASP.NET Core 2.1 implemention of [the well-known TodoBackend Spec](https://www.todobackend.com). **NB the implementation is largely dictated by spec; no architectural guidance expressed or implied ;)**. It can be run via:

    & dotnet run -f netcoreapp2.1 -p samples/Web -S es # run against eventstore, omit `es` to use in-memory store, or see PROVISIONING EVENTSTORE, below
    start https://www.todobackend.com/specs/index.html?https://localhost:5001/todos # for low-level debugging / validation of hosting arrangements
    start https://www.todobackend.com/client/index.html?https://localhost:5001/todos # Actual UI
    start http://localhost:5341/#/events # see logs triggered by `-S` above in https://getseq.net        

### [STORE, see /samples/Store](/samples/Store)

The core sample in this repo is the `Store` sample, which contains code and tests extracted from real implementations (with minor simplifications in some cases).

These facts mean that:

- some of the code may be less than approachable for a beginner (e.g. some of the code is written is in its present form for reasons of efficiency)
- some of the code may not represent official best practice guidance that the authors would necessarily stand over (e.g., the CQRS pattern is not strictly adhered to in all circumstances; some command designs are not completely correct from the point of view of making sense from an idempotency perspective)

While these things can of course be perfected through PRs, this is definitely not top of the TODO list for the purposes of this repo. (We'd be happy to link to other samples, including cleanups / rewrites of these samples written with different testing platforms, web platforms, or DDD/CQRS/ES design flavors).

## BENCHMARKS

A key facility of this repo is being able to run load tests, either in process against a nominated store, or via HTTP to a nominated instance of `samples/Web`. The following tests are implemented at present:

- `Favorite` - Simulate a very enthusiastic user that favorites things once per Second - triggering an ever-growing state which can only work efficiently if you:
  - apply a snapshotting scheme (although being unbounded, it will eventually hit the store's limits - 4MB/event for EventStore, 3MB/document for CosmosDb)
- `SaveForLater` - Simulate a happy shopper that saves 3 items per second, and empties the Save For Later list whenever it is full (when it hits 50 items)
  - Snapshotting helps a lot
  - Caching is not as essential as it is for the `Favorite` test
- `Todo` - Keeps a) getting the list b) adding an item c) clearing the list when it hits 1000 items.
  - the `Cleared` event acts as a natural event to use in the `isOrigin` check. This makes snapshotting less crucial than it is, for example, in the case of the `Favorite` test
  - the `-s` parameter can be used to adjust the maximum itme text length from the default (`100`, implying average length of 50)

### Run EventStore benchmark on Full Framework(when provisioned)

This benchmark continually reads and writes very small events across multiple streams on .NET Full Framework

    dotnet pack -c Release .\build.proj
    & .\cli\Equinox.Cli\bin\Release\net461\Equinox.Cli.exe run -f 2500 -C es

### Run EventStore benchmark on .NET Core (when provisioned)

At present, .NET Core seems to show comparable perf under normal load, but becomes very unpredictable under load. The following benchmark should produce pretty consistent levels of reads and writes, and can be used as a baseline for investigation:

    & dotnet run -c Release -f netcoreapp2.1 -p cli/equinox.cli -- run -t saveforlater -f 1000 -d 5 -C -U es

### run Web benchmark

The CLI can drive the Store and TodoBackend samples in the `samples/Web` ASP.NET Core app. Doing so requires starting a web process with an appropriate store (EventStore in this example, but can be `memory`/omitted etc. as in the other examples)

#### in Window 1

    & dotnet run -c Release -f netcoreapp2.1 -p samples/Web -- -C -U es

#### in Window 2

    & dotnet run -c Release -f netcoreapp2.1 -p cli/Equinox.Cli -- run -t saveforlater -f 200 web

### run CosmosDb benchmark (when provisioned)

    $env:EQUINOX_COSMOS_CONNECTION="AccountEndpoint=https://....;AccountKey=....=;"
    $env:EQUINOX_COSMOS_DATABASE="equinox-test"
    $env:EQUINOX_COSMOS_COLLECTION="equinox-test"

    cli/Equinox.cli/bin/Release/net461/Equinox.Cli run `
      cosmos -s $env:EQUINOX_COSMOS_CONNECTION -d $env:EQUINOX_COSMOS_DATABASE -c $env:EQUINOX_COSMOS_COLLECTION `
    dotnet run -f netcoreapp2.1 -p cli/equinox.cli -- run `
      cosmos -s $env:EQUINOX_COSMOS_CONNECTION -d $env:EQUINOX_COSMOS_DATABASE -c $env:EQUINOX_COSMOS_COLLECTION `

## PROVISIONING

### Provisioning EventStore (when not using -s or -se)

For EventStore, the tests assume a running local instance configured as follows to replicate as much as possible the external appearance of a Production EventStore Commercial cluster :-

    # requires admin privilege
    cinst eventstore-oss -y # where cinst is an invocation of the Chocolatey Package Installer on Windows
    # run as a single-node cluster to allow connection logic to use cluster mode as for a commercial cluster
    & $env:ProgramData\chocolatey\bin\EventStore.ClusterNode.exe --gossip-on-single-node --discover-via-dns 0 --ext-http-port=30778

### Provisioning CosmosDb (when not using -sc)

    dotnet run -f netcoreapp2.1 -p cli/equinox.cli -- init -ru 1000 `
        cosmos -s $env:EQUINOX_COSMOS_CONNECTION -d $env:EQUINOX_COSMOS_DATABASE -c $env:EQUINOX_COSMOS_COLLECTION

## DEPROVISIONING

### Deprovisioning (aka nuking) EventStore data resulting from tests to reset baseline

While EventStore rarely shows any negative effects from repeated load test runs, it can be useful for various reasons to drop all the data generated by the load tests by casting it to the winds:-

    # requires admin privilege
    rm $env:ProgramData\chocolatey\lib\eventstore-oss\tools\data

### Deprovisioning CosmosDb

The above provisioning step provisions RUs in DocDB for the collection, which add up quickly. *When finished running any test, it's critical to drop the RU allocations back down again via some mechanism*. 

- Kill the collection and/or database
- Use the portal to change the allocation