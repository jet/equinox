# Equinox [![Build Status](https://dev.azure.com/jet-opensource/opensource/_apis/build/status/jet.equinox?branchName=master)](https://dev.azure.com/jet-opensource/opensource/_build/latest?definitionId=4?branchName=master) [![release](https://img.shields.io/github/release/jet/equinox.svg)](https://github.com/jet/equinox/releases) [![NuGet](https://img.shields.io/nuget/vpre/Equinox.svg?logo=nuget)](https://www.nuget.org/packages/Equinox/) [![license](https://img.shields.io/github/license/jet/Equinox.svg)](LICENSE) ![code size](https://img.shields.io/github/languages/code-size/jet/equinox.svg) [![docs status](https://img.shields.io/badge/DOCUMENTATION-WIP-important.svg?style=popout)](DOCUMENTATION.md) [<img src="https://img.shields.io/badge/slack-DDD--CQRS--ES%20%23equinox-yellow.svg?logo=slack">](https://t.co/MRxpx0rLH2)

A unified programming model for event-sourced command processing and projections for stream-based stores.

... while striving to remain a humble set of _libraries_ that _you compose_ into an architecture that fits your apps needs; not a final Best Pratice Architecture/object model/processing pipeline that's going to foist a one-size-fits-all model on you. You decide what facilities make sense for your context; Equinox covers those chosen infrastructural aspects without pulling in a cascade of dependencies from a jungle. (That's not to say we don't have plenty opinions and well polished patterns; we just try to confine the impact of that to [`samples`](/samples) or [templates](https://github.com/jet/dotnet-templates), leaving judgement calls open for you to adjust as your app evolves).

The design is informed by discussions, talks and countless hours of hard and thoughtful work invested into many previous systems, [frameworks](https://github.com/NEventStore), [samples](https://github.com/thinkbeforecoding/FsUno.Prod), [forks of samples](https://github.com/bartelink/FunDomain), the outstanding continuous work of the [EventStore](https://github.com/eventstore) founders and team and the wider [DDD-CQRS-ES](https://groups.google.com/forum/#!forum/dddcqrs) community. It would be unfair to single out even a small number of people despite the immense credit that is due. _If you're looking to learn more about and/or discuss Event Sourcing and it's myriad benefits, tradeoffs and pitfalls as you apply it to your Domain, look no further than the thriving 2000+ member community on the [DDD-CQRS-ES Slack](https://github.com/ddd-cqrs-es/slack-community); you'll get patient and impartial world class advice 24x7 (psst there's an [#equinox channel](https://ddd-cqrs-es.slack.com/messages/CF5J67H6Z) there where you can ask questions or offer feedback)._ ([invite link](https://t.co/MRxpx0rLH2))

The implementations are distilled from [`Jet.com` systems dating all the way back to 2013](http://gorodinski.com/blog/2013/02/17/domain-driven-design-with-fsharp-and-eventstore/); current supported backends are:

- [EventStore](https://eventstore.org/) - this codebase itself has been in production since 2017 (see commit history), with key elements dating back to approx 2016.
- [Azure Cosmos DB](https://docs.microsoft.com/en-us/azure/cosmos-db) - contains code dating back to 2016, however [the storage model](DOCUMENTATION.md#Cosmos-Storage-Model) was arrived at based on intensive benchmarking squash-merged in [#42](https://github.com/jet/equinox/pull/42).
- In-memory store (volatile, for integration test purposes).

## QuickStart

### Spin up a [TodoBackend](https://www.todobackend.com/) `.fsproj` app ([storing in `Equinox.MemoryStore` Simulator](https://github.com/jet/equinox#store-libraries))

0. Make a scratch area

    ```powershell
    mkdir ExampleApp
    cd ExampleApp 
    ```

1. Use a `dotnet new` template to get fresh code in your repo

    ```powershell
    dotnet new -i Equinox.Templates # see source in https://github.com/jet/dotnet-templates
    dotnet new eqxweb -t # -t for todos, defaults to memory store (-m) # use --help to see options regarding storage subsystem configuration etc
    ```

2. Run the `TodoBackend`:

    ```powershell
    dotnet run -p Web
    ```

4. Run the standard `TodoMvc` frontend against your locally-hosted, fresh backend (See generated `README.md` for more details)
    - Todo JavaScript client App: https://www.todobackend.com/client/index.html?https://localhost:5001/todos 
    - Run individual JS specification tests: https://www.todobackend.com/specs/index.html?https://localhost:5001/todos

### Spin up a [TodoBackend](https://www.todobackend.com/) `.csproj` ... with C# code

While Equinox is implemented in F#, and F# is a great fit for writing event-sourced domain models, [the APIs are not F#-specific](https://docs.microsoft.com/en-us/dotnet/fsharp/style-guide/component-design-guidelines); there's a [C# edition of the template](https://github.com/jet/dotnet-templates/tree/master/equinox-web-csharp). The instructions are identical to the rest, but you need to use the `eqxwebcs` template instead of `eqxweb`.

### Store data in [EventStore](https://eventstore.org)

1. install EventStore locally (requires admin privilege)

    - For Windows, install with Chocolatey:
    
      ```powershell
      cinst eventstore-oss -y # where cinst is an invocation of the Chocolatey Package Installer on Windows
      ```

	- For OSX, download the `.pkg` from https://eventstore.org/downloads/, click in Finder to launch the installer

2. start the local EventStore instance

    - Windows

      ```powershell
      # run as a single-node cluster to allow connection logic to use cluster mode as for a commercial cluster
      & $env:ProgramData\chocolatey\bin\EventStore.ClusterNode.exe --gossip-on-single-node --discover-via-dns 0 --ext-http-port=30778
      ```

    - OSX:

  	  ```bash
      # run as a single-node cluster to allow connection logic to use cluster mode as for a commercial cluster
      /usr/local/bin/eventstored --gossip-on-single-node --discover-via-dns 0 --ext-http-port=30778
	  ```
    

3. generate sample app with EventStore wiring from template and start

    ```powershell
    dotnet new eqxweb -t -e # -t for todos, -e for eventstore
    dotnet run -p Web
    ```

### Store data in [Azure CosmosDb](https://docs.microsoft.com/en-us/azure/cosmos-db/introduction)

1. *export 3x env vars* (see [provisioning instructions](#run-cosmosdb-benchmark-when-provisioned))

    ```powershell
    $env:EQUINOX_COSMOS_CONNECTION="AccountEndpoint=https://....;AccountKey=....=;"
    $env:EQUINOX_COSMOS_DATABASE="equinox-test"
    $env:EQUINOX_COSMOS_COLLECTION="equinox-test"
    ```

2. use the `eqx` tool to initialize the database and/or collection (using preceding env vars)

    ```powershell
    dotnet tool install Equinox.Tool -g
    eqx init -ru 400 cosmos # generates a database+collection, adds optimized indexes
    ```

3. generate sample app from template, with CosmosDb wiring

    ```powershell
    dotnet new eqxweb -t -c # -t for todos, -c for cosmos
    dotnet run -p Web
    ```

4. (**`2.0.0-preview*`**) Use `eqx` tool to run a CosmosDb ChangeFeedProcessor

    ```powershell
    # TEMP: need to uninstall and use --version flag while this is in beta
    dotnet tool uninstall Equinox.Tool -g
    dotnet tool install Equinox.Tool -g --version 2.0.0-preview*

    eqx initAux -ru 400 cosmos # generates a -aux collection for the ChangeFeedProcessor to maintain consumer group progress within
    # -v for verbose ChangeFeedProcessor logging
    # `projector1` represents the consumer group - >=1 are allowed, allowing multiple independent projections to run concurrently
    # stats specifies one only wants stats regarding items (other options include `kafka` to project to Kafka)
    # cosmos specifies source overrides (using defaults in step 1 in this instance)
    eqx -v project projector1 stats cosmos
    ```

5. (**`2.0.0-preview*`**) Generate a CosmosDb ChangeFeedProcessor sample `.fsproj` (without Kafka producer/consumer), using `Equinox.Cosmos.Projection`

    ```powershell
    dotnet new -i Equinox.Templates

    # note the absence of -k means the projector code will be a skeleton that does no processing besides counting the events
    dotnet new eqxprojector

    # start one or more Projectors
    # `projector2` represents the consumer group; >=1 are allowed, allowing multiple independent projections to run concurrently
    # cosmos specifies source overrides (using defaults in step 1 in this instance)
    dotnet run -p Projector -- projector2 cosmos
    ```

6. (**`2.0.0-preview*`**) Use `eqx` tool to Run a [CosmosDb ChangeFeedProcessor](DOCUMENTATION.md#change-feed-processors), [emitting to a Kafka topic](DOCUMENTATION.md#feeding-to-kafka)

    ```powershell
    $env:EQUINOX_KAFKA_BROKER="instance.kafka.mysite.com:9092" # or use -b

    # `-v` for verbose logging
    # `projector3` represents the consumer group; >=1 are allowed, allowing multiple independent projections to run concurrently
    # `-l 5` to report ChangeFeed lags every 5 seconds
    # `kafka` specifies one wants to emit to Kafka
    # `temp-topic` is the topic to emit to
    # `cosmos` specifies source overrides (using defaults in step 1 in this instance)
    eqx -v project projector3 -l 5 kafka temp-topic cosmos
    ```

7. (**`2.0.0-preview*`**) Generate CosmosDb [Kafka Projector and Consumer](DOCUMENTATION.md#feeding-to-kafka) `.fsproj`ects (using `Jet.ConfluentKafka.FSharp` v `1.0.0`)

    ```powershell
    cat readme.md # more complete instructions regarding the code

    # -k requests inclusion of Apache Kafka support
    dotnet new eqxprojector -k

    # start one or more Projectors (see above for more examples/info re the Projector.fsproj)

    $env:EQUINOX_KAFKA_BROKER="instance.kafka.mysite.com:9092" # or use -b
    $env:EQUINOX_KAFKA_TOPIC="topic0" # or use -t
    dotnet run -p Projector -- projector4 -t topic0 cosmos

    # start one or more Consumers

    $env:EQUINOX_KAFKA_GROUP="consumer1" # or use -g
    dotnet run -p Consumer -- -t topic0 -g consumer1
    ```

## Features

- Designed not to invade application code; Domain tests can be written directly against your models without any need to involve or understand Equinox assemblies or constructs as part of writing those tests.
- Extracted from working software; currently used for all data storage within Jet's API gateway and Cart processing.
- Significant test coverage for core facilities, and with baseline and specific tests per Storage system and a comprehensive test and benchmarking story
- Encoding of events via `Equinox.Codec` provides for pluggable encoding of events based on either:
  - a [versionable convention-based approach](https://eiriktsarpalis.wordpress.com/2018/10/30/a-contract-pattern-for-schemaless-datastores/)  (using `Typeshape`'s `UnionContractEncoder` under the covers), providing for serializer-agnostic schema evolution with minimal boilerplate
  - an explicitly coded pair of `encode` and `tryDecode` functions for when you need to customize
- Independent of the store used, Equinox provides for caching using the .NET `MemoryCache` to minimize roundtrips, latency and bandwidth / Request Charges by maintaining the folded state, without necessitating making the Domain Model folded state serializable
- Logging is mature and comprehensive (using [Serilog](https://github.com/serilog/serilog) internally), with optimal performance and pluggable integration with your apps hosting context (we ourselves typically feed log info to Splunk and the metrics embedded in the `Serilog.Events.LogEvent` Properties to Prometheus; see relevant tests for examples)
- **`Equinox.EventStore` In-stream Rolling Snapshots**: Command processing can be optimized by means of 'compaction' events, meeting the following ends:
  - no additional roundtrips to the store needed at either the Load or Sync points in the flow
  - support, (via `UnionContractEncoder`) for the maintenance of multiple co-existing compaction schemas in a given stream (A 'compaction' event/snapshot isa Event) 
  - compaction events typically do not get deleted (consistent with how EventStore works), although it is safe to do so in concept
  - NB while this works well, and can deliver excellent performance (especially when allied with the Cache), [it's not a panacea, as noted in this excellent EventStore.org article on the topic](https://eventstore.org/docs/event-sourcing-basics/rolling-snapshots/index.html)
- **`Equinox.Cosmos` 'Tip with Unfolds' schema**: (In contrast to `Equinox.EventStore`'s `Access.RollingSnapshots`,) when using `Equinox.Cosmos`, optimized command processing is managed via the `Tip`; a document per stream with a well-known identity enabling Syncing the r/w Position via a single point-reads by virtue of the fact that the document maintains:
  a) the present Position of the stream - i.e. the index at which the next events will be appended for a given stream (events and the Tip share a common logical partition key)
  b) ephemeral (`deflate+base64` compressed) [_unfolds_](DOCUMENTATION.md#Cosmos-Storage-Model)
  c) (optionally) events since those unfolded events ([presently removed](https://github.com/jet/equinox/pull/58), but [should return](DOCUMENTATION.md#Roadmap), see [#109](https://github.com/jet/equinox/pull/109))
  
  This yields many of the benefits of the in-stream Rolling Snapshots approach while reducing latency and RU provisioning requirements due to meticulously tuned Request Charge costs:-
  - Writes never need to do queries or touch event documents in any way
  - when coupled with the cache, a typical read is a point read [with `IfNoneMatch` on an etag], costing 1.0 RU if in-date [to get the `302 Not Found` response] (when the stream is empty, a `404 NotFound` response pertains also costing 1.0 RU)
  - no additional roundtrips to the store needed at either the Load or Sync points in the flow

  It should be noted that from a querying perspective, the `Tip` shares the same structure as `Batch` documents (a potential future extension would be to carry some events in the `Tip` as [some interim versions of the implementation once did](https://github.com/jet/equinox/pull/58), see also [#109](https://github.com/jet/equinox/pull/109).

## Components

The components within this repository are delivered as a series of multi-targeted Nuget packages targeting `net461` (F# 3.1+) and `netstandard2.0` (F# 4.5+) profiles; each of the constituent elements is designed to be easily swappable as dictated by the task at hand. Each of the components can be inlined or customized easily:-

### Core libraries

- `Equinox[.Stream]` [![NuGet](https://img.shields.io/nuget/v/Equinox.svg)](https://www.nuget.org/packages/Equinox/): Store-agnostic decision flow runner that manages the optimistic concurrency protocol. ([depends](https://www.fuget.org/packages/Equinox) on `Serilog` (but no specific Serilog sinks, i.e. you configure to emit to `NLog` etc))
- `Equinox.Codec` [![Codec NuGet](https://img.shields.io/nuget/v/Equinox.Codec.svg)](https://www.nuget.org/packages/Equinox.Codec/): [a scheme for the serializing Events modelled as an F# Discriminated Union](https://eiriktsarpalis.wordpress.com/2018/10/30/a-contract-pattern-for-schemaless-datastores/) ([depends](https://www.fuget.org/packages/Equinox.Codec) on `TypeShape 6.*`, `Newtonsoft.Json >= 11.0.2` but can support any serializer) with the following capabilities:
  - `Equinox.Codec.NewtonsoftJson.Json`: allows tagging of F# Discriminated Union cases in a versionable manner with low-dependency `DataMember(Name=` tags using [TypeShape](https://github.com/eiriktsarpalis/TypeShape)'s [`UnionContractEncoder`](https://github.com/eiriktsarpalis/TypeShape/blob/master/tests/TypeShape.Tests/UnionContractTests.fs)
  - `Equinox.Codec.Custom`: independent of any specific serializer; enables plugging in a serializer and/or Union Encoder of your choice

### Store libraries

- `Equinox.MemoryStore` [![MemoryStore NuGet](https://img.shields.io/nuget/v/Equinox.MemoryStore.svg)](https://www.nuget.org/packages/Equinox.MemoryStore/): In-memory store for integration testing/performance baselining/providing out-of-the-box zero dependency storage for examples. ([depends](https://www.fuget.org/packages/Equinox.MemoryStore) on `Equinox`)
- `Equinox.EventStore` [![EventStore NuGet](https://img.shields.io/nuget/v/Equinox.EventStore.svg)](https://www.nuget.org/packages/Equinox.EventStore/): Production-strength [EventStore](https://eventstore.org/) Adapter instrumented to the degree necessitated by Jet's production monitoring requirements. ([depends](https://www.fuget.org/packages/Equinox.EventStore) on `Equinox`, `EventStore.Client[Api.NetCore] >= 5.0.0`, `System.Runtime.Caching`, `FSharp.Control.AsyncSeq`)
- `Equinox.Cosmos` [![Cosmos NuGet](https://img.shields.io/nuget/v/Equinox.Cosmos.svg)](https://www.nuget.org/packages/Equinox.Cosmos/): Production-strength Azure CosmosDb Adapter with integrated 'unfolds' feature, facilitating optimal read performance in terms of latency and RU costs, instrumented to the degree necessitated by Jet's production monitoring requirements. ([depends](https://www.fuget.org/packages/Equinox.Cosmos) on `Equinox`, `Microsoft.Azure.DocumentDb[.Core] >= 2`, `System.Runtime.Caching`, `Newtonsoft.Json >= 11.0.2`, `FSharp.Control.AsyncSeq`)

### Projection libraries

- `Equinox.Cosmos.Projection` [![Cosmos.Projection NuGet](https://img.shields.io/nuget/v/Equinox.Cosmos.Projection.svg)](https://www.nuget.org/packages/Equinox.Cosmos.Projection/): Wraps the [Microsoft .NET `ChangeFeedProcessor` library](https://github.com/Azure/azure-documentdb-changefeedprocessor-dotnet) providing a [processor loop](DOCUMENTATION.md#change-feed-processors) that maintains a continuous natched query loop per CosmosDb Physical Partition (Range) yielding new or updated documents (optionally unrolling events written by `Equinox.Cosmos` for processing or forwarding). Used in the [`eqx project stats cosmos`](dotnet-tool-provisioning--benchmarking-tool) tool command; see [`dotnet new eqxprojector` to generate a sample app](quickstart) using it. ([depends](https://www.fuget.org/packages/Equinox.Cosmos.Projection) on `Equinox.Cosmos`, `Microsoft.Azure.DocumentDb.ChangeFeedProcessor >= 2.2.5`)
- `Equinox.Projection` [![Projection NuGet](https://img.shields.io/nuget/v/Equinox.Projection.svg)](https://www.nuget.org/packages/Equinox.Projection/): Houses a `FeedValidator` that can be used to validate when duplicate events and/or gaps observed in the sequences of events (see [`eqx project stats` and `kafka`](dotnet-tool-provisioning--benchmarking-tool)). (WIP to replace with a more thorough impl). ([depends](https://www.fuget.org/packages/Equinox.Projection) on `Equinox.Projection.Codec`)
- `Equinox.Projection.Codec` [![Projection Codec NuGet](https://img.shields.io/nuget/v/Equinox.Projection.Codec.svg)](https://www.nuget.org/packages/Equinox.Projection.Codec/): Provides a canonical `RenderedSpan` that can be used as a default format when projecting events via e.g. the Producer/Consumer pair in `dotnet new eqxprojector -k`. ([depends](https://www.fuget.org/packages/Equinox.Projection.Codec) on `Newtonsoft.Json >= 11.0.2`, `Equinox.Codec`)
- _Related, maintained in separate repo_ `Jet.ConfluentKafka.FSharp` [![Jet.ConfluentKafka.FSharp NuGet](https://img.shields.io/nuget/vpre/Jet.ConfluentKafka.FSharp.svg)](https://www.nuget.org/packages/Jet.ConfluentKafka.FSharp/): Wraps `Confluent.Kafka` to provide efficient batched Kafka Producer and Consumer configurations, with basic logging instrumentation. Used in the [`eqx project kafka`](dotnet-tool-provisioning--benchmarking-tool) tool command; see [`dotnet new eqxprojector -k` to generate a sample app](quickstart) using it.

### `dotnet tool` provisioning / benchmarking tool

- `Equinox.Tool` [![Tool NuGet](https://img.shields.io/nuget/v/Equinox.Tool.svg)](https://www.nuget.org/packages/Equinox.Tool/): Tool incorporating a benchmark scenario runner, facilitating running representative load tests composed of transactions in `samples/Store` and `samples/TodoBackend` against any nominated store; this allows perf tuning and measurement in terms of both latency and transaction charge aspects. (Install via: `dotnet tool install Equinox.Tool -g`)

### `dotnet new` starter project templates and sample applications

- `Equinox.Templates` [![Templates NuGet](https://img.shields.io/nuget/v/Equinox.Templates.svg)](https://www.nuget.org/packages/Equinox.Templates/): [The templates repo](https://github.com/jet/dotnet-templates) has C# and F# sample apps. (Install via `dotnet new -i Equinox.Templates && dotnet new eqx --list`). See [the quickstart](quickstart) for examples of how to use it.
- [`samples/Store` (in this repo)](/samples/Store): Example domain types reflecting examples of how one applies Equinox to a diverse set of stream-based models
- [`samples/TodoBackend` (in this repo)](/samples/TodoBackend): Standard https://todobackend.com compliant backend
- [`samples/Tutorial` (in this repo)](/samples/Tutorial): Annotated `.fsx` files with sample Aggregate impls

## CONTRIBUTING

Where it makes sense, raise GitHub issues for any questions so others can benefit from the discussion, or follow the links to [the DDD-CQRS-ES #equinox Slack channel](https://ddd-cqrs-es.slack.com/messages/CF5J67H6Z) above for quick discussions.

This is an Open Source Project for many reasons, with some central goals:

- quality reference code (the code should be clean and easy to read; where it makes sense, components can be grabbed and cloned locally and used in altered form)
- optimal resilience and performance (getting performance right can add huge value for some systems)
- this code underpins non-trivial production systems (having good tests is not optional for reasons far deeper than having impressive coverage stats)

We'll do our best to be accomodating to PRs and issues, but please appreciate that [we emphasize decisiveness for the greater good of the project and its users](https://www.hanselman.com/blog/AlwaysBeClosingPullRequests.aspx); _new features [start with -100 points](https://blogs.msdn.microsoft.com/oldnewthing/20090928-00/?p=16573)_.

Within those constraints, contributions of all kinds are welcome:

- raising [Issues](https://github.com/jet/equinox/issues) (including [relevant question-Issues](https://github.com/jet/equinox/issues/56)) is always welcome (but we'll aim to be decisive in the interests of keeping the list navigable).
- bugfixes with good test coverage are always welcome; in general we'll seek to move them to NuGet prerelease and then NuGet release packages with relatively short timelines (there's unfortunately not presently a MyGet feed for CI packages rigged).
- improvements / tweaks, _subject to filing a GitHub issue outlining the work first to see if it fits a general enough need to warrant adding code to the implementation and to make sure work is not wasted or duplicated_:
- [support for new stores](https://github.com/jet/equinox/issues/62) that can fulfill the normal test cases.
- tests, examples and scenarios are always welcome; Equinox is intended to address a very broad base of usage patterns. Please note that the emphasis will always be (in order)
  1. providing advice on how to achieve your aims without changing Equinox
  2. how to open up an appropriate extension point in Equinox
  3. (when all else fails), add to the complexity of the system by adding API surface area or logic

## TEMPLATES

The best place to start, sample-wise is with the [QuickStart](quickstart), which walks you through sample code, tuned for approachibility, from `dotnet new` templates stored [in a dedicated repo](https://github.com/jet/dotnet-templates).

## SAMPLES

The `samples/` folder contains various further examples (some of the templates are derived from these), with the complementary goals of:

- being a starting point to see how one might consume the libraries.
- acting as [Consumer Driven Contracts](https://martinfowler.com/articles/consumerDrivenContracts.html) to validate new and pin existing API designs.
- providing outline (not official and complete) guidance as to things that are valid to do in an application consuming Equinox components.
- to validate that each specific Storage implementation can fulfill the needs of each of the example Services/Aggregates/Applications. (_unfortunately this concern makes a lot of the DI wiring more complex than a real application should be; it's definitely a non-goal for every Equinox app to be able to switch between backends, even though that's very much possible to achieve._)
- provide sample scripts referenced in the Tutorial

<a name="TodoBackend"></a>
### [TODOBACKEND, see samples/TodoBackend](/samples/TodoBackend)

The repo contains a vanilla ASP.NET Core 2.1 implemention of [the well-known TodoBackend Spec](https://www.todobackend.com). **NB the implementation is largely dictated by spec; no architectural guidance expressed or implied ;)**. It can be run via:

    & dotnet run -f netcoreapp2.1 -p samples/Web -S es # run against eventstore, omit `es` to use in-memory store, or see PROVISIONING EVENTSTORE
    start https://www.todobackend.com/specs/index.html?https://localhost:5001/todos # for low-level debugging / validation of hosting arrangements
    start https://www.todobackend.com/client/index.html?https://localhost:5001/todos # standard JavaScript UI
    start http://localhost:5341/#/events # see logs triggered by `-S` above in https://getseq.net        

### [STORE, see /samples/Store](/samples/Store)

The core sample in this repo is the `Store` sample, which contains code and tests extracted from real implementations (with minor simplifications in some cases).

These facts mean that:

- some of the code may be less than approachable for a beginner (e.g. some of the code is in its present form for reasons of efficiency)
- some of the code may not represent official best practice guidance that the authors would necessarily stand over (e.g., the CQRS pattern is not strictly adhered to in all circumstances; some command designs are not completely correct from an idempotency perspective)

While these things can of course be perfected through PRs, this is definitely not top of the TODO list for the purposes of this repo. (We'd be delighted to place links to other samples, including cleanups / rewrites of these samples written with different testing platforms, web platforms, or DDD/CQRS/ES design flavors right here).

### [m-r](https://github.com/gregoryyoung/m-r/tree/master/SimpleCQRS) port, [see samples/Store/Domain/InventoryItem.fs](samples/Store/Domain/InventoryItem.fs)

For fun, there's a direct translation of the `InventoryItem` Aggregate and Command Handler from Greg Young's [`m-r`](https://github.com/gregoryyoung/m-r/tree/master/SimpleCQRS) demo project [as one could write it in F# using Equinox](https://github.com/jet/equinox/blob/master/samples/Store/Domain/InventoryItem.fs). NB any typical presentation of this example includes copious provisos and caveats about it being a toy example written almost a decade ago.

### [`samples/Tutorial` (in this repo)](/samples/Tutorial): Annotated `.fsx` files with sample aggregate implementations

### [@ameier38](https://github.com/ameier38)'s Tutorial

[Andrew Meier](https://andrewcmeier.com) has written a very complete tutorial modeling a business domain using Equinox and Event Store; includes Dockerized Suave API, test suite using Expecto, build automation using FAKE, and CI using Codefresh; see [the repo](https://github.com/ameier38/equinox-tutorial) and its [overview blog post](https://andrewcmeier.com/bi-temporal-event-sourcing).

## BENCHMARKS

A key facility of this repo is being able to run load tests, either in process against a nominated store, or via HTTP to a nominated instance of `samples/Web` ASP.NET Core host app. The following test suites are implemented at present:

- `Favorite` - Simulate a very enthusiastic user that favorites something once per second
  - the test generates an ever-growing state that can only be managed efficiently if you apply either caching, snapshotting or both
  - NB due to being unbounded, even Rolling Snapshots or Unfolds will eventually hit the Store's limits (4MB/event for EventStore, 3MB/document for CosmosDb)
- `SaveForLater` - Simulate a happy shopper that saves 3 items per second, and empties the Save For Later list whenever it is full (when it hits 50 items)
  - Snapshotting helps a lot
  - Caching is not as essential as it is for the `Favorite` test (as long as oyu have either aching or snapshotting, that is)
- `Todo` - Keeps a) getting the list b) adding an item c) clearing the list when it hits 1000 items.
  - the `Cleared` event acts as a natural event to use in the `isOrigin` check. This makes snapshotting less crucial than it is, for example, in the case of the `Favorite` test
  - the `-s` parameter can be used to adjust the maximum item text length from the default (`100`, implying average length of 50)

## BUILDING

Please note the [QuickStart](quickstart) is probably the best way to gain an overview - these instructions are intended to illustrated various facilities of the build script for people making changes. 

### build and run

Run, including running the tests that assume you've got a local EventStore and pointers to a CosmosDb database and collection prepared (see [PROVISIONING](provisioning)):

    ./build.ps1

### build, skipping tests that require a Store instance

    ./build -s

### build, skipping all tests

    dotnet pack build.proj

### build, skip EventStore tests

    ./build -se

### build, skip EventStore tests, skip auto-provisioning / de-provisioning CosmosDb

    ./build -se -scp

### Run EventStore benchmark on Full Framework (when [provisioned](provisioning))

Continually reads and writes very small events across multiple streams on .NET Full Framework

    dotnet pack -c Release ./build.proj
    & ./tools/Equinox.Tool/bin/Release/net461/eqx.exe run -f 2500 -C -U es

### Run EventStore benchmark on .NET Core (when provisioned)

At present, .NET Core seems to show comparable perf under normal load, but becomes very unpredictable under load. The following benchmark should produce pretty consistent levels of reads and writes, and can be used as a baseline for investigation:

    & dotnet run -c Release -f netcoreapp2.1 -p tools/Equinox.Tool -- run -t saveforlater -f 1000 -d 5 -C -U es

### run Web benchmark

The CLI can drive the Store and TodoBackend samples in the `samples/Web` ASP.NET Core app. Doing so requires starting a web process with an appropriate store (EventStore in this example, but can be `memory` / omitted etc. as in the other examples)

#### in Window 1

    & dotnet run -c Release -f netcoreapp2.1 -p samples/Web -- -C -U es

#### in Window 2

    dotnet tool install -g Equinox.Tool # only once
    eqx run -t saveforlater -f 200 web

### run CosmosDb benchmark (when provisioned)

    $env:EQUINOX_COSMOS_CONNECTION="AccountEndpoint=https://....;AccountKey=....=;"
    $env:EQUINOX_COSMOS_DATABASE="equinox-test"
    $env:EQUINOX_COSMOS_COLLECTION="equinox-test"

    tools/Equinox.Tool/bin/Release/net461/eqx run `
      cosmos -s $env:EQUINOX_COSMOS_CONNECTION -d $env:EQUINOX_COSMOS_DATABASE -c $env:EQUINOX_COSMOS_COLLECTION
    dotnet run -f netcoreapp2.1 -p tools/Equinox.Tool -- run `
      cosmos -s $env:EQUINOX_COSMOS_CONNECTION -d $env:EQUINOX_COSMOS_DATABASE -c $env:EQUINOX_COSMOS_COLLECTION

## PROVISIONING

### Provisioning EventStore (when not using -s or -se)

For EventStore, the tests assume a running local instance configured as follows to replicate as much as possible the external appearance of a Production EventStore Commercial cluster :-

    # requires admin privilege
    cinst eventstore-oss -y # where cinst is an invocation of the Chocolatey Package Installer on Windows
    # run as a single-node cluster to allow connection logic to use cluster mode as for a commercial cluster
    & $env:ProgramData\chocolatey\bin\EventStore.ClusterNode.exe --gossip-on-single-node --discover-via-dns 0 --ext-http-port=30778

### Provisioning CosmosDb (when not using -sc)

    dotnet run -f netcoreapp2.1 -p tools/Equinox.Tool -- init -ru 1000 `
        cosmos -s $env:EQUINOX_COSMOS_CONNECTION -d $env:EQUINOX_COSMOS_DATABASE -c $env:EQUINOX_COSMOS_COLLECTION

## DEPROVISIONING

### Deprovisioning (aka nuking) EventStore data resulting from tests to reset baseline

While EventStore rarely shows any negative effects from repeated load test runs, it can be useful for various reasons to drop all the data generated by the load tests by casting it to the winds:-

    # requires admin privilege
    rm $env:ProgramData\chocolatey\lib\eventstore-oss\tools\data

### Deprovisioning CosmosDb

The [provisioning](provisioning) step spins up RUs in DocDB for the Collection, which will keep draining your account until you reach a spending limit (if you're lucky!). *When finished running any test, it's critical to drop the RU allocations back down again via some mechanism (either delete the collection or reset teh RU provision dpwn to the lowest possible value)*.

- Kill the collection and/or database
- Use the portal to change the allocation

## FAQ

### What _is_ Equinox?

OK, I've read the README and the tagline. I still don't know what it does! Really, what's the TL;DR ?

- supports storing events in [EventStore](https://eventstore.org), including working with existing data you may have (that's where it got its start)
- includes a proprietary optimized Store implementation that only needs an empty Azure CosmosDb account to get going
- provides all the necessary infrastructure to build idempotent synchronous command processing against all of the stores; your Domain code intentionally doesn't need to reference *any* Equinox modules whatsoever (although for smaller systems, you'll often group `Events`+`Fold`+`Commands`+`Service` in a single `module`, which implies a reference to [the core `Equinox` package](src/Equinox)).
- following on from the previous point: you just write the unit tests without any Equinox-specific hoops to jump through; this really works very well indeed, assuming you're writing the domain code and the tests in F#. If you're working in a more verbose language, you may end up building some test helpers. We don't envisage Equinox mandating a specific pattern on the unit testing side (consistent naming such as `Events.Event`+`evolve`+`fold`+`Command`+`interpret` can help though).
- it helps with integration testing decision processes by
  - staying out of your way as much as possible
  - providing an in-memory store that implements the same interface as the EventStore and CosmosDb stores do
- There is a projection story, but it's not the last word - any 3 proper architects can come up with at least 3 wrong and 3 right ways of running those perfectly
  - For EventStore, you use it's projections; they're great
  - for CosmosDb, you use the `Equinox.Projection.*` and `Equinox.Cosmos.Projection` libraries to work off the CosmosDb ChangeFeed using the Microsoft ChangeFeedProcessor library (and, optionally, project to/consume from Kafka) using the sample app templates

### Should I use Equinox to learn event sourcing ?

You _could_. However the Equinox codebase here is not designed to be a tutorial; it's also extracted from systems with no pedagogical mission whatsoever. [FsUno.Prod](https://github.com/thinkbeforecoding/FsUno.Prod) on the other hand has this specific intention, walking though it is highly recommended. Also [EventStore](https://eventstore.org/), being a widely implemented and well-respected open source system has some excellent learning materials and documentation with a wide usage community (search for `DDD-CQRS-ES` mailing list and slack).

Having said that, we'd love to see a set of tutorials written by people looking from different angles, and over time will likely do one too ... there's no reason why the answer to this question can't become "**of course!**"

### Can I use it for really big projects?

You can. Folks in Jet do; we also have systems where we have no plans to use it, or anything like it. That's OK; there are systems where having precise control over one's data access is critical. And (shush, don't tell anyone!) some find writing this sort of infrastructure to be a very fun design challenge that beats doing domain modelling any day ...

### Can I use it for really small projects and tiny microservices?

You can. Folks in Jet do; but we also have systems where we have no plans to use it, or anything like it as it would be overkill even for people familiar with Equinox.

### OK, but _should_ I use Equinox for a small project ?

You'll learn a lot from building your own equivalent wrapping layer. Given the array of concerns Equinox is trying to address, there's no doubt that a simpler solution is always possible if you constrain the requirements to specifics of your context with regard to a) scale b) complexity of domain c) degree to which you use or are likely to use >1 data store. You can and should feel free to grab slabs of Equinox's implementation and whack it into an `Infrastructure.fs` in your project too (note you should adhere to the rules of the [Apache 2 license](LICENSE)). If you find there's a particular piece you'd really like isolated or callable as a component and it's causing you pain as [you're using it over and over in ~ >= 3 projects](https://en.wikipedia.org/wiki/Rule_of_three_(computer_programming)), please raise an Issue though ! 

Having said that, getting good logging, some integration tests and getting lots of off-by-one errors off your plate is nice; the point of [DDD-CQRS-ES](https://ddd-cqrs-es.slack.com/) is to get beyond toy examples to the good stuff - Domain Modelling on your actual domain.

### What will Equinox _never_ do?

Hard to say; try us, raise an Issue.

### What client languages are supported ?

The main language in mind for consumption is of course F# - many would say that F# and event sourcing are a dream pairing; little direct effort has been expended polishing it to be comfortable to consume from other .NET languages, the `dotnet new eqxwebcs` template represents the current state.

## You say I can use volatile memory for integration tests, could this also be used for learning how to get started building event sourcing programs with equinox? 

The `MemoryStore` backend is intended to implement the complete semantics of a durable store [aside from caching, which would be a pyrrhic victory if implemented like in the other Stores, though arguably it may make sense should the caching layer ever get pushed out of the Stores themselves]. The main benefit of using it is that any tests using it have zero environment dependencies. In some cases this can be very useful for demo apps or generators (rather than assuming a specific store at a specific endpoint and/or credentials, there is something to point at which does not require configuration or assumptions.). The problem of course is that it's all in-process; the minute you stop the host, your TODO list has been forgotten. In general, EventStore is a very attractive option for prototyping; the open source edition is trivial to install and has a nice UI that lets you navigate events being produced etc.

### OK, so it supports CosmosDb, EventStore and might even support more in the future. I really don't intend to shift datastores. Period. Why would I take on this complexity only to get the lowest common denominator ?

Yes, you have decisions to make; Equinox is not a panacea - there is no one size fits all. While the philosophy of Equinox is a) provide an opinionated store-neutral [Programming Model](DOCUMENTATION.md#Programming-Model) with a good pull toward a big [pit of success](https://blog.codinghorror.com/falling-into-the-pit-of-success/), while not closing the door to using store-specific features where relevant, having a dedicated interaction is always going to afford you more power and control.

### Why do I need two caches if I have two stores?

- in general, individual apps will not typically be mixing data stores in the first instance
- see [The Rule of Three](https://en.wikipedia.org/wiki/Rule_of_three_(computer_programming)); the commonality may reveal itself better at a later point, but the cut and paste (with a cut and paste of the the associated acceptance tests) actually keeps the cache integration clearer at the individual store level for now. No, it's not set in stone ;)

### Is there a guide to building the simplest possible hello world "counter" sample, that simply counts with an add and a subtract event? 

See the [API Guide in DOCUMENTATION.md](DOCUMENTATION.md#api). An alternate way is to look at the `Todo.fs` files emitted by [`dotnet new equinoxweb`](https://github.com/jet/dotnet-templates) in the [QuickStart](#quickstart).

### OK, but you didn't answer my question, you just talked about stuff you wanted to talk about!

😲Please raise a question-Issue, and we'll be delighted to either answer directly, or incorporate the question and answer here

# FURTHER READING

See [`DOCUMENTATION.md`](DOCUMENTATION.md)